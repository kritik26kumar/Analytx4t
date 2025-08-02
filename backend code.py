-- Account Previleges
use role accountadmin;

-- Account Previleges of Snowflake Cortex AI
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'any_region';

-- Create database
CREATE OR REPLACE DATABASE kritik_db;
USE DATABASE kritik_db;

-- Create schema
CREATE OR REPLACE SCHEMA data;


-- Create Warehouse
CREATE WAREHOUSE IF NOT EXISTS kritik_warehouse
WITH 
    WAREHOUSE_SIZE = 'SMALL'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 5 
    AUTO_RESUME = TRUE 
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'ECONOMY';

-- 
USE DATABASE kritik_db;
USE WAREHOUSE kritik_warehouse;
USE SCHEMA data;


--ARN as AMAZON RESOURCE SERVICE
CREATE OR REPLACE STORAGE INTEGRATION dr_aws_int
  TYPE = EXTERNAL_STAGE
  ENABLED = TRUE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::850470019185:role/mykritikrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mykritikbucket/patient_reports/')
  COMMENT = 'INTEGRATION WITH AWS S3 BUCKETS';

DESC INTEGRATION dr_aws_int;

--CREATE EXTERNAL STAGE
CREATE OR REPLACE STAGE STAGE_AWS
  URL = 's3://mykritikbucket/patient_reports/'
  STORAGE_INTEGRATION = dr_aws_int
  ENCRYPTION=( TYPE = 'AWS_SSE_S3' )
  DIRECTORY = (ENABLE = TRUE);

ls @STAGE_AWS;


--Step 1: PARSE DOCUMENT
CREATE OR REPLACE TEMPORARY TABLE RAW_TEXT AS
SELECT 
    RELATIVE_PATH,
    SIZE,
    FILE_URL,
    BUILD_SCOPED_FILE_URL(@STAGE_AWS, RELATIVE_PATH) AS SCOPED_FILE_URL,
    (SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
        '@STAGE_AWS',
        RELATIVE_PATH,
        OBJECT_CONSTRUCT('mode', 'LAYOUT')
    ):content)::VARCHAR AS EXTRACTED_LAYOUT
FROM 
    DIRECTORY(@STAGE_AWS);
SELECT * FROM RAW_TEXT;


-- Step 2: Create table to store document chunks
CREATE OR REPLACE TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped URL for access
    CHUNK VARCHAR(16777216), -- Piece of text
    CHUNK_INDEX INTEGER, -- Index for the text
    CATEGORY VARCHAR(16777216) -- Document category for filtering
);
select * from DOCS_CHUNKS_TABLE;

-- Step 3: Split text into chunks using SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER
INSERT INTO DOCS_CHUNKS_TABLE(RELATIVE_PATH, SIZE, FILE_URL, SCOPED_FILE_URL, CHUNK, CHUNK_INDEX)
    SELECT 
        RELATIVE_PATH, 
        SIZE,
        FILE_URL, 
        SCOPED_FILE_URL,
        c.value::TEXT AS CHUNK,
        c.INDEX::INTEGER AS CHUNK_INDEX
    FROM 
        RAW_TEXT,
        LATERAL FLATTEN(INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            EXTRACTED_LAYOUT,
            'markdown',
            1512,
            256,
            ['\n\n', '\n', ' ', '']
        )) c;

-- Create CORTEX SEARCH SERVICE
create or replace CORTEX SEARCH SERVICE KK_SEARCH_SERVICE_CS
ON chunk
ATTRIBUTES relative_path
warehouse = kritik_warehouse
TARGET_LAG = '5 minute'
as (
    select chunk,
        relative_path,
        file_url,
        category
    from docs_chunks_table
);

-- Create chat history table if it doesn't exist
CREATE TABLE IF NOT EXISTS KRITIK_DB.DATA.CHAT_HISTORY (
    id NUMBER AUTOINCREMENT(100,1),
    user_id VARCHAR,
    org_id VARCHAR,
    question TEXT,
    answer TEXT,
    model_name VARCHAR,
    category VARCHAR,
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    related_documents VARIANT,
    suggested_questions VARIANT
);
select * from chat_history;

-- AUTOMATIC PROCESSING OF NEW DOCUMENTS
-- Create streams for insert and delete operations on stage
CREATE OR REPLACE STREAM INSERT_DOCS_STREAM ON STAGE STAGE_AWS;
CREATE OR REPLACE STREAM DELETE_DOCS_STREAM ON STAGE STAGE_AWS;

-- Create stored procedure for insert and delete operations
CREATE OR REPLACE PROCEDURE INSERT_DELETE_DOCS_SP()
  RETURNS VARCHAR
  LANGUAGE SQL
AS
$$
BEGIN
  -- Step 1: Delete removed documents
  DELETE FROM DOCS_CHUNKS_TABLE
    USING DELETE_DOCS_STREAM
    WHERE DOCS_CHUNKS_TABLE.RELATIVE_PATH = DELETE_DOCS_STREAM.RELATIVE_PATH
      AND DELETE_DOCS_STREAM.METADATA$ACTION = 'DELETE';

  -- Step 2: Parse newly inserted documents
  CREATE OR REPLACE TEMPORARY TABLE RAW_TEXT AS
    SELECT 
        RELATIVE_PATH,
        SIZE,
        FILE_URL,
        BUILD_SCOPED_FILE_URL(@STAGE_AWS, RELATIVE_PATH) AS SCOPED_FILE_URL,
        TO_VARCHAR(
            SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                '@STAGE_AWS',
                RELATIVE_PATH,
                OBJECT_CONSTRUCT('mode', 'LAYOUT')
            ):content
        ) AS EXTRACTED_LAYOUT
    FROM 
        INSERT_DOCS_STREAM
    WHERE 
        METADATA$ACTION = 'INSERT';

    -- Step 3: Insert new document chunks
  INSERT INTO DOCS_CHUNKS_TABLE (
    RELATIVE_PATH, SIZE, FILE_URL, SCOPED_FILE_URL, CHUNK, CHUNK_INDEX
  )
  SELECT 
    RELATIVE_PATH, 
    SIZE,
    FILE_URL, 
    SCOPED_FILE_URL,
    c.VALUE::TEXT AS CHUNK,
    c.INDEX::INTEGER AS CHUNK_INDEX
  FROM 
    RAW_TEXT,
    LATERAL FLATTEN(INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        EXTRACTED_LAYOUT,
        'markdown',
        1512,
        256,
        ['\n\n', '\n', ' ', '']
    )) c;

  RETURN 'Insert/Delete Document Sync Complete';
END;
$$;





-- Create task for automatic processing
CREATE OR REPLACE TASK INSERT_DELETE_DOCS_TASK
  WAREHOUSE = kritik_warehouse
  SCHEDULE = '2 minute'
  WHEN SYSTEM$STREAM_HAS_DATA('DELETE_DOCS_STREAM') OR SYSTEM$STREAM_HAS_DATA('INSERT_DOCS_STREAM')
  AS
    CALL INSERT_DELETE_DOCS_SP();
-- Resume task
ALTER TASK INSERT_DELETE_DOCS_TASK RESUME;



--TO REFRESH THE STAGE
CREATE OR REPLACE TASK REFRESH_STAGE_TASK
  WAREHOUSE = kritik_warehouse
  SCHEDULE = '1 MINUTE'
  AS
    ALTER STAGE STAGE_AWS REFRESH;
ALTER TASK REFRESH_STAGE_TASK RESUME;




-- Verify streams
SELECT * FROM DELETE_DOCS_STREAM;
SELECT * FROM INSERT_DOCS_STREAM;
select * from DOCS_CHUNKS_TABLE;




-- Force task execution for testing
EXECUTE TASK INSERT_DELETE_DOCS_TASK;

LIST @STAGE_AWS;
SELECT * FROM INSERT_DOCS_STREAM;
SHOW STREAMS;
SELECT CURRENT_ROLE();
SELECT *
FROM INFORMATION_SCHEMA.STREAMS
WHERE STREAM_NAME = 'INSERT_DOCS_STREAM';






































-- -- Create User
-- CREATE USER Amit_Kumar
--     PASSWORD = '';
--     DEFAULT_ROLE = 'ACCOUNTADMIN'
--     MUST_CHANGE_PASSWORD = FALSE;
-- GRANT ROLE ACCOUNTADMIN TO USER Amit_Kumar;

-- -- Create Role
-- CREATE ROLE cortex_user_role;
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;
-- GRANT ROLE cortex_user_role TO USER Tenwave_user;

-- Create Role
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;
GRANT ROLE ACCOUNTADMIN TO USER Analytx4tlab;

--CREATE OR REPLACE SCHEMA data2;
--CREATE OR REPLACE SCHEMA data3;



-- Create function text_chunker
create or replace function text_chunker(pdf_text string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'text_chunker'
packages = ('snowflake-snowpark-python', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def process(self, pdf_text: str):
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1512, #Adjust this as you see fit
            chunk_overlap  = 256, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
$$;


-- create internal stage
create or replace stage docs ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

-- show docs
ls @docs;

--

--

-- create TABLE DOCS_CHUNKS_TABLE
create or replace TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CATEGORY VARCHAR(16777216) -- Will hold the document category to enable filtering
);



-- insert into docs_chunks_table
--insert into docs_chunks_table (relative_path, size, file_url,
--                            scoped_file_url, chunk)
--
--    select relative_path, 
--            size,
--            file_url, 
--            build_scoped_file_url(@STAGE_AWS, relative_path) as scoped_file_url,
--            func.chunk as chunk
--    from 
--        directory(@STAGE_AWS),
--        TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@STAGE_AWS, 
--                              relative_path, {'mode': 'LAYOUT'})))) as func;

SELECT * FROM DOCS_CHUNKS_TABLE;
select * from docs_chunks_table limit 5;


-- -- create stream docs_stream on stage docs
-- create or replace stream docs_stream on stage docs;

-- create or replace task parse_and_insert_pdf_task 
--     warehouse = COMPUTE_WH
--     schedule = '1 minute'
--     when system$stream_has_data('docs_stream')
--     as
  
--     insert into docs_chunks_table (relative_path, size, file_url,
--                             scoped_file_url, chunk)
--     select relative_path, 
--             size,
--             file_url, 
--             build_scoped_file_url(@docs, relative_path) as scoped_file_url,
--             func.chunk as chunk
--     from 
--         docs_stream,
--         TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@docs, relative_path, {'mode': 'LAYOUT'})))) as func;

-- alter task parse_and_insert_pdf_task resume;

-- select * from docs_stream;

-- --
-- ALTER TASK parse_and_insert_pdf_task SUSPEND;

-- --
-- ALTER TASK parse_and_insert_pdf_task RESUME;








