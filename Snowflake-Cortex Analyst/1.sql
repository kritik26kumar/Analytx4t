-- Create database
CREATE DATABASE tenwavedb;

-- Use the database
USE tenwavedb;

-- Create the dim_branches table
CREATE TABLE dim_branches (
    hospital_id VARCHAR(255) PRIMARY KEY,
    hospital_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'Active'
);

-- Verify the table creation
DESCRIBE dim_branches;

-- Create the dim_departments table
CREATE TABLE dim_departments (
    department_id VARCHAR(255) PRIMARY KEY,
    department_name VARCHAR(255) NOT NULL,
    admission_allowed VARCHAR(50),
    opd_allowed VARCHAR(50),
    emergency_allowed VARCHAR(50),
    status VARCHAR(50)
);

-- Verify the table creation
DESCRIBE dim_departments;

-- Create the dim_insproviders table
CREATE TABLE dim_insproviders (
    insprovider_id VARCHAR(255) PRIMARY KEY,
    sponsor_name VARCHAR(255) NOT NULL
);

-- Verify the table creation
DESCRIBE dim_insproviders;

-- Create the dim_users table
CREATE TABLE dim_users (
    doctor_id VARCHAR(255) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    department_id VARCHAR(255),
    status VARCHAR(50),
    qualification VARCHAR(255),
    specialization VARCHAR(255),
    FOREIGN KEY (department_id) REFERENCES dim_departments(department_id)
);

-- Verify the table creation
DESCRIBE dim_users;

-- Create the dim_patients table
CREATE TABLE dim_patients (
    patient_id VARCHAR(255) PRIMARY KEY,
    registration_date DATE,
    birthdate DATE,
    sex VARCHAR(100),
    age INT,
    name VARCHAR(255),
    phone_number VARCHAR(20)
);

-- Verify the table creation
DESCRIBE dim_patients;

-- Create the fact_billing table
CREATE TABLE fact_billing (
    billing_id  VARCHAR(255) PRIMARY KEY,
    patient_id VARCHAR(255),
    hospital_id VARCHAR(255),
    bill_status VARCHAR(50) ,
    bill_date DATETIME,
    department_id VARCHAR(255),
    insprovider_id VARCHAR(255),
    parent_type VARCHAR(50),
    doctor_id VARCHAR(255),
    total_amount DECIMAL(20,2) NOT NULL,
    total_discount DECIMAL(10,2) DEFAULT 0.00,
    ist_bill_date DATETIME,
    FOREIGN KEY (patient_id) REFERENCES dim_patients(patient_id),
    FOREIGN KEY (hospital_id) REFERENCES dim_branches(hospital_id),
    FOREIGN KEY (department_id) REFERENCES dim_departments(department_id),
    FOREIGN KEY (insprovider_id) REFERENCES dim_insproviders(insprovider_id),
    FOREIGN KEY (doctor_id) REFERENCES dim_users(doctor_id)
);

-- Verify the table creation
DESCRIBE fact_billing;

-- Create the bridge_dim_patientinsurers table
CREATE TABLE bridge_dim_patientinsurers (
    patient_id VARCHAR(255),
    insprovider_id VARCHAR(255),
    PRIMARY KEY (patient_id, insprovider_id),
    FOREIGN KEY (patient_id) REFERENCES dim_patients(patient_id),
    FOREIGN KEY (insprovider_id) REFERENCES dim_imsproviders(insprovider_id)
);

-- Verify the table creation
DESCRIBE dim_branches;
DESCRIBE dim_departments;
DESCRIBE dim_imsproviders;
DESCRIBE dim_users;
DESCRIBE dim_patients;
DESCRIBE fact_billing;
DESCRIBE bridge_dim_patientinsurers;


-- Load Data Into dim_branches Table
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dim_branches.csv"
INTO TABLE dim_branches
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  -- Skip header row
SELECT * FROM dim_branches;

-- Load Data Into dim_departments Table
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dim_departments.csv"
INTO TABLE dim_departments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  -- Skip header row
SELECT * FROM dim_departments;

-- Load Data Into dim_insproviders Table
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dim_insproviders.csv"
INTO TABLE dim_insproviders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  -- Skip header row
SELECT * FROM dim_insproviders;

-- Load Data Into dim_users Table
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\dim_users.csv"
INTO TABLE dim_users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  -- Skip header row
SELECT * FROM dim_users;

-- Load Data Into dim_patients Table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dim_patients.csv'
INTO TABLE dim_patients
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@patient_id, @registration_date, @birthdate, @sex, @age, @name, @phone_number)
SET
  patient_id = @patient_id,
  registration_date = IF(@registration_date = '', NULL, STR_TO_DATE(@registration_date, '%d-%m-%Y')),
  birthdate = IF(@birthdate = '', NULL, STR_TO_DATE(@birthdate, '%d-%m-%Y')),
  sex = @sex,
  age = IF(@age = '', NULL, @age), 
  name = @name,
  phone_number = @phone_number;
SELECT * FROM dim_patients;

-- Load Data Into bridge_dim_patientinsurers Table
SET FOREIGN_KEY_CHECKS = 0;
LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\bridge_dim_patientinsurers.csv"
INTO TABLE bridge_dim_patientinsurers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;  -- Skip header row
SELECT * FROM bridge_dim_patientinsurers;
SET FOREIGN_KEY_CHECKS = 1;

-- Load Data Into fact_biling Table
SET FOREIGN_KEY_CHECKS = 0;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fact_billing.csv'
INTO TABLE fact_billing
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@billing_id, @patient_id, @hospital_id, @bill_status, @bill_date,
 @department_id, @insprovider_id, @parent_type, @doctor_id,
 @total_amount, @total_discount, @ist_bill_date)
SET
  billing_id = @billing_id,
  patient_id = @patient_id,
  hospital_id = @hospital_id,
  bill_status = @bill_status,
  bill_date = IF(@bill_date = '', NULL, STR_TO_DATE(@bill_date, '%d-%m-%Y %H:%i')),
  department_id = @department_id,
  insprovider_id = @insprovider_id,
  parent_type = @parent_type,
  doctor_id = @doctor_id,
  total_amount = IF(@total_amount = '', NULL, @total_amount),
  total_discount = IF(@total_discount = '', NULL, @total_discount),
  ist_bill_date = IF(@ist_bill_date = '', NULL, STR_TO_DATE(@ist_bill_date, '%d-%m-%Y %H:%i'));
SET FOREIGN_KEY_CHECKS = 1;
SELECT * FROM fact_billing;



-- Check data in all tables;
SELECT * FROM dim_branches;
SELECT * FROM dim_insproviders;
SELECT * FROM dim_patients;
SELECT * FROM dim_users;
SELECT * FROM dim_departments;
SELECT * FROM bridge_dim_patientinsurers;
SELECT * FROM fact_billing;