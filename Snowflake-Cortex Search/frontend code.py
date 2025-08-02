import streamlit as st
import pandas as pd
import json
import re
from typing import List, Dict, Any, Tuple, Set, Optional
from functools import lru_cache
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete
from snowflake.core import Root

# Configure pandas display options
pd.set_option("max_colwidth", None)

# Application Constants
APP_CONFIG = {
    "NUM_CHUNKS": 10,
    "SLIDING_WINDOW": 7,
    "MODEL": "llama3.3-70b",
    "DEFAULT_CATEGORY": "ALL",
    "INITIAL_QUESTIONS": [
        "What are the patient's profile and admission details?",
        "What is the final diagnosis mentioned in the report?",
        "What treatment protocols and medications were administered during the hospital stay?",
        "What does the discharge summary indicate regarding the patient's progress and follow-up care?"
    ],
    "SNOWFLAKE": {
        "DATABASE": "kritik_db",
        "SCHEMA": "DATA",
        "SERVICE": "KK_SEARCH_SERVICE_CS",
        "TABLE": "docs_chunks_table"
    },
    "SEARCH_COLUMNS": ["chunk", "relative_path", "category"]
}


class SnowflakeService:
    """Handles all Snowflake-related operations with caching for performance"""
    
    def __init__(self):
        self.session = get_active_session()
        self.root = Root(self.session)
        self.search_service = self._initialize_search_service()
    
    def _initialize_search_service(self):
        """Initialize the Cortex search service"""
        return self.root.databases[APP_CONFIG["SNOWFLAKE"]["DATABASE"]].schemas[
            APP_CONFIG["SNOWFLAKE"]["SCHEMA"]].cortex_search_services[APP_CONFIG["SNOWFLAKE"]["SERVICE"]]
    
    @lru_cache(maxsize=32)
    def get_available_categories(self) -> List[str]:
        """Fetch all distinct categories from the documents table (cached)"""
        categories = self.session.table(APP_CONFIG["SNOWFLAKE"]["TABLE"]).select('category').distinct().collect()
        return ['ALL'] + [cat.CATEGORY for cat in categories]
    
    @lru_cache(maxsize=1)
    def get_available_documents(self) -> pd.DataFrame:
        """List all available documents in the storage location (cached)"""
        return self.session.sql("ls @docs").collect()
    
    def get_document_url(self, path: str) -> str:
        """Generate a presigned URL for a document path"""
        try:
            # Ensure correct database and schema context
            self.session.sql("USE DATABASE kritik_db").collect()
            self.session.sql("USE SCHEMA data").collect()
            cmd = f"SELECT GET_PRESIGNED_URL(@kritik_db.data.STAGE_AWS, '{path}', 360) AS URL_LINK FROM DIRECTORY(@kritik_db.data.STAGE_AWS)"
            df_url_link = self.session.sql(cmd).to_pandas()
            return df_url_link._get_value(0, 'URL_LINK')
        except Exception as e:
            st.error(f"Error generating URL for {path}: {str(e)}")
            return "#"
    
    def search_similar_chunks(self, query: str, category: str, num_chunks: int) -> Dict[str, Any]:
        """Search for semantically similar chunks in the document collection"""
        if category == "ALL":
            response = self.search_service.search(query, APP_CONFIG["SEARCH_COLUMNS"], limit=num_chunks)
        else:
            filter_obj = {"@eq": {"category": category}}
            response = self.search_service.search(
                query, 
                APP_CONFIG["SEARCH_COLUMNS"], 
                filter=filter_obj, 
                limit=num_chunks
            )
        
        return response.json()
    
    def complete_with_llm(self, model_name: str, prompt: str) -> str:
        """Generate a completion using the specified model"""
        return Complete(model_name, prompt)

class ResponseProcessor:
    """Processes and manipulates responses for improved display"""
    
    @staticmethod
    def clean_assistant_response(content: str) -> str:
        """Remove hidden sections from displayed response"""
        patterns_to_remove = [
            r'(?:\n|\r\n)?Related Questions:.*?(?=\n\n|\Z)',
            r'(?:\n|\r\n)?You might also want to know:.*?(?=\n\n|\Z)',
            r'(?:\n|\r\n)?Suggested Questions:.*?(?=\n\n|\Z)',
            r'(?:\n|\r\n)?Common Questions:.*?(?=\n\n|\Z)',
            r'(?:\n|\r\n)?Follow-up Questions:.*?(?=\n\n|\Z)'
        ]
        
        cleaned_content = content
        for pattern in patterns_to_remove:
            cleaned_content = re.sub(pattern, '', cleaned_content, flags=re.DOTALL)
        
        # Clean up any extra newlines that might be left
        cleaned_content = re.sub(r'\n{3,}', '\n\n', cleaned_content)
        
        return cleaned_content.strip()
    
    @staticmethod
    def extract_related_questions(response: str, context_data: dict) -> List[str]:
        """Extract questions from the response to make them clickable"""
        questions = []
        
        # Extract sections containing questions
        section_patterns = [
            r'Related Questions:(.*?)(?:\n\n|\Z)',
            r'Common Issues:(.*?)(?:\n\n|\Z)',
            r'Common Issues and Solutions:(.*?)(?:\n\n|\Z)', 
            r'Scenario-Based Questions:(.*?)(?:\n\n|\Z)',
            r'Follow-up Questions:(.*?)(?:\n\n|\Z)',
            r'You might also want to know:(.*?)(?:\n\n|\Z)'
        ]
        
        # First try to get questions from explicit sections
        found_questions = False
        for pattern in section_patterns:
            section_match = re.search(pattern, response, re.DOTALL)
            if section_match:
                found_questions = True
                section_text = section_match.group(1)
                
                # Extract questions from the section
                bullet_questions = re.findall(r'[-•*]\s*(.*?\?)', section_text)
                numbered_questions = re.findall(r'\d+\.\s*(.*?\?)', section_text)
                
                for question in bullet_questions + numbered_questions:
                    clean_question = question.strip()
                    if (clean_question and clean_question not in questions and 
                            "video" not in clean_question.lower() and len(clean_question) < 100):
                        questions.append(clean_question)
        
        # If not enough questions found, extract from context
        if not found_questions or len(questions) < 3:
            ResponseProcessor._extract_questions_from_context(questions, context_data)
        
        # If still not enough questions, look for general questions in the response
        if len(questions) < 3:
            ResponseProcessor._extract_general_questions(questions, response)
        
        # Limit to 4 questions max
        return questions[:4]
    
    @staticmethod
    def _extract_questions_from_context(questions: List[str], context_data: dict):
        """Extract questions from the context data"""
        if isinstance(context_data, dict) and 'results' in context_data:
            chunks = [item.get('chunk', '') for item in context_data.get('results', [])]
            combined_chunks = ' '.join(chunks)
            
            # Look for sections containing questions
            context_patterns = [
                r'Common Issues?:(.*?)(?=\n\n|\Z)',
                r'Scenarios?:(.*?)(?=\n\n|\Z)',
                r'Use Cases?:(.*?)(?=\n\n|\Z)',
                r'Common Queries:(.*?)(?=\n\n|\Z)'
            ]
            
            for pattern in context_patterns:
                section_match = re.search(pattern, combined_chunks, re.DOTALL)
                if section_match:
                    section_text = section_match.group(1)
                    
                    # Extract questions from the section
                    context_questions = re.findall(r'[-•*]\s*(.*?\?)', section_text)
                    if not context_questions:
                        # Try to find sentences ending with question marks
                        context_questions = re.findall(r'([A-Z][^.!?]*\?)', section_text)
                    
                    for question in context_questions:
                        clean_question = question.strip()
                        if (clean_question and clean_question not in questions and 
                                "video" not in clean_question.lower() and len(clean_question) < 100):
                            questions.append(clean_question)
    
    @staticmethod
    def _extract_general_questions(questions: List[str], response: str):
        """Extract general questions from the response"""
        general_questions = re.findall(r'([A-Z][^.!?]{10,}?\?)', response)
        for question in general_questions:
            clean_question = question.strip()
            if (clean_question and clean_question not in questions and 
                    "video" not in clean_question.lower() and len(clean_question) < 100):
                questions.append(clean_question)
    
    @staticmethod
    def process_video_links(response: str) -> str:
        """Process video links to make them work with YouTube"""
        video_pattern = r'Video Guide:\s*\[(.*?)\]\((.*?)\)'
        
        def replace_video_link(match):
            video_title = match.group(1)
            video_url = match.group(2)
            
            # Replace internal path with YouTube link format
            if "youtube.com" in video_url or "youtu.be" in video_url:
                return f"Video Guide: [📹 {video_title}]({video_url})"
            elif "internal_video_path" in video_url:
                search_query = video_title.replace(" ", "+")
                youtube_search_url = f"https://www.youtube.com/results?search_query={search_query}+hospital+information+system"
                return f"Video Guide: [📹 {video_title}]({youtube_search_url})"
            else:
                return f"Video Guide: [📹 {video_title}]({video_url})"
        
        # Replace video links with proper YouTube search links
        return re.sub(video_pattern, replace_video_link, response)


class PromptBuilder:
    """Builds prompts for LLM interaction"""
    
    @staticmethod
    def create_chat_summary_prompt(chat_history: List[Dict[str, str]], question: str) -> str:
        """Create a prompt to summarize the conversation context"""
        return f"""
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natual language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>"""
    
    def create_main_prompt(question: str, chat_history: List, prompt_context: Dict) -> str:
        """Create a streamlined main prompt for the LLM with retrieved context for medical assistance."""
        return f"""
        You are a specialized medical assistant designed to assist healthcare providers in extracting and analyzing patient information from medical records, discharge summaries, clinical notes, and external lab reports. Your goal is to provide accurate, concise medical information based solely on the data within the <context> and </context> tags, while considering prior interactions in the <chat_history> and </chat_history> tags.
        
        ### Guidelines for Answering
        - Respond directly to the query using precise medical terminology where appropriate.
        - Extract and organize patient information from the provided context efficiently.
        - For queries with multiple clinical questions, address each part separately in clear, labeled sections.
        - If the query asks for a complete summary (e.g., "everything in that report"), provide all available details from the context, organized by categories such as patient demographics, medical history, diagnoses, treatments, procedures, lab results, and clinical notes.
        - Use bullet points, short paragraphs, or tables for readability, emphasizing key findings, diagnoses, treatments, and recommendations.
        - If information is missing, state: "The patient record does not contain information about that specific question."
        - Avoid speculation, external knowledge, or excessive elaboration unless a full summary is requested.
        
        ### Patient Context Awareness
        - Identify the patient in the query and confirm if it matches the context or differs from chat history.
        - If the query references a new patient (by name, ID, or case), use only the current context for that patient.
        - If no matching information is found for a queried patient, respond: "I don’t have information about that patient in the current records. The information I have is for [current patient name/ID]."
        - If patient identity is unclear, note: "Patient identity is unclear in the context; using available data for [current patient name/ID if determinable]."
        - Never mix data from different patients.
        
        ### External Lab Reports Integration
        - Match lab reports to the patient using:
          * Unique Health ID (UHID) as the primary identifier
          * Patient name (allow for minor spelling variations)
          * Age, gender, referring doctor, and test date (if aligned with treatment timeline)
        - Correlate lab findings with clinical history and current presentation, noting the source (e.g., "Per external lab report").
        - If internal and external lab results conflict, present both clearly, attributing each source.
        - If lab data is incomplete or unmatchable, state: "External lab report data is incomplete or does not match the patient reliably."
        
        ### Response Structure
        - Confirm the patient identity (name/ID) if available in the context.
        - Break down complex queries into components and address each in a logical, clinical order.
        - For comprehensive summary requests (e.g., "everything in that report"), structure the response with clear sections:
          * Patient Demographics: Name, ID, age, gender, etc.
          * Medical History: Past conditions, surgeries, allergies, etc.
          * Diagnoses: Current and past diagnoses from the context.
          * Treatments: Medications, therapies, interventions, etc.
          * Procedures: Surgeries, interventions, or other procedures.
          * Lab Results: Internal and external lab findings, with sources.
          * Clinical Notes: Key observations, progress notes, etc.
        - Prioritize recent data from the context over older chat history if discrepancies arise.
        
        <chat_history>
        {chat_history}
        </chat_history>
        
        <context>
        {prompt_context}
        </context>
        
        <question>
        {question}
        </question>
        """
        
    



class ChatState:
    """Manages the application state"""
    
    @staticmethod
    def initialize():
        """Initialize session state variables with defaults"""
        if "messages" not in st.session_state:
            st.session_state.messages = []
            
        # Initialize configurable parameters
        defaults = {
            "model_name": APP_CONFIG["MODEL"],
            "category_value": APP_CONFIG["DEFAULT_CATEGORY"],
            "use_chat_history": True,
            "debug": False,
            "num_chunks": APP_CONFIG["NUM_CHUNKS"],
            "sliding_window": APP_CONFIG["SLIDING_WINDOW"],
            "suggested_questions": APP_CONFIG["INITIAL_QUESTIONS"],
            "question_asked": False,
            "current_question": "",
            "related_documents": []
        }
        
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    @staticmethod
    def clear_conversation():
        """Reset the conversation history"""
        st.session_state.messages = []
        st.session_state.suggested_questions = APP_CONFIG["INITIAL_QUESTIONS"]
        st.session_state.question_asked = False
        st.session_state.related_documents = []


class ChatApp:
    """Main chat application class"""
    
    def __init__(self):
        self.snowflake_service = SnowflakeService()
        self.response_processor = ResponseProcessor()
    
    def run(self):
        """Run the chat application"""
        # Set page config
        st.set_page_config(
            page_title="Doctor Summary",
            page_icon="❄️",
            layout="wide",
            initial_sidebar_state="collapsed"  # Changed from "expanded" to "collapsed"
        )
        
        # Apply custom CSS
        self._apply_custom_css()
        
        # Initialize state
        ChatState.initialize()
        
        # Render UI
        self._render_main_interface()
        
        # Handle user input
        self._handle_user_input()
    
    def _apply_custom_css(self):
        """Apply custom CSS for improved UI"""
        st.markdown("""
        <style>
        .stButton > button {
            background-color: #f0f2f6;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 10px;
            font-size: 14px;
            transition: all 0.3s;
            text-align: left;
        }
        .stButton > button:hover {
            background-color: #e6f0ff;
            border-color: #4da6ff;
        }
        .chat-message {
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 10px;
        }
        /* Add custom header styling */
        .app-header {
            text-align: center;
            margin-bottom: 20px;
        }
        .app-header h1 {
            color: #1E88E5;
        }
        /* Improved question button styling */
        .question-button {
            background-color: #f8f9fa;
            border-radius: 12px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            transition: all 0.2s ease;
        }
        .question-button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }
        /* Style for document links */
        .document-link {
            padding: 8px;
            border-radius: 8px;
            background-color: #f8f9fa;
            margin-bottom: 5px;
            display: block;
        }
        /* Containers for layout */
        .chat-container {
            padding: 10px;
        }
        .image-container {
            padding: 10px;
            text-align: center;
        }
        /* Clear button styling */
        .clear-button {
            margin-top: 10px;
            width: 100%;
        }
        </style>
        """, unsafe_allow_html=True)
    
    def _render_main_interface(self):
        """Render the main interface with a two-column layout"""
        st.markdown("<div class='app-header'>", unsafe_allow_html=True)
        st.title("Doctor Summary")
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Create a two-column layout
        col1, col2 = st.columns([7, 3])
        
        with col1:
            # Chat interface in left column
            self._render_chat_interface()
        
        with col2:
            # Image and related documents in right column
            st.markdown("<div class='image-container'>", unsafe_allow_html=True)
            st.image(
                'https://i.ibb.co/TBZ6Z9Ds/unnamed.png',
                caption='Hospital Information System',
                width=250
            )
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Add clear button below the logo
            if st.button("Clear Conversation", key="clear_button", use_container_width=True):
                ChatState.clear_conversation()
                st.experimental_rerun()
            
    
    def _render_chat_interface(self):
        """Render the chat messages and input area"""
        st.markdown("<div class='chat-container'>", unsafe_allow_html=True)
        
        # Display chat messages
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                if message["role"] == "assistant":
                    cleaned_content = ResponseProcessor.clean_assistant_response(message["content"])
                    st.markdown(cleaned_content)
                else:
                    st.markdown(message["content"])
        
        # Display suggested questions if no conversation has started
        if not st.session_state.messages:
            st.write("👋 Welcome! You can ask me questions about the Patient Information or choose from these common questions:")
            self._display_suggested_questions()
            
        st.markdown("</div>", unsafe_allow_html=True)
    
    def _display_suggested_questions(self):
        """Display clickable suggested questions buttons"""
        cols = st.columns(2)
        for i, question in enumerate(st.session_state.suggested_questions):
            col_idx = i % 2
            with cols[col_idx]:
                if st.button(f"🔍 {question}", key=f"suggested_q_{i}", use_container_width=True):
                    st.session_state.current_question = question
                    st.session_state.question_asked = True
    
    def _handle_user_input(self):
        """Process user input from chat interface"""
        # Process flagged questions from button clicks
        if st.session_state.question_asked and st.session_state.current_question:
            self._process_question(st.session_state.current_question)
            st.session_state.question_asked = False
            st.session_state.current_question = ""
            st.experimental_rerun()
        
        # Get user input from chat interface
        question = st.chat_input("Type your question here...")
        
        if question:
            # Add user message to chat history
            st.session_state.messages.append({"role": "User", "content": question})
            
            # Display user message
            with st.chat_message("User"):
                st.markdown(question)
                
            # Process and display assistant response
            with st.chat_message("assistant"):
                message_placeholder = st.empty()
                response, relative_paths = self._process_user_input(question)
                
                cleaned_response = ResponseProcessor.clean_assistant_response(response)
                message_placeholder.markdown(cleaned_response)
                
                # Update related documents
                self._update_related_documents(relative_paths)
                
            # Add assistant message to chat history
            st.session_state.messages.append({"role": "assistant", "content": response})
            
            st.experimental_rerun()
    
    def _process_question(self, question: str):
        """Process a question from button click"""
        # Add user message to chat history
        st.session_state.messages.append({"role": "User", "content": question})
        
        # Process and get assistant response
        response, relative_paths = self._process_user_input(question)
        
        # Add assistant message to chat history
        st.session_state.messages.append({"role": "assistant", "content": response})
        
        # Update related documents
        self._update_related_documents(relative_paths)
    
    def _process_user_input(self, question: str) -> Tuple[str, Set[str]]:
        """Process user question and generate response"""
        # Sanitize question
        question = question.replace("'", "")
        
        # Create prompt and get response
        prompt, relative_paths, context_data = self._create_prompt(question)
        
        with st.spinner(f"Thinking..."):
            response = self.snowflake_service.complete_with_llm(st.session_state.model_name, prompt)
            
        # Process video links and sanitize response
        response = ResponseProcessor.process_video_links(response)
        response = response.replace("'", "")
        
        # Extract questions from response to update suggested questions
        new_questions = ResponseProcessor.extract_related_questions(response, context_data)
        if new_questions:
            st.session_state.suggested_questions = new_questions
        
        return response, relative_paths
    
    def _create_prompt(self, question: str) -> Tuple[str, Set[str], Dict]:
        """Create the prompt for the LLM with retrieved context"""
        chat_history = []
        prompt_context = {}
        
        # Decide whether to use history and how to retrieve similar chunks
        if st.session_state.use_chat_history and len(st.session_state.messages) > 0:
            chat_history = self._get_chat_history()
            if chat_history:
                summary = self._summarize_conversation(chat_history, question)
                prompt_context = self.snowflake_service.search_similar_chunks(
                    summary, 
                    st.session_state.category_value, 
                    st.session_state.num_chunks
                )
            else:
                prompt_context = self.snowflake_service.search_similar_chunks(
                    question, 
                    st.session_state.category_value, 
                    st.session_state.num_chunks
                )
        else:
            prompt_context = self.snowflake_service.search_similar_chunks(
                question, 
                st.session_state.category_value, 
                st.session_state.num_chunks
            )
            
        # Extract document paths from search results
        json_data = json.loads(prompt_context) if isinstance(prompt_context, str) else prompt_context
        relative_paths = set(item['relative_path'] for item in json_data['results'])
        
        # Build the enhanced prompt
        prompt = PromptBuilder.create_main_prompt(question, chat_history, prompt_context)
        
        return prompt, relative_paths, json_data
    
    def _get_chat_history(self) -> List[Dict[str, str]]:
        """Get recent chat history based on sliding window"""
        window_size = st.session_state.sliding_window
        start_index = max(0, len(st.session_state.messages) - window_size)
        return st.session_state.messages[start_index:len(st.session_state.messages)]
    
    def _summarize_conversation(self, chat_history: List[Dict[str, str]], question: str) -> str:
        """Summarize the conversation to provide better context for retrieval"""
        prompt = PromptBuilder.create_chat_summary_prompt(chat_history, question)
        summary = self.snowflake_service.complete_with_llm(st.session_state.model_name, prompt)
        
        return summary.replace("'", "")
    
    def _update_related_documents(self, relative_paths: Set[str]):
        """Update related documents in session state for display in the right column"""
        if relative_paths:
            documents = []
            for path in relative_paths:
                url_link = self.snowflake_service.get_document_url(path)
                documents.append((path, url_link))
            
            st.session_state.related_documents = documents


def main():
    """Main application entry point"""
    app = ChatApp()
    app.run()


if __name__ == "__main__":
    main()