"""
Tenwave Cortex Analyst App
==========================
This app lets you chat with your Healthcare_Billing semantic model.
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import _snowflake
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException

# ✅ Your semantic model path — adjust if needed!
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "TENWAVE_DB.DATA.HEALTHCARE_DATA_STAGE/tenwave_healthcare.yaml"
]

API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000  # ms

session = get_active_session()

def main():
    if "messages" not in st.session_state:
        reset_session_state()
    show_header_and_sidebar()
    if len(st.session_state.messages) == 0:
        process_user_input("What can I ask?")
    display_conversation()
    handle_user_inputs()
    handle_error_notifications()
    display_warnings()

def reset_session_state():
    st.session_state.messages = []
    st.session_state.active_suggestion = None
    st.session_state.warnings = []
    st.session_state.form_submitted = {}

def show_header_and_sidebar():
    st.title("Tenwave Cortex Analyst")
    st.markdown(
        "Ask questions about healthcare billing, patients, doctors, hospitals and more."
    )

    with st.sidebar:
        st.selectbox(
            "Semantic Model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        _, btn, _ = st.columns([2, 6, 2])
        if btn.button("Clear Chat History", use_container_width=True):
            reset_session_state()

def handle_user_inputs():
    user_input = st.chat_input("Ask a question...")
    if user_input:
        process_user_input(user_input)
    elif st.session_state.active_suggestion:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)

def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("⚠️ API error occurred!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False

def process_user_input(prompt: str):
    st.session_state.warnings = []

    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    }
    st.session_state.messages.append(new_user_message)
    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message(new_user_message["content"], user_msg_index)

    with st.chat_message("analyst"):
        with st.spinner("Waiting for Cortex Analyst..."):
            time.sleep(1)
            response, error_msg = get_analyst_response(st.session_state.messages)
            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    "content": response["message"]["content"],
                    "request_id": response["request_id"],
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response["request_id"],
                }
                st.session_state["fire_API_error_notify"] = True

            if "warnings" in response:
                st.session_state.warnings = response["warnings"]

            st.session_state.messages.append(analyst_message)
            st.rerun()

def display_warnings():
    for warning in st.session_state.warnings:
        st.warning(warning["message"], icon="⚠️")

def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }

    resp = _snowflake.send_snow_api_request(
        "POST", API_ENDPOINT, {}, {}, request_body, None, API_TIMEOUT
    )
    parsed_content = json.loads(resp["content"])

    if resp["status"] < 400:
        return parsed_content, None
    else:
        err_msg = f"""
🚨 Analyst API error 🚨

* Status: `{resp['status']}`
* Request ID: `{parsed_content['request_id']}`
* Code: `{parsed_content['error_code']}`

Message:
        """
        return parsed_content, err_msg

def display_conversation():
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            if role == "analyst":
                display_message(content, idx, message["request_id"])
            else:
                display_message(content, idx)

def display_message(content: List[Dict], idx: int, request_id: Optional[str] = None):
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            for s_idx, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion, key=f"suggestion_{idx}_{s_idx}"
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            display_sql_query(item["statement"], idx, item["confidence"], request_id)

@st.cache_data(show_spinner=False)
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)

def display_sql_query(sql: str, idx: int, confidence: dict, request_id: Optional[str]):
    with st.expander("SQL Query"):
        st.code(sql, language="sql")
        display_sql_confidence(confidence)

    with st.expander("Results"):
        with st.spinner("Running SQL..."):
            df, err = get_query_exec_result(sql)
            if df is None:
                st.error(f"Error: {err}")
            elif df.empty:
                st.write("No data found")
            else:
                tabs = st.tabs(["Data", "Chart"])
                with tabs[0]:
                    st.dataframe(df, use_container_width=True)
                with tabs[1]:
                    display_charts_tab(df, idx)

    if request_id:
        display_feedback_section(request_id)

def display_sql_confidence(confidence: dict):
    if confidence and confidence["verified_query_used"]:
        verified = confidence["verified_query_used"]
        with st.popover("Verified Query Used"):
            st.text(f"Name: {verified['name']}")
            st.text(f"Question: {verified['question']}")
            st.text(f"Verified by: {verified['verified_by']}")
            st.text(
                f"Verified at: {datetime.fromtimestamp(verified['verified_at'])}"
            )
            st.code(verified["sql"], language="sql")

def display_charts_tab(df: pd.DataFrame, idx: int):
    if len(df.columns) >= 2:
        col1, col2 = st.columns(2)
        x = col1.selectbox("X axis", df.columns, key=f"x_{idx}")
        y = col2.selectbox(
            "Y axis", [c for c in df.columns if c != x], key=f"y_{idx}"
        )
        chart = st.selectbox(
            "Chart type", ["Line Chart", "Bar Chart"], key=f"chart_{idx}"
        )
        if chart == "Line Chart":
            st.line_chart(df.set_index(x)[y])
        else:
            st.bar_chart(df.set_index(x)[y])
    else:
        st.write("At least 2 columns needed for chart.")

def display_feedback_section(request_id: str):
    with st.popover("📝 Feedback"):
        if request_id not in st.session_state.form_submitted:
            with st.form(f"feedback_{request_id}", clear_on_submit=True):
                pos = st.radio("Rate the SQL", ["👍", "👎"], horizontal=True)
                pos = pos == "👍"
                msg = st.text_input("Feedback message (optional)")
                if st.form_submit_button("Submit"):
                    err = submit_feedback(request_id, pos, msg)
                    st.session_state.form_submitted[request_id] = {"error": err}
                    st.rerun()
        elif st.session_state.form_submitted[request_id]["error"] is None:
            st.success("Feedback submitted ✔️")
        else:
            st.error(st.session_state.form_submitted[request_id]["error"])

def submit_feedback(request_id: str, positive: bool, message: str) -> Optional[str]:
    body = {
        "request_id": request_id,
        "positive": positive,
        "feedback_message": message,
    }
    resp = _snowflake.send_snow_api_request(
        "POST", FEEDBACK_API_ENDPOINT, {}, {}, body, None, API_TIMEOUT
    )
    if resp["status"] == 200:
        return None
    parsed = json.loads(resp["content"])
    return f"""
🚨 Feedback API error 🚨
* Status: `{resp['status']}`
* Request ID: `{parsed['request_id']}`
* Code: `{parsed['error_code']}`

Message:
    """

if __name__ == "__main__":
    main()
