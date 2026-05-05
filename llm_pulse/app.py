import streamlit as st
import os

st.set_page_config(page_title="LLM Pulse Test", layout="wide")
st.title("⚡ LLM Pulse — Connection Test")

st.write("Checking environment variables...")

host  = os.getenv("DATABRICKS_SERVER_HOSTNAME", "NOT SET")
path  = os.getenv("DATABRICKS_WAREHOUSE_HTTP_PATH", "NOT SET")
token = os.getenv("DATABRICKS_TOKEN", "NOT SET")

st.write(f"HOST:  {host}")
st.write(f"PATH:  {path}")
st.write(f"TOKEN: {'SET' if token != 'NOT SET' else 'NOT SET'}")

st.divider()
st.write("Trying SQL connection...")

try:
    from databricks import sql
    conn = sql.connect(
        server_hostname = host,
        http_path       = path,
        access_token    = token
    )
    cursor = conn.cursor()
    cursor.execute("SELECT 1 AS test")
    result = cursor.fetchone()
    st.success(f"Connection successful! Result: {result}")
except Exception as e:
    st.error(f"Connection failed: {e}")