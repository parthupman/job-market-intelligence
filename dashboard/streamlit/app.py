import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Job Market Intelligence Dashboard",
    page_icon="📊",
    layout="wide"
)

# --- DATABASE CONNECTION ---
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        os.getenv("DATABASE_URL", "postgresql://jobmarket:your_password_here@localhost:5432/job_market")
    )

conn = init_connection()

@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)

# --- UI HEADER ---
st.title("📊 Job Market Intelligence Dashboard")
st.markdown("Real-time insights from Adzuna, RemoteOK, and The Muse job postings.")

# --- METADATA/FILTERS ---
st.sidebar.header("Filters")
# Simple static options for demonstration, could be dynamic
category = st.sidebar.selectbox(
    "Job Category",
    ("All", "Data Analyst", "Data Scientist", "Business Analyst", "Analytics Engineer")
)
seniority = st.sidebar.selectbox(
    "Seniority Level",
    ("All", "Junior", "Mid-Level", "Senior", "Lead", "Management")
)

# --- DAILY METRICS KPIs ---
try:
    metrics_query = """
        SELECT * FROM marts.agg_daily_metrics 
        ORDER BY posted_date DESC 
        LIMIT 1
    """
    latest_metrics = run_query(metrics_query)
    
    if not latest_metrics.empty:
        st.header("Today's Market Snapshot")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Jobs Tracked", f"{latest_metrics['total_jobs_posted'].iloc[0]:,}")
        col2.metric("Unique Companies", f"{latest_metrics['unique_companies'].iloc[0]:,}")
        col3.metric("Median Salary", f"${latest_metrics['median_salary'].iloc[0]:,.0f}")
        col4.metric("% with Salary Info", f"{latest_metrics['pct_with_salary'].iloc[0]}%")
        st.markdown("---")
except Exception as e:
    st.warning(f"Waiting for dbt pipeline to populate `marts.agg_daily_metrics`. Error: {e}")


# --- SALARY BY ROLE CHART ---
st.header("💰 Salary Distribution by Role & Seniority")
try:
    salary_query = "SELECT * FROM marts.agg_salary_by_role"
    df_salary = run_query(salary_query)
    
    if not df_salary.empty:
        # Filter logic
        if category != "All":
            df_salary = df_salary[df_salary["job_category"] == category]
        if seniority != "All":
            df_salary = df_salary[df_salary["seniority_level"] == seniority]
            
        st.bar_chart(
            data=df_salary,
            x="job_category",
            y=["p25_salary", "median_salary", "p75_salary"],
            color=["#FF6B6B", "#4ECDC4", "#45B7D1"]
        )
        st.dataframe(df_salary, use_container_width=True)
except Exception as e:
    st.info("Run the dbt pipeline to see salary aggregations.")

# --- LATEST JOBS TABLE ---
st.header("🆕 Latest Job Postings")
try:
    jobs_query = """
        SELECT 
            job_title as "Title",
            company_name as "Company",
            location_raw as "Location",
            job_category as "Category",
            seniority_level as "Seniority",
            salary_midpoint as "Estimated Salary",
            posted_date as "Date"
        FROM marts.fct_job_postings
        ORDER BY posted_date DESC
        LIMIT 100
    """
    df_jobs = run_query(jobs_query)
    
    if not df_jobs.empty:
        if category != "All":
            df_jobs = df_jobs[df_jobs["Category"] == category]
        if seniority != "All":
            df_jobs = df_jobs[df_jobs["Seniority"] == seniority]
            
        st.dataframe(df_jobs, use_container_width=True, hide_index=True)
except Exception as e:
    st.info("Run the extraction pipeline to see real-time job listings here.")

st.sidebar.markdown("""
---
**Powered by:**
- Apache Airflow
- PostgreSQL
- dbt (Data Build Tool)
""")
