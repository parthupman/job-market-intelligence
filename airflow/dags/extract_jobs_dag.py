from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default args
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    "job_market_pipeline",
    default_args=default_args,
    description="Daily job market data extraction and transformation",
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["job-market", "etl", "production"],
)


# Task 1: Extract data from APIs
def extract_jobs():
    import sys
    import os
    # Add the extract directory to path so we can import from it
    sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'extract'))
    from run_extraction import run_full_extraction
    
    df = run_full_extraction(
        search_terms=[
            "data analyst",
            "business analyst",
            "data scientist",
            "analytics engineer"
        ],
        output_to_db=True,
        output_to_csv=False
    )
    
    return {"rows_extracted": len(df)}


extract_task = PythonOperator(
    task_id="extract_jobs",
    python_callable=extract_jobs,
    dag=dag,
)


# Task 2: Run dbt models (will be active after dbt is configured)
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
        cd /opt/dbt_project && \
        dbt run --profiles-dir . --target prod
    """,
    dag=dag,
)


# Task 3: Run dbt tests (will be active after dbt is configured)
dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
        cd /opt/dbt_project && \
        dbt test --profiles-dir . --target prod
    """,
    dag=dag,
)

# Task dependencies
# Un-comment dbt tasks once dbt is set up
extract_task # >> dbt_run >> dbt_test
