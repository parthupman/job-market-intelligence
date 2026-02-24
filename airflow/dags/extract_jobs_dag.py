from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago

# Default args
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
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
    from extract.run_extraction import run_full_extraction
    
    df = run_full_extraction(
        search_terms=[
            "data analyst",
            "business analyst",
            "data scientist",
            "analytics engineer"
        ],
        output_to_db=True
    )
    
    return {"rows_extracted": len(df)}


extract_task = PythonOperator(
    task_id="extract_jobs",
    python_callable=extract_jobs,
    dag=dag,
)


# Task 2: Run dbt models
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
        cd /opt/dbt_project && \
        dbt run --profiles-dir . --target prod
    """,
    dag=dag,
)


# Task 3: Run dbt tests
dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
        cd /opt/dbt_project && \
        dbt test --profiles-dir . --target prod
    """,
    dag=dag,
)


# Task 4: Run Data Quality
def run_data_quality():
    import great_expectations as gx
    
    context = gx.get_context(context_root_dir="/opt/airflow/quality/great_expectations")
    result = context.run_checkpoint(checkpoint_name="daily_validation")
    
    if not result.success:
        raise Exception("Data quality checks failed!")
    
    return {"status": "passed"}


quality_check = PythonOperator(
    task_id="data_quality_check",
    python_callable=run_data_quality,
    dag=dag,
)

# Task 5: Run ML Salary Prediction
ml_predict_task = BashOperator(
    task_id="ml_salary_predict",
    bash_command="""
        python /opt/airflow/scripts/predict_salaries.py
    """,
    dag=dag,
)


# Task 6: Slack notification on success
slack_success = SlackWebhookOperator(
    task_id="slack_success",
    slack_webhook_conn_id="slack_webhook",
    message="""
        ✅ Job Market Pipeline Success!
        
        📊 Daily extraction complete
        🧪 All data quality checks passed
        🤖 ML Salary Predictions computed
        📈 Dashboard updated
    """,
    dag=dag,
)


# Task dependencies
extract_task >> dbt_run >> dbt_test >> quality_check >> ml_predict_task >> slack_success
