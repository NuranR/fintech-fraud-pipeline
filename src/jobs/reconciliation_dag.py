from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add current directory to path so Airflow can import the logic script
sys.path.append(os.path.dirname(__file__))
from generate_report import generate_daily_report

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False, # If yesterday failed, run today anyway
    'email_on_failure': False, # Disable alerts for dev env
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'daily_reconciliation_job',
    default_args=default_args,
    description='Aggregates validated transactions from Data Lake',
    schedule_interval='0 */6 * * *',  # Every 6 hours (00:00, 06:00, 12:00, 18:00)
    start_date=datetime(2023, 1, 1),
    catchup=False, # Don't run for past dates
    tags=['fintech', 'reporting'],
) as dag:

    run_report = PythonOperator(
        task_id='generate_daily_stats',
        python_callable=generate_daily_report
    )

    run_report