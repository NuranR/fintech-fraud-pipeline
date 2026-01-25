from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add src directory to path for config import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    import config
    DAG_SCHEDULE = config.DAG_SCHEDULE_INTERVAL
except ImportError:
    DAG_SCHEDULE = "0 */6 * * *"

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
    schedule_interval=DAG_SCHEDULE,  # From config: Every 6 hours
    start_date=datetime(2023, 1, 1),
    catchup=False,  # Don't run for past dates
    tags=['fintech', 'reporting'],
) as dag:

    run_report = PythonOperator(
        task_id='generate_daily_stats',
        python_callable=generate_daily_report
    )

    run_report