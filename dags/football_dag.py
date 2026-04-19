from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for our robot
default_args = {
    'owner': 'arda_de',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# The DAG (The movie director)
with DAG(
    'football_pipeline_v1',
    default_args=default_args,
    description='Automated Football Data Pipeline: Extract -> Load -> Transform',
    schedule_interval=timedelta(days=1), # Runs every day
    catchup=False
) as dag:

    # Task 1: Extract and Load
    extract_task = BashOperator(
        task_id='extract_and_load',
        bash_command='python /opt/airflow/scripts/extract_data.py',
    )

    # Task 2: Transform with dbt
    transform_task = BashOperator(
        task_id='transform_with_dbt',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
    )

    # The Flow: First extract, then transform
    extract_task >> transform_task
