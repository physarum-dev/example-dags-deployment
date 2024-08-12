from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 9),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'git_sync_example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
)

# Define the start task
start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

# Define the processing task
def process_data():
    print("Processing data...")

processing_task = PythonOperator(
    task_id='processing_task',
    python_callable=process_data,
    dag=dag,
)

# Define the end task
end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Set task dependencies
start_task >> processing_task >> end_task

