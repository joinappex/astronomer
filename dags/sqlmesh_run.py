from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'sqlmesh_bash_dag',  # DAG name
    default_args=default_args,
    description='A simple DAG to run sqlmesh',
    schedule_interval=None,  # Set to None for manual trigger
    start_date=datetime(2024, 1, 1),  # Replace with your desired start date
    catchup=False,  # Don't backfill
) as dag:

    # Define the BashOperator task
    run_sqlmesh = BashOperator(
        task_id='run_sqlmesh',
        bash_command='cd /usr/local/airflow/dags/sqlmesh && sqlmesh run',
    )

    # Add more tasks if needed and define dependencies
    run_sqlmesh
