from airflow import DAG
from fivetran_provider_async.sensors import FivetranSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'sqlmesh_run_fivetran',
    default_args=default_args,
    description='Run sqlmesh after Fivetran sync completes',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Fivetran sensor to wait for the sync to complete
    wait_for_fivetran_sync = FivetranSensor(
        task_id='wait_for_fivetran_sync',
        connector_id='golf_makeover',
        poke_interval=5,
        completed_after_time="{{ data_interval_end + macros.timedelta(minutes=1) }}",
    )

    # Bash task to run sqlmesh command
    sqlmesh_run = BashOperator(
        task_id='sqlmesh_run',
        bash_command='cd /usr/local/airflow/dags/sqlmesh && sqlmesh run',
    )

    # Set task dependencies
    wait_for_fivetran_sync >> sqlmesh_run
