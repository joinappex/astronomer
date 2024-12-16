from airflow import DAG
from airflow.providers.fivetran.sensors.fivetran import FivetranSensor
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
        connector_id='golf_makeover',  # Fivetran connector ID
        # fivetran_conn_id='fivetran_default',  # Replace with your Fivetran connection ID if needed
        mode='reschedule',  # Frees worker slots while waiting
        poke_interval=300,  # Check every 5 minutes
        timeout=3600,  # Timeout after 1 hour
        always_wait_when_syncing=True,
    )

    # Bash task to run sqlmesh command
    sqlmesh_run = BashOperator(
        task_id='sqlmesh_run',
        bash_command='cd /usr/local/airflow/dags/sqlmesh && sqlmesh run',
    )

    # Set task dependencies
    wait_for_fivetran_sync >> sqlmesh_run
