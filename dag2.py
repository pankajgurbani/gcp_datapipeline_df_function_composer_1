from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 3),
    'depends_on_past': False,
    'email': ['pankajgurbani01@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('fetch_cricket_stats1',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

run_script_task = BashOperator(
    task_id='run_script',
    bash_command='python /home/airflow/gcs/dags/scripts/extract_and_push_gcs.py',
    dag = dag
    # bash_command='python /home/airflow/gcs/dags/scripts/extract_and_push_gcs.py',
)
 
run_script_task