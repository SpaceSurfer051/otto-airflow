from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sel_ex import run_selenium

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'selenium_dag',
    default_args=default_args,
    description='A simple Selenium DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def run_selenium_script():
    
    run_selenium()

run_selenium_task = PythonOperator(
    task_id='run_selenium',
    python_callable=run_selenium_script,
    dag=dag,
)

run_selenium_task
