from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from test_folder.test_redshift_connection import *
# 폴더 테스트

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'redshift_connection_test',
    default_args=default_args,
    description='Test connection to Redshift',
    schedule_interval=None,
)



# 태스크 정의
test_connection_task = PythonOperator(
    task_id='test_connection',
    python_callable=test_redshift_connection,
    dag=dag,
)

test_connection_task
