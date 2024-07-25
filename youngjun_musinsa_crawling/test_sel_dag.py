from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from test_sel import read_s3_and_print_links  # 이 부분에서 정확한 모듈 경로를 설정하세요

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'print_new_links_dag',
    default_args=default_args,
    description='DAG to print new links from S3',
    schedule_interval=None,
)

print_links_task = PythonOperator(
    task_id='read_s3_and_print_links',
    python_callable=read_s3_and_print_links,
    dag=dag,
)

print_links_task
