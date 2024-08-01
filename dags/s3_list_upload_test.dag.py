from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime

def list_files_in_s3(bucket_name, prefix):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    files2 = files[-1]
    return files2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('s3_list_files_dag',
        default_args=default_args,
        schedule_interval='@once',
        catchup=False) as dag:

    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files_in_s3,
        op_kwargs={
            'bucket_name': 'otto-glue',
            'prefix': 'integrated-data/products/'
        },
    )

    list_files_task
