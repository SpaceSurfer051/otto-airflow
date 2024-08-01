from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime

def list_product_in_s3(bucket_name, prefix_product,prefix_review):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    files_product = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix_product)
    files_product2 = files_product[-1]
    print(files_product2)
    
    
    files_review = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix_review)
    files_review2 = files_review[-1]
    print(files_review2)
    
    return files_product2

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

    list_product_in_s3 = PythonOperator(
        task_id='list_files',
        python_callable=list_product_in_s3,
        op_kwargs={
            'bucket_name': 'otto-glue',
            'prefix_product': 'integrated-data/products/',
            'prefix_review': 'integrated-data/reviews/'
        },
    )

    list_product_in_s3 
