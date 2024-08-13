from airflow import DAG
from datetime import datetime
from test_operator import S3ListOperator,crawlingOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    's3_list_dag_v1',
    default_args=default_args,
    description='DAG for listing files in S3 using a custom operator_v2',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # S3 버킷의 마지막 파일 목록을 가져오는 작업
    list_s3_files = S3ListOperator(
        task_id='list_s3_files',
        aws_conn_id='aws_default',
        bucket_name='otto-glue',
        s3_root='integrated-data/products/',
    )

    test_crawling_files = crawlingOperator(
        task_id = 'crawling_test',
        aws_conn_id='aws_default',
        bucket_name='otto-glue',
        reviews_s3_root = 'integrated-data/reviews/',
        products_s3_root='integrated-data/products/',
        
    )
    
    list_s3_files >> test_crawling_files