import logging
import pandas as pd
from datetime import timedelta
from io import StringIO

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator

# 기본 인자 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# DAG 설정
dag = DAG(
    "s3_to_dataframe_dag5",
    default_args=default_args,
    description="DAG to read CSV from S3 and print first 5 rows",
    schedule_interval=timedelta(days=1),
)

def read_csv_from_s3_and_print_head():
    try:
        logging.info("Starting to read CSV from S3")
        print("Starting to read CSV from S3")
        
        s3_hook = S3Hook(aws_conn_id='aws_s3')
        logging.info("Created S3Hook")
        print("Created S3Hook")
        
        bucket_name = 'otto-glue'
        key = 'non-integrated-data/products_with_size_color.csv'
        
        s3_client = s3_hook.get_conn()
        logging.info("Connected to S3")
        print("Connected to S3")
        
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        logging.info(f"Retrieved object from S3: {key}")
        print(f"Retrieved object from S3: {key}")
        
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        logging.info("Read CSV into DataFrame")
        print("Read CSV into DataFrame")
        
        logging.info("First 5 rows of the dataframe:\n%s", df.head())
        print("First 5 rows of the dataframe:")
        print(df.head())
    except Exception as e:
        logging.error("Error reading CSV from S3: %s", e)
        print(f"Error reading CSV from S3: {e}")

# PythonOperator를 사용하여 태스크를 정의합니다.
read_csv_task = PythonOperator(
    task_id='read_csv_from_s3_and_print_head',
    python_callable=read_csv_from_s3_and_print_head,
    dag=dag,
)

# DAG 내에 태스크를 추가합니다.
with dag:
    read_csv_task
