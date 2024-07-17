
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_crawling_and_processing',
    default_args=default_args,
    description='A simple data crawling and processing workflow',
    schedule_interval=timedelta(days=1),
)

def run_review_product_data_crawling():
    subprocess.run(["python", "/path/to/airflow_product_review.py"], check=True)

def run_color_size_crawling():
    subprocess.run(["python", "/path/to/airflow_size_color.py"], check=True)

def run_test2():
    subprocess.run(["python", "/path/to/airflow_data_preprocessing.py"], check=True)

t1 = PythonOperator(
    task_id='review_product_data_crawling',
    python_callable=run_review_product_data_crawling,
    dag=dag,
)

t2 = PythonOperator(
    task_id='color_size_crawling',
    python_callable=run_color_size_crawling,
    dag=dag,
)

t3 = PythonOperator(
    task_id='data_processing_and_loading',
    python_callable=run_test2,
    dag=dag,
)

t1 >> t2 >> t3
