'''
task 순서


실행 주기 : (하루에 한 번 정도가 적당한 것 같음)

1. review_product_data_crawling.py - review와 product 정보 크롤링

2. color_size_crawling.py - 컬러와 사이즈 정보 크롤링

3. 데이터 전처리 ( 예정 )


'''
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
    subprocess.run(["python", "/path/to/review_product_data_crawling.py"], check=True)

def run_color_size_crawling():
    subprocess.run(["python", "/path/to/color_size_crawling.py"], check=True)

def run_test2():
    subprocess.run(["python", "/path/to/test2.py"], check=True)

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
