from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# 통합 데이터 스크립트 가져오기
#import sys
#sys.path.insert(0, '/mnt/data')  # 필요한 경로로 설정
from airflow_data_integrated import integrate_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'integrate_data_dag',
    default_args=default_args,
    description='A DAG to integrate data and upload to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def run_integrate_data():
    integrate_data()

# PythonOperator 정의
integrate_data_task = PythonOperator(
    task_id='integrate_data_task',
    python_callable=run_integrate_data,
    dag=dag,
)

integrate_data_task
