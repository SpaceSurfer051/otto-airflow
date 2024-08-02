from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow_add_brand_file import process_data  # 모듈에서 함수 import
from datetime import timedelta

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # 시작 날짜를 현재 시점으로 설정
    'retries': 1,  # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=5),  # 재시도 간격
}

# DAG 정의
dag = DAG(
    'process_brand_info_dag_v2',
    default_args=default_args,
    description='S3에서 제품 브랜드 정보를 처리하는 DAG',
    schedule_interval='@daily',  # 매일 실행
    catchup=False,  # 지나간 날짜의 작업은 수행하지 않음
)

# Task 정의
process_brand_data_task = PythonOperator(
    task_id='process_brand_data',
    python_callable=process_data,  # Import한 함수 호출
    dag=dag,
)

# Task 설정
process_brand_data_task

