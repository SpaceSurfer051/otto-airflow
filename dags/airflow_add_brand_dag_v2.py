# airflow_add_brand_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow_add_brand_file2 import process_musinsa_products, process_29cm_products, process_zigzag_products, combine_and_upload
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
    'process_brand_info_dag_v8',
    default_args=default_args,
    description='S3에서 제품 브랜드 정보를 처리하는 DAG',
    schedule_interval='@daily',  # 매일 실행
    catchup=False,  # 지나간 날짜의 작업은 수행하지 않음
)

# Task 정의
process_musinsa_task = PythonOperator(
    task_id='process_musinsa_products',
    python_callable=process_musinsa_products,
    provide_context=True,
    dag=dag,
)

process_29cm_task = PythonOperator(
    task_id='process_29cm_products',
    python_callable=process_29cm_products,
    provide_context=True,
    dag=dag,
)

process_zigzag_task = PythonOperator(
    task_id='process_zigzag_products',
    python_callable=process_zigzag_products,
    provide_context=True,
    dag=dag,
)

combine_and_upload_task = PythonOperator(
    task_id='combine_and_upload',
    python_callable=combine_and_upload,
    provide_context=True,
    dag=dag,
)

# Task 설정
[process_musinsa_task, process_29cm_task, process_zigzag_task] >> combine_and_upload_task
