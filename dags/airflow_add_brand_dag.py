# airflow_add_brand_dag.py
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_add_brand_file import process_musinsa_products, process_29cm_products, process_zigzag_products, combine_and_upload
from datetime import timedelta
import logging
import boto3
import pandas as pd
import io
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
import time
from airflow.hooks.S3_hook import S3Hook
from webdriver_manager.chrome import ChromeDriverManager
import logging

'''
패치내역
v5
 - musinsa, 29cm에서 브랜드를 긁어오고, print해보게끔 함. zigzag는 나중에 하기.


v6
 - musinsa에서 예외가 되는 부분을 발견함 이 부분을 수정함.
 - 29cm에서 예외처리를 추가해줬음.
 - musinsa와 29cm에서 예외처리를 추가해줬음.
 - 결합 전 old_product와 brand_info 길이가 같아야 추가할 수 있음. 그러니 길이가 같은지 테스트부터 진행.
 
v7
 - zigzag crawling start
 - create combind both old_data_frame(old_product) and brand_info
 - modify dag
 
v8
 - 3가지 플랫폼에서 데이터 수집 후 데이터 프레임으로 합친 이후, csv파일로 저장하는 부분 추가
 
 
v9
 - 29cm,musinsa,zigzag를 3개의 task로 분리하고 병렬로 처리한 이후, 결합하여 s3에 올리는 구조
 - 실험결과
   - driver.find_element(By.XPATH, xpath).text 로 가져오는게 제일 빠름.
   - 결과를 근거로, 코드에 적용 (초기 코드 실행 결과 32분 나옴, local airflow 기준)
   - 향후 작업 방안
        - 옷 상세 정보도 가져와보기
        - 브랜드 정보가 이미 s3에 있으면 그거 가져와서 시간 단축하는 구조로 만들기
        
        
    v9_1
    - local 테스트 결과 이상 없었으나, 클라우드 환경에서 진행하니 리소스 부족으로 task가 up_for_retry 상태에 빠짐
        - 원인 파악 결과, signal 15가 호출되어 task가 비정상 종료가 되었고, 이는 리소스 부족으로 추측
        -(https://stackoverflow.com/questions/77096452/gunicorn-worker-getting-exit-in-airflow-web-server-with-received-signal-15-clo)
        - 리소스 최적화를 위해 병렬처리는 일단 보류
        
v10
 - local 환경에서 테스트를 하며 brand가 추가된 csv 파일이 현재 s3://integrated-data/brand/ 아래에 존재함.
 - 이를 활용하여 이미 존재하면 True, 존재하지 않으면 False로 접근하고자 함.(분기 처리)
    
    
'''

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # 시작 날짜를 현재 시점으로 설정
    'retries': 1,  # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=5),  # 재시도 간격
}

# CSV 파일 존재 여부 확인 함수
def check_file_exists():
    bucket_name = 'otto-glue'
    s3_key = 'integrated-data/brand/combined_products_with_brands.csv'
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix='integrated-data/brand/')
    
    if s3_key in keys:
        logging.info(f"File {s3_key} exists.")
        return 'print_csv_head_task'
    else:
        logging.info(f"File {s3_key} does not exist.")
        return 'process_zigzag_products'


# CSV 파일에서 상위 10개 행을 출력하는 함수
def print_csv_head():
    bucket_name = 'otto-glue'
    s3_key = 'integrated-data/brand/combined_products_with_brands.csv'
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_object = s3_hook.get_key(s3_key, bucket_name)
    
    s3_data = s3_object.get()['Body'].read().decode('utf-8')
    data = pd.read_csv(io.StringIO(s3_data))
    
    logging.info("CSV file head:")
    logging.info(data.head(10))

# DAG 정의
dag = DAG(
    'process_brand_info_dag_v9_2',
    default_args=default_args,
    description='S3에서 제품 브랜드 정보를 처리하는 DAG',
    schedule_interval='@daily',  # 매일 실행
    catchup=False,  # 지나간 날짜의 작업은 수행하지 않음
)

# Branching task 정의
branching_task = BranchPythonOperator(
    task_id='check_file_exists_task',
    python_callable=check_file_exists,
    dag=dag,
)

# CSV 파일에서 상위 10개 행을 출력하는 태스크
print_csv_head_task = PythonOperator(
    task_id='print_csv_head_task',
    python_callable=print_csv_head,
    dag=dag,
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

# Task dependencies 설정
branching_task >> [process_zigzag_task, print_csv_head_task]
process_zigzag_task >> process_musinsa_task >> process_29cm_task >> combine_and_upload_task
