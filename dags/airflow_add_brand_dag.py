# airflow_add_brand_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow_add_brand_file import process_musinsa_products, process_29cm_products, process_zigzag_products, combine_and_upload
from datetime import timedelta

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
'''

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
    'process_brand_info_dag_v9_1',
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
[process_zigzag_task,process_musinsa_task,  process_29cm_task] >> combine_and_upload_task
