from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from airflow.models import XCom
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session as SQLASession
import logging
from all_update_brand.airflow_add_brand_file import (
    process_musinsa_products,
    process_29cm_products,
    process_zigzag_products,
    combine_and_upload,
    prepare_update_urls,
    update_musinsa_crawling,
    update_29cm_crawling,
    update_zigzag_crawling,
    combine_and_upload_updated,
    fetch_old_product_info,
    fetch_new_product_info,    
)
from datetime import timedelta

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
'''
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
    
    v10_1
        - 분기처리하여, update하는 코드를 추가 했음.
    v10_3
        - update할만한 데이터가 없으면 모든 task가 종료
        
v11
 - 데이터가 예상치 못하게 중복저장 되는 중.
    - 문제 원인 파악 결과, 지속적인 테스트를 위한 dag 호출 결과 key-value 형태로 구성된 xcom에서 여러 데이터를 가져오는 문제 확인
    - xcom 문제가 아닌 것 같음. 그냥 중복제거 처리함.
    - 그냥 중복제거로 처리 (product table 갯수 446개)

    
'''
# XCom 데이터 초기화 함수
@provide_session
def clear_xcom_data(session: SQLASession = None, **context):
    session.query(XCom).filter(XCom.dag_id == context['dag'].dag_id).delete()
    session.commit()
    logging.info("XCom data cleared")

# CSV 파일 존재 여부 및 업데이트 여부 결정 함수
def check_file_and_decide_update(ti):
    # 파일이 S3에 있는지 확인
    bucket_name = 'otto-glue'
    s3_key = 'integrated-data/brand/combined_products_with_brands.csv'
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix='integrated-data/brand/')
    
    if s3_key in keys:
        logging.info(f"File {s3_key} exists.")
        
        # old_product와 new_product 데이터를 가져옵니다.
        old_product = fetch_old_product_info()
        old_product.drop_duplicates(inplace=True)
        new_product = fetch_new_product_info()
        new_product.drop_duplicates(inplace=True)
        
        logging.info(f"Old product count: {len(old_product)}, New product count: {len(new_product)}")

        # old_product가 new_product보다 더 많은 행을 가지고 있는지 확인
        if len(old_product) > len(new_product):
            logging.info("old_product가 new_product보다 많은 행을 가지고 있습니다. 업데이트를 진행합니다.")
            
            # 업데이트할 URL을 준비합니다.
            update_urls = old_product[~old_product['description'].isin(new_product['description'])]
            
            # update_urls가 None이 아니고, 비어있지 않으면 업데이트 진행
            if len(update_urls) > 0:
                return 'prepare_update_urls_task'
            else:
                logging.info("업데이트할 항목이 없습니다. 모든 태스크를 건너뜁니다.")
                return 'skip_update_tasks'
        else:
            logging.info("old_product의 행 수가 new_product의 행 수와 같거나 적습니다. 업데이트를 중단합니다.")
            return 'skip_update_tasks'
    else:
        logging.info(f"File {s3_key} does not exist.")
        return 'process_zigzag_products'

# DAG 정의
dag = DAG(
    'process_brand_info_dag_v11_5',
    default_args=default_args,
    description='S3에서 제품 브랜드 정보를 처리하는 DAG',
    schedule_interval='@daily',
    catchup=False,
)

# XCom 초기화 Task (시작 전)
clear_xcom_start = PythonOperator(
    task_id='clear_xcom_start',
    python_callable=clear_xcom_data,
    provide_context=True,
    dag=dag,
)

# XCom 초기화 Task (종료 후)
clear_xcom_end = PythonOperator(
    task_id='clear_xcom_end',
    python_callable=clear_xcom_data,
    provide_context=True,
    dag=dag,
)

# Branching task 정의 - 수정된 함수 사용
branching_task = BranchPythonOperator(
    task_id='check_file_and_decide_update_task',
    python_callable=check_file_and_decide_update,
    dag=dag,
)

# 업데이트할 URL 목록 준비 태스크
prepare_update_urls_task = PythonOperator(
    task_id='prepare_update_urls_task',
    python_callable=prepare_update_urls,
    dag=dag,
)

# Musinsa 업데이트 크롤링 태스크
update_musinsa_task = PythonOperator(
    task_id='update_musinsa_crawling',
    python_callable=update_musinsa_crawling,
    dag=dag,
)

# 29cm 업데이트 크롤링 태스크
update_29cm_task = PythonOperator(
    task_id='update_29cm_crawling',
    python_callable=update_29cm_crawling,
    dag=dag,
)

# Zigzag 업데이트 크롤링 태스크
update_zigzag_task = PythonOperator(
    task_id='update_zigzag_crawling',
    python_callable=update_zigzag_crawling,
    dag=dag,
)

# 업데이트된 결과를 결합하여 S3에 업로드하는 태스크
combine_and_upload_updated_task = PythonOperator(
    task_id='combine_and_upload_updated',
    python_callable=combine_and_upload_updated,
    dag=dag,
)

# 업데이트가 없는 경우 모든 업데이트 태스크를 건너뛰고 종료 태스크로 이동
skip_update_tasks = PythonOperator(
    task_id='skip_update_tasks',
    python_callable=lambda: logging.info("업데이트할 항목이 없어 태스크를 건너뜁니다."),
    dag=dag,
)

# 기존 크롤링 태스크
process_musinsa_task = PythonOperator(
    task_id='process_musinsa_products',
    python_callable=process_musinsa_products,
    dag=dag,
)

process_29cm_task = PythonOperator(
    task_id='process_29cm_products',
    python_callable=process_29cm_products,
    dag=dag,
)

process_zigzag_task = PythonOperator(
    task_id='process_zigzag_products',
    python_callable=process_zigzag_products,
    dag=dag,
)

combine_and_upload_task = PythonOperator(
    task_id='combine_and_upload',
    python_callable=combine_and_upload,
    dag=dag,
)

# Task dependencies 설정

clear_xcom_start >> branching_task
branching_task >> [prepare_update_urls_task, process_zigzag_task, skip_update_tasks]
prepare_update_urls_task >> [update_musinsa_task, update_29cm_task, update_zigzag_task] >> combine_and_upload_updated_task
process_zigzag_task >> process_musinsa_task >> process_29cm_task >> combine_and_upload_task


#process_zigzag_task >> process_musinsa_task >> process_29cm_task >> combine_and_upload_task

