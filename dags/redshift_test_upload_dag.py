from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'otto_redshift_data_upload',
    default_args=default_args,
    description='Upload data to Redshift with deduplication',
    schedule_interval=None,
)

# S3에서 데이터를 읽고 데이터프레임으로 변환하는 함수
def read_s3_to_dataframe(bucket_name, key, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_object = s3_hook.get_key(key, bucket_name)
    s3_data = s3_object.get()['Body'].read().decode('utf-8')
    data = pd.read_csv(io.StringIO(s3_data))
    return data

# 데이터베이스에 연결하여 데이터프레임으로 변환하는 함수
def fetch_product_names():
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    sql = "SELECT product_name FROM otto.product_table"
    connection = redshift_hook.get_conn()
    return pd.read_sql(sql, connection)

# S3에서 리뷰 데이터를 읽는 태스크
def read_review_data(**kwargs):
    bucket_name = 'otto-glue'
    review_key = 'integrated-data/reviews/combined_reviews_2024-07-29 08:38:46.040114.csv'
    review_df = read_s3_to_dataframe(bucket_name, review_key)
    kwargs['ti'].xcom_push(key='review_df', value=review_df.to_json())

# Redshift에서 product_name 목록을 가져오는 태스크
def get_existing_product_names(**kwargs):
    existing_product_names_df = fetch_product_names()
    kwargs['ti'].xcom_push(key='existing_product_names_df', value=existing_product_names_df.to_json())

# 리뷰 데이터를 필터링하고 Redshift에 삽입하는 태스크
def process_and_upload_review_data(**kwargs):
    ti = kwargs['ti']
    review_df = pd.read_json(ti.xcom_pull(key='review_df', task_ids='read_review_data'))
    existing_product_names_df = pd.read_json(ti.xcom_pull(key='existing_product_names_df', task_ids='get_existing_product_names'))

    # product_name이 존재하지 않는 리뷰만 필터링
    new_reviews_df = review_df[~review_df['product_name'].isin(existing_product_names_df['product_name'])]

    if not new_reviews_df.empty:
        redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()

        for index, row in new_reviews_df.iterrows():
            cursor.execute("""
                INSERT INTO otto.reviews (review_id, product_name, color, size, height, gender, weight, top_size, bottom_size, size_comment, quality_comment, color_comment, thickness_comment, brightness_comment, comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))
        
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Inserted {len(new_reviews_df)} new rows into otto.reviews")

# 태스크 정의
read_review_data_task = PythonOperator(
    task_id='read_review_data',
    python_callable=read_review_data,
    provide_context=True,
    dag=dag,
)

get_existing_product_names_task = PythonOperator(
    task_id='get_existing_product_names',
    python_callable=get_existing_product_names,
    provide_context=True,
    dag=dag,
)

process_and_upload_review_data_task = PythonOperator(
    task_id='process_and_upload_review_data',
    python_callable=process_and_upload_review_data,
    provide_context=True,
    dag=dag,
)

# 태스크 순서 정의
read_review_data_task >> get_existing_product_names_task >> process_and_upload_review_data_task
