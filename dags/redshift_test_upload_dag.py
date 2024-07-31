from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import io
import random
import string

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'otto_redshift_data_upload_real_21_2',
    default_args=default_args,
    description='Upload product and review data to Redshift with deduplication',
    schedule_interval=None,
)

# 스키마를 생성하는 함수
def create_schema():
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS otto;")
    connection.commit()
    cursor.close()
    connection.close()

# 테이블을 생성하는 함수
def create_tables():
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("""
    DROP TABLE IF EXISTS otto.reviews CASCADE;
    DROP TABLE IF EXISTS otto.product_table CASCADE;

    CREATE TABLE IF NOT EXISTS otto.product_table (
        product_id TEXT,
        rank FLOAT,
        product_name TEXT PRIMARY KEY,
        category TEXT,
        price FLOAT,
        image_url TEXT,
        description TEXT,
        color TEXT,
        size TEXT,
        platform TEXT,
        UNIQUE (product_name)
    );

    CREATE TABLE IF NOT EXISTS otto.reviews (
        review_id TEXT PRIMARY KEY,
        product_name TEXT,
        color TEXT,
        size TEXT,
        height TEXT,
        gender TEXT,
        weight TEXT,
        top_size TEXT,
        bottom_size TEXT,
        size_comment TEXT,
        quality_comment TEXT,
        color_comment TEXT,
        thickness_comment TEXT,
        brightness_comment TEXT,
        comment varchar(max),
        FOREIGN KEY (product_name) REFERENCES otto.product_table (product_name)
    );
    """)
    connection.commit()
    cursor.close()
    connection.close()


def get_latest_s3_key(bucket_name, prefix):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_client = s3_hook.get_conn()
    
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        sorted_objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
        return sorted_objects[0]['Key']
    else:
        return None


# S3에서 데이터를 읽고 데이터프레임으로 변환하는 함수
def read_s3_to_dataframe(bucket_name, key):
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

# 랜덤으로 고유한 review_id를 생성하는 함수
def generate_unique_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))

# S3에서 제품 데이터를 읽고 Redshift에 삽입하는 함수
def upload_product_data(**kwargs):
    bucket_name = 'otto-glue'
    product_key_prefix = 'integrated-data/products/'
    fallback_product_key = 'integrated-data/products/combined_products_2024-07-29 08:38:46.040114.csv'

    try:
        # S3에서 최신 제품 데이터를 가져옴
        latest_product_key = get_latest_s3_key(bucket_name, product_key_prefix)
        if not latest_product_key:
            raise FileNotFoundError("No latest product key found, using fallback.")
        
        product_df = read_s3_to_dataframe(bucket_name, latest_product_key)
    except Exception as e:
        # 예외 발생 시 fallback key 사용
        print(f"Error fetching latest product key: {e}. Using fallback key.")
        product_df = read_s3_to_dataframe(bucket_name, fallback_product_key)

    product_df['price'] = product_df['price'].str.replace(',', '').astype(float)  # 쉼표 제거 및 float 변환

    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()

    for index, row in product_df.iterrows():
        cursor.execute("SELECT 1 FROM otto.product_table WHERE product_name = %s", (row['product_name'],))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute("""
                INSERT INTO otto.product_table (product_id, rank, product_name, category, price, image_url, description, color, size, platform)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))
    
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Inserted {len(product_df)} rows into otto.product_table")


# S3에서 리뷰 데이터를 읽는 태스크
def read_review_data(**kwargs):
    bucket_name = 'otto-glue'
    review_key_prefix = 'integrated-data/reviews/'
    fallback_review_key = 'integrated-data/reviews/combined_reviews_2024-07-29 08:38:46.040114.csv'

    try:
        # S3에서 최신 리뷰 데이터를 가져옴
        latest_review_key = get_latest_s3_key(bucket_name, review_key_prefix)
        if not latest_review_key:
            raise FileNotFoundError("No latest review key found, using fallback.")
        
        review_df = read_s3_to_dataframe(bucket_name, latest_review_key)
    except Exception as e:
        # 예외 발생 시 fallback key 사용
        print(f"Error fetching latest review key: {e}. Using fallback key.")
        review_df = read_s3_to_dataframe(bucket_name, fallback_review_key)

    kwargs['ti'].xcom_push(key='review_df', value=review_df.to_json())


# Redshift에서 product_name 목록을 가져오는 태스크
def get_existing_product_names(**kwargs):
    existing_product_names_df = fetch_product_names()
    kwargs['ti'].xcom_push(key='existing_product_names_df', value=existing_product_names_df.to_json())

# 리뷰 데이터를 필터링하고 Redshift에 삽입하는 함수
def process_and_upload_review_data(**kwargs):
    ti = kwargs['ti']
    review_df = pd.read_json(ti.xcom_pull(key='review_df', task_ids='read_review_data'))
    existing_product_names_df = pd.read_json(ti.xcom_pull(key='existing_product_names_df', task_ids='get_existing_product_names'))

    # product_name이 존재하는 리뷰만 필터링
    new_reviews_df = review_df[review_df['product_name'].isin(existing_product_names_df['product_name'])]

    if not new_reviews_df.empty:
        redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()

        for index, row in new_reviews_df.iterrows():
            cursor.execute("SELECT 1 FROM otto.reviews WHERE review_id = %s", (generate_unique_id(),))
            exists = cursor.fetchone()
            if not exists:
                cursor.execute("""
                    INSERT INTO otto.reviews (review_id, product_name, color, size, height, gender, weight, top_size, bottom_size, size_comment, quality_comment, color_comment, thickness_comment, brightness_comment, comment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (generate_unique_id(), row['product_name'], row['color'], row['size'], row['height'], row['gender'], row['weight'], row['top_size'], row['bottom_size'], row['size_comment'], row['quality_comment'], row['color_comment'], row['thickness_comment'], row['brightness_comment'], row['comment']))
        
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Inserted {len(new_reviews_df)} new rows into otto.reviews")

# 각 열의 최대 길이를 식별하는 함수
def identify_max_lengths(**kwargs):
    bucket_name = 'otto-glue'
    review_key = 'integrated-data/reviews/combined_reviews_2024-07-29 08:38:46.040114.csv'
    review_df = read_s3_to_dataframe(bucket_name, review_key)
    max_lengths = review_df.applymap(lambda x: len(str(x)) if pd.notnull(x) else 0).max()
    print("Max lengths of each column:")
    print(max_lengths)

# 태스크 정의
create_schema_task = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    dag=dag,
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

upload_product_data_task = PythonOperator(
    task_id='upload_product_data',
    python_callable=upload_product_data,
    provide_context=True,
    dag=dag,
)

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

identify_max_lengths_task = PythonOperator(
    task_id='identify_max_lengths',
    python_callable=identify_max_lengths,
    provide_context=True,
    dag=dag,
)

# 태스크 순서 정의 테스트
create_schema_task >> create_tables_task >> upload_product_data_task >> read_review_data_task >> get_existing_product_names_task >> identify_max_lengths_task >> process_and_upload_review_data_task
