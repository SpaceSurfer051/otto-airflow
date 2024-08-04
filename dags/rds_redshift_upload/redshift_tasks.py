from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import io
import random
import string

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
        product_id varchar(max),
        rank FLOAT,
        product_name varchar(max) PRIMARY KEY,
        category varchar(max),
        price FLOAT,
        image_url varchar(max),
        description varchar(max),
        color varchar(max),
        size varchar(max),
        platform varchar(max),
        brand varchar(max),
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
    prefix_product = 'integrated-data/products/brand/'
    s3_hook = S3Hook(aws_conn_id='aws_default')
    files_product = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix_product)
    product_key = files_product[-1]
    print(product_key)

    # S3에서 제품 데이터를 읽음
    product_df = read_s3_to_dataframe(bucket_name, product_key)
    # rank 열에서 'none' 값을 0으로 변환하고 타입을 float로 변환
    product_df['rank'] = product_df['rank'].replace('none', 0).astype(float)
    # 가격 데이터 전처리
    try:
        # 쉼표 제거 및 숫자로 변환
        product_df['price'] = product_df['price'].astype(str).str.replace(',', '').astype(float)
    except Exception as e:
        print(f"Error processing price column: {e}")
        product_df['price'] = 9000.0  # 오류 발생 시 기본값으로 9000.0 설정

    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()

    for index, row in product_df.iterrows():
        cursor.execute("SELECT 1 FROM otto.product_table WHERE product_name = %s", (row['product_name'],))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute("""
                INSERT INTO otto.product_table (product_id, rank, product_name, category, price, image_url, description, color, size, platform,brand)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
                """, tuple(row))
    
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Inserted {len(product_df)} rows into otto.product_table")


# S3에서 리뷰 데이터를 읽는 태스크
def read_review_data(**kwargs):
    bucket_name = 'otto-glue'

    prefix_reviews = 'integrated-data/reviews/'
    s3_hook = S3Hook(aws_conn_id='aws_default')       
    files_reviews = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix_reviews) 
    review_key = files_reviews[-1]       
    print(review_key)

    
    
    
    #review_key = 'integrated-data/reviews/combined_reviews_2024-07-29 08:38:46.040114.csv'
    review_df = read_s3_to_dataframe(bucket_name, review_key)
    kwargs['ti'].xcom_push(key='review_df', value=review_df.to_json())

# Redshift에서 product_name 목록을 가져오는 태스크
def get_existing_product_names(**kwargs):
    existing_product_names_df = fetch_product_names()
    kwargs['ti'].xcom_push(key='existing_product_names_df', value=existing_product_names_df.to_json())

# 리뷰 데이터를 필터링하고 Redshift에 삽입하는 함수
def process_and_upload_review_data(**kwargs):
    ti = kwargs['ti']
    review_df_json = ti.xcom_pull(key='review_df', task_ids='redshift_part.read_review_data')
    
    if review_df_json is None:
        raise ValueError("No review data found in XCom.")

    review_df = pd.read_json(review_df_json)
    existing_product_names_df = pd.read_json(ti.xcom_pull(key='existing_product_names_df', task_ids='redshift_part.get_existing_product_names'))

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
    else:
        print("No new reviews to insert.")


