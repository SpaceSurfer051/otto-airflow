from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import io
import random
import string

def create_schema():
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS otto;")
    connection.commit()
    cursor.close()
    connection.close()

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
        comment TEXT,
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

def read_s3_to_dataframe(bucket_name, key):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_object = s3_hook.get_key(key, bucket_name)
    s3_data = s3_object.get()['Body'].read().decode('utf-8')
    data = pd.read_csv(io.StringIO(s3_data))
    return data

def fetch_product_names():
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    sql = "SELECT product_name FROM otto.product_table"
    connection = redshift_hook.get_conn()
    return pd.read_sql(sql, connection)

def generate_unique_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))

def upload_product_data(**kwargs):
    bucket_name = 'otto-glue'
    product_key_prefix = 'integrated-data/products/'
    fallback_product_key = 'integrated-data/products/combined_products_2024-07-29 08:38:46.040114.csv'

    try:
        latest_product_key = get_latest_s3_key(bucket_name, product_key_prefix)
        if not latest_product_key:
            raise FileNotFoundError("No latest product key found, using fallback.")
        
        product_df = read_s3_to_dataframe(bucket_name, latest_product_key)
    except Exception as e:
        print(f"Error fetching latest product key: {e}. Using fallback key.")
        product_df = read_s3_to_dataframe(bucket_name, fallback_product_key)

    product_df['price'] = product_df['price'].str.replace(',', '').astype(float)

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

def read_review_data(**kwargs):
    bucket_name = 'otto-glue'
    review_key_prefix = 'integrated-data/reviews/'
    fallback_review_key = 'integrated-data/reviews/combined_reviews_2024-07-29 08:38:46.040114.csv'

    try:
        latest_review_key = get_latest_s3_key(bucket_name, review_key_prefix)
        if not latest_review_key:
            raise FileNotFoundError("No latest review key found, using fallback.")
        
        review_df = read_s3_to_dataframe(bucket_name, latest_review_key)
    except Exception as e:
        print(f"Error fetching latest review key: {e}. Using fallback key.")
        review_df = read_s3_to_dataframe(bucket_name, fallback_review_key)

    kwargs['ti'].xcom_push(key='review_df', value=review_df.to_json())

def get_existing_product_names(**kwargs):
    existing_product_names_df = fetch_product_names()
    kwargs['ti'].xcom_push(key='existing_product_names_df', value=existing_product_names_df.to_json())

def process_and_upload_review_data(**kwargs):
    ti = kwargs['ti']
    review_df = pd.read_json(ti.xcom_pull(key='review_df', task_ids='read_review_data'))
    existing_product_names_df = pd.read_json(ti.xcom_pull(key='existing_product_names_df', task_ids='get_existing_product_names'))

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

def identify_max_lengths(**kwargs):
    bucket_name = 'otto-glue'
    review_key = 'integrated-data/reviews/combined_reviews_2024-07-29 08:38:46.040114.csv'
    review_df = read_s3_to_dataframe(bucket_name, review_key)
    max_lengths = review_df.applymap(lambda x: len(str(x)) if pd.notnull(x) else 0).max()
    print("Max lengths of each column:")
    print(max_lengths)
