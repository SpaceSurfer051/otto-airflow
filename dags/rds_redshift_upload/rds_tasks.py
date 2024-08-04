import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_rds_tables():
    rds_hook = PostgresHook(postgres_conn_id='otto_rds')
    connection = rds_hook.get_conn()
    cursor = connection.cursor()
    
    cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS otto;
    
    DROP TABLE IF EXISTS otto.reviews CASCADE;
    DROP TABLE IF EXISTS otto.product_table CASCADE;
    
    CREATE TABLE IF NOT EXISTS otto.product_table (
        product_id varchar(1024),
        rank FLOAT,
        product_name varchar(1024) PRIMARY KEY,
        category varchar(1024),
        price FLOAT,
        image_url varchar(1024),
        description varchar(1024),
        color varchar(1024),
        size varchar(1024),
        platform varchar(1024),
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

def transfer_data_to_rds():
    # Redshift에서 데이터 가져오기
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    connection = redshift_hook.get_conn()
    query = "SELECT * FROM otto.product_table"
    product_df = pd.read_sql(query, connection)
    
    query = "SELECT * FROM otto.reviews"
    reviews_df = pd.read_sql(query, connection)
    
    # RDS에 데이터 삽입
    rds_hook = PostgresHook(postgres_conn_id='otto_rds')
    connection = rds_hook.get_conn()
    cursor = connection.cursor()

    # Product data insert
    for _, row in product_df.iterrows():
        cursor.execute("""
            INSERT INTO otto.product_table (product_id, rank, product_name, category, price, image_url, description, color, size, platform)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_name) DO NOTHING
        """, tuple(row))
    
    # Reviews data insert
    for _, row in reviews_df.iterrows():
        cursor.execute("""
            INSERT INTO otto.reviews (review_id, product_name, color, size, height, gender, weight, top_size, bottom_size, size_comment, quality_comment, color_comment, thickness_comment, brightness_comment, comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO NOTHING
        """, tuple(row))

    connection.commit()
    cursor.close()
    connection.close()
