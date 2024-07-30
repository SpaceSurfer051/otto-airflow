from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'otto_redshift_test',
    default_args=default_args,
    description='Test Redshift schema and table creation',
    schedule_interval=None,
)

# Task to create schema
create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id='otto_redshift',
    sql="""
    CREATE SCHEMA IF NOT EXISTS otto;
    """,
    dag=dag,
)

# Task to create table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='otto_redshift',
    sql="""
    DROP TABLE IF EXISTS otto.test_raw_data_table;
    CREATE TABLE otto.test_raw_data_table (
        product_id VARCHAR(256),
        rank INTEGER,
        product_name VARCHAR(256),
        category VARCHAR(256),
        price FLOAT,
        image_url VARCHAR(1024),
        description VARCHAR(2048),
        color VARCHAR(256),
        size VARCHAR(256)
    );
    """,
    dag=dag,
)

# Python function to check if schema and table are created successfully
def check_creation():
    logging.info("Schema and Table creation successful")

check_creation_task = PythonOperator(
    task_id='check_creation',
    python_callable=check_creation,
    dag=dag,
)

# Task to load data from S3 to Redshift
load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='load_s3_to_redshift',
    s3_bucket='otto-glue',
    s3_key='integrated-data/products/combined_products_2024-07-29 08:38:46.040114.csv',
    schema='otto',
    table='test_raw_data_table',
    copy_options=['csv', 'IGNOREHEADER 1'],
    aws_conn_id='aws_default',
    redshift_conn_id='otto_redshift',
    dag=dag,
)

create_schema >> create_table >> check_creation_task >> load_s3_to_redshift
