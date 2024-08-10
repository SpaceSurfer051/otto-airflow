from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import ast
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3
import psycopg2


default_args = {
    "owner": "SB    ",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "ml_serving",
    default_args=default_args,
    description="머신러닝 서빙 자동화",
    schedule_interval=None,
)
'''

def fetch_and_process_data(**kwargs):
    products_df, reviews_df = fetch_data_from_redshift()
    ml_df = process_data(products_df, reviews_df)
    kwargs["ti"].xcom_push(key="ml_df", value=ml_df.to_json())


def upload_ml_data(**kwargs):
    ti = kwargs["ti"]
    ml_df_json = ti.xcom_pull(key="ml_df")
    ml_df = pd.read_json(ml_df_json)
    upload_ml_table_to_redshift(ml_df)
    
    
def upload_ml_table_to_redshift(ml_df):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()

    # Create ML table
    cursor.execute(
        """
    
    );
    """
    )
    
    connection.commit()
    cursor.close()
    connection.close()
    
    
    
def fetch_data_from_redshift():
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    products_query = """ SELECT * FROM otto."29cm_product" """
    reviews_query = """ SELECT * FROM otto."29cm_reviews" """

    connection = redshift_hook.get_conn()
    products_df = pd.read_sql(products_query, connection)
    reviews_df = pd.read_sql(reviews_query, connection)

    connection.close()
    return products_df, reviews_df

'''









def unload_data_to_s3():
    # Airflow에 설정된 Redshift 연결 ID
    redshift_conn_id = 'otto_redshift'

    # S3 정보
    s3_bucket = 'otto-ml-1'
    s3_prefix = 'ml_table'
    iam_role = 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240726T150655'

    # UNLOAD SQL 쿼리
    unload_query = f"""
    UNLOAD ('SELECT * FROM otto.ml_table')
    TO 's3://{s3_bucket}/{s3_prefix}'
    IAM_ROLE '{iam_role}'
    DELIMITER ','
    ADDQUOTES
    ALLOWOVERWRITE
    PARALLEL OFF;
    """

    try:
        # PostgresHook을 사용해 Redshift에 연결
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        
        # UNLOAD 쿼리 실행
        redshift_hook.run(unload_query)

        print("Data unloaded to S3 successfully.")

    except Exception as e:
        print(f"Error unloading data to S3: {e}")
        
        
with dag:
    
    unload_data_task = PythonOperator(
        task_id="unload_data_to_s3",
        python_callable=unload_data_to_s3,
        provide_context=False,
    )

    unload_data_task