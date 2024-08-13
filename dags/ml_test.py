from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

def generate_unload_query(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    column_query = """
        SELECT STRING_AGG(column_name, ', ')
        FROM information_schema.columns
        WHERE table_name = 'otto.ml_table';
    """
    
    # 컬럼명 추출
    columns = redshift_hook.get_first(sql=column_query)[0]

    # 컬럼명 포함한 UNLOAD 쿼리 생성
    unload_query = f"""
    UNLOAD ('
        SELECT {columns}
        UNION ALL
        SELECT {columns} FROM otto.ml_table
    ')
    TO 's3://'otto-ml-1'/'ml_table.csv'/'
    ACCESS_KEY_ID 'your_access_key_id'
    IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240726T150655'
    ADDQUOTES
    ALLOWOVERWRITE
    PARALLEL OFF;
    """
    
    return unload_query

def execute_unload(**kwargs):
    unload_query = kwargs['ti'].xcom_pull(task_ids='generate_unload_query')
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    redshift_hook.run(unload_query)

# DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'dynamic_unload_to_s3',
    default_args=default_args,
    schedule_interval='@daily',
)

generate_query = PythonOperator(
    task_id='generate_unload_query',
    python_callable=generate_unload_query,
    provide_context=True,
    dag=dag,
)

execute_unload = PythonOperator(
    task_id='execute_unload',
    python_callable=execute_unload,
    provide_context=True,
    dag=dag,
)

generate_query >> execute_unload
