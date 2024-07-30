from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'redshift_connection_test',
    default_args=default_args,
    description='Test connection to Redshift',
    schedule_interval=None,
)

# Redshift에 연결하여 쿼리를 실행하는 함수
def test_redshift_connection():
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    if result is None or result[0] != 1:
        raise ValueError("Failed to execute test query")
    print("Connection to Redshift established successfully.")

# 태스크 정의
test_connection_task = PythonOperator(
    task_id='test_connection',
    python_callable=test_redshift_connection,
    dag=dag,
)

test_connection_task
