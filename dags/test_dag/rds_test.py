from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'rds_connection_test_dag',
    default_args=default_args,
    description='Test connection to RDS',
    schedule_interval=None,
)

def test_rds_connection():
    try:
        # 'otto_rds' 연결을 사용하여 RDS에 연결
        rds_hook = PostgresHook(postgres_conn_id='otto_rds')
        rds_conn = rds_hook.get_conn()
        cursor = rds_conn.cursor()
        
        # 간단한 쿼리 실행 (예: 현재 시간 가져오기)
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        
        print(f"Connected to RDS. Current time: {result[0]}")
        
        cursor.close()
        rds_conn.close()
    except Exception as e:
        print(f"Failed to connect to RDS: {e}")
        raise

test_rds_connection_task = PythonOperator(
    task_id='test_rds_connection',
    python_callable=test_rds_connection,
    dag=dag,
)

test_rds_connection_task
