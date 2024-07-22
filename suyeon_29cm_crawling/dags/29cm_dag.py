from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow_product_review import read_s3_and_compare_links

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'crawl_dag',
    default_args=default_args,
    description='DAG to crawl and compare links',
    schedule_interval=None,
)

crawl_task = PythonOperator(
    task_id='read_s3_and_compare_links',
    python_callable=read_s3_and_compare_links,
    dag=dag,
)

crawl_task
