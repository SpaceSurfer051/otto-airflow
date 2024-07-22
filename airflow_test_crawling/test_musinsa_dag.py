from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from code_test.airflow_product_review import read_s3_and_compare_links
from code_test.airflow_size_color import read_s3_and_add_size_color
from code_test.airflow_data_preprocessing import *

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'crawling_dag_test',
    default_args=default_args,
    description='DAG to crawl and compare links, then add size and color information',
    schedule_interval=None,
)

test_preprocessing = PythonOperator(
    task_id='test_preprocessing',
    python_callable=test_print,
    dag=dag,
)




test_preprocessing
