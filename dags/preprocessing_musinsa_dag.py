# musinsa_preprocessing_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from preprocessing_musinsa_tasks import (
    fetch_data_from_redshift,
    process_data,
    save_data_to_redshift,
)

default_args = {
    "owner": "ys",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "musinsa_data_processing",
    default_args=default_args,
    description="Redshift 데이터 처리 후 결과를 다시 Redshift에 저장하는 DAG",
    schedule_interval=None,
)

fetch_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data_from_redshift,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

save_task = PythonOperator(
    task_id="save_data",
    python_callable=save_data_to_redshift,
    provide_context=True,
    dag=dag,
)

fetch_task >> process_task >> save_task
