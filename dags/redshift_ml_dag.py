from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from redshift_ml_tasks import (
    fetch_data_from_redshift,
    process_data,
    upload_ml_table_to_redshift,
)

default_args = {
    "owner": "suyeon",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "otto_redshift_to_ml_table",
    default_args=default_args,
    description="Redshift에서 전처리 완료된 데이터를 가져와 ML에 맞는 컬럼으로 변환한 후, ML 테이블 생성2",
    schedule_interval=None,
)


def fetch_and_process_data(**kwargs):
    products_df, reviews_df = fetch_data_from_redshift()
    ml_df = process_data(products_df, reviews_df)
    kwargs["ti"].xcom_push(key="ml_df", value=ml_df.to_json())


def upload_ml_data(**kwargs):
    ti = kwargs["ti"]
    ml_df_json = ti.xcom_pull(key="ml_df")
    ml_df = pd.read_json(ml_df_json)
    upload_ml_table_to_redshift(ml_df)


with dag:
    fetch_process_task = PythonOperator(
        task_id="fetch_and_process_data",
        python_callable=fetch_and_process_data,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_ml_data",
        python_callable=upload_ml_data,
        provide_context=True,
    )

    fetch_process_task >> upload_task
