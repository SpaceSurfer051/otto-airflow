from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from create_gender_table_task import create_gender_df
from create_gender_table_task import fetch_data_from_redshift
from create_gender_table_task import upload_gender_table_to_rds
from create_gender_table_task import upload_gender_table_to_redshift

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "ELT_gender_table",
    default_args=default_args,
    description="Redshift에서 product, review를 불러온 뒤 gender 테이블 생성",
    schedule_interval=None,
)


def fetch_and_process_data(**kwargs):
    product_df, reviews_df = fetch_data_from_redshift()
    gender_df = create_gender_df(product_df, reviews_df)
    upload_gender_table_to_redshift(gender_df)


def upload_to_rds(**kwargs):
    upload_gender_table_to_redshift()


with dag:
    fetch_process_task = PythonOperator(
        task_id="fetch_and_process_data",
        python_callable=fetch_and_process_data,
        provide_context=True,
    )
    upload_rds = PythonOperator(
        task_id="upload_rds_from_redshift",
        python_callable=upload_to_rds,
        provide_context=True,
    )

    fetch_process_task >> upload_rds
