from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import redshift_tasks
import rds_tasks

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'otto_redshift_data_upload_real_21_2',
    default_args=default_args,
    description='Upload product and review data to Redshift with deduplication and move data to RDS',
    schedule_interval=None,
)

create_schema_task = PythonOperator(
    task_id='create_schema',
    python_callable=redshift_tasks.create_schema,
    dag=dag,
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=redshift_tasks.create_tables,
    dag=dag,
)

upload_product_data_task = PythonOperator(
    task_id='upload_product_data',
    python_callable=redshift_tasks.upload_product_data,
    provide_context=True,
    dag=dag,
)

read_review_data_task = PythonOperator(
    task_id='read_review_data',
    python_callable=redshift_tasks.read_review_data,
    provide_context=True,
    dag=dag,
)

get_existing_product_names_task = PythonOperator(
    task_id='get_existing_product_names',
    python_callable=redshift_tasks.get_existing_product_names,
    provide_context=True,
    dag=dag,
)

process_and_upload_review_data_task = PythonOperator(
    task_id='process_and_upload_review_data',
    python_callable=redshift_tasks.process_and_upload_review_data,
    provide_context=True,
    dag=dag,
)

identify_max_lengths_task = PythonOperator(
    task_id='identify_max_lengths',
    python_callable=redshift_tasks.identify_max_lengths,
    provide_context=True,
    dag=dag,
)

create_rds_tables_task = PythonOperator(
    task_id='create_rds_tables',
    python_callable=rds_tasks.create_rds_tables,
    dag=dag,
)

transfer_data_to_rds_task = PythonOperator(
    task_id='transfer_data_to_rds',
    python_callable=rds_tasks.transfer_data_to_rds,
    provide_context=True,
    dag=dag,
)

create_schema_task >> create_tables_task >> upload_product_data_task >> read_review_data_task >> get_existing_product_names_task >> identify_max_lengths_task >> process_and_upload_review_data_task >> create_rds_tables_task >> transfer_data_to_rds_task
