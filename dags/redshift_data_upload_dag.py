from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import redshift_tasks
import rds_tasks
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'otto_redshift_data_upload_real_22_1',
    default_args=default_args,
    description='Upload product and review data to Redshift with deduplication and move data to RDS',
    schedule_interval=None,
)

with dag:
    with TaskGroup('redshift_part', tooltip='redshift_data_upload_part') as redshift_part:
        create_schema_task = PythonOperator(
            task_id='create_schema',
            python_callable=redshift_tasks.create_schema,
        )

        create_tables_task = PythonOperator(
            task_id='create_tables',
            python_callable=redshift_tasks.create_tables,
        )

        upload_product_data_task = PythonOperator(
            task_id='upload_product_data',
            python_callable=redshift_tasks.upload_product_data,
            provide_context=True,
        )

        read_review_data_task = PythonOperator(
            task_id='read_review_data',
            python_callable=redshift_tasks.read_review_data,
            provide_context=True,
        )

        get_existing_product_names_task = PythonOperator(
            task_id='get_existing_product_names',
            python_callable=redshift_tasks.get_existing_product_names,
            provide_context=True,
        )

        process_and_upload_review_data_task = PythonOperator(
            task_id='process_and_upload_review_data',
            python_callable=redshift_tasks.process_and_upload_review_data,
            provide_context=True,
        )

        identify_max_lengths_task = PythonOperator(
            task_id='identify_max_lengths',
            python_callable=redshift_tasks.identify_max_lengths,
            provide_context=True,
        )

        create_schema_task >> create_tables_task >> upload_product_data_task >> read_review_data_task >> get_existing_product_names_task >> identify_max_lengths_task >> process_and_upload_review_data_task

    with TaskGroup('rds_part', tooltip='rds_part') as rds_part:
        create_rds_tables_task = PythonOperator(
            task_id='create_rds_tables',
            python_callable=rds_tasks.create_rds_tables,
        )

        transfer_data_to_rds_task = PythonOperator(
            task_id='transfer_data_to_rds',
            python_callable=rds_tasks.transfer_data_to_rds,
            provide_context=True,
        )

        create_rds_tables_task >> transfer_data_to_rds_task

    redshift_part >> rds_part
