from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from rds_redshift_upload.rds_tasks import *
from rds_redshift_upload.redshift_tasks import *

from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    #test 24_8
    'otto_redshift_data_upload_real_24_7',
    default_args=default_args,
    description='Upload product and review data to Redshift with deduplication and move data to RDS',
    schedule_interval=None,
)

with dag:
    with TaskGroup('redshift_part', tooltip='redshift_data_upload_part') as redshift_part:
        create_schema_task = PythonOperator(
            task_id='create_schema',
            python_callable=create_schema,
        )

        create_tables_task = PythonOperator(
            task_id='create_tables',
            python_callable = create_tables,
        )

        upload_product_data_task = PythonOperator(
            task_id='upload_product_data',
            python_callable=upload_product_data,
            provide_context=True,
        )

        read_review_data_task = PythonOperator(
            task_id='read_review_data',
            python_callable=read_review_data,
            provide_context=True,
        )

        get_existing_product_names_task = PythonOperator(
            task_id='get_existing_product_names',
            python_callable=get_existing_product_names,
            provide_context=True,
        )

        process_and_upload_review_data_task = PythonOperator(
            task_id='process_and_upload_review_data',
            python_callable=process_and_upload_review_data,
            provide_context=True,
        )



        create_schema_task >> create_tables_task >> upload_product_data_task >> read_review_data_task >> get_existing_product_names_task  >> process_and_upload_review_data_task

    with TaskGroup('rds_part', tooltip='rds_part') as rds_part:
        create_rds_tables_task = PythonOperator(
            task_id='create_rds_tables',
            python_callable=create_rds_tables,
        )

        transfer_data_to_rds_task = PythonOperator(
            task_id='transfer_data_to_rds',
            python_callable=transfer_data_to_rds,
            provide_context=True,
        )

        create_rds_tables_task >> transfer_data_to_rds_task

    redshift_part >> rds_part
