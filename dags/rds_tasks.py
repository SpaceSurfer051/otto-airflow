from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine

def transfer_data_to_rds(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id='otto_redshift')
    rds_hook = PostgresHook(postgres_conn_id='otto_rds')

    redshift_conn = redshift_hook.get_conn()
    rds_conn = rds_hook.get_conn()

    product_query = "SELECT * FROM otto.product_table"
    review_query = "SELECT * FROM otto.reviews"

    product_df = pd.read_sql(product_query, redshift_conn)
    review_df = pd.read_sql(review_query, redshift_conn)

    product_df.to_sql('product_table', rds_conn, schema='otto', if_exists='replace', index=False)
    review_df.to_sql('reviews', rds_conn, schema='otto', if_exists='replace', index=False)
