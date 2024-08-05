import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_data_from_redshift():
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    products_query = "SELECT * FROM otto.product_table"
    reviews_query = "SELECT * FROM otto.reviews"

    connection = redshift_hook.get_conn()
    products_df = pd.read_sql(products_query, connection)
    reviews_df = pd.read_sql(reviews_query, connection)

    connection.close()

    return products_df, reviews_df


def create_gender_df(product_df, reviews_df):
    merged_df = pd.merge(product_df, reviews_df, on="product_name", how="left")
    merged_df.loc[merged_df["platform"] == "zigzag", "gender"] = "female"
    merged_df = merged_df[merged_df["platform"].notna()]
    result_df = merged_df[["product_name", "gender"]].drop_duplicates()

    return result_df


def upload_gender_table_to_redshift(df):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute(
        """
        DROP TABLE IF EXISTS otto.gender CASCADE;
        DROP TABLE IF EXISTS otto.gender_table CASCADE;
        CREATE TABLE IF NOT EXISTS otto.gender_table (
            product_name TEXT,
            gender TEXT
        );
        """
    )

    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO otto.gender_table (product_name, gender)
            VALUES (%s, %s)
            """,
            (row["product_name"], row["gender"]),
        )

    connection.commit()
    cursor.close()
    connection.close()
