import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_data_from_redshift(table):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    query = f"SELECT * FROM otto.{table}"

    connection = redshift_hook.get_conn()
    df = pd.read_sql(query, connection)

    connection.close()

    return df


def create_gender_df(product_df, reviews_df):
    # product_df와 reviews_df를 product_name을 기준으로 병합
    merged_df = pd.merge(product_df, reviews_df, on="product_name", how="left")

    # 'platform'이 'zigzag'인 경우 gender를 'female'로 설정
    merged_df.loc[merged_df["platform"] == "zigzag", "gender"] = "female"

    # 'platform'이 null인 경우 해당 행을 삭제
    merged_df = merged_df[merged_df["platform"].notna()]

    # 필요한 컬럼만 선택
    result_df = merged_df[["product_name", "gender"]].drop_duplicates()

    return result_df


def upload_ml_table_to_redshift(df):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()

    # Create ML table
    cursor.execute(
        """
        DROP TABLE IF EXISTS otto.gender CASCADE;
        CREATE TABLE IF NOT EXISTS otto.gender (
            product_name TEXT,
            gender TEXT
        );
        """
    )

    # Insert data into ML table
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
