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


def upload_gender_table_to_rds():
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    rds_hook = PostgresHook(postgres_conn_id="otto_rds")

    # Redshift로부터 gender_table 데이터를 가져오기
    redshift_connection = redshift_hook.get_conn()
    redshift_cursor = redshift_connection.cursor()

    redshift_cursor.execute("SELECT product_name, gender FROM otto.gender_table")
    gender_data = redshift_cursor.fetchall()

    # RDS에 연결
    rds_connection = rds_hook.get_conn()
    rds_cursor = rds_connection.cursor()

    # RDS에 gender_table 생성 쿼리 실행
    rds_cursor.execute(
        """
        DROP TABLE IF EXISTS otto.gender_table CASCADE;
        CREATE TABLE IF NOT EXISTS otto.gender_table (
            product_name TEXT,
            gender TEXT
        );
        """
    )

    # Redshift로부터 가져온 데이터를 RDS에 적재
    insert_query = (
        "INSERT INTO otto.gender_table (product_name, gender) VALUES (%s, %s)"
    )
    for row in gender_data:
        rds_cursor.execute(insert_query, row)

    # 변경사항 커밋
    rds_connection.commit()

    # 커서와 연결 닫기
    redshift_cursor.close()
    redshift_connection.close()
    rds_cursor.close()
    rds_connection.close()
