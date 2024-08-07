from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine, text
import pandas as pd
import re
import random
import json

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "otto_redshift_data_processing",
    default_args=default_args,
    description="Redshift 데이터 처리 후 결과를 다시 Redshift에 저장하는 DAG",
    schedule_interval=None,
)


def fetch_data_from_redshift(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    conn = redshift_hook.get_conn()
    sql_product = """
        SELECT product_name, size, category, platform, brand
        FROM otto.product_table
        WHERE platform = '29cm';
    """
    sql_reviews = """
        SELECT product_name, size, height, weight, gender, size_comment
        FROM otto.reviews
        WHERE product_name IN (
            SELECT product_name
            FROM otto.product_table
            WHERE platform = '29cm'
        );
    """

    product_df = pd.read_sql(sql_product, conn)
    reviews_df = pd.read_sql(sql_reviews, conn)

    # Push DataFrames to XCom
    ti = kwargs["ti"]
    ti.xcom_push(key="product_df", value=product_df.to_json())
    ti.xcom_push(key="reviews_df", value=reviews_df.to_json())


def process_data(**kwargs):
    ti = kwargs["ti"]
    product_df_json = ti.xcom_pull(key="product_df")
    reviews_df_json = ti.xcom_pull(key="reviews_df")

    product_df = pd.read_json(product_df_json)
    reviews_df = pd.read_json(reviews_df_json)

    def clean_size_column(size_str):
        size_list_upper = ["XXS", "XS", "S", "M", "L", "XL", "XXL"]
        if isinstance(size_str, str):
            if re.match(r"[가-힣\s]+$", size_str) or "상세" in size_str:
                return "none"
            if (
                size_str.lower() in ["free", "f", "one size"]
                or "chest" in size_str.lower()
            ):
                return ["F"]
            if "~" in size_str:
                pattern = re.compile(r"\b(?:" + "|".join(size_list_upper) + r")\b")
                found_sizes = pattern.findall(size_str.upper())
                if found_sizes:
                    start_size = found_sizes[0]
                    end_size = found_sizes[-1]
                    if start_size in size_list_upper and end_size in size_list_upper:
                        start_index = size_list_upper.index(start_size)
                        end_index = size_list_upper.index(end_size)
                        return size_list_upper[start_index : end_index + 1]
            if "," in size_str:
                size_str = size_str.split(",")
            elif "/" in size_str:
                size_str = size_str.split("/")
            else:
                size_str = ["F"]
        if isinstance(size_str, list):
            cleaned_sizes = []
            for s in size_str:
                s = re.sub(r"\s*\(.*?\)\s*", "", s).strip()
                s = re.split(r"\s+", s, maxsplit=1)[0].strip()
                match = re.search(r"(S|M|L|F)", s)
                if match:
                    s = s[: match.end()].strip()
                cleaned_sizes.append(s)
            size_str = list(dict.fromkeys(cleaned_sizes))
        return size_str

    def select_last_smlf(size_str):
        size_patterns = [
            "3XS",
            "2XS",
            "XXS",
            "XS",
            "S",
            "M",
            "L",
            "XL",
            "XXL",
            "2XL",
            "3XL",
            "F",
        ]
        pattern = re.compile("|".join(size_patterns), re.IGNORECASE)
        size_strip = re.sub(r"\s*\(.*?\)\s*", "", size_str).strip()
        matches = pattern.findall(size_strip)
        if matches:
            size_str = matches[-1].upper()
        else:
            size_str = size_strip
        return size_str

    def convert_size_string_to_list(size_str):
        if isinstance(size_str, str):
            try:
                size_list = eval(size_str)
                if isinstance(size_list, list):
                    return size_list
            except:
                pass
        return []

    size_ranges = {
        "XXXS": {
            "남성": {"height": (140, 150), "weight": (35, 45)},
            "여성": {"height": (130, 140), "weight": (30, 40)},
        },
        "3XS": {
            "남성": {"height": (140, 150), "weight": (35, 45)},
            "여성": {"height": (130, 140), "weight": (30, 40)},
        },
        "XXS": {
            "남성": {"height": (150, 160), "weight": (45, 55)},
            "여성": {"height": (140, 150), "weight": (40, 50)},
        },
        "2XS": {
            "남성": {"height": (150, 160), "weight": (45, 55)},
            "여성": {"height": (140, 150), "weight": (40, 50)},
        },
        "XS": {
            "남성": {"height": (160, 165), "weight": (50, 60)},
            "여성": {"height": (150, 155), "weight": (45, 55)},
        },
        "0": {
            "남성": {"height": (160, 165), "weight": (50, 60)},
            "여성": {"height": (150, 155), "weight": (45, 55)},
        },
        "S": {
            "남성": {"height": (165, 170), "weight": (55, 65)},
            "여성": {"height": (155, 160), "weight": (50, 60)},
        },
        "0.5": {
            "남성": {"height": (165, 170), "weight": (55, 65)},
            "여성": {"height": (155, 160), "weight": (50, 60)},
        },
        "M": {
            "남성": {"height": (170, 175), "weight": (60, 70)},
            "여성": {"height": (160, 165), "weight": (55, 65)},
        },
        "1": {
            "남성": {"height": (170, 175), "weight": (60, 70)},
            "여성": {"height": (160, 165), "weight": (55, 65)},
        },
        "L": {
            "남성": {"height": (175, 180), "weight": (70, 80)},
            "여성": {"height": (165, 170), "weight": (60, 70)},
        },
        "1.5": {
            "남성": {"height": (175, 180), "weight": (70, 80)},
            "여성": {"height": (165, 170), "weight": (60, 70)},
        },
        "XL": {
            "남성": {"height": (180, 185), "weight": (80, 90)},
            "여성": {"height": (170, 175), "weight": (70, 80)},
        },
        "2": {
            "남성": {"height": (180, 185), "weight": (80, 90)},
            "여성": {"height": (170, 175), "weight": (70, 80)},
        },
        "XXL": {
            "남성": {"height": (185, 190), "weight": (90, 100)},
            "여성": {"height": (175, 180), "weight": (80, 90)},
        },
        "2XL": {
            "남성": {"height": (185, 190), "weight": (90, 100)},
            "여성": {"height": (175, 180), "weight": (80, 90)},
        },
        "XXXL": {
            "남성": {"height": (190, 200), "weight": (100, 110)},
            "여성": {"height": (180, 190), "weight": (90, 100)},
        },
        "3XL": {
            "남성": {"height": (190, 200), "weight": (100, 110)},
            "여성": {"height": (180, 190), "weight": (90, 100)},
        },
        "F": {
            "남성": {"height": (165, 185), "weight": (55, 85)},
            "여성": {"height": (155, 175), "weight": (50, 75)},
        },
    }

    def generate_random_value(size, gender, attribute):
        if size in size_ranges and gender in size_ranges[size]:
            min_val, max_val = size_ranges[size][gender][attribute]
        else:
            min_val, max_val = size_ranges["F"][gender][attribute]
        return round(random.uniform(min_val, max_val), 2)

    product_df["size"] = product_df["size"].apply(clean_size_column)
    reviews_df["size"] = reviews_df["size"].apply(select_last_smlf)
    reviews_df["height"] = reviews_df.apply(
        lambda row: generate_random_value(row["size"], row["gender"], "height"), axis=1
    )
    reviews_df["weight"] = reviews_df.apply(
        lambda row: generate_random_value(row["size"], row["gender"], "weight"), axis=1
    )

    processed_product_df = product_df[
        ["product_name", "size", "category", "platform", "brand"]
    ]
    processed_reviews_df = reviews_df[
        ["product_name", "size", "height", "weight", "gender", "size_comment"]
    ]

    ti = kwargs["ti"]
    ti.xcom_push(key="processed_product_df", value=processed_product_df.to_json())
    ti.xcom_push(key="processed_reviews_df", value=processed_reviews_df.to_json())


def save_data_to_redshift(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    conn = redshift_hook.get_conn()
    engine = create_engine("postgresql+psycopg2://username:password@host:port/dbname")

    ti = kwargs["ti"]
    processed_product_df_json = ti.xcom_pull(key="processed_product_df")
    processed_reviews_df_json = ti.xcom_pull(key="processed_reviews_df")

    processed_product_df = pd.read_json(processed_product_df_json)
    processed_reviews_df = pd.read_json(processed_reviews_df_json)

    # Ensure tables exist and use schema and table names as specified
    with engine.connect() as connection:
        connection.execute(
            text(
                """
            DROP TABLE IF EXISTS otto."29cm_product" CASCADE;
            CREATE TABLE IF NOT EXISTS otto."29cm_product" (
                product_name TEXT,
                size TEXT,
                category TEXT,
                platform TEXT,
                brand TEXT
            );
        """
            )
        )

        connection.execute(
            text(
                """
            DROP TABLE IF EXISTS otto."29cm_reviews" CASCADE;
            CREATE TABLE IF NOT EXISTS otto."29cm_reviews" (
                product_name TEXT,
                size TEXT,
                height NUMERIC,
                weight NUMERIC,
                gender TEXT,
                size_comment TEXT
            );
        """
            )
        )

        # Insert data into tables
        processed_product_df.to_sql(
            "29cm_product", con=engine, schema="otto", if_exists="append", index=False
        )
        processed_reviews_df.to_sql(
            "29cm_reviews", con=engine, schema="otto", if_exists="append", index=False
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
