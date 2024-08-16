import pandas as pd
import ast
import json
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_data_from_redshift():
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    products_query = """ SELECT * FROM otto."29cm_product" UNION ALL SELECT * FROM otto.musinsa_product UNION ALL SELECT * FROM otto.zigzag_product """
    reviews_query = """ SELECT * FROM otto."29cm_reviews" UNION ALL SELECT * FROM otto.musinsa_reviews UNION ALL SELECT * FROM otto.zigzag_reviews """

    connection = redshift_hook.get_conn()
    products_df = pd.read_sql(products_query, connection)
    reviews_df = pd.read_sql(reviews_query, connection)

    connection.close()
    return products_df, reviews_df


def process_data(products_df, reviews_df):
    # Filtering rows where required columns are not 'none'
    filtered_reviews_df = reviews_df[
        (reviews_df["product_name"] != "none")
        & (reviews_df["gender"] != "none")
        & (reviews_df["size"] != "none")
        & (reviews_df["height"] != "none")
        & (reviews_df["weight"] != "none")
        & (reviews_df["size_comment"] != "none")
    ]

    def recommend_size(row, size_list):
        try:
            index = size_list.index(row["size"])
            if row["size_comment"] == "-1":
                return (
                    size_list[index + 1]
                    if index + 1 < len(size_list)
                    else size_list[index]
                )
            elif row["size_comment"] == "1":
                return size_list[index - 1] if index > 0 else size_list[index]
            else:
                return size_list[index]
        except ValueError:
            return row["size"]

    # Generating size recommendations
    size_recommendations = []
    for _, review in filtered_reviews_df.iterrows():
        product_sizes = products_df.loc[
            products_df["product_name"] == review["product_name"], "size"
        ]

        if product_sizes.empty:
            size_recommendations.append(review["size"])
        else:
            size_list = product_sizes.values[0]
            if isinstance(size_list, list):
                # 이미 리스트인 경우 그대로 사용
                pass
            elif isinstance(size_list, str):
                try:
                    # 문자열로 저장된 리스트를 파싱
                    size_list = ast.literal_eval(size_list)
                    if isinstance(size_list, str):
                        size_list = json.loads(size_list)
                except (ValueError, SyntaxError, json.JSONDecodeError):
                    logging.info(f"Failed to parse size_list: {size_list}")
                    size_list = [review["size"]]

            recommended_size = recommend_size(review, size_list)
            size_recommendations.append(recommended_size)

    filtered_reviews_df["size_recommend"] = size_recommendations

    # Selecting required columns
    ml_df = filtered_reviews_df[
        [
            "product_name",
            "gender",
            "size",
            "height",
            "weight",
            "size_comment",
            "size_recommend",
        ]
    ]

    return ml_df


def upload_ml_table_to_redshift(ml_df):
    redshift_hook = PostgresHook(postgres_conn_id="otto_redshift")
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()

    # Create ML table
    cursor.execute(
        """
    DROP TABLE IF EXISTS otto.ml_table CASCADE;
    CREATE TABLE IF NOT EXISTS otto.ml_table (
        product_name TEXT,
        gender TEXT,
        size TEXT,
        height TEXT,
        weight TEXT,
        size_comment TEXT,
        size_recommend TEXT
    );
    """
    )

    # Insert data into ML table
    for _, row in ml_df.iterrows():
        cursor.execute(
            """
            INSERT INTO otto.ml_table (product_name, gender, size, height, weight, size_comment, size_recommend)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
            (
                row["product_name"],
                row["gender"],
                row["size"],
                row["height"],
                row["weight"],
                row["size_comment"],
                row["size_recommend"],
            ),
        )

    connection.commit()
    cursor.close()
    connection.close()
