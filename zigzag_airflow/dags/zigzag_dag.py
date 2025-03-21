from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from io import StringIO

from zigzag_crawling import get_product_id
from zigzag_crawling import product_crawling
from zigzag_crawling import review_crawling

import logging
import pandas as pd


# 웹 드라이버를 생성하는 함수
def get_driver():
    logging.info("creating driver.")
    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--headless")  # GUI를 표시하지 않음
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    remote_webdriver = "http://remote_chromedriver:4444/wd/hub"
    driver = webdriver.Remote(command_executor=remote_webdriver, options=options)
    logging.info("driver created.")
    return driver


# S3에서 CSV 파일을 읽어오는 함수
def get_csv_from_s3(bucket_name, key):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_s3")
        s3_client = s3_hook.get_conn()
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8-sig")))
        logging.info(f"successfully get csv file from {key}")
        logging.info(f"loaded csv length ::: {len(df)}")
        return df
    except Exception as e:
        logging.info(e)
        if "products" in key:
            columns = [
                "product_id",
                "category",
                "description",
                "product_name",
                "price",
                "image_url",
                "size",
                "color",
                "rank",
            ]
        else:
            columns = [
                "review_id",
                "product_id",
                "color",
                "size",
                "size_comment",
                "quality_comment",
                "color_comment",
                "height",
                "weight",
                "comment",
                "top_size",
                "bottom_size",
                "product_name",
            ]
        return pd.DataFrame(columns=columns)


# 데이터프레임을 S3에 저장하는 함수
def save_df_to_s3(df, bucket_name, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    s3_hook = S3Hook(aws_conn_id="aws_s3")
    s3_hook.load_string(csv_buffer.getvalue(), key, bucket_name, replace=True)


# 데이터프레임에 순위를 설정하는 함수
def set_rank(df, sorted_product_list):
    df["rank"] = "none"
    rank_dict = {
        product_id: rank + 1 for rank, product_id in enumerate(sorted_product_list)
    }
    df["rank"] = df["product_id"].map(rank_dict).fillna("none")
    return df


# 크롤링 데이터를 업데이트하는 함수
def update_crawling_data(bucket_name, product_max_num=100, review_max_num=20):
    # 크롤링에 필요한 URL 및 카테고리 ID 설정
    products_url = "https://zigzag.kr/categories/-1?title=%EC%9D%98%EB%A5%98&category_id=-1&middle_category_id={id}&sort=201"
    category_ids = {"top": "474", "bottom": "547"}

    # S3에서 기존 데이터 가져오기
    product_df = get_csv_from_s3(
        bucket_name, "/non-integrated-data/zigzag_products.csv"
    )
    review_df = get_csv_from_s3(bucket_name, "/non-integrated-data/zigzag_reviews.csv")
    product_set = set(product_df["product_id"])
    logging.info(f"origin link's length ==> {len(product_set)}")

    driver = get_driver()

    for category, id in category_ids.items():
        logging.info(f"start {category} product list crawling")
        url = products_url.format(id=id)
        product_list = get_product_id(driver, url, max_num=product_max_num)
        logging.info(f"done. new {len(product_list)} links crawled.")

        logging.info(f"start {category} product information crawling")
        product_info = product_crawling(
            driver, category, product_list, product_set=product_set
        )
        product_info_df = pd.DataFrame(product_info).T
        product_df = pd.concat([product_df, product_info_df], ignore_index=True)
        product_df = set_rank(product_df, product_list)
        logging.info("done.")
        logging.info(f"length:: {len(product_df)}")

        logging.info(f"start {category} review crawling")
        review_list = review_crawling(
            driver,
            product_list,
            review_max_num,
            category=category,
            product_set=product_set,
        )
        review_list_df = pd.DataFrame(review_list).T
        review_df = pd.concat([review_df, review_list_df], ignore_index=True)
        logging.info("done.")
        logging.info(f"length:: {len(review_df)}")

    save_df_to_s3(product_df, bucket_name, "/non-integrated-data/zigzag_products.csv")
    save_df_to_s3(review_df, bucket_name, "/non-integrated-data/zigzag_reviews.csv")

    driver.quit()


# Airflow DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "update_crawling_data",
    default_args=default_args,
    description="A DAG to update new product and review data",
    schedule_interval="@daily",
    start_date=days_ago(1),
    tags=["update_dag"],
)

# PythonOperator를 이용하여 update_crawling_data 함수 호출
update_task = PythonOperator(
    task_id="update_crawling_data",
    python_callable=update_crawling_data,
    op_kwargs={"bucket_name": "otto-glue"},
    dag=dag,
)

update_task
