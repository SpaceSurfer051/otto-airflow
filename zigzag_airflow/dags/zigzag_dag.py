from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains

from zigzag_crawling import get_product_id, product_crawling, review_crawling

import pandas as pd
import os
import logging

from io import StringIO
  

def get_driver():
    logging.info('creating driver.')
    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument('--headless')  # GUI를 표시하지 않음
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    remote_webdriver = 'http://remote_chromedriver:4444/wd/hub'
    driver = webdriver.Remote(command_executor=remote_webdriver, options=options)
    logging.info('driver created.')
    return driver


def get_csv_from_s3(bucket_name, key):
    try:
        s3_hook = S3Hook(aws_conn_id='s3_aws')

        s3_client = s3_hook.get_conn()
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8-sig')))
        logging.info(f'successfully get csv file from {key}')
        logging.info(f'loaded csv length ::: {len(df)}')

        return df
    except Exception as e:
        logging.info(e)
        return pd.DataFrame()


def save_df_to_s3(df, bucket_name, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    s3_hook = S3Hook(aws_conn_id='s3_aws')
    s3_hook.load_string(csv_buffer.getvalue(), key, bucket_name, replace=True)


def update_crawling_data(bucket_name, product_max_num=10, review_max_num=20):
    products_url = 'https://zigzag.kr/categories/-1?title=%EC%9D%98%EB%A5%98&category_id=-1&middle_category_id={id}&sort=201'
    category_ids = {
        'top' : '474',
        'bottom' : '547'
    }
    product_df = get_csv_from_s3(bucket_name, 'zigzag/zigzag_product_infos.csv')
    review_df = get_csv_from_s3(bucket_name, 'zigzag/zigzag_reviews.csv')
    link_set = set(product_df['product_url'])
    logging.info(f'origin link\'s length ==> {len(link_set)}')
    
    driver = get_driver()
    
    for category, id in category_ids.items():
        logging.info(f'start {category} product list crawling')
        url = products_url.format(id=id)
        product_list = get_product_id(driver, url, max_num=product_max_num, link_set=link_set)
        logging.info(f'done. new {len(product_list)} links crawled.')

        logging.info(f'start {category} product information crawling')
        product_info = product_crawling(driver, category, product_list)
        product_info_df = pd.DataFrame(product_info).T
        product_df = pd.concat([product_df, product_info_df], ignore_index=True)
        logging.info('done.')
        logging.info(f'length:: {len(product_df)}')

        logging.info(f'start {category} review crawling')
        review_list = review_crawling(driver, product_list, review_max_num, category=category)
        review_list_df = pd.DataFrame(review_list).T
        review_df = pd.concat([review_df, review_list_df], ignore_index=True)
        logging.info('done.')
        logging.info(f'length:: {len(review_df)}')

    save_df_to_s3(product_df, bucket_name, 'zigzag/zigzag_product_infos_updated.csv')
    save_df_to_s3(review_df, bucket_name, 'zigzag/zigzag_reviews_updated.csv')

    driver.quit()
    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'update_crawling_data',
    default_args=default_args,
    description='A DAG to update new product and review data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['update_dag'],
)

download_task = PythonOperator(
    task_id='update_crawling_data',
    python_callable=update_crawling_data,
    op_kwargs={
        'bucket_name': 'otto-default'
    },
    dag=dag,
)

download_task
