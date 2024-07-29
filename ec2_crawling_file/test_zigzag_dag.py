import logging
import time
from io import StringIO

import pandas as pd
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

# zigzag
from zigzag_crawling import update_crawling_data

# 기본 인자 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
}

# DAG 설정
dag = DAG(
    "zigzag_update_dag",
    default_args=default_args,
    description="DAG to crawl and compare links, then add size and color information",
    schedule_interval=timedelta(days=1),
)

with dag:
    with TaskGroup('zigzag_task_group', tooltip="Tasks for zigzag data update") as task_zigzag_group:
        update_zigzag_task = PythonOperator(
            task_id='update_zigzag_data_crawling',
            python_callable=update_crawling_data,
            op_kwargs={'bucket_name': 'otto-glue'},
        )

    task_zigzag_group
