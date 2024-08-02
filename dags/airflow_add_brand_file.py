# import
import boto3
import pandas as pd
import io
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
from airflow.hooks.S3_hook import S3Hook
from webdriver_manager.chrome import ChromeDriverManager
import logging

def process_data():
    
    
    
    
    ############## s3 정보 가져오는 파트 ############################

    # s3에서 최신 product 정보를 가져옴, old_product_df로 저장

    # s3에서 최신 product_add_brand 정보를 가져옴, 존재하는 경우 new_product_df로 저장, 존재하지 않는 경우 반환 값은 empty dataframe

    # 반환 값이 empty인 경우(아예 없는 경우), 전체 데이터 프레임을 가져옴

    # 새로운 columns brand를 추가.

    # pltform이 29cm 인 경우, columns에 brand 추가하고, brand를 이 컬럼에 추가

    # platform이 무신사인 경우, columns에 brand에 추가하고, brand를 이 컬럼에 추가

    # platform이 zigzag인 경우, colums에 brand에 추가하고, brand를 이 컬럼에 추가

    # 이 것들이 끝나면 이 old_product_df를 s3에 brand/combined_products_add_brand.csv에 추가.


    # new_product_df로 저장이 된 경우(파일이 존재하는 경우)

    def info_to_dataframe(bucket_name,file_key,prefix):
        try:
            # S3 객체 가져오기
            s3_hook = S3Hook(aws_conn_id='aws_default')
            s3_object = s3_hook.get_key(file_key, bucket_name)

            # S3 객체의 데이터를 읽어 DataFrame으로 변환
            s3_data = s3_object.get()['Body'].read().decode('utf-8')
            data = pd.read_csv(io.StringIO(s3_data))
            logging.info(f"{file_key} 파일을 성공적으로 읽었습니다.")
            return data
        except Exception as e:
            logging.error(f"S3에서 {file_key}를 읽는 중 오류 발생: {e}")
            return pd.DataFrame()


    # old_product_info s3에서 리스트를 가져오고 최신 버전 파일 이름 가져오기
    def old_product_info():
        bucket_name = 'otto-glue'
        prefix_old_product = 'integrated-data/products/'
        s3_hook = S3Hook(aws_conn_id='aws_default')
        old_product_file_list = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix_old_product) 
        old_product_key = old_product_file_list[-1]       
        print(old_product_key)
        old_product = info_to_dataframe(bucket_name,old_product_key,prefix_old_product)
        crawling_test(old_product)

    # new_product_info, s3에서 리스트를 가져오고 최신 버전 파일 이름 가져오기
    def new_product_info():  
        bucket_name = 'otto-glue'
        prefix_new_product = 'integrated-data/reviews/brand/'
        s3_hook = S3Hook(aws_conn_id='aws_default')

        try:
            # S3에서 파일 리스트 가져오기
            new_product_file_list = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix_new_product)
            new_product_key = new_product_file_list[-1]  # 가장 최신 파일 선택
            logging.info(f"가져온 최신 제품 추가 정보 파일 키: {new_product_key}")

            # DataFrame으로 변환하여 반환
            new_product = info_to_dataframe(bucket_name, new_product_key)
            return new_product
        except Exception as e:
            logging.error(f"최신 제품 추가 정보를 가져오는 중 오류 발생(테스트임): {e}")
            return pd.DataFrame()
        
    
    #crawling part
    def crawling_test(old_product):
        # 컬럼명 product_id	rank	product_name	category	price	image_url	description	color	size	platform

        service = Service('/usr/local/bin/chromedriver')
        options = Options()
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument('--headless')  # GUI를 표시하지 않음
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(service=service, options=options)
        visit_url = old_product('description')
        

        for i in range(len(visit_url)):
            URL = visit_url[i]
            print(URL)
    old_product_info()
    new_product_info()