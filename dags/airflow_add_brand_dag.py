'''

selenium 연결
webdrvier 연결

product 최신 버전 s3 가져오기



if 만약 파일이 존재하지 않은 경우
    가져온 s3의 description를 리스트에 추가
    29cm, zigzag, musinsa의 경우 나눠서 브랜드를 수집
    이후 파일을 최종/combined_products_add_brand.csv 로 저장
    
elif 최신버전의  최종/combined_products_add_brand.csv 파일이 존재하는 경우
    s3의 최신 combined_products_{now}.csv의 description에서
    최종/combined_products_add_brand.csv의 description의 중복되지 않는 경우만 탐색.
    


'''
# import
import boto3
import pandas as pd
from io import StringIO
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


############## s3 정보 가져오는 파트 ############################

# s3에서 최신 product 정보를 가져옴, old_product_df로 저장


# s3에서 최신 product_add_brand 정보를 가져옴, 존재하는 경우 new_product_df로 저장, 존재하지 않는 경우 반환 값은 "1"

    # 반환 값이 1인 경우(아예 없는 경우), 전체 데이터 프레임을 가져옴

        # 새로운 columns brand를 추가.

            # pltform이 29cm 인 경우, columns에 brand 추가하고, brand를 이 컬럼에 추가
            
            # platform이 무신사인 경우, columns에 brand에 추가하고, brand를 이 컬럼에 추가
            
            # platform이 zigzag인 경우, colums에 brand에 추가하고, brand를 이 컬럼에 추가
            
        # 이 것들이 끝나면 이 old_product_df를 s3에 최종/combined_products_add_brand.csv에 추가.

