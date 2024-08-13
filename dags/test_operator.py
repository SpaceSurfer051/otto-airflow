import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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




class S3ListOperator(BaseOperator):
    """
    S3 버킷의 파일 목록을 출력하는 사용자 정의 오퍼레이터.
    """

    @apply_defaults
    def __init__(self, aws_conn_id, bucket_name, s3_root, *args, **kwargs):
        super(S3ListOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.s3_root = s3_root
        
    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        files = s3_hook.list_keys(bucket_name=self.bucket_name, prefix=self.s3_root)
        
        if files:
            for i, file in enumerate(files):
                print(f"{i} 번째 파일 : {file}")
            
            last_file = files[-1]
            self.log.info(f"Found files in {self.s3_root}. Last file: {last_file}")
        else:
            self.log.info(f"No files found in {self.s3_root}.")

class CrawlingOperator(BaseOperator):
    """
    크롤링 작업을 수행하는 사용자 정의 오퍼레이터.
    """
    @apply_defaults
    def __init__(self, aws_conn_id, bucket_name, reviews_s3_root, products_s3_root, *args, **kwargs):
        super(CrawlingOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.reviews_s3_root = reviews_s3_root
        self.products_s3_root = products_s3_root
        
        # WebDriver 공통 설정
        self.options = Options()
        self.options.add_experimental_option("excludeSwitches", ["enable-logging"])
        self.options.add_argument('--headless')  # GUI를 표시하지 않음
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        
        self.service = Service('/usr/local/bin/chromedriver')
        self.driver = webdriver.Chrome(service=self.service, options=self.options)

        # 공통 데이터 프레임 컬럼 정의
        self.product_df_col_name = ["product_id", "rank", "product_name", "category", "price", 
                                    "image_url", "description", "color", "size", "platform"]
        
        self.review_df_col_name = ["review_id", "product_name", "color", "size", "height", 
                                "gender", "weight", "top_size", "bottom_size", 
                                "size_comment", "quality_comment", "color_comment", 
                                "thickness_comment", "brightness_comment", "comment"]

    def execute(self, context):
        # 빈 데이터프레임 생성
        product_df = pd.DataFrame(columns=self.product_df_col_name)
        review_df = pd.DataFrame(columns=self.review_df_col_name)

        # 각 크롤링 메서드 호출 시 빈 데이터프레임과 WebDriver를 전달
        df_musinsa_product, df_musinsa_review = self.crawling_musinsa(product_df, review_df, self.driver, context)
        #df_29cm_product, df_29cm_review = self.crawling_29cm(product_df, review_df, self.driver, context)
        #df_zigzag_product, df_zigzag_review = self.crawling_zigzag(product_df, review_df, self.driver, context)

        # 크롤링 완료 후 WebDriver 종료
        self.driver.quit()

    def crawling_musinsa(self, product_df, review_df, driver, context):
        ################# URL 탐색 파트 시작 #################
        href_links = set()
        URL = "https://www.musinsa.com/categories/item/001?device=mw&sortCode=emt_high"
        driver.get(URL)
        time.sleep(3)
        # WebDriverWait와 Actions 객체 생성
        wait = WebDriverWait(driver, 10)
        actions = webdriver.ActionChains(driver)
        
        # 링크 수집을 위한 get_href_links 로직
        while len(href_links) < 100:  # 100개의 링크를 수집할 때까지 반복
            actions.send_keys(Keys.END).perform()
            time.sleep(2)
            
            for x in range(1, 46):
                try:
                    xpath = f'//*[@id="root"]/main/div/section[3]/div[1]/div/div[{x}]/div/div[2]/a[2]'                    
                    element = driver.find_element(By.XPATH, f'//*[@id="root"]/main/div/section[3]/div[1]/div/div[{x}]/div/div[2]/a[2]').text
                    
                    href = element.get_attribute('href')
                    href_links.add(href)
                except (NoSuchElementException, TimeoutException):
                    continue

            print(f"Current number of unique href links: {len(href_links)}")
            if len(href_links) >= 100:
                break

        href_links = list(href_links)
        print(f"Total href links collected: {len(href_links)}")
        
        
        
        
        
        
        
        
        
        
        ################# URL 탐색 파트 끝 #################
        
        
        
        
        ################# 데이터 크롤링 파트 시작#################
        
        ########### product crawling 시작 ###########
        
        ########### product crawling 끝 ###########
        
        
        ########### review crawling 시작 ###########
        
        ########### review crawling 끝 ###########
        
        
        
        ################# 데이터 크롤링 파트 끝 #################
        
        
        return product_df, review_df
'''

    def crawling_29cm(self, product_df, review_df, driver, context):
        # URL 설정 및 웹페이지 접근
        url = "https://www.29cm.co.kr/shop/list.php?cate_id=010102"
        driver.get(url)

        # 크롤링 작업을 진행하여 데이터프레임 업데이트
        # 예: product_df.loc[len(product_df)] = [크롤링된 데이터]
        
        print("Visited URL for 29cm:", url)
        print("Updated product DataFrame for 29cm:", product_df)
        print("Updated review DataFrame for 29cm:", review_df)
        
        return product_df, review_df

    def crawling_zigzag(self, product_df, review_df, driver, context):
        # URL 설정 및 웹페이지 접근
        url = "https://www.zigzag.kr/category/001"
        driver.get(url)

        # 크롤링 작업을 진행하여 데이터프레임 업데이트
        # 예: product_df.loc[len(product_df)] = [크롤링된 데이터]
        
        print("Visited URL for Zigzag:", url)
        print("Updated product DataFrame for Zigzag:", product_df)
        print("Updated review DataFrame for Zigzag:", review_df)
        
        return product_df, review_df
        
'''
