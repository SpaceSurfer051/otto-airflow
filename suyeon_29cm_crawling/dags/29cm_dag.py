from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from io import StringIO
import time
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
import logging
from airflow.utils.dates import days_ago
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'crawling_29cm_dag',
    default_args=default_args,
    description='Crawl 29cm women tops and reviews and upload to S3',
    schedule_interval=timedelta(days=1),
)


def parse_review(lines):
    try:
        if len(lines) == 2:
            if '옵션' in lines[0]:
                return lines[1].strip(), 'none', 'none', 'none'
            else:
                return 'none', 'none', 'none', 'none'
        elif len(lines) == 4:
            if '체형' in lines[2]:
                height, weight = lines[3].split(',')
                return lines[1].strip(), height.strip().replace('cm', ''), weight.strip().replace('kg', ''), 'none'
            else:
                return lines[1].strip(), 'none', 'none', lines[3].strip()
        else:
            height, weight = lines[3].split(',')
            return lines[1].strip(), height.strip().replace('cm', ''), weight.strip().replace('kg', ''), lines[5].strip()
    except Exception as e:
        print(f"Error in parse_review: {e}")
        return 'none', 'none', 'none', 'none'


def get_product_details(driver, product, already_in_s3):
    
    try:
        product_name = product.find('a', class_='css-5cm1aq').get('title')
        product_link = product.find('a', class_='css-5cm1aq').get('href')
        if product_link in already_in_s3:
            logging.info("\n{} is already in s3\n".format(product_name))
            return None, None, None, None, None, None, None
        image_link = product.find('img').get('src')
        driver.get(product_link)
        sleep(1)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        product_info = soup.find('div', class_='css-uz7uc7 e31km200')
        price_element = product_info.find('div', class_='css-1rr4qq7 ejuizc30').find('p', class_='css-1bci2fm ejuizc31')

        if price_element:
            price = price_element.text
        else:
            price = soup.select_one('#pdp_product_price').text

        price = int(price.replace(',', '').replace('원', ''))
        detail_info = soup.find('table', class_='e1hw6jas2 css-1x7jfi1 exbpx9h0').find_all('td', "css-q35or5 exbpx9h2")
        color = detail_info[1].text
        size = detail_info[2].text
        return product_name, image_link, product_link, product_name, price, color, size
    except Exception as e:
        print(f"Error while fetching product information: {e}")
        return None, None, None, None, None, None, None


def get_reviews(driver, product_name, review_data):
    try:
        actions = driver.find_element(By.CSS_SELECTOR, 'body')
        sleep(1)
        actions.send_keys(Keys.END)
        sleep(1)
        wait = WebDriverWait(driver, 10)
        review_list = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'css-31l7gp.eji1c1x1')))
    except Exception as e:
        print(f"Error while processing scroll: {e}")
        return False
    
    for review in review_list:
        try:
            divs = review.find_element(By.CLASS_NAME, 'css-4oetsc.eji1c1x9').text
            comment = review.find_element(By.TAG_NAME, 'p').text

            lines = divs.split('\n')
            option, height, weight, size_comment = parse_review(lines)
            review_data['product_name'].append(product_name)
            review_data['option'].append(option)
            review_data['height'].append(height)
            review_data['weight'].append(weight)
            review_data['size_comment'].append(size_comment)
            review_data['comment'].append(comment)

        except Exception as e:
            print(f"Error while processing review: {e}")
    
    return True


def update_29cm(already_in_s3):
    logging.info("\nstart update_29cm\n")
    
    start_time = time.time()
    
    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    options.add_argument('user-agent=' + user_agent)
    remote_webdriver = 'http://remote_chromedriver:4444/wd/hub'
    
    try:
        driver = webdriver.Remote(command_executor=remote_webdriver, options=options)
    except Exception as e:
        print(f"Error initializing WebDriver: {e}")
        return

    product_data = {'product_name': [], 'category' : [], 'price': [], 'image_url': [], 'description' : [], 'color' : [], 'size' : []}
    review_data = {'product_name': [], 'option' : [], 'height': [], 'weight' : [], 'size_comment': [], 'comment' : []}

    try:
        start_url = "https://shop.29cm.co.kr/category/list?categoryLargeCode=268100100&categoryMediumCode=268103100&sort=REVIEW&defaultSort=RECOMMEND&sortOrder=DESC&page=1"
        driver.get(start_url)
        sleep(2)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        products = soup.find_all('li', class_='css-1teigi4 e1114pfz0')
        for product in products:
            product_name, image_link, product_link, product_name, price, color, size = get_product_details(driver, product, already_in_s3)
            if product_link and product_name:
                flag = get_reviews(driver, product_name, review_data)
                if flag:
                    logging.info("\nproduce_name : {}\n".format(product_name))
                    product_data['product_name'].append(product_name)
                    product_data['category'].append("top")
                    product_data['price'].append(price)
                    product_data['image_url'].append(image_link)
                    product_data['description'].append(product_link)
                    product_data['color'].append(color)
                    product_data['size'].append(size)
                driver.back()
                sleep(1)
    except Exception as e:
        print(f"Error during main crawling process: {e}")
    finally:
        driver.quit()

        try:
            product_df = pd.DataFrame(product_data)
            review_df = pd.DataFrame(review_data)

            return product_df, review_df
            logging.info(product_df["product_name"].tolist())
        except Exception as e:
            print(f"Error while saving CSV files: {e}")

        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info("크롤링에 걸린 시간: {}초".format(elapsed_time))
        logging.info("\nfinish update_29cm\n")


def upload_to_s3(old_product_df, old_review_df, new_product_df, new_review_df):
    logging.info("\nstart upload_to_s3\n")
    updated_product_df = pd.concat([old_product_df, new_product_df], ignore_index=True)
    updated_review_df = pd.concat([old_review_df, new_review_df], ignore_index=True)
    
    logging.info(updated_product_df.head())
    
    updated_product_csv = StringIO()
    updated_review_csv = StringIO()
    updated_product_df.to_csv(updated_product_csv, index=False, encoding='utf-8-sig')
    updated_review_df.to_csv(updated_review_csv, index=False, encoding='utf-8-sig')
    
    s3_hook = S3Hook('s3_conn_id')
    s3_client = s3_hook.get_conn()
    s3_bucket = "otto-default"
    product_key = '29cm_data/new_29cm_women_top_products.csv'
    review_key = '29cm_data/new_29cm_women_top_reviews.csv'

    s3_client.put_object(Bucket=s3_bucket, Key=product_key, Body=updated_product_csv.getvalue())
    s3_client.put_object(Bucket=s3_bucket, Key=review_key, Body=updated_review_csv.getvalue())
        
    logging.info("\nfinish upload_to_s3\n")


def read_s3():
    # S3에서 CSV 파일 읽기
    logging.info("\nstart read_s3\n")
    
    s3_hook = S3Hook(aws_conn_id='s3_conn_id')
    bucket_name = 'otto-default'
    product_key = '29cm_data/29cm_women_top_products.csv'
    review_key = '29cm_data/29cm_women_top_reviews.csv'

    s3_client = s3_hook.get_conn()
    product_obj = s3_client.get_object(Bucket=bucket_name, Key=product_key)
    review_obj = s3_client.get_object(Bucket=bucket_name, Key=review_key)
    product_df = pd.read_csv(StringIO(product_obj['Body'].read().decode('utf-8-sig')))
    review_df = pd.read_csv(StringIO(review_obj['Body'].read().decode('utf-8-sig')))
    links = set(product_df["description"])
    
    logging.info("\nfinish read_s3\n")
    return product_df, review_df, links


def run_crawling_29cm():
    logging.info("\nstart run_crawling_29cm\n")
    
    old_product_df, old_review_df, already_in_s3 = read_s3()
    new_product_df, new_review_df = update_29cm(already_in_s3)
    upload_to_s3(old_product_df, old_review_df, new_product_df, new_review_df)
    
    logging.info("\nfinish run_crawling_29cm\n")


crawling_task = PythonOperator(
    task_id='run_crawling_29cm',
    python_callable=run_crawling_29cm,
    dag=dag,
)

#upload_task = PythonOperator(
#    task_id='upload_to_s3',
#    python_callable=upload_to_s3,
#    dag=dag,
#)


crawling_task
