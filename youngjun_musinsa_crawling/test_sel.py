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

def get_href_links(driver, wait, actions, num_items_to_fetch=100):
    href_links = set()
    while len(href_links) < num_items_to_fetch:
        actions.send_keys(Keys.END)
        time.sleep(2)
        
        for x in range(1, 46):
            try:
                xpath = f'//*[@id="root"]/main/div/section[3]/div[1]/div/div[{x}]/div/div[2]/a[2]'
                element = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                href = element.get_attribute('href')
                href_links.add(href)
            except (NoSuchElementException, TimeoutException):
                continue

        print(f"Current number of unique href links: {len(href_links)}")
        if len(href_links) >= num_items_to_fetch:
            break

    return list(href_links)

def read_s3_and_print_links():
    # S3에서 CSV 파일 읽기
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    bucket_name = 'otto-glue'
    key = 'non-integrated-data/products_with_size_color.csv'

    s3_client = s3_hook.get_conn()
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    csv_links = df['description'].tolist()

    # Selenium을 사용하여 href 링크 가져오기
    URL = "https://www.musinsa.com/categories/item/001?device=mw&sortCode=emt_high"
    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument('--headless')  # GUI를 표시하지 않음
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    remote_webdriver = 'http://remote_chromedriver:4444/wd/hub'
    driver = webdriver.Remote(command_executor=remote_webdriver, options=options)
    driver.get(URL)
    time.sleep(3)

    driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[3]/div/button').click()
    time.sleep(1)

    wait = WebDriverWait(driver, 10)
    actions = driver.find_element(By.CSS_SELECTOR, 'body')

    href_links = get_href_links(driver, wait, actions, num_items_to_fetch=100)
    driver.quit()

    # 새로운 링크 프린트
    new_links = list(set(href_links) - set(csv_links))
    if not new_links:
        print("No new links found.")
    else:
        for link in new_links:
            print(link)

if __name__ == '__main__':
    read_s3_and_print_links()
