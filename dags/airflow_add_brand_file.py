import boto3
import pandas as pd
import io
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
import time
from airflow.hooks.S3_hook import S3Hook
from webdriver_manager.chrome import ChromeDriverManager
import logging



# S3에서 데이터를 DataFrame으로 가져오는 함수
def info_to_dataframe(bucket_name, file_key):
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


# S3에서 최신 old_product 데이터를 가져오는 함수
def fetch_old_product_info():
    bucket_name = 'otto-glue'
    prefix_old_product = 'integrated-data/products/'
    s3_hook = S3Hook(aws_conn_id='aws_default')
    old_product_file_list = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix_old_product)
    old_product_key = old_product_file_list[-1]
    logging.info(f"Old product file key: {old_product_key}")
    return info_to_dataframe(bucket_name, old_product_key)


# Musinsa 플랫폼의 데이터를 처리하는 함수
def process_musinsa_products(ti):
    old_product = fetch_old_product_info()
    musinsa_products = old_product[old_product['platform'] == 'musinsa']
    brand_info = []
    urls = musinsa_products['description'].tolist()
    
    service = Service('/usr/local/bin/chromedriver')
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=service, options=options)

    for URL in urls:
        driver.get(URL)
        time.sleep(1)  # 페이지 로드를 위한 대기 시간

        brand_found = False
        # XPath 순회
        for j in range(11, 18):
            for z in range(2, 5):
                try:
                    # 브랜드 정보를 찾고 추가
                    brand_musinsa = driver.find_element(By.XPATH, f'//*[@id="root"]/div[{j}]/div[{z}]/dl[1]/dd/a').text
                    logging.info(f"브랜드: {brand_musinsa}, 플랫폼: musinsa")
                    brand_info.append(brand_musinsa)
                    brand_found = True
                    break  # 성공적으로 찾았으면 내부 루프 종료
                except NoSuchElementException:
                    continue  # 현재 XPath에서 요소를 찾을 수 없는 경우 다음으로 넘어감
                except Exception as e:
                    logging.error(f"예상치 못한 오류 발생: {str(e)}")
                    break
            
            if brand_found:
                break  # 외부 루프 종료
        
        if not brand_found:
            logging.info("브랜드 정보 없음")
            brand_info.append("none")

    driver.quit()
    musinsa_products['brand'] = brand_info
    # XCom에 데이터 저장
    ti.xcom_push(key='musinsa_products', value=musinsa_products.to_dict('records'))



# 29cm 플랫폼의 데이터를 처리하는 함수
def process_29cm_products(ti):
    old_product = fetch_old_product_info()
    cm29_products = old_product[old_product['platform'] == '29cm']
    brand_info = []
    urls = cm29_products['description'].tolist()
    
    service = Service('/usr/local/bin/chromedriver')
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=service, options=options)

    for URL in urls:
        driver.get(URL)
        time.sleep(1)

        try:
            brand_29cm = driver.find_element(By.XPATH, '//*[@id="__next"]/div[5]/div[1]/div/a/div/h3').text
            logging.info(f"브랜드: {brand_29cm}, 플랫폼: 29cm")
            brand_info.append(brand_29cm)
        except NoSuchElementException:
            logging.info("브랜드 정보 없음")
            brand_info.append("none")
        except Exception as e:
            logging.error(f"예상치 못한 오류 발생: {str(e)}")
            brand_info.append("none")

    driver.quit()
    cm29_products['brand'] = brand_info
    # XCom에 데이터 저장
    ti.xcom_push(key='29cm_products', value=cm29_products.to_dict('records'))

# zigzag 플랫폼의 데이터를 처리하는 함수
def process_zigzag_products(ti):
    old_product = fetch_old_product_info()
    zigzag_products = old_product[old_product['platform'] == 'zigzag']
    brand_info = []
    urls = zigzag_products['description'].tolist()
    
    service = Service('/usr/local/bin/chromedriver')
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 15)  # 대기 시간을 15초로 설정

    for URL in urls:
        driver.get(URL)
        time.sleep(2)  # 페이지 로드를 위한 대기 시간

        try:
            # 지정된 XPath를 클릭
            brand_found = False
            for xpath in ['//*[@id="__next"]/div[1]/div/div/div[11]/div/div/div/div/div[2]/div[1]/h2',
                          '//*[@id="__next"]/div[1]/div/div/div[12]/div/div/div/div/div[2]/div[1]/h2']:
                try:
                    brand_zigzag = driver.find_element(By.XPATH, xpath).text
                    logging.info(f"브랜드: {brand_zigzag}, 플랫폼: zigzag")
                    brand_info.append(brand_zigzag)
                    brand_found = True
                    break  # 성공적으로 찾았으면 반복 종료
                except NoSuchElementException:
                    continue
            
            if not brand_found:
                # 어떤 xpath에서도 브랜드 정보를 찾지 못한 경우
                logging.info("브랜드 정보 없음")
                brand_info.append("none")

        except Exception as e:
            logging.error(f"예상치 못한 오류 발생: {str(e)}")
            brand_info.append("none")

    driver.quit()  # 드라이버 종료
    zigzag_products['brand'] = brand_info
    # XCom에 데이터 저장
    ti.xcom_push(key='zigzag_products', value=zigzag_products.to_dict('records'))


# 결과를 결합하여 S3에 업로드하는 함수
def combine_and_upload(ti):
    # 기존 제품 정보 가져오기.
    old_product = fetch_old_product_info()
    
    # XCom에서 병렬 처리된 플랫폼별 제품 데이터를 가져옴
    musinsa_products = ti.xcom_pull(key='musinsa_products', task_ids='process_musinsa_products')
    cm29_products = ti.xcom_pull(key='29cm_products', task_ids='process_29cm_products')
    zigzag_products = ti.xcom_pull(key='zigzag_products', task_ids='process_zigzag_products')
    
    # XCom에서 가져온 데이터를 데이터프레임으로 변환
    musinsa_df = pd.DataFrame(musinsa_products)
    cm29_df = pd.DataFrame(cm29_products)
    zigzag_df = pd.DataFrame(zigzag_products)

    # 기존 데이터프레임과 새로 크롤링된 데이터를 병합
    # description과 URL을 기준으로 브랜드 정보를 병합
    combined_df = pd.concat([musinsa_df, cm29_df, zigzag_df], ignore_index=True)


    # 여기서는 description 컬럼을 기준으로 merge를 수행하니까, 결과가 정렬되서 나옴.
    new_product_df = pd.merge(old_product, combined_df[['description', 'brand']], on='description', how='left')

    # DataFrame을 CSV 형식으로 변환 (io.StringIO는 local에 저장되는게 아니라 저 파일이 버퍼에 저장이 되는거)
    csv_buffer = io.StringIO()
    new_product_df.to_csv(csv_buffer, index=False)

    # S3에 CSV 파일을 업로드
    bucket_name = 'otto-glue'
    s3_key = 'integrated-data/brand/combined_products_with_brands.csv'
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # 이미 존재하는 S3 파일을 덮어씌우도록 replace=True 옵션을 사용
    s3_hook.load_string(
        csv_buffer.getvalue(),
        key=s3_key,
        bucket_name=bucket_name,
        replace=True  # 덮어쓰기 옵션
    )
    
    logging.info(f"Combined products uploaded to {s3_key}")
