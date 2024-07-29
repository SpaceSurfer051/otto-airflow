import logging
import pandas as pd
import re
import time
from datetime import timedelta
from io import StringIO

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

#from code_test.airflow_product_review import read_s3_and_compare_links
#from code_test.airflow_size_color import read_s3_and_add_size_color
#from code_test.airflow_data_preprocessing import data_processing
#from code_test.airflow_data_integrated import integrate_data


# musinsa
from airflow_product_review import read_s3_and_compare_links
from airflow_size_color import read_s3_and_add_size_color
from airflow_data_preprocessing import data_processing
from airflow_data_integrated import integrate_data

# zigzag
from zigzag_crawling import update_crawling_data

#from zigzag_crawling import get_product_id, product_crawling, review_crawling


# 기본 인자 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# DAG 설정
dag = DAG(
    "all_update_dag",
    default_args=default_args,
    description="DAG to crawl and compare links, then add size and color information",
    schedule_interval=timedelta(days=1),
)

with dag:
    with TaskGroup('musinsa_task_group', tooltip="Tasks for Musinsa data processing") as musinsa_task_group:
        crawl_task = PythonOperator(
            task_id='read_s3_and_compare_links',
            python_callable=read_s3_and_compare_links,
        )

        size_color_task = PythonOperator(
            task_id='read_s3_and_add_size_color',
            python_callable=read_s3_and_add_size_color,
        )

        data_processing_task = PythonOperator(
            task_id="data_processing_task",
            python_callable=data_processing,
        )

        all_data_integrate_task = PythonOperator(
            task_id='all_data_integrate_task',
            python_callable=integrate_data,
        )

        crawl_task >> size_color_task >> data_processing_task >> all_data_integrate_task

    with TaskGroup('29cm_task_group', tooltip="Tasks for 29cm data processing") as task_29cm_group:
        # 리뷰를 파싱하는 함수
        def parse_review(lines):
            try:
                if len(lines) == 2:
                    if "옵션" in lines[0]:
                        return lines[1].strip(), "none", "none", "none"
                    else:
                        return "none", "none", "none", "none"
                elif len(lines) == 4:
                    if "체형" in lines[2]:
                        height, weight = lines[3].split(",")
                        return (
                            lines[1].strip(),
                            height.strip().replace("cm", ""),
                            weight.strip().replace("kg", ""),
                            "none",
                        )
                    else:
                        return lines[1].strip(), "none", "none", lines[3].strip()
                else:
                    height, weight = lines[3].split(",")
                    return (
                        lines[1].strip(),
                        height.strip().replace("cm", ""),
                        weight.strip().replace("kg", ""),
                        lines[5].strip(),
                    )
            except Exception as e:
                print(f"Error in parse_review: {e}")
                return "none", "none", "none", "none"

        # 제품 세부 정보를 가져오는 함수
        def get_product_details(driver, product, already_in_s3):
            try:
                product_name = product.find("a", class_="css-5cm1aq").get("title")
                product_link = product.find("a", class_="css-5cm1aq").get("href")
                if product_link in already_in_s3:
                    logging.info("\n{} is already in s3\n".format(product_name))
                    return None, "none", product_link, "none", "none", "none"
                logging.info("\n{}\n".format(product_name))
                image_link = product.find("img").get("src")
                driver.get(product_link)
                time.sleep(1)

                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")

                product_info = soup.find("div", class_="css-uz7uc7 e31km200")
                price_element = product_info.find("div", class_="css-1rr4qq7 ejuizc30").find(
                    "p", class_="css-1bci2fm ejuizc31"
                )

                if price_element:
                    price = price_element.text
                else:
                    price = soup.select_one("#pdp_product_price").text

                price = int(price.replace(",", "").replace("원", ""))
                detail_info = soup.find(
                    "table", class_="e1hw6jas2 css-1x7jfi1 exbpx9h0"
                ).find_all("td", "css-q35or5 exbpx9h2")
                color = detail_info[1].text
                size = detail_info[2].text

                return product_name, image_link, product_link, price, color, size
            except Exception as e:
                print(f"Error while fetching product information: {e}")
                return "none", "none", None, "none", "none", "none"

        # 리뷰를 가져오는 함수
        def get_reviews(driver, product_name, review_data, gender):
            try:
                actions = driver.find_element(By.CSS_SELECTOR, "body")
                time.sleep(1)
                actions.send_keys(Keys.END)
                time.sleep(1)
                wait = WebDriverWait(driver, 10)
                review_list = wait.until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "css-31l7gp.eji1c1x1"))
                )
            except Exception as e:
                print(f"Error while processing scroll: {e}")
                return False

            for review in review_list:
                try:
                    divs = review.find_element(By.CLASS_NAME, "css-4oetsc.eji1c1x9").text
                    comment = review.find_element(By.TAG_NAME, "p").text

                    lines = divs.split("\n")
                    option, height, weight, size_comment = parse_review(lines)

                    review_data["review_id"].append("none")
                    review_data["top_size"].append("none")
                    review_data["bottom_size"].append("none")
                    review_data["quality_comment"].append("none")
                    review_data["color_comment"].append("none")
                    review_data["thickness_comment"].append("none")
                    review_data["brightness_comment"].append("none")
                    review_data["product_name"].append(product_name)
                    review_data["gender"].append(gender)

                    color_match = re.search(r"\[(Color|color|컬러)\](.*?)\[", option)
                    size_match = re.search(r"\[(Size|size|사이즈)\](.*?)\,", option)

                    color = color_match.group(2).strip() if color_match else "none"
                    size = size_match.group(2).strip() if size_match else "none"

                    review_data["color"].append(color)
                    review_data["size"].append(size)
                    review_data["height"].append(height)
                    review_data["weight"].append(weight)
                    review_data["size_comment"].append(size_comment)
                    review_data["comment"].append(comment)

                except Exception as e:
                    print(f"Error while processing review: {e}")

            return True

        # 29cm 사이트에서 제품 정보를 업데이트하는 함수
        def update_29cm(already_in_s3):
           # logging.info("\nstart update_29cm\n")

            start_time = time.time()

            # 크롬 드라이버 옵션 설정
            options = Options()
            options.add_experimental_option("excludeSwitches", ["enable-logging"])
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            options.add_argument("user-agent=" + user_agent)
            #remote_webdriver = "http://remote_chromedriver:4444/wd/hub"

    # 크롬 드라이버 초기화
            try:
                service = Service('/usr/local/bin/chromedriver')  # 크롬 드라이버 경로
                driver = webdriver.Chrome(service=service, options=options)
            except Exception as e:
                print(f"Error initializing WebDriver: {e}")
                return

            # 제품 및 리뷰 데이터 초기화
            product_data = {
                "product_id": [],
                "rank": [],
                "product_name": [],
                "category": [],
                "price": [],
                "image_url": [],
                "description": [],
                "color": [],
                "size": [],
            }
            review_data = {
                "review_id": [],
                "product_name": [],
                "color": [],
                "size": [],
                "height": [],
                "gender": [],
                "weight": [],
                "top_size": [],
                "bottom_size": [],
                "size_comment": [],
                "quality_comment": [],
                "color_comment": [],
                "thickness_comment": [],
                "brightness_comment": [],
                "comment": [],
            }

            try:
                # 크롤링할 URL 리스트 설정
                start_urls = [
                    "https://shop.29cm.co.kr/category/list?categoryLargeCode=268100100&categoryMediumCode=268103100&sort=REVIEW&defaultSort=RECOMMEND&sortOrder=DESC&page=1",
                    "https://shop.29cm.co.kr/category/list?colors=&categoryLargeCode=268100100&categoryMediumCode=268106100&categorySmallCode=&minPrice=&maxPrice=&isFreeShipping=&excludeSoldOut=&isDiscount=&brands=&sort=REVIEW&defaultSort=RECOMMEND&sortOrder=DESC&tag=&extraFacets=",
                    "https://shop.29cm.co.kr/category/list?categoryLargeCode=272100100&categoryMediumCode=272103100&sort=REVIEW&defaultSort=RECOMMEND&sortOrder=DESC&page=1",
                    "https://shop.29cm.co.kr/category/list?colors=&categoryLargeCode=272100100&categoryMediumCode=272104100&categorySmallCode=&minPrice=&maxPrice=&isFreeShipping=&excludeSoldOut=&isDiscount=&brands=&sort=REVIEW&defaultSort=RECOMMEND&sortOrder=DESC&tag=&extraFacets=",
                ]
                # 각 URL에 대해 크롤링 수행
                for i in range(len(start_urls)):
                    if i < 2:
                        gender = "여성"
                    else:
                        gender = "남성"

                    if i % 2 == 0:
                        category = "top"
                    else:
                        category = "bottom"

                    driver.get(start_urls[i])
                    time.sleep(2)

                    html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")

                    products = soup.find_all("li", class_="css-1teigi4 e1114pfz0")
                    rank = 0
                    for product in products:
                        # 제품 정보 추출
                        rank += 1
                        product_name, image_link, product_link, price, color, size = (
                            get_product_details(driver, product, already_in_s3)
                        )
                        if product_link and product_name:
                            flag = get_reviews(driver, product_name, review_data, gender)
                            if flag:
                                product_data["product_id"].append("none")
                                product_data["rank"].append(rank)
                                product_data["product_name"].append(product_name)
                                product_data["category"].append(category)
                                product_data["price"].append(price)
                                product_data["image_url"].append(image_link)
                                product_data["description"].append(product_link)
                                product_data["color"].append(color)
                                product_data["size"].append(size)
                            driver.back()
                            time.sleep(1)
                        elif product_link:
                            product_data["product_id"].append("none")
                            product_data["rank"].append(rank)
                            product_data["product_name"].append("none")
                            product_data["category"].append(category)
                            product_data["price"].append(price)
                            product_data["image_url"].append(image_link)
                            product_data["description"].append(product_link)
                            product_data["color"].append(color)
                            product_data["size"].append(size)

                            driver.back()
                            time.sleep(1)

            except Exception as e:
                print(f"Error during main crawling process: {e}")
            finally:
                driver.quit()

                try:
                    product_df = pd.DataFrame(product_data)
                    review_df = pd.DataFrame(review_data)

                    logging.info(product_df["product_name"].tolist())
                    return product_df, review_df
                except Exception as e:
                    print(f"Error while saving CSV files: {e}")

                # 크롤링 완료 시간 기록
                end_time = time.time()
                elapsed_time = end_time - start_time
                logging.info("크롤링에 걸린 시간: {}초".format(elapsed_time))
                logging.info("\nfinish update_29cm\n")

        # 데이터를 S3에 업로드하는 함수
        def upload_to_s3(old_product_df, old_review_df, new_product_df, new_review_df):
            logging.info("\nstart upload_to_s3\n")

            # 제품 데이터를 S3에 업로드 - 새로 크롤링한 데이터 중 이전 데이터에도 있는 값은 rank값만 바꾸고, 없는 값은 새로 추가한다.
            updated_data = []
            for _, new_row in new_product_df.iterrows():
                old_row = old_product_df[
                    old_product_df["description"] == new_row["description"]
                ]
                if old_row.empty or old_row.iloc[0]["product_name"] == "none":
                    size = new_row["size"]
                    color = new_row["color"]
                    product_name = new_row["product_name"]
                    category = new_row["category"]
                    price = new_row["price"]
                    image_url = new_row["image_url"]
                else:
                    size = old_row.iloc[0]["size"]
                    color = old_row.iloc[0]["color"]
                    product_name = old_row.iloc[0]["product_name"]
                    category = old_row.iloc[0]["category"]
                    price = old_row.iloc[0]["price"]
                    image_url = old_row.iloc[0]["image_url"]
                updated_data.append(
                    [
                        "none",
                        new_row["rank"],
                        product_name,
                        category,
                        price,
                        image_url,
                        new_row["description"],
                        color,
                        size,
                    ]
                )

            # 제품 데이터를 S3에 업로드 - 이전 데이터에는 있지만, 새로 크롤링한 데이터에 없는 값은 rank를 "none"으로 저장한다.
            for _, old_row in old_product_df.iterrows():
                new_row = new_product_df[
                    new_product_df["description"] == old_row["description"]
                ]
                if new_row.empty:
                    updated_data.append(
                        [
                            old_row["product_id"],
                            "none",
                            old_row["product_name"],
                            old_row["category"],
                            old_row["price"],
                            old_row["image_url"],
                            old_row["description"],
                            old_row["color"],
                            old_row["size"],
                        ]
                    )

            updated_product_df = pd.DataFrame(
                updated_data,
                columns=[
                    "product_id",
                    "rank",
                    "product_name",
                    "category",
                    "price",
                    "image_url",
                    "description",
                    "color",
                    "size",
                ],
            ).fillna("")
            logging.info("\nfinish updated_product_df\n")

            # 리뷰 데이터를 S3에 업로드
            updated_review_df = pd.concat([old_review_df, new_review_df], ignore_index=True)

            updated_product_csv = StringIO()
            updated_review_csv = StringIO()
            updated_product_df.to_csv(updated_product_csv, index=False, encoding="utf-8-sig")
            updated_review_df.to_csv(updated_review_csv, index=False, encoding="utf-8-sig")

            s3_hook = S3Hook("aws_s3")
            s3_client = s3_hook.get_conn()
            s3_bucket = "otto-glue"
            product_key = "non-integrated-data/29cm_products.csv"
            review_key = "non-integrated-data/29cm_reviews.csv"

            s3_client.put_object(
                Bucket=s3_bucket, Key=product_key, Body=updated_product_csv.getvalue()
            )
            s3_client.put_object(
                Bucket=s3_bucket, Key=review_key, Body=updated_review_csv.getvalue()
            )

            logging.info("\nfinish upload_to_s3\n")

        def read_s3():
            # S3에서 CSV 파일 읽기
            logging.info("\nstart read_s3\n")

            s3_hook = S3Hook(aws_conn_id="aws_s3")
            bucket_name = "otto-glue"
            product_key = "non-integrated-data/29cm_products.csv"
            review_key = "non-integrated-data/29cm_reviews.csv"

            s3_client = s3_hook.get_conn()
            product_obj = s3_client.get_object(Bucket=bucket_name, Key=product_key)
            review_obj = s3_client.get_object(Bucket=bucket_name, Key=review_key)
            product_df = pd.read_csv(StringIO(product_obj["Body"].read().decode("utf-8")))
            review_df = pd.read_csv(StringIO(review_obj["Body"].read().decode("utf-8")))
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
            task_id="run_crawling_29cm",
            python_callable=run_crawling_29cm,
            dag=dag,
        )
    
    
    with TaskGroup('zigzag_task_group', tooltip= "Tasks for zigzag data update") as task_zigzag_group:
        update_zigzag_task = PythonOperator(
            task_id='update_zigzag_data_crawling',
            python_callable= update_crawling_data,
            op_kwargs={'bucket_name': 'otto-glue'},
        )

    task_zigzag_group >> task_29cm_group >> musinsa_task_group



        
