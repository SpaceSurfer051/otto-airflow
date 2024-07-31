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
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime

# 웹 드라이버를 생성하는 함수
def get_driver():
    logging.info("creating driver.")
    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument('--headless')  # GUI를 표시하지 않음
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    service = Service('/usr/local/bin/chromedriver')
    driver = webdriver.Chrome(service=service, options=options)
    logging.info("driver created.")
    return driver

# S3에서 CSV 파일을 읽어오는 함수
def get_csv_from_s3(bucket_name, key):
    try:
        s3_hook = S3Hook(aws_conn_id="aws_s3")
        s3_client = s3_hook.get_conn()
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8-sig")))
        df = df.drop_duplicates()  # 중복 제거
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
    # 리스트 타입의 열을 문자열로 변환
    for col in df.columns:
        if df[col].dtype == 'object':
            if any(isinstance(i, list) for i in df[col]):
                df[col] = df[col].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else x)
                
    df = df.drop_duplicates()  # 중복 제거
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

def create_log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{now} ==> {msg}"

def scroll_down(driver):
    actions = driver.find_element(By.CSS_SELECTOR, "body")
    actions.send_keys(Keys.END)
    actions.send_keys(Keys.END)
    actions.send_keys(Keys.END)
    actions.send_keys(Keys.END)
    actions.send_keys(Keys.END)
    time.sleep(0.5)

def get_or_none(webelement, xpath, by=By.XPATH):
    try:
        time.sleep(0.1)
        return webelement.find_element(by, xpath).text
    except NoSuchElementException as e:
        return

def crawling_product_name(wait):
    try:
        tag = wait.until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, "BODY_15.REGULAR.css-1qjogoj.e1wgb8lp0")
            )
        )
        time.sleep(0.5)
        return tag.text
    except TimeoutException as e:
        logging.info(f"exception at `crawling_product_name` => {e}")
        return

def crawling_product_price(wait):
    try:
        tag = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "css-14j45be.eizm2tm0"))
        )
        time.sleep(0.5)
        return tag.text
    except TimeoutException as e:
        try:
            tag = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "css-1fo6xrw.e1o0mpyu3"))
            )
            time.sleep(0.5)
            return tag.text
        except TimeoutException as e:
            logging.info(f"exception at `crawling_product_price` => {e}")
            return

def crawling_product_img_url(wait):
    try:
        tag = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "show-skeleton.css-12ywniv"))
        )
        src = tag.find_element(By.XPATH, ".//picture/img").get_attribute("src")
        return src
    except TimeoutException as e:
        try:
            div = wait.until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, "swiper-slide.swiper-slide-active")
                )
            )
            src = div.find_element(By.XPATH, ".//div/div/picture/img").get_attribute("src")
            logging.info("crawled from first exception")
            return src
        except TimeoutException as e:
            logging.info(f"exception at crawling_product_img_url` => {e}")
            return

def get_color_tag_list(wait):
    color_table = wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "css-0.e1u2d7n04"))
    )
    return color_table.find_elements(By.TAG_NAME, "li")

def color_crawling(driver):
    color_string_tag = {"색상", "color", "컬러"}

    color_set = set()
    wait = WebDriverWait(driver, 3)
    buy_button = wait.until(
        EC.presence_of_element_located(
            (By.CLASS_NAME, "BODY_17.BOLD.css-hbld7k.e1yh52zv0")
        )
    )
    ActionChains(driver).click(buy_button).perform()
    toggle_tag = wait.until(
        EC.presence_of_element_located(
            (By.CLASS_NAME, "BODY_15.SEMIBOLD.css-utqis4.e1cn5bmz0")
        )
    )
    tag_string = toggle_tag.text.lower()
    tag_list = driver.find_element(By.CLASS_NAME, "css-0.e1u2d7n04").find_elements(
        By.TAG_NAME, "li"
    )
    if tag_string in color_string_tag:
        for color_tag in tag_list:
            time.sleep(0.1)
            color = color_tag.text.split("\n")[0]
            color_set.add(color)
    else:
        for i in range(len(tag_list)):
            time.sleep(0.1)
            tag = tag_list[i]
            time.sleep(1)
            tag.click()
            color_tag_list = driver.find_element(
                By.CLASS_NAME, "css-0.e1u2d7n04"
            ).find_elements(By.TAG_NAME, "li")
            driver.implicitly_wait(1)
            for color_tag in color_tag_list:
                time.sleep(0.1)
                color = color_tag.text.split("\n")[0]
                color_set.add(color)
            tag_button = driver.find_element(By.CLASS_NAME, "css-oa28ah.e1u2d7n06")
            tag_button.click()
            time.sleep(1)
            tag_list = driver.find_element(
                By.CLASS_NAME, "css-0.e1u2d7n04"
            ).find_elements(By.TAG_NAME, "li")
            driver.implicitly_wait(1)

    return list(color_set)

def size_crawling(driver, url):
    button_tags = driver.find_elements(
        By.CLASS_NAME, "BODY_16.SEMIBOLD.css-1qe1foo.e1wqfudt0"
    )
    if len(button_tags) < 4:
        return ["Free"]

    driver.get(url + "?tab=size")
    wait = WebDriverWait(driver, 3)
    try:
        size_tag_list = wait.until(
            EC.presence_of_all_elements_located(
                (By.CLASS_NAME, "head.CAPTION_11.SEMIBOLD.css-oyuqpv.eqs9ftl0")
            )
        )
        return list(map(lambda tag: tag.text, size_tag_list))
    except:
        return "none"

def product_crawling(driver, category, product_list, product_set=set()):
    logging.info("start product crawling")
    product_url = "https://zigzag.kr/catalog/products/{product_id}"
    product_info = {}
    for i, product_id in enumerate(product_list):
        if product_id in product_set:
            logging.info(f"product {product_id} already crawled")
            continue
        logging.info(product_id)
        temp = {}
        url = product_url.format(product_id=product_id)
        driver.get(url)
        wait = WebDriverWait(driver, 3)
        temp["product_id"] = product_id
        temp["category"] = category
        temp["description"] = url
        temp["product_name"] = crawling_product_name(wait)
        temp["price"] = crawling_product_price(wait)
        temp["image_url"] = crawling_product_img_url(wait)

        temp["size"] = size_crawling(driver, url)
        try:
            temp["color"] = color_crawling(driver)
        except NoSuchElementException as e:
            logging.info(f"error at product id (NSEE) :: {product_id}")
            temp["color"] = "none"
        except ElementNotInteractableException as e:
            logging.info(f"error at product id (ENIE) :: {product_id}")
            temp["color"] = "none"

        temp["rank"] = i + 1

        product_info[product_id] = temp
        time.sleep(2)

    return product_info

def review_crawling(driver, product_list, max_num=10, category="top", product_set=set()):
    logging.info("start review crawling")
    review_url = "https://zigzag.kr/review/list/{product_id}"
    xpath = "/html/body/div/div[1]/div/div/div/div[2]/div/div/section/div[{i}]/div[1]/div[3]"
    reviews = {}

    for product_id in product_list:
        logging.info(product_id)
        url = review_url.format(product_id=product_id)
        if product_id in product_set:
            logging.info(f"product {product_id} review already crawled")
            continue
        driver.get(url)
        wait = WebDriverWait(driver, 3)

        for i in range(1, max_num + 1):
            time.sleep(0.1)
            x = xpath.format(i=i)
            if i % 4 == 0:
                scroll_down(driver)
            try:
                review_tag = wait.until(EC.presence_of_element_located((By.XPATH, x)))
            except TimeoutException as ee:
                logging.info("no more review")
                break

            time.sleep(1)

            selected_color = get_or_none(review_tag, ".//div/div[1]/span/span[1]")
            selected_size = get_or_none(review_tag, ".//div/div[1]/span/span[2]")
            size_opinion = get_or_none(review_tag, ".//div/div[2]/div[1]/span")
            quality_opinion = get_or_none(review_tag, ".//div/div[2]/div[2]/span")
            color_opinion = get_or_none(review_tag, ".//div/div[2]/div[3]/span")

            height = get_or_none(review_tag, ".//div/div[3]/span/span[1]")
            weight = get_or_none(review_tag, ".//div/div[3]/span/span[2]")
            size = get_or_none(review_tag, ".//div/div[3]/span/span[3]")

            detail_text = get_or_none(review_tag, "BODY_14.REGULAR.css-epr5m6.e1j2jqj72", by=By.CLASS_NAME)
            review_id = f"{product_id}_{i}"

            logging.info(review_id)

            temp = {
                "review_id": review_id,
                "product_id": product_id,
                "color": selected_color,
                "size": selected_size,
                "size_comment": size_opinion,
                "quality_comment": quality_opinion,
                "color_comment": color_opinion,
                "height": height,
                "weight": weight,
                "comment": detail_text,
            }

            if category == "top":
                temp["top_size"] = size
                temp["bottom_size"] = "none"
            else:
                temp["top_size"] = "none"
                temp["bottom_size"] = size

            reviews[review_id] = temp

    return reviews

def get_product_id(driver, url, max_num=10):
    id_set = set()
    id_list = []
    driver.get(url)
    action = ActionChains(driver)
    driver.implicitly_wait(20)
    time.sleep(1)

    for i in range(max_num):
        time.sleep(1)

        try:
            next_raw = driver.find_element(By.CSS_SELECTOR, f'div[data-index="{i}"]')
            driver.implicitly_wait(20)
            driver.execute_script("arguments[0].scrollIntoView(true);", next_raw)
            action.move_to_element_with_offset(next_raw, 0, 100).perform()
            raw = next_raw.find_elements(By.CLASS_NAME, "css-1jo7xgn")
            driver.implicitly_wait(20)
            for div in raw:
                a = div.find_element(By.XPATH, ".//div/a")
                href = a.get_attribute("href")
                time.sleep(0.1)
                product_id = href.split("/")[-1]
                logging.info(product_id)

                if product_id not in id_set:
                    id_list.append(product_id)
                    id_set.add(product_id)
                time.sleep(0.5)

                if len(id_list) >= max_num:
                    return id_list
            time.sleep(0.5)

        except NoSuchElementException as e:
            logging.info("No more products")
            break

        except WebDriverException as e:
            logging.info("Java TimeoutException")
            continue

    return id_list

def add_product_name(products, reviews):
    for review_id, review in reviews.items():
        product_id = review["product_id"]
        if product_id in products:
            reviews[review_id]["product_name"] = products[product_id]["product_name"]
    return reviews

def update_crawling_data(bucket_name, product_max_num=10, review_max_num=10):
    products_url = "https://zigzag.kr/categories/-1?title=%EC%9D%98%EB%A5%98&category_id=-1&middle_category_id={id}&sort=201"
    category_ids = {"top": "474", "bottom": "547"}

    product_df = get_csv_from_s3(bucket_name, "non-integrated-data/zigzag_products.csv")
    review_df = get_csv_from_s3(bucket_name, "non-integrated-data/zigzag_reviews.csv")
    product_set = set(product_df["product_id"])
    logging.info(f"origin link's length ==> {len(product_set)}")

    driver = get_driver()

    for category, id in category_ids.items():
        logging.info(f"start {category} product list crawling")
        url = products_url.format(id=id)
        product_list = get_product_id(driver, url, max_num=product_max_num)
        logging.info(f"done. new {len(product_list)} links crawled.")

        logging.info(f"start {category} product information crawling")
        product_info = product_crawling(driver, category, product_list, product_set=product_set)
        product_info_df = pd.DataFrame(product_info).T
        product_df = pd.concat([product_df, product_info_df], ignore_index=True)
        product_df = set_rank(product_df, product_list)
        logging.info("done.")
        logging.info(f"length:: {len(product_df)}")

        logging.info(f"start {category} review crawling")
        review_list = review_crawling(driver, product_list, review_max_num, category=category, product_set=product_set)
        review_list = add_product_name(product_info, review_list)
        review_list_df = pd.DataFrame(review_list).T
        review_df = pd.concat([review_df, review_list_df], ignore_index=True)
        logging.info("done.")
        logging.info(f"length:: {len(review_df)}")

    save_df_to_s3(product_df, bucket_name, "non-integrated-data/zigzag_products.csv")
    save_df_to_s3(review_df, bucket_name, "non-integrated-data/zigzag_reviews.csv")

    driver.quit()
