from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains

from datetime import datetime
import time
import pandas as pd

import logging


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
            src = div.find_element(By.XPATH, ".//div/div/picture/img").get_attribute(
                "src"
            )
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


def product_crawling(driver, category, product_list, link_set=set()):
    logging.info("start product crawling")
    product_url = "https://zigzag.kr/catalog/products/{product_id}"
    product_info = {}
    for i, product_id in enumerate(product_list):
        if product_url in link_set:
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


def review_crawling(driver, product_list, max_num=10, category="top", link_set=set()):
    logging.info("start review crawling")
    review_url = "https://zigzag.kr/review/list/{product_id}"
    xpath = "/html/body/div/div[1]/div/div/div/div[2]/div/div/section/div[{i}]/div[1]/div[3]"
    reviews = {}

    for product_id in product_list:
        logging.info(product_id)
        url = review_url.format(product_id=product_id)
        if url in link_set:
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

            detail_text = get_or_none(
                review_tag, "BODY_14.REGULAR.css-epr5m6.e1j2jqj72", by=By.CLASS_NAME
            )
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
    for id, product in products.items():
        for review_id, review in reviews.items():
            reviews[review_id]["product_name"] = product["product_name"]
    return reviews


def main():
    category_ids = {
        # 'top' : '474',
        "bottom": "547"
    }
    ## 리뷰순으로 정렬된 url
    products_url = "https://zigzag.kr/categories/-1?title=%EC%9D%98%EB%A5%98&category_id=-1&middle_category_id={id}&sort=201"

    product_infos = {}
    reviews = {}

    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    with webdriver.Chrome(
        service=Service(ChromeDriverManager().install(), options=options)
    ) as driver:
        for category, id in category_ids.items():
            print(category)
            url = products_url.format(id=id)
            product_list = get_product_id(driver, url, 100)
            print(product_list)
            product_info_list = product_crawling(driver, category, product_list)
            print(product_info_list)
            review_list = review_crawling(driver, product_list, 20, category=category)
            print(review_list)

            product_infos.update(product_info_list)
            reviews.update(review_list)

    reviews = add_product_name(product_infos, reviews)

    pd_product_infos = pd.DataFrame(product_infos).T
    pd_reviews = pd.DataFrame(reviews).T

    pd_product_infos.to_csv(
        "zigzag_product_infos.csv", encoding="utf-8-sig", index=True
    )
    pd_reviews.to_csv("zigzag_reviews.csv", encoding="utf-8-sig", index=True)


def test():
    category_ids = {"top": "474", "bottom": "547"}
    ## 리뷰순으로 정렬된 url
    products_url = "https://zigzag.kr/categories/-1?title=%EC%9D%98%EB%A5%98&category_id=-1&middle_category_id={id}&sort=201"

    options = Options()
    options.add_argument("--headless")
    with webdriver.Chrome(
        service=Service(ChromeDriverManager().install(), options=options)
    ) as driver:
        all_links = []
        # for category, id in category_ids.items():
        #     url = products_url.format(id=id)

        #     # ## 제품 아이디 불러오는 거 확인
        #     link = get_product_id(driver, url, max_num=30)
        #     print(f'loaded {len(link)} from {category}')
        #     all_links.extend(link)
        # print(f'total links => {len(all_links)}')

        ## 제품 아이디로 img_url 불러오기

        # products = product_crawling(driver, 'top', ['107705764'])
        # print(products)

        # ## 제품 아이디를 get 해놓은 driver가 들어왔을 때, 색상 및 사이즈 불러오는 거 테스트
        # driver.get("https://zigzag.kr/catalog/products/123206351")
        # info = crawling_color_and_size(driver)
        # print(info)

        # ## 제품 아이디가 들어왔을 때, 해당 제품 정보를 불러오는 거 확인
        # product_data = product_crawling(driver, category, all_links)
        # print(product_data)

        # ## 제품 아이디가 들어왔을 때, 리뷰 크롤링 테스트
        # product_ids = ['112538672', '100485995', '100511186']
        # products_reviews = review_crawling(driver, product_ids)
        # for product, review in products_reviews.items():
        #     print(product)
        #     print(len(review))
        #     print(review)

        # ## 색상 잘 불러오는 지 테스트
        # for link in ['https://zigzag.kr/catalog/products/132082569', 'https://zigzag.kr/catalog/products/122210777', 'https://zigzag.kr/catalog/products/112059662']:
        #     driver.get(link)
        #     colors = color_crawling(driver)
        #     print(colors)


if __name__ == "__main__":
    # test()
    main()
