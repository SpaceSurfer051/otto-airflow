from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains

from datetime import datetime
import time
import pandas as pd

def create_log(msg):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return f'{now} ==> {msg}'


def scroll_down(driver):
    actions = driver.find_element(By.CSS_SELECTOR, 'body')
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
        tag = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'BODY_15.REGULAR.css-1qjogoj.e1wgb8lp0')))
        time.sleep(0.5)
        return tag.text
    except TimeoutException as e:
        print(f"exception at `crawling_product_name` => {e}")
        return


def crawling_product_price(wait):
    try:
        tag = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'css-14j45be.eizm2tm0')))
        time.sleep(0.5)
        return tag.text
    except TimeoutException as e:
        try:
            tag = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'css-1fo6xrw.e1o0mpyu3')))
            time.sleep(0.5)
            return tag.text
        except TimeoutException as e:
            print(f"exception at `crawling_product_price` => {e}")
            return


def crawling_product_img_url(wait):
    try:
        tag = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'show-skeleton.css-12ywniv')))
        src = tag.find_element(By.XPATH, './/picture/img').get_attribute('src')
        return src
    except TimeoutException as e:
        try:
            div = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'swiper-slide.swiper-slide-active')))
            src = div.find_element(By.XPATH, './/div/div/picture/img').get_attribute('src')
            print("crawled from first exception")
            return src
        except TimeoutException as e:
            print(f"exception at crawling_product_img_url` => {e}")
            return


def get_color_tag_list(wait):
    color_table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'css-0.e1u2d7n04')))
    return color_table.find_elements(By.TAG_NAME, 'li')


def size_crawling(driver, url):
    button_tags = driver.find_elements(By.CLASS_NAME, 'BODY_16.SEMIBOLD.css-1qe1foo.e1wqfudt0')
    if len(button_tags) < 4:
        return ['Free']

    driver.get(url + '?tab=size')
    wait = WebDriverWait(driver, 3)
    try:
        size_tag_list = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'head.CAPTION_11.SEMIBOLD.css-oyuqpv.eqs9ftl0')))
        return list(map(lambda tag: tag.text, size_tag_list))
    except:
        return None


def product_crawling(driver, category, product_list):
    print(create_log('start product crawling'))
    product_url = 'https://zigzag.kr/catalog/products/{product_id}'
    product_info = {}
    for product_id in product_list:
        print(create_log(product_id))
        temp = {}
        url = product_url.format(product_id=product_id)
        driver.get(url)
        wait = WebDriverWait(driver, 3)
        temp['product_id'] = product_id
        temp['product_url'] = url
        temp['name'] = crawling_product_name(wait)
        temp['price'] = crawling_product_price(wait)
        temp['img_url'] = crawling_product_img_url(wait)

        size = size_crawling(driver, url)
        temp['size'] = size
        temp['category'] = category

        product_info[product_id] = temp
        time.sleep(2)
    
    return product_info


def review_crawling(driver, product_list, max_num=10):
    print(create_log('start review crawling'))
    review_url = 'https://zigzag.kr/review/list/{product_id}'
    xpath = '/html/body/div/div[1]/div/div/div/div[2]/div/div/section/div[{i}]/div[1]/div[3]'
    reviews = {}

    for product_id in product_list:
        print(create_log(product_id))
        url = review_url.format(product_id=product_id)
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
                print('no more review')
                break
            
            time.sleep(1)
        
            selected_color = get_or_none(review_tag, './/div/div[1]/span/span[1]')
            selected_size = get_or_none(review_tag, './/div/div[1]/span/span[2]')
            size_opinion = get_or_none(review_tag, './/div/div[2]/div[1]/span')
            quality_opinion = get_or_none(review_tag, './/div/div[2]/div[2]/span')
            color_opinion = get_or_none(review_tag, './/div/div[2]/div[3]/span')

            height = get_or_none(review_tag, './/div/div[3]/span/span[1]')
            weight = get_or_none(review_tag, './/div/div[3]/span/span[2]')
            top_size = get_or_none(review_tag, './/div/div[3]/span/span[3]')

            detail_text = get_or_none(review_tag, 'BODY_14.REGULAR.css-epr5m6.e1j2jqj72', by=By.CLASS_NAME)
            review_id = f'{product_id}_{i}'

            print(review_id)

            temp = {
                'review_id': review_id,
                'product_id': product_id,
                'selected_color': selected_color,
                'selected_size': selected_size,
                'size_opinion': size_opinion,
                'quality_opinion': quality_opinion,
                'color_opinion': color_opinion,
                'height': height,
                'weight': weight,
                'top_size': top_size,
                'detail_text': detail_text
            }
            reviews[review_id] = temp
            
    
    return reviews


def get_product_id(driver, url, max_num=10):
    id_set = set()
    id_list = []
    driver.get(url)
    action = ActionChains(driver)
    driver.implicitly_wait(1)
    time.sleep(1)

    for i in range(max_num):
        if len(id_list) > max_num:
            break
        try:
            next_raw = driver.find_element(By.CSS_SELECTOR, f'div[data-index="{i}"]')
            driver.implicitly_wait(2)
            driver.execute_script("arguments[0].scrollIntoView(true);", next_raw)
            action.move_to_element_with_offset(next_raw, 0, 100).perform()
            raw = next_raw.find_elements(By.CLASS_NAME, 'css-1jo7xgn')
            driver.implicitly_wait(2)
            for div in raw:
                a = div.find_element(By.XPATH, './/div/a')
                href = a.get_attribute('href')
                product_id = href.split('/')[-1]
                if product_id not in id_set:
                    id_list.append(product_id)
                    id_set.add(product_id)
                time.sleep(0.5)
            time.sleep(0.5)

        except NoSuchElementException as e:
            print("No more products")
            break

    return id_list


def main():
    category_ids = {
        '상의' : '474',
        # '하의' : '547'
    }
    ## 리뷰순으로 정렬된 url
    products_url = 'https://zigzag.kr/categories/-1?title=%EC%9D%98%EB%A5%98&category_id=-1&middle_category_id={id}&sort=201'

    product_infos = {}
    reviews = {}

    options = Options()
    options.add_argument("--headless")
    with webdriver.Chrome(\
        service=Service(ChromeDriverManager().install(),\
        options=options\
    )) as driver:
        for category, id in category_ids.items():
            url = products_url.format(id=id)
            product_list = get_product_id(driver, url, 3)
            # product_info_list = product_crawling(driver, category, product_list)
            review_list = review_crawling(driver, product_list, 5)

            # product_infos.update(product_info_list)
            reviews.update(review_list)

    # print(product_infos)
    print(reviews)
    # pd_product_infos = pd.DataFrame(product_infos).T
    pd_reviews = pd.DataFrame(reviews).T

    # pd_product_infos.to_csv("zigzag_product_infos.csv", encoding='utf-8-sig', index=True)
    pd_reviews.to_csv("zigzag_reviews.csv", encoding='utf-8-sig', index=True)


def test():
    category_ids = {
        'top' : '474',
        'bottom' : '547'
    }
    ## 리뷰순으로 정렬된 url
    products_url = 'https://zigzag.kr/categories/-1?title=%EC%9D%98%EB%A5%98&category_id=-1&middle_category_id={id}&sort=201'

    options = Options()
    options.add_argument("--headless")
    with webdriver.Chrome(\
        service=Service(ChromeDriverManager().install(),\
        options=options\
    )) as driver:
        all_links = []
        # for category, id in category_ids.items():
        #     url = products_url.format(id=id)
            
        #     # ## 제품 아이디 불러오는 거 확인
        #     link = get_product_id(driver, url, max_num=1)
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


if __name__ == '__main__':
    # test()
    main()

