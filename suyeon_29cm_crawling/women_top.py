import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

def parse_review(lines):
    """Parse the review lines to extract option, height, weight, and size comment."""
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

def get_product_details(driver, product, product_data):
    """Extract product details and reviews."""
    try:
        product_name = product.find('a', class_='css-5cm1aq').get('title')
        image_link = product.find('img').get('src')
        
        product_data['category'].append("top")
        product_data['product_name'].append(product_name)
        product_data['image_url'].append(image_link)

        # Move to product detail page
        product_link = product.find('a', class_='css-5cm1aq').get('href')
        product_data['description'].append(product_link)
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

        product_data['price'].append(price)
        product_data['color'].append(color)
        product_data['size'].append(size)

        return product_link, product_name
    except Exception as e:
        print(f"Error while fetching product information: {e}")
        return None, None

def get_reviews(driver, product_name, product_link, review_data, retry_list):
    """Extract reviews for the given product."""
    try:
        actions = driver.find_element(By.CSS_SELECTOR, 'body')
        sleep(1)
        actions.send_keys(Keys.END)
        sleep(1)
        wait = WebDriverWait(driver, 10)
        review_list = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'css-31l7gp.eji1c1x1')))
    except Exception as e:
        retry_list.append([product_link, product_name])
        print(f"Error while processing scroll: {e}")
        return

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

def crawling_29cm(max_item):
    """Main crawling function to get product details and reviews."""
    start_time = time.time()
    
    options = webdriver.ChromeOptions()
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    options.add_argument('user-agent=' + user_agent)
    
    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Error initializing WebDriver: {e}")
        return

    product_data = {'product_name': [], 'category' : [], 'price': [], 'image_url': [], 'description' : [], 'color' : [], 'size' : []}
    review_data = {'product_name': [], 'option' : [], 'height': [], 'weight' : [], 'size_comment': [], 'comment' : []}
    retry_list = []

    try:
        start_url = "https://shop.29cm.co.kr/category/list?categoryLargeCode=268100100&categoryMediumCode=268103100&sort=REVIEW&defaultSort=RECOMMEND&sortOrder=DESC&page=1"
        driver.get(start_url)
        sleep(2)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        products = soup.find_all('li', class_='css-1teigi4 e1114pfz0', limit=max_item)
        for product in products:
            product_link, product_name = get_product_details(driver, product, product_data)
            if product_link and product_name:
                get_reviews(driver, product_name, product_link, review_data, retry_list)
                driver.back()
                sleep(1)
    except Exception as e:
        print(f"Error during main crawling process: {e}")
    finally:
        driver.quit()

        try:
            product_df = pd.DataFrame(product_data)
            review_df = pd.DataFrame(review_data)

            product_df.to_csv("29cm_women_top_products.csv", encoding='utf-8-sig', index=False)
            review_df.to_csv("29cm_women_top_reviews.csv", encoding='utf-8-sig', index=False)
        except Exception as e:
            print(f"Error while saving CSV files: {e}")

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"크롤링에 걸린 시간: {elapsed_time}초")
        
        return retry_list
    
def retry_once_fail(retry_list):
    """Retry to get reviews for failed products."""
    options = webdriver.ChromeOptions()
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    options.add_argument('user-agent=' + user_agent)
    
    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Error initializing WebDriver in retry_once_fail: {e}")
        return
    
    review_data = {'product_name': [], 'option' : [], 'height': [], 'weight' : [], 'size_comment': [], 'comment' : []}

    for retry_link in retry_list:
        print(retry_link)
        sleep(1)
        driver.get(retry_link[0])
        sleep(1)                
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        sleep(1)
        
        try:
            actions = driver.find_element(By.CSS_SELECTOR, 'body')
            sleep(3)
            actions.send_keys(Keys.END)
            sleep(3)
            wait = WebDriverWait(driver, 20)
            review_list = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'css-31l7gp.eji1c1x1')))
        except Exception as e:
            review_list = []
            print(f"Error while processing scroll: {e}")

        for review in review_list:
            try:
                divs = review.find_element(By.CLASS_NAME, 'css-4oetsc.eji1c1x9').text
                comment = review.find_element(By.TAG_NAME, 'p').text

                lines = divs.split('\n')
                option, height, weight, size_comment = parse_review(lines)
                
                review_data['product_name'].append(retry_link[1])
                review_data['option'].append(option)
                review_data['height'].append(height)
                review_data['weight'].append(weight)
                review_data['size_comment'].append(size_comment)
                review_data['comment'].append(comment)
            except Exception as e:
                print(f"Error while processing review: {e}")

    driver.quit()
    sleep(1)
    
    review_df = pd.DataFrame(review_data)
    review_df.to_csv("29cm_women_top_reviews_retry.csv", encoding='utf-8-sig', index=False)

if __name__ == '__main__':
    max_item = 5
    retry = crawling_29cm(max_item)
    retry_once_fail(retry)
