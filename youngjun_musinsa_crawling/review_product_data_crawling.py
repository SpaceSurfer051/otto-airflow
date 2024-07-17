from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
import concurrent.futures
from multiprocessing import Value, Lock


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

def extract_reviews(driver, wait):
    actions = driver.find_element(By.CSS_SELECTOR, 'body')
    time.sleep(1)
    actions.send_keys(Keys.END)
    time.sleep(2)
    actions.send_keys(Keys.END)
    time.sleep(2)
    actions.send_keys(Keys.END)
    time.sleep(1)

    reviews = []
    for n in range(3, 10):
        try:
            review_id = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[1]/div/div[1]/p[1]'))).text
            #print(f"review_id: {review_id}")
            
            try:
                weight_height_gender = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[1]/div/div[2]/p[1]'))).text
                #print(f"weight_height_gender: {weight_height_gender}")
            except:
                weight_height_gender = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[1]/div/div[2]/p'))).text
                #print(f"weight_height_gender (alternative): {weight_height_gender}")
            
            top_size = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[1]/ul/li[1]/span'))).text
            #print(f"top_size: {top_size}")
            
            brightness_comment = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[1]/ul/li[2]/span'))).text
            #print(f"brightness_comment: {brightness_comment}")
            
            color_comment = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[1]/ul/li[3]/span'))).text
            #print(f"color_comment: {color_comment}")
            
            thickness_comment = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[1]/ul/li[4]/span'))).text
            #print(f"thickness_comment: {thickness_comment}")
            
            purchased_product_id = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[2]/div[2]/a'))).get_attribute('href')
            #print(f"purchased_product_id: {purchased_product_id}")
            
            purchased_size = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[2]/div[2]/p/span'))).text
            #print(f"purchased_size: {purchased_size}")
            
            comment = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[2]'))).text
            #print(f"comment: {comment}")

            review = {
                "weight_height_gender": weight_height_gender,
                "review_id": review_id,
                "top_size": top_size,
                "brightness_comment": brightness_comment,
                "color_comment": color_comment,
                "thickness_comment": thickness_comment,
                "purchased_product_id": purchased_product_id,
                "purchased_size": purchased_size,
                "comment": comment
            }
            reviews.append(review)
        except (NoSuchElementException, TimeoutException):
            print(f"Review information not found for element index: {n}")
            continue
    
    return reviews

def get_product_info(driver, wait, href_links, count, lock):
    products = []
    for index, link in enumerate(href_links):
        driver.get(link)
        time.sleep(2)

        try:
            product_id = f"top{index + 1}"
            
            try:
                product_name = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="root"]/div[3]/h2'))).text
            except (NoSuchElementException, TimeoutException):
                product_name = "N/A"
                print(f"Product name not found for link: {link}")
                
            try:
                category = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="root"]/div[2]/a[1]'))).text
            except (NoSuchElementException, TimeoutException):
                category = "N/A"
                print(f"Category not found for link: {link}")

            try:
                price = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="root"]/div[5]/div/div/span'))).text.strip()
            except (NoSuchElementException, TimeoutException):
                price = "N/A"
                print(f"Price not found for link: {link}")

            try:
                image_element = wait.until(EC.presence_of_element_located((By.XPATH, '//img[@class="sc-1jl6n79-4 AqRjD"]')))
                image_url = image_element.get_attribute('src')
            except (NoSuchElementException, TimeoutException):
                image_url = "N/A"
                print(f"Image URL not found for link: {link}")

            description = link

            sizes = []
            try:
                ul_element = wait.until(EC.presence_of_element_located((By.XPATH, '//ul[contains(@class, "sc-1sxlp32-1") or contains(@class, "sc-8wsa6t-1 Qtsoe")]')))
                li_elements = ul_element.find_elements(By.TAG_NAME, 'li')
                for li in li_elements:
                    size_text = li.text.strip()
                    if size_text not in ["cm", "MY"]:
                        sizes.append(size_text)
            except (NoSuchElementException, TimeoutException):
                print(f"Size information not found for link: {link}")

            reviews = extract_reviews(driver, wait)

            product = {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "price": price,
                "image_url": image_url,
                "description": description,
                "size": sizes,
                "reviews": reviews
            }
            products.append(product)
            
            print(product)

        except Exception as e:
            print(f"Failed to extract data for link: {link} due to {e}")
            continue

        with lock:
            count.value += 1
            print(f"Processed {count.value} products")

    return products

def save_to_csv(products, filename="products_3.csv"):
    df = pd.DataFrame(products)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Data saved to {filename}")

def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def process_links(links, count, lock):
    option = webdriver.ChromeOptions()
    option.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=option)
    wait = WebDriverWait(driver, 10)

    products = get_product_info(driver, wait, links, count, lock)
    driver.quit()
    return products

def main():
    URL = "https://www.musinsa.com/categories/item/001?device=mw&sortCode=emt_high"
    option = webdriver.ChromeOptions()
    option.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=option)
    driver.get(URL)
    time.sleep(3)

    driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[3]/div/button').click()
    time.sleep(1)

    wait = WebDriverWait(driver, 10)
    actions = driver.find_element(By.CSS_SELECTOR, 'body')

    href_links = get_href_links(driver, wait, actions, num_items_to_fetch=100)
    driver.quit()

    sub_lists = split_list(href_links, 4)

    count = Value('i', 0)
    lock = Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_links, sub_list, count, lock) for sub_list in sub_lists]
        all_products = []
        for future in concurrent.futures.as_completed(futures):
            products = future.result()
            all_products.extend(products)

    save_to_csv(all_products)
    #color_size_crawling.main()

if __name__ == '__main__':
    main()
