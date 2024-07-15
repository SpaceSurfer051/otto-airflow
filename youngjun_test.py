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
            print(f"review_id: {review_id}")

            weight_height_gender = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[1]/div/div[2]/p[1]'))).text
            print(f"weight_height_gender: {weight_height_gender}")
            
            top_size = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[1]/ul/li[1]/span'))).text
            print(f"top_size: {top_size}")
            brightness_comment = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[1]/ul/li[2]/span'))).text
            print(f"brightness_comment: {brightness_comment}")
            
            color_comment = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[1]/ul/li[3]/span'))).text
            print(f"color_comment: {color_comment}")
            
            thickness_comment = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[1]/ul/li[4]/span'))).text
            print(f"thickness_comment: {thickness_comment}")
            
            purchased_product_id = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[2]/div[2]/a'))).get_attribute('href')
            print(f"purchased_product_id: {purchased_product_id}")
            
            purchased_size = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[2]/div[2]/p/span'))).text
            print(f"purchased_size: {purchased_size}")
            
            comment = wait.until(EC.presence_of_element_located((By.XPATH, f'//*[@id="style_estimate_list"]/div/div[{n}]/div[4]/div[2]'))).text
            print(f"comment: {comment}")

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

def get_product_info(driver, wait, href_links):
    products = []
    for index, link in enumerate(href_links):
        driver.get(link)
        time.sleep(2)  # 페이지 로드 대기

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

            # 사이즈 정보 가져오기
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

            # 리뷰 정보 가져오기
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
            
            # 데이터 출력
            print(product)

        except Exception as e:
            print(f"Failed to extract data for link: {link} due to {e}")
            continue
    
    return products

def save_to_csv(products, filename="products.csv"):
    df = pd.DataFrame(products)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Data saved to {filename}")

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
    print(f"Number of unique href links: {len(href_links)}")

    products = get_product_info(driver, wait, href_links)
    print("Products extracted:")
    for product in products:
        print(product)

    save_to_csv(products)

    # driver.quit()

if __name__ == '__main__':
    main()
'''
구조 - product
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
구조 - review           
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



예시 출력 - review
review_id: LV.3 눅눅감자
weight_height_gender: 여성 · 164cm · 74kg
top_size: 보통이에요
brightness_comment: 보통이에요
color_comment: 보통이에요
thickness_comment: 보통이에요
purchased_product_id: https://www.musinsa.com/app/goods/2612829/0
purchased_size: 화이트/M
comment: 작을까봐 걱정했는데 딱 맞아서 좋아요!! 원단도 두껍지 않아서 여름에 잘 입을 것 같습니당◠‿◠
review_id: LV.3 김떡뻥
weight_height_gender: 여성 · 160cm · 57kg
top_size: 보통이에요
brightness_comment: 보통이에요
color_comment: 선명해요
thickness_comment: 얇아요
purchased_product_id: https://www.musinsa.com/app/goods/2612829/0
purchased_size: 화이트/S
comment: 🌱 사이즈: 원래 오버핏을 즐겨입는 편인데 이거는 남녀공용이라그런지 S사이즈 입어도 오버핏이 아주 이쁘고 낭낭하게 잘 나와요🥹
🌱 재질, 마감: 일반적인 면 재질이고 마감 깔끔하고 목부분이 짱짱한게 입을때마다 느껴져요(이거 정말 중요한 디테일인거 아시죠)
🌱 착용 후 느낀점 : 기본템이라 손이 엄청 자주가요. 일주일 내내 두 컬러 돌려입고 있어요ㅎㅎ 어느 하의랑 매치해도 컬러감이 아주 찰떡으로 잘 어울리고 너무 이쁩니다. 그리고 팔 부분 두 번 접어 입으면 느낌이 좀 더 활동적이고 핏이 이뻐져요. 다들 접어서 입어보세요🤭 날이 좀 더 시원해지면 이너티로 스타일링 해도 아주그냥 이쁠거같아요.
🌱 가성비: 구매할 때 티 2개를 3만원 대에 구매해서 이렇게 매일 입는거보면 가성비 탑입니다🥹 이런 기본티는 세일이나 특가 뜰 때마다 쟁여놔야 하는거 아시죠?! 꼭 사세요 다덜❤️



예시 출력 - products

{'product_id': 'top4', 
'product_name': '[베이직 OR 링거 선택] [SET] CGP 아치 로고 티셔츠', 
'category': '상의', 
'price': '45,900원',
'image_url': 'https://image.msscdn.net/images/goods_img/20220614/2612829/2612829_17193674550457_500.jpg', 
'description': 'https://www.musinsa.com/app/goods/2612829', '
size': ['S', 'M', 'L', 'XL']


아직 색깔정보 미포함했고, 테스트코드라서 쓸대없는 print가 많습니다.

'''
