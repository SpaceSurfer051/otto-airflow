import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException
import time
import concurrent.futures

# CSV 파일을 읽어 데이터 프레임으로 변환
df = pd.read_csv('products_3.csv', encoding='utf-8-sig')

# description 열만 추출하여 리스트로 변환
description_list = df['description'].tolist()
#테스트 끝나면 이부분삭제###################################
#description_list = description_list[:5]

def get_li_texts(driver, xpath):
    ul_element = driver.find_element(By.XPATH, xpath)
    li_elements = ul_element.find_elements(By.TAG_NAME, 'li')
    return [li.text for li in li_elements]

def visit_website(url):
    option = webdriver.ChromeOptions()
    option.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=option)
    
    size_options = []
    color_options = []
    try:
        driver.get(url)
        time.sleep(2)  # 페이지 로드 대기
        print(f"Visited: {url}")
        
        button_clicked = False
        for n in range(31, 42):
            try:
                xpath = f'//*[@id="root"]/div[{n}]/div/button'
                button = driver.find_element(By.XPATH, xpath)
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(1)  # 스크롤 후 대기
                button.click()
                print(f"Clicked button at div[{n}] on {url}")
                button_clicked = True

                # 다음 단계 수행
                next_n = n + 2
                try:
                    button_1_xpath = f'//*[@id="root"]/div[{next_n}]/div[1]/div[1]/div[1]/div[1]/button[1]'
                    button_2_xpath = f'//*[@id="root"]/div[{next_n}]/div[1]/div[1]/div[1]/div[1]/button[2]'
                    
                    driver.find_element(By.XPATH, button_1_xpath).click()
                    print(f"Clicked button 1 at div[{next_n}] on {url}")
                    test_option_1 = get_li_texts(driver, f'//*[@id="root"]/div[{next_n}]/div[2]/ul')
                    
                    try:
                        driver.find_element(By.XPATH, f'//*[@id="root"]/div[{next_n}]/div[2]/ul/li[1]').click()
                    except NoSuchElementException:
                        pass

                    driver.find_element(By.XPATH, button_2_xpath).click()
                    print(f"Clicked button 2 at div[{next_n}] on {url}")
                    test_option_2 = get_li_texts(driver, f'//*[@id="root"]/div[{next_n}]/div[2]/ul')
                
                except NoSuchElementException:
                    try:
                        button_xpath = f'//*[@id="root"]/div[{next_n}]/div[1]/div[1]/div[1]/div[1]/button'
                        driver.find_element(By.XPATH, button_xpath).click()
                        print(f"Clicked button at div[{next_n}] on {url}")
                        test_option_1 = get_li_texts(driver, f'//*[@id="root"]/div[{next_n}]/div[2]/ul')
                        test_option_2 = ['0']
                    except NoSuchElementException:
                        print(f"No additional buttons found at div[{next_n}] on {url}")
                        test_option_1 = []
                        test_option_2 = []

                print(f"test_option_1 for {url}: {test_option_1}")
                print(f"test_option_2 for {url}: {test_option_2}")
                size_options.append(test_option_1)
                color_options.append(test_option_2)
                break  # 버튼을 클릭했으면 루프를 벗어납니다

            except (NoSuchElementException, TimeoutException):
                continue  # 현재 div[{n}]에서 버튼을 찾지 못했으면 다음으로 넘어갑니다
            except ElementClickInterceptedException:
                print(f"Element click intercepted on {url} at div[{n}], trying next element.")
                continue  # 클릭이 차단된 경우 다음으로 넘어갑니다

        if not button_clicked:
            print(f"No button found on {url}")
            size_options.append([])
            color_options.append([])

    except Exception as e:
        print(f"Failed to visit {url} due to {e}")
        size_options.append([])
        color_options.append([])

    driver.quit()
    return size_options[0], color_options[0]

def visit_websites(description_list):
    size_options = []
    color_options = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(visit_website, url): url for url in description_list}
        for future in concurrent.futures.as_completed(futures):
            url = futures[future]
            try:
                size_option, color_option = future.result()
                size_options.append(size_option)
                color_options.append(color_option)
            except Exception as e:
                print(f"Exception occurred while visiting {url}: {e}")
                size_options.append([])
                color_options.append([])

    return size_options, color_options

# 각 description URL을 순회하며 방문하여 size와 color 정보를 수집
size_options, color_options = visit_websites(description_list)

# 수집한 size와 color 정보를 원래 데이터프레임에 추가
df['size_options'] = size_options
df['color_options'] = color_options

# 업데이트된 데이터프레임을 CSV 파일로 저장
df.to_csv('products_with_size_color.csv', index=False, encoding='utf-8-sig')
print("Data saved to products_with_size_color.csv")

def main():
    size_options, color_options = visit_websites(description_list)
    df['size_options'] = size_options
    df['color_options'] = color_options
    df.to_csv('products_with_size_color_3.csv', index=False, encoding='utf-8-sig')

if __name__ == '__main__':
    main()
