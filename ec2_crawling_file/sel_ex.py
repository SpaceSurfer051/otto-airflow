from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

def run_selenium():
    service = Service('/usr/local/bin/chromedriver')
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # GUI가 없는 서버 환경에서는 headless 모드를 사용
    driver = webdriver.Chrome(service=service, options=options)
    driver.get('https://www.google.com')
    search_box = driver.find_element(By.NAME, 'q')
    search_box.send_keys('Selenium')
    search_box.send_keys(Keys.RETURN)
    time.sleep(5)
    driver.quit()
