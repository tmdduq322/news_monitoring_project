import os
import re
import logging
import pandas as pd
from datetime import datetime
import undetected_chromedriver as uc
import shutil

# 👇 [필수] 이 줄이 빠져 있어서 에러가 났습니다. 꼭 추가하세요!
from selenium import webdriver 
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# 실행날짜 변수 및 폴더 생성
today = datetime.now().strftime("%y%m%d")
if not os.path.exists(f'log'):
    os.makedirs(f'log')

def setup_driver():
    logging.info("웹드라이버 시작")

    options = Options()
    # options.binary_location = "/usr/bin/chromium" # 필요시 주석 해제
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--headless') # 디버깅 시에는 주석 처리 가능
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.page_load_strategy = 'eager'

    # [핵심 수정] webdriver_manager가 알아서 버전을 맞춰 설치하게 함
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # 탐지 방지 설정
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
    })
        
    return driver

# ... (나머지 result_csv_data, save_to_csv, clean_title 함수는 그대로 유지) ...
def result_csv_data(search, platform, subdir, base_path='csv'):
    file_path = os.path.join(base_path, subdir, today, f'{platform}_{search}.csv')
    if not os.path.isfile(file_path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        return df
    except Exception as e:
        print(f"[오류] CSV 읽기 실패 ({file_path}): {e}")
        return pd.DataFrame()

def save_to_csv(df, file_name):
    try:
        if os.path.isfile(file_name):
            df.to_csv(file_name, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df.to_csv(file_name, index=False, encoding='utf-8')
        print(f"저장완료 : {file_name}")
    except Exception as e:
        print(f"파일 저장 오류: {e}")

def clean_title(title):
    title = re.sub(r'\d+$', '', title).strip()
    title = re.sub(r'\.(jpg|png|gif|mp4|avi|mkv|webm|jpeg)$', '', title, flags=re.IGNORECASE).strip()
    title = re.sub(r'^[ㄱ-ㅎㅏ-ㅣ]+$', '', title).strip()
    title = title.replace('"', '').strip()
    return title