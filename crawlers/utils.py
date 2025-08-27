import os
import re
import logging
import pandas as pd
from datetime import datetime
import undetected_chromedriver as uc
import shutil
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

# 실행날짜 변수 및 폴더 생성
today = datetime.now().strftime("%y%m%d")
if not os.path.exists(f'log'):
    os.makedirs(f'log')

def setup_driver():
    logging.info("웹드라이버 시작")

    # 시스템 기본 위치
    original_path = "/usr/local/bin/chromedriver"
    # airflow가 쓰기 가능한 임시 위치
    safe_path = "/tmp/chromedriver"

    # 아직 복사 안 했으면 복사하고 실행 권한 부여
    if not os.path.exists(safe_path):
        shutil.copy(original_path, safe_path)
        os.chmod(safe_path, 0o755)

    options = Options()
    options.binary_location = "/usr/bin/chromium"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--headless')
    options.add_argument('--disable-blink-features=AutomationControlled')

    service = ChromeService()
    driver = webdriver.Chrome(service=service, options=options)

    return driver

def result_csv_data(search, platform, subdir, base_path='csv'):

    file_path = os.path.join(base_path, subdir, today, f'{platform}_{search}.csv')

    if not os.path.isfile(file_path):
        # print(f"[스킵] 파일이 존재하지 않음: {file_path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        return df
    except Exception as e:
        print(f"[오류] CSV 읽기 실패 ({file_path}): {e}")
        return pd.DataFrame()

# csv 저장
def save_to_csv(df, file_name):
    try:
        if os.path.isfile(file_name):
            # 기존 파일이 존재하는 경우
            df.to_csv(file_name, mode='a', header=False, index=False, encoding='utf-8')
        else:
            # 새로운 파일을 만드는 경우, 헤더 포함
            df.to_csv(file_name, index=False, encoding='utf-8')
        print(f"저장완료 : {file_name}")
    except Exception as e:
        print(f"파일 저장 오류: {e}")


def clean_title(title):
    # 제목 뒤 넘버링 제거
    title = re.sub(r'\d+$', '', title).strip()
    # 파일 확장자 제거 (.jpg, .mp4 등)
    title = re.sub(r'\.(jpg|png|gif|mp4|avi|mkv|webm|jpeg)$', '', title, flags=re.IGNORECASE).strip()
    # 초성 제거 (자음만 있는 경우)
    title = re.sub(r'^[ㄱ-ㅎㅏ-ㅣ]+$', '', title).strip()
    # 따옴표 제거
    title = title.replace('"', '').strip()
    return title

