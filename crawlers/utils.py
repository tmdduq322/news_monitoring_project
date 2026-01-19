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
    
    # [중요] 페이지 로딩 전략: Eager (내용만 뜨면 진행)
    options.page_load_strategy = 'eager'
    
    # [핵심 수정] User-Agent 강제 설정 삭제!
    # Dockerfile이 최신 크롬을 설치하므로, 셀레니움이 알아서 최신 UA를 쓰게 둬야 합니다.
    # 대신 한국어 설정은 필수입니다 (루리웹 차단 방지)
    options.add_argument("--lang=ko_KR")
    options.add_argument("accept-language=ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
    
    # Docker/Server 환경 필수 설정
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--headless=new') 
    
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--start-maximized')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    try:
        # ChromeDriverManager가 설치된 크롬 버전을 감지해 맞는 드라이버를 가져옴
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # 기본 타임아웃 설정
        driver.set_page_load_timeout(30)
        
        # 봇 탐지 스크립트 우회
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            """
        })
        
        return driver
        
    except Exception as e:
        logging.error(f"❌ 웹드라이버 실행 실패: {e}")
        # 캐시 충돌 방지용 삭제 로직
        wdm_cache = os.path.expanduser("~/.wdm")
        if os.path.exists(wdm_cache):
            shutil.rmtree(wdm_cache)
        raise e
    
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