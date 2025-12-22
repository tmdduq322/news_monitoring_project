import os
import re
import random
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime

from .utils import setup_driver, save_to_csv, clean_title, result_csv_data


def mlb_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작:{search}: {url}")
        wd.get(f'{url}')
        sleep_random_time = random.uniform(2, 4)
        time.sleep(sleep_random_time)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'ar_txt')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        writer_list = []
        title_list = []
        content_list = []
        url_list = []
        search_plt_list = []
        search_word_list = []
        date_list = []
        now_date = []

        raw_title = soup.find('div', class_='titles').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content_tag = soup.find('div', class_='ar_txt')
        content_text = content_tag.get_text(separator=' ', strip=True)
        content_cleaned = re.sub(r'https?://[^\s]+', '', content_text).strip()
        content_list.append(content_cleaned)
        logging.info("내용 추출 성공 ")

        search_plt_list.append('웹페이지(엠엘비파크)')
        url_list.append(url)
        search_word_list.append(search)

        date_str = soup.find('div', class_='val').find('span').get_text()
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        date_list.append(date)

        writer_list.append(soup.find('strong', class_="nick").get_text().strip())
        now_date.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            '게시물 내용': content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": now_date,
        })

        # [수정] 절대 경로 및 target_date 사용
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '21.엠엘비파크', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'엠엘비파크_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f'저장 완료: {file_name}')

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def mlb_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'엠엘비파크_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"                  엠엘비파크 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()
    
    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        page_num = 1
        
        while True:
            if stop_event.is_set():
                break
            try:
                url_dp1 = f'https://mlbpark.donga.com/mp/b.php?p={page_num}&m=search&b=bullpen&query={search}&select=sct&user='
                logging.info(f"접속: {url_dp1}")
                wd_dp1.get(url_dp1)
                
                # captcha 우회
                try:
                    WebDriverWait(wd_dp1, 2).until(EC.presence_of_element_located((By.ID, 'captcha_wrapper')))
                    logging.warning("reCAPTCHA detected. Please solve it manually.")
                    time.sleep(60) 
                except:
                    pass

                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'tbl_type01')))
                sleep_random_time = random.uniform(2, 4)
                time.sleep(sleep_random_time)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                tr_tags = soup_dp1.find('table', class_='tbl_type01').find('tbody').find_all('tr')
                
                if not tr_tags:
                    break

                for tr in tr_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False
                    
                    try:
                        date_str = tr.find('span', class_='date').text
                        date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue
                        
                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url = tr.find('div', class_='tit').find('a', class_='txt').get('href')
                    logging.info(f"url 찾음.")
                    mlb_crw(wd, url, search, target_date)

                if after_start_date:
                    break
                else:
                    page_num += 30  # 페이지 수 증가

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break

    wd.quit()
    wd_dp1.quit()

    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '엠엘비파크')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='엠엘비파크', subdir=f'21.엠엘비파크/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'엠엘비파크_raw data_{target_date}.csv'), encoding='utf-8', index=False)