import re
import os
import time
import logging
import random
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime

from .utils import setup_driver, save_to_csv, clean_title, result_csv_data


def inven_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작:{search}: {url}")
        wd.get(f'{url}')
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'articleTitle')))
        sleep_random_time = random.uniform(2, 4)
        time.sleep(sleep_random_time)
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        writer_list = []
        title_list = []
        content_list = []
        url_list = []
        search_plt_list = []
        search_word_list = []
        date_list = []
        now_date = []

        raw_title = soup.find('div', class_='articleTitle').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content_tag = soup.find('div', id='powerbbsContent')
        content_text = content_tag.get_text(separator=' ', strip=True)
        content_cleaned = re.sub(r'https?://[^\s]+', '', content_text).strip()
        content_list.append(content_cleaned)
        logging.info("내용 추출 성공 ")

        search_plt_list.append('웹페이지(인벤)')
        url_list.append(url)
        search_word_list.append(search)

        date_str = soup.find('div', class_='articleDate').get_text()
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
        date_list.append(date)
        
        writer_list.append(soup.find('div', class_="articleWriter").get_text().strip())
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
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '3.인벤', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'인벤_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f"저장완료: {file_name}")
        
    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def inven_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'인벤_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"                    인벤 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()
    
    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        page_num = 1
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        
        while True:
            if stop_event.is_set():
                break
            try:
                url_dp1 = f'https://www.inven.co.kr/search/webzine/article/{search}/{page_num}?sDate={start_date_str}&eDate={end_date_str}&dt=s'
                logging.info(f"접속: {url_dp1}")
                wd_dp1.get(url_dp1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'section_body')))
                sleep_random_time = random.uniform(1, 3)
                time.sleep(sleep_random_time)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                noresult = soup_dp1.find('ul', class_='noresult')
                if noresult:
                    break 

                page_tag = soup_dp1.find_all('a', class_="pg")
                page_numbers = [int(tag.text) for tag in page_tag if tag.text.isdigit()]
                max_page_num = max(page_numbers) if page_numbers else 1

                li_tags = soup_dp1.find('ul', class_='news_list').find_all('li')
                logging.info(f"검색결과 찾음")
                
                for li in li_tags:
                    if stop_event.is_set():
                        break
                    url = li.find('a', class_='name').get('href')
                    logging.info(f"url 찾음 : {url}")
                    inven_crw(wd, url, search, target_date)

                if page_num >= max_page_num:
                    break

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break
            page_num += 1
            
    wd.quit()
    wd_dp1.quit()

    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '인벤')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='인벤', subdir=f'3.인벤/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'인벤_raw data_{target_date}.csv'), encoding='utf-8', index=False)