import os
import re
import time
import random
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime

from .utils import setup_driver, save_to_csv, clean_title, result_csv_data


def dc_crw(wd, url, search, target_date):
    try:
        wd.get(f'{url}')
        sleep_random_time = random.uniform(2, 4)
        time.sleep(sleep_random_time)
        WebDriverWait(wd, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".view_content_wrap")))

        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []

        raw_title = soup.find('h3', class_='title ub-word').find('span', class_='title_subject').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content_div = soup.find('div', class_='write_div')
        for og_tag in content_div.find_all('a', class_='og-wrap'):
            og_tag.decompose()

        for a_tag in content_div.find_all('a'):
            if (not a_tag.find('img') and not a_tag.find('span', class_='scrap_img') and
                not a_tag.find('video') and not (a_tag.find('iframe') and 'youtube.com' in a_tag.decode_contents())):
                a_tag.decompose()

        post_content = content_div.get_text(separator='\n', strip=True)
        post_content = re.sub(r'http[s]?://\S+', '', post_content)
        content_list.append(post_content)

        search_plt_list.append('웹페이지(dcinside)')
        url_list.append(url)
        search_word_list.append(search)

        date_str = soup.find('span', class_='gall_date').text
        date = datetime.strptime(date_str, '%Y.%m.%d %H:%M:%S').date()
        date_list.append(date)

        nickname = soup.find('span', class_='nickname').get_text()
        ip_tag = soup.find('span', class_='ip')
        ip_address = ip_tag.get_text() if ip_tag else ''
        writer = f"{nickname}{ip_address}"
        writer_list.append(writer)

        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
        })

        # [수정] 절대 경로 저장
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '22.디시인사이드', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'디시인사이드_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f"저장완료: {file_name}")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def dc_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'디시인사이드_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"                 디시인사이드 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()
    
    for search in searchs:
        page_num = 1
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        while True:
            if stop_event.is_set():
                break
            try:
                if page_num == 121:
                    break
                url_dp1 = f'https://search.dcinside.com/post/p/{page_num}/sort/latest/q/{search}'
                wd_dp1.get(url_dp1)
                sleep_random_time = random.uniform(2, 4)
                time.sleep(sleep_random_time)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                li_tags = soup_dp1.find('ul', class_='sch_result_list').find_all('li')

                for li in li_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False

                    try:
                        date_str = li.find('span', class_='date_time').text
                        date = datetime.strptime(date_str, '%Y.%m.%d %H:%M').date()
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url = li.find('a', class_='tit_txt').get('href')
                    logging.info(f"url 찾음.")
                    dc_crw(wd, url, search, target_date)

                if after_start_date:
                    break
                else:
                    page_num += 1

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break

    wd.quit()
    wd_dp1.quit()

    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '디시인사이드')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='디시인사이드', subdir=f'22.디시인사이드/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'디시인사이드_raw data_{target_date}.csv'), encoding='utf-8', index=False)