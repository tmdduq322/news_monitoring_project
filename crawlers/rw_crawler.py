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


def rw_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(random.uniform(1, 4))
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'view_content.autolink')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        writer_list = []
        title_list = []
        content_list = []
        url_list = []
        search_plt_list = []
        search_word_list = []
        date_list = []

        content_div = soup.find('div', class_='view_content autolink')

        raw_title = soup.find('span', class_='subject_inner_text').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content_tag = soup.find('div', class_='view_content autolink')
        content_text = content_tag.get_text(separator=' ', strip=True)
        content_cleaned = re.sub(r'https?://[^\s]+', '', content_text).strip()

        link_box_tag = soup.find('div', class_='source_url box_line_with_shadow')
        extra = ""
        if link_box_tag:
            extra_text = link_box_tag.get_text(separator=' ', strip=True)
            extra = re.sub(r'https?://[^\s]+', '', extra_text).strip()

        full_content = f"{content_cleaned} {extra}".strip()
        content_list.append(full_content)
        logging.info("내용 추출 성공 (URL 제거 + 띄어쓰기 유지)")

        search_plt_list.append('웹페이지(루리웹)')
        url_list.append(url)
        search_word_list.append(search)

        rw_date_str = soup.find('span', class_='regdate').text.strip().split(' ')[0]
        date_list.append(rw_date_str)
        writer_list.append(soup.find('a', class_='nick').get_text())

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
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '4.루리웹', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'루리웹_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f"저장완료: {file_name}")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def rw_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'루리웹_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"            루리웹 크롤링 시작 (Date: {target_date})")
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
                url_dp1 = f'https://bbs.ruliweb.com/search?q={search}&page={page_num}#board_search&gsc.tab=0&gsc.q={search}&gsc.page=1'
                wd_dp1.get(url_dp1)
                wd_dp1.refresh()

                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.ID, 'board_search')))
                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
                li_tags = soup_dp1.find('div', id='board_search').find_all('li', class_="search_result_item")

                for li in li_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False

                    try:
                        date_str = li.find('span', class_='time').get_text()
                        date = datetime.strptime(date_str, '%Y.%m.%d').date()
                        logging.info(f"날짜 찾음")
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url = li.find('a', class_='title text_over').get('href')
                    logging.info(f"url 찾음.")
                    rw_crw(wd, url, search, target_date)

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
        result_dir = os.path.join(project_root, '결과', '루리웹')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='루리웹', subdir=f'4.루리웹/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'루리웹_raw data_{target_date}.csv'), encoding='utf-8', index=False)