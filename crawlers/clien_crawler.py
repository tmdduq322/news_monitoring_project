import re
import os
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from datetime import datetime

from .utils import setup_driver, save_to_csv, clean_title, result_csv_data


def clien_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")

        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'post_content')))
        time.sleep(1)
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        writer_list = []
        title_list = []
        content_list = []
        url_list = []
        search_plt_list = []
        search_word_list = []
        date_list = []
        now_date = []

        raw_title = soup.find('h3', class_='post_subject').find('span').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        try:
            content_div = soup.find('div', class_='post_content')
            content = content_div.get_text(separator=' ', strip=True)
            content_cleaned = re.sub(r'https?://[^\s]+', '', content).strip()
            content_list.append(content_cleaned)
            logging.info("내용 추출 성공")
        except Exception as e:
            content_list.append('')
            logging.error(f"본문 추출 실패: {e}")

        search_plt_list.append('웹페이지(클리앙)')
        url_list.append(url)
        search_word_list.append(search)

        date_str = soup.find(class_="view_count date").text.strip()
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', date_str)
        date = date_match.group()
        date_list.append(date)

        writer_tag = soup.find('span', class_='nickname')
        writer_strip = ' '.join(writer_tag.text.split())
        writer_list.append(writer_strip)

        now_date.append(datetime.now().strftime('%Y-%m-%d'))

        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": now_date,
        })

        # [수정] 절대 경로 저장
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '2.클리앙', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'클리앙_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f"저장완료: {file_name}")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def clien_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'클리앙_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"            클리앙 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()

    for search in searchs:
        page_num = 1
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        while True:
            try:
                url_dp1 = f'https://www.clien.net/service/search?q={search}&sort=recency&p={page_num}&boardCd=&isBoard=false'
                wd_dp1.get(url_dp1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'nav_content')))
                time.sleep(1)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                li_tags = soup_dp1.find_all('div', class_='list_item symph_row jirum')
                if not li_tags:
                    break
                for li in li_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False

                    try:
                        date_str = li.find('div', class_='list_time').find('span', class_="timestamp").get_text()
                        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').date()
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url_dp2_num = li.find('a', class_='subject_fixed').get('href')
                    url = 'https://www.clien.net' + url_dp2_num
                    logging.info(f"url 찾음.")
                    clien_crw(wd, url, search, target_date)

                if after_start_date:
                    break
                elif page_num == 50:
                    break
                else:
                    page_num += 1

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break

    wd.quit()
    wd_dp1.quit()
    
    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '클리앙')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='클리앙', subdir=f'2.클리앙/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'클리앙_raw data_{target_date}.csv'), encoding='utf-8', index=False)