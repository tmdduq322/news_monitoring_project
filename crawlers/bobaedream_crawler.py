import os
import re
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


def bobaedream_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'content02')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        current_date_list = []

        raw_title = soup.find('div', class_='writerProfile').find('dt').get('title')
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content_tag = soup.find('div', class_='bodyCont')
        content_text = content_tag.get_text(separator=' ', strip=True)
        content_cleaned = re.sub(r'https?://[^\s]+', '', content_text).strip()
        content_list.append(content_cleaned)

        search_plt_list.append('웹페이지(보배드림)')
        url_list.append(url)
        search_word_list.append(search)

        date_str_tag = soup.find('div', class_='writerProfile').find('span', class_='countGroup').text
        date_str = re.search(r'\d{4}\.\d{2}\.\d{2}', date_str_tag).group()
        date = datetime.strptime(date_str, '%Y.%m.%d')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date_str}")

        writer_list.append(soup.find('dd', class_='proflieInfo').find_all('li')[0].find('span', class_='proCont').get_text().lstrip())

        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list
        })

        # [수정] 절대 경로 저장
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '8.보배드림', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'보배드림_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f'저장 완료: {file_name}')

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def bobaedream_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'보배드림_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"            보배드림 크롤링 시작 (Date: {target_date})")
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
                logging.info(f"크롤링 시작-검색어: {search}")
                wd_dp1.get('https://www.bobaedream.co.kr')
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'gnb-container')))
                time.sleep(1)

                try:
                    search_button = wd_dp1.find_element(By.CSS_SELECTOR, "button.square-util.btn-search.js-btn-srch")
                    search_button.click()
                    keyword_input = wd_dp1.find_element(By.ID, "keyword")
                    keyword_input.send_keys(search)
                    submit_button = wd_dp1.find_element(By.CSS_SELECTOR, "button.btn-submit")
                    submit_button.click()
                except Exception as e:
                    logging.error(f"검색 실패: {e}")

                time.sleep(1)
                community_btn = wd_dp1.find_element(By.XPATH, "//div[@class='lnb']//a[contains(text(), '커뮤니티')]")
                community_btn.click()
                time.sleep(1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'search_Community')))
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                li_tags = soup_dp1.find('div', class_='search_Community').find_all('li')
                
                while True:
                    if stop_event.is_set():
                        break
                    for li in li_tags:
                        if stop_event.is_set():
                            break
                        after_start_date = False

                        try:
                            date_str = li.find('dd', class_='path').find_all('span', class_='next')[1].text
                            date_str = '20' + date_str
                            date = datetime.strptime(date_str, '%Y. %m. %d').date()
                            logging.info(f"날짜 찾음 : {date}")
                        except Exception as e:
                            logging.error(f"날짜 오류 발생: {e}")
                            continue

                        if date > end_date:
                            continue
                        if date < start_date:
                            after_start_date = True
                            break

                        url = 'https://www.bobaedream.co.kr' + li.find('dt').find('a').get('href')
                        logging.info(f"url 찾음.")
                        bobaedream_crw(wd, url, search, target_date)

                    if after_start_date:
                        break
                    else:
                        try:
                            wd_dp1.find_element(By.CSS_SELECTOR, "a.next").click()
                            time.sleep(1)
                            WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'search_Community')))
                            soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
                            li_tags = soup_dp1.find('div', class_='search_Community').find_all('li')
                            logging.info("다음 페이지로 이동 및 파싱 완료")
                        except Exception as e:
                            logging.error(f"페이징 오류 발생: {e}")
                            break
                break # while loop break

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break

    wd.quit()
    wd_dp1.quit()
    
    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '보배드림')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='보배드림', subdir=f'8.보배드림/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'보배드림_raw data_{target_date}.csv'), encoding='utf-8', index=False)