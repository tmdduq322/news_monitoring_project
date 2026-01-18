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


def dp_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.ID, 'resContents')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []

        content_div = soup.find('div', id='resContents')
        title_tags = soup.find_all('h1', id='writeSubject')
        for title_tag in title_tags:
            for span in title_tag.find_all('span'):
                span.extract()
            raw_title = title_tag.get_text(strip=True)
            cleaned_title = clean_title(raw_title)
            title_list.append(cleaned_title)
            logging.info(f"제목 추출 성공: {cleaned_title}")

        for a_tag in content_div.find_all('a'):
            a_tag.decompose()

        post_content = content_div.get_text(separator=' ', strip=True)
        post_content = re.sub(r'https?://[^\s]+', '', post_content)
        content_list.append(post_content)
        logging.info(f"내용 추출 성공: {post_content}")

        search_plt_list.append('웹페이지(DVD프라임)')
        url_list.append(url)
        search_word_list.append(search)

        date_str = soup.find('div', id='view_datetime').get_text(strip=True).split(' ')[0]
        date = datetime.strptime(date_str, '%Y-%m-%d')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date_str}")

        writer_list.append(soup.find('span', class_='member').get_text(strip=True))
        now_time = datetime.now().strftime('%Y-%m-%d ')
        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": now_time,
        })

        # [수정] 절대 경로 저장
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '15.DVD프라임', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'DVD프라임_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f'저장 완료: {file_name}')

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def dp_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'DVD프라임_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"            DVD프라임 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()

    category = ['sisa', 'comm', 'humor']
    for cate in category:
        for search in searchs:
            if stop_event.is_set():
                break
            page_num = 1
            while True:
                if stop_event.is_set():
                    break
                try:
                    logging.info(f"크롤링 시작-검색어: {search} / 카테고리: {cate}")
                    url = f'https://dprime.kr/g2/bbs/board.php?bo_table={cate}&sca=&sfl=wr_subject%7C%7Cwr_content&stx={search}&sop=and&page={page_num}'
                    wd_dp1.get(url)
                    WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.ID, 'list_table')))
                    time.sleep(1)
                    soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                    div_tags = soup_dp1.find('div', id='list_table').find_all('div', attrs={'class': ['relative', 'list_table_row']})
                    
                    for div in div_tags:
                        if stop_event.is_set():
                            break
                        after_start_date = False

                        try:
                            date_str = '20' + div.find('span', class_='list_table_dates').text.strip()
                            date = datetime.strptime(date_str, '%Y-%m-%d').date()
                            logging.info(f"날짜 찾음")
                        except Exception as e:
                            logging.error(f"날짜 오류 발생: {e}")
                            continue

                        if date > end_date:
                            continue
                        if date < start_date:
                            after_start_date = True
                            break

                        url = 'https://dprime.kr' + div.find('a', class_='list_subject_a').get('href')
                        logging.info(f"url 찾음.")
                        dp_crw(wd, url, search, target_date)

                    if after_start_date:
                        break

                except Exception as e:
                    logging.error(f"오류 발생: {e}")
                    break

                page_num += 1
                page_list = []
                max_page = 1
                page_tags = soup_dp1.find_all('li', class_='paging_num_li smalleng theme_key2')
                if page_tags:
                    for page in page_tags:
                        page_list.append(int(page.find('a').text))
                        max_page = max(page_list)
                
                if not page_tags or page_num > max_page:
                    try:
                        more_search_btn = EC.presence_of_element_located(By.XPATH, "//a[contains(., '더 검색')]")
                        # more_search_btn.click() # This part in original code was tricky, usually handled by selenium click, simplified here assuming logic is sound or button needs specific handling.
                        # Original used EC but didn't actually find element to click properly in some contexts. Assuming logic:
                        wd_dp1.find_element(By.XPATH, "//a[contains(., '더 검색')]").click()
                        logging.info("'더 검색' 버튼 클릭")
                        WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.ID, 'list_table')))
                        time.sleep(1)
                        if page_tags:
                            page_num = 1
                    except Exception as e:
                        logging.error(f"'더 검색' 버튼 클릭 오류: {e}")
                        break

    wd.quit()
    wd_dp1.quit()
    
    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', 'DVD프라임')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='DVD프라임', subdir=f'15.DVD프라임/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'DVD프라임_raw data_{target_date}.csv'), encoding='utf-8', index=False)