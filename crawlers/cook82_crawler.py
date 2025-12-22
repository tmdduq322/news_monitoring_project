import os
import re
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime

from .utils import setup_driver, save_to_csv, clean_title, result_csv_data


def cook82_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'wrap')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        current_date_list = []

        content_div = soup.find('div', id='articleBody')

        raw_title = soup.find('h4', class_='title bbstitle').find('span').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        for a_tag in content_div.find_all('a'):
            if (not a_tag.find('img') and not a_tag.find('span', class_='scrap_img') and
                not a_tag.find('video') and not (a_tag.find('iframe') and 'youtube.com' in a_tag.decode_contents())):
                a_tag.decompose()

        post_content = content_div.get_text(separator=' ', strip=True)
        post_content = re.sub(r'https?://[^\s]+', '', post_content)
        content_list.append(post_content)
        logging.info(f"내용 추출 성공 (URL 제거 및 띄어쓰기 유지)")

        search_plt_list.append('웹페이지(82쿡)')
        url_list.append(url)
        search_word_list.append(search)

        date_text = soup.find('div', class_='readRight').get_text(strip=True)
        date_str = date_text.split()[2]
        date = datetime.strptime(date_str, '%Y-%m-%d')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date_str}")

        writer_list.append(soup.find('div', class_='readLeft').find('a').get_text())
        current_date_list.append(datetime.now().strftime('%Y-%m-%d'))

        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": current_date_list,
        })

        # [수정] 절대 경로 저장
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '12.82쿡', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'82쿡_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f"저장완료: {file_name}")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def cook82_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'82쿡_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"            82쿡 크롤링 시작 (Date: {target_date})")
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
                logging.info(f"크롤링 시작-검색어: {search}")
                url = f'https://www.82cook.com/entiz/enti.php?bn=15&searchType=search&search1=1&keys={search}&page={page_num}'
                wd_dp1.get(url)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'skin1')))
                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
                tr_tags = soup_dp1.find('div', id='bbs').find('tbody', ).find_all('tr')
                td_test = soup_dp1.find('div', id='bbs').find('tbody', ).find('tr').find('td', class_='title')
                if not td_test:
                    break
                
                for tr in tr_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False

                    if 'noticeList' in tr.get('class', []):
                        continue

                    try:
                        date_str = tr.find('td', class_='regdate numbers').text
                        date = datetime.strptime(date_str, '%Y/%m/%d').date()
                        logging.info(f"날짜 찾음")
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url_str = tr.find('td', class_='title').find('a').get('href')
                    url = 'https://www.82cook.com/entiz/' + url_str
                    logging.info(f"url 찾음.")
                    cook82_crw(wd, url, search, target_date)

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
        result_dir = os.path.join(project_root, '결과', '82쿡')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='82쿡', subdir=f'12.82쿡/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'82쿡_raw data_{target_date}.csv'), encoding='utf-8', index=False)