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


def scline_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")

        WebDriverWait(wd, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'txtBox')))
        time.sleep(2)
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []

        content_div = soup.find('div', class_='txtBox')
        raw_title = soup.find('div', class_='titBox').find('h2').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content_div = soup.find('div', class_='txtBox')
        post_content = content_div.get_text(separator='\n', strip=True)
        post_content = re.sub(r'https?://\S+', '', post_content)
        post_content = re.sub(r'\n+', '\n', post_content).strip()
        content_list.append(post_content)
        logging.info(f"내용 추출 성공: {post_content}")

        search_plt_list.append('웹페이지(사커라인)')
        url_list.append(url)
        search_word_list.append(search)

        date_tag = soup.find('div', class_='dataBox').find_all('span')[0]
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', date_tag.text)

        if date_match:
            date_str = date_match.group(0)
            date = datetime.strptime(date_str.split()[0], '%Y-%m-%d')
            date_list.append(date)
            logging.info(f"날짜 추출 성공: {date_str}")
        else:
            logging.error('날짜를 찾을 수 없음.')

        writer_list.append(soup.find('div', class_='nameBox').get_text())
        
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
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '16.사커라인', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'사커라인_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f"저장완료: {file_name}")

    except TimeoutException as e:
        logging.error(f"페이지 로딩 시간 초과: {e}")
        return None
    except WebDriverException as e:
        logging.error(f"웹드라이버 에러: {e}")
        return None
    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return None


def scline_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'사커라인_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"            사커라인 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()
    
    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        page_num = 0
        no_search_flag = True
        
        while True:
            if stop_event.is_set():
                break
            try:
                logging.info(f"크롤링 시작-검색어: {search}")
                url = f'https://soccerline.kr/board?page={page_num}&categoryDepth01=0&searchWindow=&searchType=0&searchText={search}'
                wd_dp1.get(url)

                WebDriverWait(wd_dp1, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'brdList')))
                time.sleep(5)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
                td_tags = soup_dp1.find('div', id='boardListContainer').find_all('tr')[2:]
                
                no_search_flag = False
                if not td_tags:
                    break

                for td in td_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False

                    try:
                        date_str = td.find_all('td')[3].text
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

                    url = 'https://soccerline.kr' + td.find('td', class_='desc').find('a').get('href')
                    logging.info(f"url 찾음.")
                    scline_crw(wd, url, search, target_date)

                if no_search_flag:
                    break
                if len(td_tags) < 25:
                    break
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
        result_dir = os.path.join(project_root, '결과', '사커라인')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='사커라인', subdir=f'16.사커라인/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'사커라인_raw data_{target_date}.csv'), encoding='utf-8', index=False)