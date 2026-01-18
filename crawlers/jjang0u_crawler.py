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


def jjang0u_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.ID, 'container')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []

        content_div = soup.find('section', id='post_content')

        raw_title = soup.find('h2', id='view_title').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        for a_tag in content_div.find_all('a'):
            a_tag.decompose()

        post_content = content_div.get_text(separator='\n', strip=True)
        post_content = re.sub(r'https?://\S+', '', post_content)
        post_content = re.sub(r'\n{2,}', '\n', post_content).strip()

        content_list.append(post_content)
        logging.info(f"내용 추출 성공: {post_content}")

        search_plt_list.append('웹페이지(짱공유닷컴)')
        url_list.append(url)
        search_word_list.append(search)

        date_str = soup.find('div', class_='left').find('span', class_='date').text
        date_re_str = re.search(r'작성일 (\d{2}\.\d{2}\.\d{2})', date_str)
        original_date = date_re_str.group(1)
        formatted_date = datetime.strptime(original_date, '%y.%m.%d').strftime('%Y-%m-%d')
        date_list.append(formatted_date)
        logging.info(f"날짜 추출 성공: {formatted_date}")

        writer_list.append(soup.find('div', class_='left').find('span', class_='global-nick').find('a').text)
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

        # [수정] 절대 경로 및 target_date 사용
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '19.짱공유닷컴', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'짱공유닷컴_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f'저장 완료: {file_name}')

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def jjang0u_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'짱공유닷컴_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"                    짱공유닷컴 크롤링 시작 (Date: {target_date})")
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
            after_start_date = False
            try:
                logging.info(f"크롤링 시작-검색어: {search}")
                url = f'https://www.jjang0u.com/search/doc?q={search}&page={page_num}'
                wd_dp1.get(url)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.ID, 'search-container')))
                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
                li_tags = soup_dp1.find('ul', class_='search-result__list search-result__document').find_all('li')
                logging.info(f"검색목록 찾음.")

                if not li_tags:
                    break

                for li in li_tags:
                    if stop_event.is_set():
                        break
                    try:
                        date_str = li.find('span', class_='date').text.strip()
                        match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)

                        if match:
                            original_date = match.group(1)
                            date = datetime.strptime(original_date, '%Y-%m-%d').date()
                            logging.info(f"[{search}] 파싱된 날짜: {date}")
                        else:
                            logging.warning(f"[{search}] 날짜 매칭 실패: {date_str}")
                            continue

                    except Exception as e:
                        logging.error(f"[{search}] 날짜 오류: {e}")
                        continue

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url = 'https://www.jjang0u.com' + li.find('a', class_='title').get('href')
                    logging.info(f"url 찾음.")
                    jjang0u_crw(wd, url, search, target_date)

                if after_start_date:
                    break
                else:
                    page_num += 1

            except Exception as e:
                print(f"오류 발생: {e}")
                logging.error(f"오류 발생: {e}")
                break

    wd.quit()
    wd_dp1.quit()
    
    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '짱공유닷컴')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='짱공유닷컴', subdir=f'19.짱공유닷컴/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        
        all_data.to_csv(os.path.join(result_dir, f'짱공유닷컴_raw data_{target_date}.csv'), encoding='utf-8', index=False)