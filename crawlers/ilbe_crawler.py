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

# 한페이지 크롤링
def ilbe_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'post-content')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        current_date_list = []

        content_div = soup.find('div', class_='post-content')
        raw_title = soup.find('div', class_='post-header').find('h3').find('a').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        # <a> 태그 제거 (기사 링크, 유튜브 등)
        for a_tag in content_div.find_all('a'):
            a_tag.decompose()

        # 본문 추출 (띄어쓰기 유지)
        post_content = content_div.get_text(separator=' ', strip=True)

        # URL 제거
        post_content_cleaned = re.sub(r'https?://[^\s]+', '', post_content).strip()

        content_list.append(post_content_cleaned)
        logging.info("내용 추출 성공 (URL 제거됨)")

        search_plt_list.append('웹페이지(일간베스트)')
        url_list.append(url)
        search_word_list.append(search)

        date_str = soup.find('div', class_='post-count').find('span', class_='date').text.strip().split()[0]
        date = datetime.strptime(date_str, '%Y-%m-%d')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date_str}")

        writer_list.append(soup.find('span', class_='global-nick nick').find('a').get_text())
        current_date_list.append(datetime.now().strftime('%Y-%m-%d '))

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

        # [수정] 절대 경로 및 target_date 사용
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '10.일간베스트', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'일간베스트_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f'저장 완료: {file_name}')

    except TimeoutException as e:
        logging.error(f"페이지 로딩 시간 초과: {e}")
        return None
    except WebDriverException as e:
        logging.error(f"웹드라이버 에러: {e}")
        return None
    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return None


def ilbe_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'일간베스트_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"             일간베스트 크롤링 시작 (Date: {target_date})")
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
                url = f'https://www.ilbe.com/search?docType=doc&searchType=title_content&page={page_num}&q={search}'
                wd_dp1.get(url)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'search-list')))
                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
                li_tags = soup_dp1.find('div', class_='search-list').find_all('li')
                logging.info(f"검색목록 찾음.")

                if not li_tags:
                    break

                for li in li_tags:
                    if stop_event.is_set():
                        break
                    date_str = li.find('span', class_='date').text
                    date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').date()
                    logging.info(f"날짜 찾음")

                    after_start_date = False

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url = 'https://www.ilbe.com' + li.find('a', class_='title').get('href')
                    logging.info(f"url 찾음.")
                    ilbe_crw(wd, url, search, target_date)

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
        result_dir = os.path.join(project_root, '결과', '일간베스트')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='일간베스트', subdir=f'10.일간베스트/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'일간베스트_raw data_{target_date}.csv'), encoding='utf-8', index=False)