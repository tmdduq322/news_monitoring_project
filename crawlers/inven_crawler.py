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

from .utils import setup_driver, save_to_csv, clean_title,result_csv_data

# 실행날짜 변수 및 폴더 생성
today = datetime.now().strftime("%y%m%d")
if not os.path.exists(f'log'):
    os.makedirs(f'log')

# 로그 설정
logging.basicConfig(
    filename=f'인벤_log_{today}.txt',  # 로그 파일 이름
    level=logging.INFO,          # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'             # 인코딩 설정
)

def inven_crw(wd, url, search):
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
        image_check_list = []

        # 확인용
        now_date = []
        # 이미지 유무 추출
        # content_div = soup.find('div', class_='articleContent')
        raw_title = soup.find('div', class_='articleTitle').get_text()
        cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
        # title_strip = ' '.join(clean_title.split())
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content_tag = soup.find('div', id='powerbbsContent')
        # 본문 추출 (띄어쓰기 유지)
        content_text = content_tag.get_text(separator=' ', strip=True)
        # URL 제거
        content_cleaned = re.sub(r'https?://[^\s]+', '', content_text).strip()
        content_list.append(content_cleaned)
        logging.info("내용 추출 성공 ")

        search_plt_list.append('웹페이지(인벤)')
        url_list.append(url)

        search_word_list.append(search)

        # 날짜 출력
        date_str = soup.find('div', class_='articleDate').get_text()
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
        date_list.append(date)
        # 채널명
        writer_list.append(soup.find('div', class_="articleWriter").get_text().strip())

        # 추출시간
        now_date.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # 임시 데이터프레임 생성
        main_temp = pd.DataFrame({

            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            '게시물 내용': content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": now_date,
            # "이미지유무": image_check_list
        })

        # 데이터 저장
        save_to_csv(main_temp, f'data/raw/3.인벤/{today}/인벤_{search}.csv')
        logging.info(f"저장완료: {search}")
    except Exception as e:
        logging.error(f"오류 발생: {e}")
        print(f"오류 발생: {e}")
        return pd.DataFrame()


def inven_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/3.인벤/{today}'):
        os.makedirs(f'data/raw/3.인벤/{today}')
        print(f"폴더 생성 완료: {today}")
    else:
        print(f"해당 폴더 존재")
    logging.info(f"========================================================")
    logging.info(f"                    인벤 크롤링 시작")
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
                logging.info(f"접속")
                wd_dp1.get(url_dp1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'section_body')))
                sleep_random_time = random.uniform(1, 3)
                time.sleep(sleep_random_time)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 없음
                noresult = soup_dp1.find('ul', class_='noresult')

                if noresult:
                    break  # 결과가 없으면 함수 종료

                # 페이지 수 가져오기
                page_tag = soup_dp1.find_all('a', class_="pg")
                page_numbers = [int(tag.text) for tag in page_tag if tag.text.isdigit()]
                max_page_num = max(page_numbers) if page_numbers else 1  # 최대 페이지 번호

                # 검색결과 리스트
                li_tags = soup_dp1.find('ul', class_='news_list').find_all('li')
                logging.info(f"검색결과 찾음")
                for li in li_tags:
                    if stop_event.is_set():
                        break
                    url = li.find('a', class_='name').get('href')
                    logging.info(f"url 찾음 : {url}")
                    inven_crw(wd, url, search)

                if page_num >= max_page_num:
                    break

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                print(f"오류 발생: {e}")
                break
            page_num += 1  # 페이지 수 증가
    wd.quit()
    wd_dp1.quit()

    if not stop_event.is_set():
        result_dir = '결과/인벤'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='인벤', subdir='3.인벤')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/인벤_raw data_{today}.csv', encoding='utf-8', index=False)
