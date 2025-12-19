import re
import os
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime

from .utils import setup_driver, save_to_csv, clean_title, result_csv_data

# [수정] 전역 변수 'today' 및 전역 logging 설정 제거 (함수 내부로 이동)

# 한페이지 크롤링
def pp_crw(wd, url, search, target_date):  # [수정] target_date 인자 추가
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'board-contents')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 추후 수정하기
        writer_list = []

        title_list = []
        content_list = []
        url_list = []

        search_plt_list = []
        search_word_list = []
        date_list = []

        # 추출날짜 추후 삭제
        now_date = []
        image_check_list = []

        content_div = soup.find('td', class_='board-contents')
        
        # 제목 추출
        try:
            raw_title = soup.find('div', id='topTitle').find('h1').get_text()
            cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
            title_list.append(cleaned_title)
            logging.info(f"제목 추출 성공: {cleaned_title}")
        except Exception as e:
            logging.error(f"제목 추출 실패: {e}")
            return pd.DataFrame()

        search_plt_list.append('웹페이지(뽐뿌)')
        url_list.append(url)

        try:
            content_div = soup.find('td', class_='board-contents')

            # 기사(div.scrap_bx) 제외
            for scrap_box in content_div.find_all('div', class_='scrap_bx'):
                scrap_box.decompose()

            # 본문 텍스트 추출 (띄어쓰기 유지)
            post_content = content_div.get_text(separator=' ', strip=True)

            # URL 제거
            post_content_cleaned = re.sub(r'https?://[^\s]+', '', post_content).strip()

            content_list.append(post_content_cleaned)
            logging.info("내용 추출 성공 (기사 제외 + URL 제거)")

        except Exception as e:
            content_list.append('')
            logging.error(f"본문 추출 실패: {e}")

        search_word_list.append(search)

        # 날짜 출력
        try:
            pp_date_str = soup.find('ul', class_='topTitle-mainbox').find_all('li')[1].get_text()
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', pp_date_str)
            date_list = date_match.group(1)
        except:
            date_list = ""

        # 채널명
        try:
            name_element = soup.find('a', class_='baseList-name')
            if name_element:
                name = name_element.get_text()
            else:
                name = soup.find('strong', class_="none").get_text()
            writer_list.append(name)
        except:
            writer_list.append("Unknown")

        # 추출시간
        now_date.append(datetime.now().strftime('%Y-%m-%d'))

        # 임시 데이터프레임 생성
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
        
        base_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', '1.뽐뿌')
        
        # [수정] 파일명에 target_date 사용 (Airflow가 요청한 날짜)
        file_name = os.path.join(base_path, f'뽐뿌_{target_date}_{search}.csv')
        
        save_to_csv(main_temp, file_name)

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        print(f"오류 발생: {e}")
        return pd.DataFrame()


def pp_main_crw(searchs, start_date, end_date, stop_event):
    # [수정] start_date를 이용하여 target_date 문자열 생성 (yymmdd 형식)
    target_date = start_date.strftime("%y%m%d")

    # [수정] 로그 설정 (함수 내부로 이동, 파일명에 target_date 적용, force=True 추가)
    if not os.path.exists('log'):
        os.makedirs('log')
        
    logging.basicConfig(
        filename=f'log/뽐뿌_log_{target_date}.txt',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True 
    )

    # 폴더 생성 (기존 로직 유지하되 target_date 사용)
    # 참고: pp_crw는 '1.뽐뿌' 폴더에 직접 저장하므로, 이 날짜 폴더는 사용되지 않을 수 있지만 기존 구조 유지를 위해 남김
    raw_date_dir = f'../data/raw/1.뽐뿌/{target_date}'
    if not os.path.exists(raw_date_dir):
        os.makedirs(raw_date_dir)
        print(f"폴더 생성 완료: {target_date}")

    logging.info(f"========================================================")
    logging.info(f"             뽐뿌 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()

    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        page_num = 1

        while True:
            try:
                url_dp1 = f'https://www.ppomppu.co.kr/search_bbs.php?search_type=sub_memo&page_no={page_num}&keyword={search}&page_size=50&bbs_id=&order_type=date&bbs_cate=2'
                wd_dp1.get(url_dp1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'results_board')))
                time.sleep(1)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 리스트
                li_tags = soup_dp1.find('div', class_='results_board').find_all('div', class_="content")

                # 검색 결과 없으면 종료 조건 (추가 권장)
                if not li_tags:
                    break

                for li in li_tags:
                    try:
                        date_str = li.find('p', class_='desc').find_all('span')[2].get_text()
                        date = datetime.strptime(date_str, '%Y.%m.%d').date()
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue

                    after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url_dp2_num = li.find('span', class_='title').find('a').get('href')
                    url = 'https://www.ppomppu.co.kr' + url_dp2_num
                    logging.info(f"url 찾음: {url}")
                    
                    # [수정] pp_crw 호출 시 target_date 전달
                    pp_crw(wd, url, search, target_date)

                if after_start_date:
                    break
                else:
                    page_num += 1

            except Exception as e:
                print(f"오류 발생: {e}")
                break

            page_num += 1  # 페이지 수 증가

    wd.quit()
    wd_dp1.quit()

    result_dir = 'data/raw'
    os.makedirs(result_dir, exist_ok=True)

    # [주의] result_csv_data가 파일을 읽어올 때 '1.뽐뿌' 폴더 내의 패턴을 찾습니다.
    # 파일명이 '뽐뿌_{target_date}_{search}.csv'로 저장되었으므로 잘 동작할 것입니다.
    all_data = pd.concat([
        result_csv_data(search, platform='뽐뿌', subdir='1.뽐뿌', base_path='data/raw')
        for search in searchs
    ])

    # [수정] 최종 병합 파일명에도 target_date 사용
    all_data.to_csv(f'{result_dir}/뽐뿌_raw_{target_date}.csv', encoding='utf-8', index=False)