import os
import re
import time
import random
import logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# [추가] 안정적인 로딩을 위한 Selenium 대기 모듈
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from .utils import setup_driver, save_to_csv, clean_title, result_csv_data

# 한페이지 크롤링 (상세 페이지)
def fm_crw(wd, url, search, target_date):
    try:
        wd.get(url)
        # [개선] 본문(article)이 로딩될 때까지 최대 10초 대기
        try:
            WebDriverWait(wd, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
        except:
            logging.error(f"❌ 페이지 로딩 타임아웃 또는 차단됨: {url}")
            return

        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 1. 본문 영역 찾기 (제공된 HTML 구조 반영)
        content_div = soup.find('article')
        if not content_div:
            logging.error(f"❌ 본문(article) 영역을 찾을 수 없음: {url}")
            return

        # 2. 제목 추출 (np_18px_span 클래스 사용)
        title_tag = soup.find('span', class_='np_18px_span')
        raw_title = title_tag.get_text() if title_tag else "제목 없음"
        cleaned_title = clean_title(raw_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        # 3. 본문 내 불필요한 태그 제거 (<a> 태그 내 미디어 없는 경우)
        a_tags = content_div.find_all('a')
        for a_tag in a_tags:
            if (
                    not a_tag.find('img') and
                    not a_tag.find('span', class_='scrap_img') and
                    not a_tag.find('video') and
                    not (a_tag.find('iframe') and 'youtube.com' in a_tag.decode_contents())
            ):
                a_tag.decompose()

        post_content = content_div.get_text(separator='\n', strip=True)
        post_content = re.sub(r'http[s]?://\S+', '', post_content)
        logging.info(f"내용 추출 성공 (길이: {len(post_content)})")

        # 4. 날짜 및 작성자 추출 (제공된 HTML 구조 반영)
        # 날짜: <span class="date m_no">2026.01.17 23:30</span>
        date_tag = soup.find('span', class_="date m_no")
        date_val = date_tag.text.split()[0].replace('.', '-') if date_tag else target_date

        # 작성자: <a class="member_...">
        writer_tag = soup.find('a', class_=re.compile(r'^member_\d+'))
        writer_val = writer_tag.get_text() if writer_tag else "익명"

        now_time = datetime.now().strftime('%Y-%m-%d ')
           
        main_temp = pd.DataFrame({
            "검색어": [search],
            "플랫폼": ['웹페이지(에펨코리아)'],
            "게시물 URL": [url],
            "게시물 제목": [cleaned_title],
            "게시물 내용": [post_content],
            "게시물 등록일자": [date_val],
            "계정명": [writer_val],
            "수집시간": [now_time],
        })

        # 5. 저장 경로 설정 및 저장
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '23.에펨코리아', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'에펨코리아_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f'✅ 저장완료 : {file_name}')

    except Exception as e:
        logging.error(f"🛑 상세 페이지 크롤링 중 오류 발생: {e}")


def fm_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    # 로그 설정
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, f'에펨코리아_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"🚀 에펨코리아 크롤링 시작 (Date: {target_date})")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()

    for search in searchs:
        if stop_event.is_set():
            break
        page_num = 1

        while True:
            if stop_event.is_set():
                break
            try:
                url_dp1 = f'https://www.fmkorea.com/search.php?act=IS&is_keyword={search}&mid=home&where=document&page={page_num}'
                wd_dp1.get(url_dp1)
                time.sleep(random.uniform(2, 4))
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # [수정] 검색결과 리스트 영역 존재 여부 확인 (에러 방지)
                search_result_ul = soup_dp1.find('ul', class_='searchResult')
                if not search_result_ul:
                    logging.info(f"검색 결과가 더 이상 없거나 차단됨 (Page: {page_num})")
                    break

                li_tags = search_result_ul.find_all('li')
                after_start_date = False

                for li in li_tags:
                    try:
                        # 리스트에서의 날짜 추출 (구조 확인 필요, 일반적으로 span.time 혹은 span.date 사용)
                        date_tag = li.find('span', class_=re.compile(r'time|date'))
                        if not date_tag: continue
                        
                        date_str = date_tag.text.strip()
                        # 리스트 날짜 형식에 따른 파싱 (YYYY-MM-DD HH:MM 또는 YYYY.MM.DD)
                        if '.' in date_str:
                            item_date = datetime.strptime(date_str.split()[0], '%Y.%m.%d').date()
                        else:
                            item_date = datetime.strptime(date_str.split()[0], '%Y-%m-%d').date()
                    except Exception as e:
                        continue

                    if item_date > end_date:
                        continue
                    if item_date < start_date:
                        after_start_date = True
                        break

                    a_tag = li.find('a')
                    if a_tag:
                        url = 'https://www.fmkorea.com' + a_tag.get('href')
                        fm_crw(wd, url, search, target_date)

                if after_start_date:
                    break
                else:
                    page_num += 1

            except Exception as e:
                logging.error(f"⚠️ 검색 리스트 파싱 중 오류 발생: {e}")
                break
                
    wd.quit()
    wd_dp1.quit()
    
    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '에펨코리아')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='에펨코리아', subdir=f'23.에펨코리아/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'에펨코리아_raw data_{target_date}.csv'), encoding='utf-8', index=False)