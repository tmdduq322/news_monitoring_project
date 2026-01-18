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

def parse_blind_date(date_str, current_year=2025):
    from datetime import datetime, timedelta
    date_str = date_str.strip()
    date_str = date_str.replace("작성시간", "").replace("작성일", "").strip()
    if date_str.endswith('.'):
        date_str = date_str[:-1]

    now = datetime.now()
    if '시간' in date_str:
        hours = int(date_str.replace('시간', '').strip())
        return (now - timedelta(hours=hours)).date()
    elif '일' in date_str:
        days = int(date_str.replace('일', '').strip())
        return (now - timedelta(days=days)).date()
    elif '주' in date_str:
        weeks = int(date_str.replace('주', '').strip())
        return (now - timedelta(weeks=weeks)).date()
    elif '달' in date_str:
        months = int(date_str.replace('달', '').strip())
        return (now - timedelta(days=months * 30)).date()
    elif date_str.count('.') == 1:
        try:
            month, day = date_str.split('.')
            return datetime.strptime(f"{current_year}-{month.zfill(2)}-{day.zfill(2)}", "%Y-%m-%d").date()
        except:
            return None
    elif date_str.count('.') == 2:
        try:
            return datetime.strptime(date_str, '%Y.%m.%d').date()
        except:
            return None
    return None

def blind_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(2)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'contents')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []

        content_div = soup.find('p', id='contentArea')
        raw_title = soup.find('div', class_='article-view-head').find('h2').text
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        for a_tag in content_div.find_all('a'):
            if (not a_tag.find('img') and not a_tag.find('span', class_='scrap_img') and
                not a_tag.find('video') and not (a_tag.find('iframe') and 'youtube.com' in a_tag.decode_contents())):
                a_tag.decompose()

        post_content = content_div.get_text(separator='\n', strip=True)
        post_content = re.sub(r'https?://\S+', '', post_content)
        content_list.append(post_content)
        logging.info(f"내용 추출 성공: {post_content}")

        search_plt_list.append('웹페이지(블라인드)')
        url_list.append(url)
        search_word_list.append(search)

        date_tag = soup.find('div', class_='wrap-info').find('span', class_='date').text.strip()
        date_only = date_tag.replace('작성일', '').strip()
        month, day = date_only.split('.')
        formatted_date = f"2024-{month.zfill(2)}-{day.zfill(2)}"
        date_list.append(formatted_date)
        logging.info(f"날짜 추출 성공: {formatted_date}")

        writer = soup.find('div', class_='name').text.strip()
        writert_strip = ' '.join(writer.split())
        writer_list.append(writert_strip)
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
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '20.블라인드', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'블라인드_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f"저장완료: {file_name}")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def blind_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'블라인드_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"            블라인드 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")

    wd_dp1 = setup_driver()
    wd = setup_driver()
    wd_dp1.maximize_window()
    wd.maximize_window()

    current_year = 2025
    MAX_SCROLL_COUNT = 50

    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        collected_urls = set()
        logging.info(f"[{search}] 크롤링 시작")

        search_url = f'https://www.teamblind.com/kr/search/"{search}"'
        wd_dp1.get(search_url)

        try:
            WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.sort > select')))
            time.sleep(1)
            wd_dp1.execute_script("""
                const select = document.querySelector('div.sort > select');
                if (select) { select.value = 'id'; select.dispatchEvent(new Event('change', { bubbles: true })); }
            """)
            logging.info(f"[{search}] 최신순 정렬 적용 완료")
            time.sleep(2)
        except Exception as e:
            logging.warning(f"[{search}] 최신순 정렬 실패: {e}")

        scroll_count = 0
        after_start_date = False

        while scroll_count < MAX_SCROLL_COUNT and not after_start_date:
            if stop_event.is_set():
                break
            prev_height = wd_dp1.execute_script("return document.body.scrollHeight")
            time.sleep(2)

            soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
            article_list_div = soup_dp1.find('div', class_='article-list')
            if not article_list_div:
                break
            
            div_tags = article_list_div.find_all('div', class_=re.compile(r'\barticle-list-pre\b'))
            if not div_tags:
                break

            for div in div_tags:
                if stop_event.is_set():
                    break
                info_div = div.find('div', class_='info_fnc')
                if not info_div: continue
                date_anchor = info_div.find('a', class_='past')
                if not date_anchor: continue

                date_str = date_anchor.text.strip()
                parsed_date = parse_blind_date(date_str, current_year)
                if not parsed_date: continue
                
                date_txt = parsed_date
                logging.info(f"[{search}] 날짜 찾음: {date_txt}")

                if date_txt > end_date:
                    continue
                if date_txt < start_date:
                    after_start_date = True
                    break

                try:
                    post_url = div.find('div', class_='tit').find('h3').find('a')['href']
                    full_url = 'https://www.teamblind.com' + post_url
                except Exception as e:
                    logging.warning(f"[{search}] URL 추출 실패: {e}")
                    continue

                if full_url not in collected_urls:
                    collected_urls.add(full_url)
                    blind_crw(wd, full_url, search, target_date)

            wd_dp1.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            current_height = wd_dp1.execute_script("return document.body.scrollHeight")
            if current_height == prev_height:
                break
            scroll_count += 1

    wd.quit()
    wd_dp1.quit()
    
    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '블라인드')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='블라인드', subdir=f'20.블라인드/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'블라인드_raw data_{target_date}.csv'), encoding='utf-8', index=False)