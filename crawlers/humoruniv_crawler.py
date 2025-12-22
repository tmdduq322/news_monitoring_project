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

# 한페이지 크롤링
def humoruniv_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.ID, 'cnts')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        current_date_list = []

        content_div = soup.find('div', id='cnts')
        tb = soup.find('table', id='profile_table').find('table')
        raw_title = tb.find('span', id='ai_cm_title').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        # <a> 태그 제거
        for a_tag in content_div.find_all('a'):
            a_tag.decompose()

        post_content = content_div.get_text(separator=' ', strip=True)
        post_content_cleaned = re.sub(r'https?://[^\s]+', '', post_content)
        content_list.append(post_content_cleaned)
        logging.info("내용 추출 성공 (URL 제거 및 띄어쓰기 유지)")

        search_plt_list.append('웹페이지(웃긴대학)')
        url_list.append(url)
        search_word_list.append(search)

        date_str = tb.find('div', id='content_info').find_all('span')[4].get_text().strip().split(' ')[0]
        date = datetime.strptime(date_str, '%Y-%m-%d')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date_str}")

        writer_list.append(tb.find('span', class_='hu_nick_txt').get_text())
        current_date_list.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
        })

        # [수정] 절대 경로 및 target_date 사용
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '11.웃긴대학', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'웃긴대학_{search}.csv')
        save_to_csv(main_temp, file_name)
        logging.info(f'저장 완료: {file_name}')

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def humoruniv_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'웃긴대학_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"             웃긴대학 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()
    
    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        while True:
            if stop_event.is_set():
                break
            try:
                logging.info(f"크롤링 시작-검색어: {search}")
                url = f'https://web.humoruniv.com/main.html'
                wd_dp1.get(url)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.ID, 'wrap_sch')))
                time.sleep(1)

                try:
                    keyword_input_frm = wd_dp1.find_element(By.ID, 'search_text')
                    keyword_input_frm.click()
                    keyword_input = wd_dp1.find_element(By.ID, 'search_text')
                    keyword_input.send_keys(search)
                    submit_button = wd_dp1.find_element(By.XPATH, '//input[@alt="검색"]')
                    submit_button.click()
                except Exception as e:
                    logging.error(f"검색 실패: {e}")
                time.sleep(3)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                date_flag = False
                after_start_date = False

                tables = soup_dp1.find_all('table', {
                    'width': '100%',
                    'border': '0',
                    'cellspacing': '0',
                    'cellpadding': '5',
                    'bordercolor': '#666666',
                    'style': 'border-collapse:collapse;'
                })
                logging.info(f"검색목록 찾음.")
                
                while True:
                    if stop_event.is_set():
                        break
                    for tb in tables:
                        if stop_event.is_set():
                            break
                        after_start_date = False
                        date_flag = False

                        try:
                            date_str = tb.find('font', class_='gray').text.split(' ')[0]
                            date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        except:
                            continue

                        if date > start_date:
                            after_start_date = True

                        if start_date <= date <= end_date:
                            date_flag = True
                            url = 'https:' + tb.find('a').get('href')
                            logging.info(f"url 찾음.")
                            humoruniv_crw(wd, url, search, target_date)

                    if not after_start_date and not date_flag:
                        logging.info("루프종료")
                        break

                    try:
                        wd_dp1.find_element(By.CSS_SELECTOR, "def arrow").click()
                        time.sleep(3)
                        soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
                        tables = soup_dp1.find_all('table', {
                            'width': '100%',
                            'border': '0',
                            'cellspacing': '0',
                            'cellpadding': '5',
                            'bordercolor': '#666666',
                            'style': 'border-collapse:collapse;'
                        })
                        logging.info("다음 페이지로 이동 및 파싱 완료")
                    except Exception as e:
                        logging.error(f"페이징 오류 발생: {e}")
                        break

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break

            if not after_start_date and not date_flag:
                break

    wd.quit()
    wd_dp1.quit()

    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '웃긴대학')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='웃긴대학', subdir=f'11.웃긴대학/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'웃긴대학_raw data_{target_date}.csv'), encoding='utf-8', index=False)