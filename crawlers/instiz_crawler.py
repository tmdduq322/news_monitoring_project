import os
import re
import random
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from datetime import datetime, timedelta

from .utils import setup_driver, save_to_csv, result_csv_data


def parse_date(date_str):
    try:
        if "어제" in date_str:
            return (datetime.now() - timedelta(days=1)).date()
        if "일 전" in date_str:
            days_ago = int(date_str.split("일 전")[0].strip())
            return (datetime.now() - timedelta(days=days_ago)).date()
        if "시간 전" in date_str:
            hours_ago = int(date_str.split("시간 전")[0].strip())
            return (datetime.now() - timedelta(hours=hours_ago)).date()
        if "분 전" in date_str:
            minutes_ago = int(date_str.split("분 전")[0].strip())
            return (datetime.now() - timedelta(minutes=minutes_ago)).date()
        if "개월 전" in date_str:
            months_ago = int(date_str.split("개월 전")[0].strip())
            return (datetime.now() - relativedelta(months=months_ago)).date()
        if "/" in date_str:
            return datetime.strptime(date_str.replace(" - ", "").strip(), '%Y/%m/%d').date()
        return datetime.strptime(date_str.replace(" - ", "").strip(), '%Y. %m. %d').date()
    except Exception as e:
        logging.error(f"날짜 파싱 오류: {e} :: 원본 날짜: {date_str}")
        return None


def random_sleep(min_time=1, max_time=3):
    sleep_time = random.uniform(min_time, max_time)
    logging.info(f"랜덤 대기 시간: {sleep_time:.2f}초")
    time.sleep(sleep_time)


# 한페이지 크롤링
def instiz_crw(wd, url, search, date, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")

        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'memo_content')))
        random_sleep(2, 5)

        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        current_date_list = []

        # 제목 추출
        title_elem = soup.find('td', class_='tb_top').find('span', id='nowsubject')
        
        # 태그 정리
        for tag in title_elem.find_all(['span', 'i']):
            tag.extract()
            
        title_text = title_elem.get_text().strip()
        title_list.append(title_text)

        # 작성자 추출
        try:
            tb_left_div = soup.find('div', class_='tb_left')
            writer_name = '익명'
            if tb_left_div:
                writer_tag = tb_left_div.find('a', onclick=re.compile("prlayer_print"))
                if writer_tag:
                    writer_name = writer_tag.get_text().strip()
        except Exception:
            writer_name = '익명'
        writer_list.append(writer_name)

        # 본문 추출
        content_tag = soup.find('div', id='memo_content_1')
        if content_tag.find('span', class_='sorrybaby'):
            logging.info("회원에게만 공개된 글")
            return None
        else:
            content = content_tag.get_text(separator=' ', strip=True)
            content_cleaned = re.sub(r'https?://[^\s]+', '', content).strip()
            content_list.append(content_cleaned)
            logging.info("내용 추출 성공")

        search_plt_list.append('웹페이지(인스티즈)')
        url_list.append(url)
        search_word_list.append(search)
        date_list.append(date)
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
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '7.인스티즈', target_date)
        os.makedirs(save_path, exist_ok=True)
        
        file_name = os.path.join(save_path, f'인스티즈_{search}.csv')
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


def result_soup(wd, wd_dp1, start_date, end_date, search, collected_urls, stop_event, target_date):
    soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
    div_tags = soup_dp1.find_all('div', class_='result_search')
    logging.info(f"검색목록 찾음.")
    
    after_start_date = False
    
    for div in div_tags:
        if stop_event.is_set():
            break
        after_start_date = False

        try:
            date_str = div.find('span', class_='search_content').find('span', class_='minitext3').text
            date = parse_date(date_str)
            if date is None:
                logging.info(f"날짜 파싱 실패: {date_str}")
                continue
            logging.info(f"날짜 찾음 : {date}")
        except Exception as e:
            logging.info(f"날짜 에러 : {e}")
            continue

        if date > end_date:
            continue
        if date < start_date:
            after_start_date = True
            break

        url = div.find('a').get('href')
        if url not in collected_urls:
            if stop_event.is_set():
                break
            logging.info(f"url 찾음: {url}")
            collected_urls.add(url)
            instiz_crw(wd, url, search, date, target_date)

    return after_start_date


def instiz_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'인스티즈_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    logging.info(f"========================================================")
    logging.info(f"                    인스티즈 크롤링 시작 (Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()

    category = ['pt', 'name', 'name_enter']
    
    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        for cate in category:
            if stop_event.is_set():
                break
            collected_urls = set()

            try:
                logging.info(f"크롤링 시작-검색어: {search}")
                url = f'https://www.instiz.net/popup_search.htm?id={cate}&k={search}'
                wd_dp1.get(url)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'search_container')))
                time.sleep(1)

                while True:
                    if stop_event.is_set():
                        break
                    
                    after_start_date = result_soup(wd, wd_dp1, start_date, end_date, search, collected_urls, stop_event, target_date)

                    if after_start_date:
                        break
                    else:
                        try:
                            logging.info("더보기 버튼 클릭.")
                            more_button = wd_dp1.find_element(By.CSS_SELECTOR, "div.morebutton a")
                            actions = ActionChains(wd_dp1)
                            actions.move_to_element(more_button).perform()
                            more_button.click()
                            random_sleep(2, 5)
                            result_soup(wd, wd_dp1, start_date, end_date, search, collected_urls, stop_event, target_date)
                        except Exception as e:
                            logging.error(f"더보기 버튼 오류 :: 검색어: {search}, 오류: {e}")
                            break

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break
                
    wd.quit()
    wd_dp1.quit()
    
    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '인스티즈')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='인스티즈', subdir=f'7.인스티즈/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'인스티즈_raw data_{target_date}.csv'), encoding='utf-8', index=False)