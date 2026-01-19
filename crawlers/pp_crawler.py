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

# 한페이지 크롤링
def pp_crw(wd, url, search, target_date):
    try:
        logging.info(f"크롤링 시작: {url}")
        try:
            wd.get(f'{url}')
        except TimeoutException:
            logging.warning(f"⏰ 접속 타임아웃 (30초 초과): {url} -> 스킵합니다.")
            return pd.DataFrame() # 빈 데이터프레임 반환하고 종료
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'board-contents')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        writer_list = []
        title_list = []
        content_list = []
        url_list = []
        search_plt_list = []
        search_word_list = []
        date_list = []
        now_date = []

        content_div = soup.find('td', class_='board-contents')
        
        # 제목 추출
        try:
            raw_title = soup.find('div', id='topTitle').find('h1').get_text()
            cleaned_title = clean_title(raw_title)
            title_list.append(cleaned_title)
            logging.info(f"제목 추출 성공: {cleaned_title}")
        except Exception as e:
            logging.error(f"제목 추출 실패: {e}")
            return pd.DataFrame()

        search_plt_list.append('웹페이지(뽐뿌)')
        url_list.append(url)

        # 본문 추출
        try:
            content_div = soup.find('td', class_='board-contents')

            # 기사(div.scrap_bx) 제외
            for scrap_box in content_div.find_all('div', class_='scrap_bx'):
                scrap_box.decompose()

            # 본문 텍스트 추출
            post_content = content_div.get_text(separator=' ', strip=True)
            post_content_cleaned = re.sub(r'https?://[^\s]+', '', post_content).strip()

            content_list.append(post_content_cleaned)
            logging.info("내용 추출 성공 (기사 제외 + URL 제거)")

        except Exception as e:
            content_list.append('')
            logging.error(f"본문 추출 실패: {e}")

        search_word_list.append(search)

        # 날짜 추출
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

        now_date.append(datetime.now().strftime('%Y-%m-%d'))

        # 데이터프레임 생성
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
        
        # [수정 1] 저장 경로: data/raw/1.뽐뿌/{target_date}/
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '1.뽐뿌', target_date)
        
        # 폴더 자동 생성
        os.makedirs(save_path, exist_ok=True)
        
        # 파일명: 뽐뿌_{검색어}.csv (폴더에 날짜가 있으므로 파일명은 깔끔하게)
        file_name = os.path.join(save_path, f'뽐뿌_{search}.csv')
        
        save_to_csv(main_temp, file_name)
        logging.info(f'저장 완료: {file_name}')

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def pp_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")

    # 경로 설정 (절대 경로)
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    # 로그 폴더 생성
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        filename=os.path.join(log_dir, f'뽐뿌_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True 
    )

    # 데이터 저장용 폴더 미리 생성 (안전장치)
    data_dir = os.path.join(project_root, 'data', 'raw', '1.뽐뿌', target_date)
    os.makedirs(data_dir, exist_ok=True)

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

                li_tags = soup_dp1.find('div', class_='results_board').find_all('div', class_="content")

                if not li_tags:
                    break

                for li in li_tags:
                    try:
                        date_str = li.find('p', class_='desc').find_all('span')[2].get_text()
                        date = datetime.strptime(date_str, '%Y.%m.%d').date()
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue

                    after_start_date = False

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url_dp2_num = li.find('span', class_='title').find('a').get('href')
                    url = 'https://www.ppomppu.co.kr' + url_dp2_num
                    logging.info(f"url 찾음: {url}")
                    
                    pp_crw(wd, url, search, target_date)

                if after_start_date:
                    break
                else:
                    page_num += 1

            except Exception as e:
                print(f"오류 발생: {e}")
                break

            page_num += 1

    wd.quit()
    wd_dp1.quit()

    # 결과 병합 및 저장
    result_dir = os.path.join(project_root, 'data', 'raw') # 필요시 '결과/뽐뿌'로 변경 가능
    os.makedirs(result_dir, exist_ok=True)

    # [수정 2] subdir에 target_date를 포함하여 해당 날짜 폴더에서 데이터를 읽어오도록 설정
    try:
        all_data = pd.concat([
            result_csv_data(search, platform='뽐뿌', subdir=f'1.뽐뿌/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        # 최종 파일명
        all_data.to_csv(os.path.join(result_dir, f'뽐뿌_raw_{target_date}.csv'), encoding='utf-8', index=False)
    except ValueError:
        logging.warning("수집된 데이터가 없어 병합할 파일이 없습니다.")