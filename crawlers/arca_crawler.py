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

# .utils 모듈에서 필요한 함수들 임포트
from .utils import setup_driver, save_to_csv, clean_title, result_csv_data


def arca_crw(wd, url, search, target_date):
    """
    개별 게시글 크롤링 함수
    :param target_date: 수집 대상 날짜 (YYMMDD 형식) - 저장 경로 생성에 사용
    """
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        
        # 본문이 로딩될 때까지 대기
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'article-body')))
        time.sleep(1)
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 리스트 초기화
        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        current_date_list = []
        
        # 1. 제목 추출
        try:
            div_tag = soup.find('div', class_='title')
            # span 태그(말머리 등) 제거
            for span in div_tag.find_all('span'):
                span.extract()
            raw_title = div_tag.text
            cleaned_title = clean_title(raw_title)
            title_list.append(cleaned_title)
            logging.info(f"제목 추출 성공: {cleaned_title}")
        except Exception as e:
            logging.error(f"제목 추출 실패: {e}")
            return None

        # 2. 본문 추출
        try:
            content_div = soup.find('div', class_='article-body')
            # 본문 텍스트 추출 (띄어쓰기 유지)
            post_content = content_div.get_text(separator=' ', strip=True)
            # URL 제거
            post_content_cleaned = re.sub(r'https?://[^\s]+', '', post_content).strip()
            content_list.append(post_content_cleaned)
            logging.info("내용 추출 성공 (URL 제거됨)")
        except Exception as e:
            logging.error(f"본문 추출 실패: {e}")
            content_list.append("")

        # 3. 기본 정보 추가
        search_plt_list.append('웹페이지(아카라이브)')
        url_list.append(url)
        search_word_list.append(search)

        # 4. 날짜 추출
        try:
            date_str = soup.find('div', class_='info-row').find('time').get_text()
            # "2023-10-25 14:30:00" 같은 형식일 경우 앞부분만 자름
            date_obj = datetime.strptime(date_str.split()[0], '%Y-%m-%d')
            date_list.append(date_obj)
            logging.info(f"날짜 추출 성공: {date_str}")
        except Exception as e:
            logging.error(f"날짜 추출 실패: {e}")
            date_list.append("")

        # 5. 작성자(채널명) 추출
        try:
            writer = soup.find('div', class_='info-row').find('span', class_='user-info').find('a').get_text()
            writer_list.append(writer)
        except:
            writer_list.append("Unknown")
            
        current_date_list.append(datetime.now().strftime('%Y-%m-%d'))

        # 데이터프레임 생성
        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": current_date_list
        })

        # [핵심 수정] 저장 경로를 절대 경로로 계산 (pp_crawler 방식)
        # 현재 파일 위치(.../crawlers) -> 상위(.../project) -> data -> raw -> 9.아카라이브 -> 날짜
        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '9.아카라이브', target_date)
        
        # 폴더가 없으면 생성 (exist_ok=True 필수)
        os.makedirs(save_path, exist_ok=True)

        # 파일 저장
        file_name = os.path.join(save_path, f'아카라이브_{search}.csv')
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


def arca_main_crw(searchs, start_date, end_date, stop_event):
    # 1. target_date 생성
    target_date = start_date.strftime("%y%m%d")
    
    # 2. 경로 설정 (절대 경로)
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    # 3. 로그 설정
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        filename=os.path.join(log_dir, f'아카라이브_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True 
    )

    logging.info(f"========================================================")
    logging.info(f"            아카라이브 크롤링 시작(Date: {target_date})")
    logging.info(f"========================================================")
    
    wd = setup_driver()
    wd_dp1 = setup_driver()
    
    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        page_num = 1
        time.sleep(1)
        
        while True:
            if stop_event.is_set():
                break
            try:
                logging.info(f"크롤링 시작-검색어: {search}")
                # 아카라이브 검색 URL (p=페이지번호)
                url = f'https://arca.live/b/breaking?keyword={search}&p={page_num}'
                wd_dp1.get(url)
                
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'article-list')))
                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 리스트
                tr_tags = soup_dp1.find('div', class_='list-table table').find_all('a', class_='vrow column')
                logging.info(f"검색목록 찾음.")

                if not tr_tags:
                    break

                for tr in tr_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False 
                    try:
                        date_str = tr.find('span', class_='vcol col-time').find('time').text
                        # 날짜 처리 (오늘 글 등)
                        if ':' in date_str: 
                             date = datetime.now().date()
                        else:
                             date = datetime.strptime(date_str, '%Y.%m.%d').date()
                        logging.info(f"날짜 찾음: {date}")
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue
                        
                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url = 'https://arca.live' + tr.get('href')
                    logging.info(f"url 찾음.")
                    
                    # [핵심] 함수 호출 시 target_date 전달
                    arca_crw(wd, url, search, target_date)

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
        # [핵심] 결과 저장 폴더 절대 경로 생성
        # data/raw 폴더가 아니라 별도의 '결과' 폴더를 원하신다면 아래 경로를 사용
        result_dir = os.path.join(project_root, '결과', '아카라이브')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='아카라이브', subdir=f'9.아카라이브/{target_date}', base_path='data/raw')
            for search in searchs
        ])

        all_data.to_csv(os.path.join(result_dir, f'아카라이브_raw_{target_date}.csv'), encoding='utf-8', index=False)