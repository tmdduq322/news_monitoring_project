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

from .utils import setup_driver, save_to_csv, clean_title,result_csv_data

# ---------------------------------------------------------
# [상세 페이지 수집 함수] orbi_crw
# ---------------------------------------------------------
def orbi_crw(wd, url, search, target_date):
    try:
        logging.info(f"상세 수집 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(url)
        
        # 상세 페이지 로딩 대기 (content-body가 뜰 때까지)
        try:
            WebDriverWait(wd, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'content-body')))
        except:
            logging.warning(f"본문 로딩 실패 또는 구조 다름: {url}")
            return None

        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 1. 제목 추출
        try:
            # 구조가 다를 수 있어 여러 시도
            raw_title = soup.find('div', class_='post-header').find('h1').get_text(strip=True)
        except AttributeError:
            # post-header가 없는 경우 title 태그 등 대체 시도 (필요 시 로직 추가)
            logging.warning("제목 태그를 찾을 수 없음")
            raw_title = "제목 없음"
            
        cleaned_title = clean_title(raw_title)

        # 2. 본문 추출
        content_div = soup.find('div', class_='content-body')
        if content_div:
            # 불필요한 태그 제거 (이미지, 영상 등)
            for tag in content_div.find_all(['img', 'video', 'iframe', 'span']):
                if tag.name == 'span' and 'scrap_img' not in tag.get('class', []):
                    continue # scrap_img가 아닌 span은 놔둠 (상황에 따라 조정)
                if tag.name in ['img', 'video', 'iframe']:
                    tag.decompose()
            
            post_content = content_div.get_text(separator=' ', strip=True)
            post_content = re.sub(r'https?://[^\s]+', '', post_content) # URL 제거
        else:
            post_content = ""

        # 3. 작성자 추출
        try:
            writer = soup.find('span', class_='nick').get_text(strip=True)
        except:
            writer = "익명"

        # 4. 날짜 추출 (상세 페이지 내 메타 정보)
        try:
            date_str = soup.find('div', class_='post-meta').find('abbr')['title']
            # 형식: @YYYY-MM-DD HH:MM:SS
            date_str = date_str.replace('@', '').strip().split(' ')[0]
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        except:
            # 실패 시 수집 당일 날짜로 대체하거나 None
            date_obj = datetime.now()

        # 데이터 반환 (저장은 main에서 일괄 처리 권장하지만, 기존 구조 유지를 위해 Dict 반환)
        return {
            "검색어": search,
            "플랫폼": '웹페이지(오르비)',
            "게시물 URL": url,
            "게시물 제목": cleaned_title,
            "게시물 내용": post_content,
            "게시물 등록일자": date_obj,
            "계정명": writer,
            "수집시간": datetime.now().strftime('%Y-%m-%d'),
        }

    except Exception as e:
        logging.error(f"상세 수집 중 오류: {e}")
        return None


# ---------------------------------------------------------
# [메인 수집 함수] orbi_main_crw
# ---------------------------------------------------------
def orbi_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    
    log_dir = os.path.join(project_root, 'log')
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f'오르비_log_{target_date}.txt'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        force=True
    )

    wd = setup_driver()     # 상세 페이지용
    wd_dp1 = setup_driver() # 목록 페이지용
    
    for search in searchs:
        if stop_event.is_set(): break
        
        logging.info(f"=== 검색어 크롤링 시작: {search} ===")
        page_num = 1
        results_list = [] # 데이터 모아서 저장하기 위한 리스트

        while True:
            if stop_event.is_set(): break
            
            try:
                # [수정됨] URL 구조 변경 (통합 검색)
                url = f'https://orbi.kr/search?q={search}&type=keyword&page={page_num}'
                logging.info(f"목록 페이지 진입: {url}")
                
                wd_dp1.get(url)
                
                # 목록 로딩 대기 (post-list 클래스가 뜰 때까지)
                try:
                    WebDriverWait(wd_dp1, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'post-list')))
                except:
                    logging.info("게시글 목록을 찾을 수 없음 (마지막 페이지거나 로딩 실패)")
                    break

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
                ul_tag = soup_dp1.find('ul', class_='post-list')
                
                if not ul_tag:
                    break
                    
                li_tags = ul_tag.find_all('li')
                if not li_tags:
                    break

                page_data_count = 0 # 현재 페이지에서 유효한 데이터 수

                for li in li_tags:
                    if stop_event.is_set(): break
                    
                    # [중요] 광고 배너나 구조가 다른 li 태그 예외 처리
                    # HTML 구조상 제목은 <p class="title"> 안에 있음
                    title_p = li.find('p', class_='title')
                    if not title_p: 
                        continue # 제목 없으면(광고 등) 건너뜀

                    # 날짜 추출 및 필터링
                    after_start_date = False
                    try:
                        # HTML: <abbr title="@2026-01-08 00:32:46">
                        abbr = li.find('abbr')
                        if not abbr:
                            continue # 날짜 없으면 건너뜀
                            
                        date_raw = abbr['title'] # 예: @2026-01-08 ...
                        date_str = date_raw.replace('@', '').strip().split(' ')[0]
                        post_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        
                    except Exception as e:
                        logging.warning(f"날짜 파싱 실패: {e}")
                        continue

                    # 날짜 범위 체크
                    if post_date > end_date:
                        continue # 범위보다 최신글은 패스 (다음 글 확인)
                    if post_date < start_date:
                        after_start_date = True # 범위보다 과거글 나옴 -> 크롤링 종료 신호
                        break

                    # 상세 URL 추출
                    try:
                        link_tag = title_p.find('a')
                        if not link_tag: continue
                        
                        href = link_tag.get('href')
                        full_url = 'https://orbi.kr' + href
                        
                        # 상세 수집 함수 호출
                        data = orbi_crw(wd, full_url, search, target_date)
                        if data:
                            results_list.append(data)
                            page_data_count += 1
                            
                    except Exception as e:
                        logging.error(f"상세 URL 추출 실패: {e}")
                        continue

                # 날짜가 start_date보다 과거로 넘어가면 루프 종료
                if after_start_date:
                    logging.info("설정된 날짜 범위를 벗어나 수집 종료")
                    break
                
                # 현재 페이지에 유효한 글이 하나도 없으면 (혹은 페이지 끝) 종료 체크
                if page_data_count == 0 and len(li_tags) < 5: 
                    # 게시글이 너무 적으면 마지막 페이지일 가능성 높음
                    # (정확한 종료 조건은 아니지만 무한루프 방지용)
                    pass

                page_num += 1
                time.sleep(1) # 페이지 넘김 매너 딜레이

            except Exception as e:
                logging.error(f"메인 루프 오류: {e}")
                break
        
        # 검색어 하나 끝날 때마다 저장 (I/O 부하 감소)
        if results_list:
            df = pd.DataFrame(results_list)
            save_path = os.path.join(current_dir, '..', 'data', 'raw', '13.오르비', target_date)
            os.makedirs(save_path, exist_ok=True)
            
            file_name = os.path.join(save_path, f'오르비_{search}.csv')
            save_to_csv(df, file_name)
            logging.info(f"== {search} 저장 완료 ({len(df)}건) ==")
        else:
            logging.info(f"== {search} 수집된 데이터 없음 ==")

    wd.quit()
    wd_dp1.quit()

    if not stop_event.is_set():
        result_dir = os.path.join(project_root, '결과', '오르비')
        os.makedirs(result_dir, exist_ok=True)

        all_data = pd.concat([
            result_csv_data(search, platform='오르비', subdir=f'13.오르비/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'오르비_raw data_{target_date}.csv'), encoding='utf-8', index=False)