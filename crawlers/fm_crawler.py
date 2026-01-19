import os
import re
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# 안정적인 로딩을 위한 Selenium 모듈
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# utils.py에서 필요한 함수들 가져오기
from .utils import setup_driver, save_to_csv, clean_title, result_csv_data

# [상세 페이지 크롤링 함수]
def fm_crw(wd, url, search, target_date):
    try:
        wd.get(url)
        
        # 본문 영역(div.xe_content)이 뜰 때까지 최대 10초 대기
        try:
            WebDriverWait(wd, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "xe_content"))
            )
        except:
            logging.error(f"❌ 페이지 로딩 타임아웃 또는 차단됨: {url}")
            return

        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 데이터 담을 리스트 초기화
        writer_list = []
        title_list = []
        content_list = []
        url_list = []
        search_plt_list = []
        search_word_list = []
        date_list = []
        now_date_list = []

        # 1. 제목 추출 (업데이트된 선택자: span.np_18px_span)
        try:
            # h1 태그 아래 span.np_18px_span
            title_tag = soup.find('span', class_='np_18px_span')
            if not title_tag: # 혹시 다른 클래스일 경우 대비
                 title_tag = soup.find('div', class_='top_area').find('h1')
            
            raw_title = title_tag.get_text()
            cleaned_title = clean_title(raw_title)
            title_list.append(cleaned_title)
        except Exception as e:
            logging.error(f"제목 추출 실패: {e}")
            return 

        search_plt_list.append('웹페이지(에펨코리아)')
        url_list.append(url)

        # 2. 본문 추출 (업데이트된 선택자: div.xe_content)
        try:
            content_div = soup.find('div', class_='xe_content')
            if content_div:
                post_content = content_div.get_text(separator=' ', strip=True)
                post_content_cleaned = re.sub(r'https?://[^\s]+', '', post_content).strip()
                content_list.append(post_content_cleaned)
            else:
                content_list.append('')
        except Exception:
            content_list.append('')

        search_word_list.append(search)

        # 3. 날짜 추출 (업데이트된 선택자: span.date.m_no)
        # 상세페이지 형식: 2026.01.19 12:59
        try:
            date_str = soup.find('span', class_='date m_no').get_text().strip()
            # YYYY.MM.DD -> YYYY-MM-DD로 변환
            clean_date = date_str.split(' ')[0].replace('.', '-')
            date_list.append(clean_date)
        except:
            date_list.append(target_date)

        # 4. 작성자 추출 (업데이트된 선택자: a.member_plate)
        try:
            writer_tag = soup.find('a', class_='member_plate')
            writer_list.append(writer_tag.get_text(strip=True) if writer_tag else "Unknown")
        except:
            writer_list.append("Unknown")

        now_date_list.append(datetime.now().strftime('%Y-%m-%d'))

        # 데이터프레임 생성 및 저장
        df = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": now_date_list,
        })

        current_dir = os.path.dirname(__file__)
        save_path = os.path.join(current_dir, '..', 'data', 'raw', '23.에펨코리아', target_date)
        os.makedirs(save_path, exist_ok=True)
        file_name = os.path.join(save_path, f'에펨코리아_{search}.csv')
        
        save_to_csv(df, file_name)
        logging.info(f"✅ 수집 성공: {cleaned_title[:15]}...")
        
    except Exception as e:
        logging.error(f"상세 페이지 파싱 오류: {e}")


# [메인 크롤링 진입점]
def fm_main_crw(searchs, start_date, end_date, stop_event):
    target_date = start_date.strftime("%y%m%d")
    
    # 로깅 설정
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
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
    
    wd = setup_driver()     # 목록 탐색용
    wd_dp1 = setup_driver() # 상세 페이지용

    for search in searchs:
        if stop_event.is_set():
            break
            
        page_num = 1
        while True:
            try:
                # https://m.kpedia.jp/w/7006 통합검색(문서 탭) URL 구조 (where=document 필수)
                url_list_page = (
                    f"https://www.fmkorea.com/index.php?act=IS&is_keyword={search}"
                    f"&mid=home&where=document&page={page_num}"
                )
                
                wd.get(url_list_page)
                
                # 검색 결과 리스트(ul.searchResult) 로딩 대기
                try:
                    WebDriverWait(wd, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'searchResult'))
                    )
                except:
                    logging.info("검색 결과 없음 또는 페이지 로딩 실패")
                    break

                soup = BeautifulSoup(wd.page_source, 'html.parser')
                
                # [중요] 게시글 목록(ul.searchResult) 가져오기
                # 주의: 'comment' 클래스가 포함된 ul은 댓글 검색 결과이므로 제외해야 함
                result_uls = soup.find_all('ul', class_='searchResult')
                target_ul = None
                
                for ul in result_uls:
                    # class 리스트에 'comment'가 없으면 우리가 찾는 문서 리스트임
                    if 'comment' not in ul.get('class', []):
                        target_ul = ul
                        break
                
                if not target_ul:
                    logging.info("게시글 목록을 찾을 수 없습니다. (댓글 목록만 있거나 결과 없음)")
                    break
                    
                li_tags = target_ul.find_all('li')
                if not li_tags:
                    break

                stop_crawling = False

                for li in li_tags:
                    try:
                        # [날짜 추출] 목록의 날짜: 2026-01-19 12:59 (형식: YYYY-MM-DD HH:MM)
                        time_span = li.find('span', class_='time')
                        if not time_span:
                            continue
                            
                        date_str = time_span.get_text().strip()
                        post_date_str = date_str.split(' ')[0] 
                        post_date = datetime.strptime(post_date_str, '%Y-%m-%d').date()

                        # 날짜 필터링
                        if post_date > end_date:
                            continue 
                        if post_date < start_date:
                            stop_crawling = True
                            break

                        # [링크 및 제목 추출] dt > a href
                        dt_tag = li.find('dt')
                        if dt_tag and dt_tag.find('a'):
                            a_tag = dt_tag.find('a')
                            link_part = a_tag['href']
                            
                            # 링크가 /939... 형태라면 앞에 도메인 붙이기
                            if link_part.startswith('/'):
                                full_url = f"https://www.fmkorea.com{link_part}"
                            else:
                                full_url = link_part
                            
                            logging.info(f"url 찾음: {full_url}")
                            fm_crw(wd_dp1, full_url, search, target_date)
                            
                    except Exception as e:
                        logging.error(f"리스트 아이템 파싱 에러: {e}")
                        continue

                if stop_crawling:
                    logging.info(f"설정 기간({start_date}) 이전 데이터 도달. 다음 검색어로 이동.")
                    break
                
                page_num += 1
                time.sleep(1) # 과부하 방지

            except Exception as e:
                logging.error(f"페이지 순회 중 치명적 오류: {e}")
                break

    wd.quit()
    wd_dp1.quit()
    
    # 결과 병합
    result_dir = os.path.join(project_root, 'data', 'raw')
    try:
        all_data = pd.concat([
            result_csv_data(search, platform='에펨코리아', subdir=f'23.에펨코리아/{target_date}', base_path='data/raw')
            for search in searchs
        ])
        all_data.to_csv(os.path.join(result_dir, f'에펨코리아_raw_{target_date}.csv'), encoding='utf-8', index=False)
        logging.info(f"최종 병합 완료: 에펨코리아_raw_{target_date}.csv")
    except ValueError:
        logging.warning("수집된 데이터가 없습니다.")