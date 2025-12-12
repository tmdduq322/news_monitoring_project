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

logging.basicConfig(
    filename=f'루리웹_log_{today}.txt',  # 로그 파일 이름
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


# 게시글 제목 정리
def clean_title(title):
    # 제목 뒤 넘버링 제거
    title = re.sub(r'\d+$', '', title).strip()
    # 파일 확장자 제거 (.jpg, .mp4 등)
    title = re.sub(r'\.(jpg|png|gif|mp4|avi|mkv|webm|jpeg)$', '', title, flags=re.IGNORECASE).strip()
    # 초성 제거 (자음만 있는 경우)
    title = re.sub(r'^[ㄱ-ㅎㅏ-ㅣ]+$', '', title).strip()
    # 따옴표 제거
    title = title.replace('"', '').strip()
    return title


# 한페이지 크롤링
def rw_crw(wd, url, search):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(random.uniform(1, 4))
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'view_content.autolink')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 리스트 초기화
        writer_list = []
        title_list = []
        content_list = []
        url_list = []
        search_plt_list = []
        search_word_list = []
        date_list = []
        image_check_list = []

        content_div = soup.find('div', class_='view_content autolink')

        # 제목 추출
        raw_title = soup.find('span', class_='subject_inner_text').get_text()
        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        # 본문 추출
        content_tag = soup.find('div', class_='view_content autolink')
        content_text = content_tag.get_text(separator=' ', strip=True)

        # URL 제거
        content_cleaned = re.sub(r'https?://[^\s]+', '', content_text).strip()

        # 추가 정보 영역도 URL 제거 + 띄어쓰기 유지
        link_box_tag = soup.find('div', class_='source_url box_line_with_shadow')
        extra = ""
        if link_box_tag:
            extra_text = link_box_tag.get_text(separator=' ', strip=True)
            extra = re.sub(r'https?://[^\s]+', '', extra_text).strip()

        # 최종 본문 구성
        full_content = f"{content_cleaned} {extra}".strip()
        content_list.append(full_content)
        logging.info("내용 추출 성공 (URL 제거 + 띄어쓰기 유지)")

        search_plt_list.append('웹페이지(루리웹)')
        url_list.append(url)
        search_word_list.append(search)

        # # 이미지 유무 확인 (루리웹 구조 반영)
        # try:
        #     content_div = soup.find('div', class_='view_content autolink')
        #
        #     # 1. 일반 이미지 (img 태그) 확인
        #     images = content_div.find_all('img')
        #
        #     # 2. 비디오 확인 (video 태그)
        #     videos = content_div.find_all('video')
        #
        #     # 3. 유튜브 영상 확인 (iframe 태그의 youtube.com 포함 여부)
        #     iframes = content_div.find_all('iframe')
        #     youtube_videos = [iframe for iframe in iframes if iframe.get('src') and 'youtube.com' in iframe['src']]
        #
        #     # 4. 하이퍼링크로 포함된 모든 URL
        #     article_links = content_div.find_all('a', href=True)
        #     link_urls = [a['href'] for a in article_links if 'http' in a['href']]
        #
        #     # 5. 텍스트 안에 포함된 URL 찾기 (일반 텍스트 URL 감지)
        #     text_content = content_div.get_text()
        #     text_urls = re.findall(r'(https?://[^\s]+)', text_content)
        #
        #     # 이미지, 비디오, 유튜브 영상이 하나라도 있으면 'O', 없으면 ' '
        #     if images or videos or youtube_videos or link_urls or text_urls:
        #         image_check_list.append('O')
        #         logging.info(f"이미지 있음: {url}")
        #     else:
        #         image_check_list.append(' ')
        #         logging.info(f"이미지 없음: {url}")
        #
        # except Exception as e:
        #     logging.error(f"미디어 확인 오류: {e}")
        #     image_check_list.append(' ')

        # 날짜 출력
        rw_date_str = soup.find('span', class_='regdate').text.strip().split(' ')[0]
        date_list.append(rw_date_str)

        # 채널명 추출
        writer_list.append(soup.find('a', class_='nick').get_text())

        # 임시 데이터프레임 생성
        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            # "이미지 유무": image_check_list
        })

        # 데이터 저장
        save_to_csv(main_temp, f'data/raw/4.루리웹/{today}/루리웹_{search}.csv')
        logging.info(f"저장완료: {search}")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        print(f"오류 발생: {e}")
        return pd.DataFrame()


def rw_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/4.루리웹/{today}'):
        os.makedirs(f'data/raw/4.루리웹/{today}')
        print(f"폴더 생성 완료: {today}")

    logging.info(f"========================================================")
    logging.info(f"                    루리웹 크롤링 시작")
    logging.info(f"========================================================")
    wd = setup_driver()
    wd_dp1 = setup_driver()

    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        page_num = 1

        while True:
            if stop_event.is_set():
                break
            try:
                url_dp1 = f'https://bbs.ruliweb.com/search?q={search}&page={page_num}#board_search&gsc.tab=0&gsc.q={search}&gsc.page=1'
                wd_dp1.get(url_dp1)
                wd_dp1.refresh()

                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.ID, 'board_search')))
                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 리스트
                li_tags = soup_dp1.find('div', id='board_search').find_all('li', class_="search_result_item")

                for li in li_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                    try:
                        date_str = li.find('span', class_='time').get_text()
                        date = datetime.strptime(date_str, '%Y.%m.%d').date()
                        logging.info(f"날짜 찾음")
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    # if date.day <= 7:
                    url = li.find('a', class_='title text_over').get('href')
                    logging.info(f"url 찾음.")
                    rw_crw(wd, url, search)

                if after_start_date:
                    break
                else:
                    page_num += 1

            except Exception as e:
                print(f"오류 발생: {e}")
                break
    wd.quit()
    wd_dp1.quit()
    if not stop_event.is_set():
        result_dir = '결과/루리웹'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='루리웹', subdir='4.루리웹')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/루리웹_raw data_{today}.csv', encoding='utf-8', index=False)


