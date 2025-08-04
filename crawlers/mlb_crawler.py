import os
import re
import random
import time
import logging
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
filename=f'엠엘비파크_log_{today}.txt',  # 로그 파일 이름
level=logging.INFO,  # 로그 레벨
format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
encoding='utf-8'  # 인코딩 설정
)


# 한페이지 크롤링
def mlb_crw(wd, url, search):
    try:
        logging.info(f"크롤링 시작:{search}: {url}")
        wd.get(f'{url}')
        sleep_random_time = random.uniform(2, 4)
        time.sleep(sleep_random_time)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'ar_txt')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 추후 수정하기
        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        current_date_list = []
        image_check_list = []

        content_div = soup.find('div', id='articleBody')

        # 1. 제목 div 찾기
        title_div = soup.find('div', class_='titles')

        if title_div and title_div.find('span', class_='word'):
            title_div.find('span', class_='word').decompose()

        raw_title = title_div.get_text(strip=True) if title_div else ''

        cleaned_title = clean_title(raw_title)
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        # title = soup.find('div', class_='titles')
        # if title.find('span', class_='word'):
        #     title.find('span', class_='word').decompose()

        # title_list.append(title.get_text(strip=True))
        # logging.info(f"제목 추출 성공: {title.get_text(strip=True)}")

        # content_tag = soup.find('div', id='contentDetail')
        # if content_tag:
        #     content_tag.find('div', class_='tool_cont').decompose()
        #     content = content_tag.get_text().strip()
        #     content_strip = ' '.join(content.split())
        #     content_list.append(content_strip)
        # else:
        #     content_list.append('')
        # logging.info("내용 추출 성공")

        content_div = soup.find('div', id='contentDetail')
        content_tag = soup.find('div', id='contentDetail')

        if content_tag:
            # 필요 없는 내부 요소 제거
            tool_div = content_tag.find('div', class_='tool_cont')
            if tool_div:
                tool_div.decompose()

            # <a> 태그 중 이미지나 미디어 없는 경우만 제거
            for a_tag in content_tag.find_all('a'):
                if (
                        not a_tag.find('img') and
                        not a_tag.find('span', class_='scrap_img') and
                        not a_tag.find('video') and
                        not (a_tag.find('iframe') and 'youtube.com' in str(a_tag))
                ):
                    a_tag.decompose()

        post_content = content_tag.get_text(separator='\n', strip=True)
        post_content = re.sub(r'http[s]?://\S+', '', post_content)
        content_list.append(post_content)
        logging.info(f"내용 추출 성공: {post_content}")

    except Exception as e:
        content_list.append('')
        logging.info("내용 태그 없음, 빈 문자열 저장")

    search_plt_list.append('웹페이지(MLBPARK)')
    url_list.append(url)

    search_word_list.append(search)

    date_str = soup.find('div', class_='text3').find('span', class_='val').get_text()
    date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
    date_list.append(date.strftime('%Y-%m-%d'))
    logging.info(f"날짜 추출 성공: {date.strftime('%Y-%m-%d')}")

    writer_list.append(soup.find('div', class_='text1 bat').find('span', class_='nick').get_text())

    # 이미지/비디오/유튜브 유무 확인
    # try:
    #     # 1. scrap_img로 표시된 background-image 확인
    #     bg_images = content_div.find_all('span', class_='scrap_img')
    #
    #     # 2. 일반 이미지 (img 태그) 확인
    #     images = content_div.find_all('img')
    #
    #     # 3. 비디오 확인 (video 태그)
    #     videos = content_div.find_all('video')
    #
    #     # 4. 유튜브 영상 확인 (iframe 태그의 youtube.com 포함 여부)
    #     iframes = content_div.find_all('iframe')
    #     youtube_videos = [iframe for iframe in iframes if iframe.get('src') and 'youtube.com' in iframe['src']]
    #
    #     # 5. 하이퍼링크로 포함된 모든 URL
    #     article_links = content_div.find_all('a', href=True)
    #     link_urls = [
    #         a['href'] for a in article_links if 'http' in a['href']
    #     ]
    #
    #     # 6. 텍스트 안에 포함된 URL 찾기 (일반 텍스트 URL 감지)
    #     text_content = content_div.get_text()
    #     text_urls = re.findall(r'(https?:\/\/[^\s]+|https?:)', text_content)
    #
    #     # 이미지, 비디오, 유튜브 영상이 하나라도 있으면 'O', 없으면 ' '
    #     if bg_images or images or videos or youtube_videos or link_urls or text_urls:
    #         image_check_list.append('O')
    #     else:
    #         image_check_list.append(' ')
    #         logging.info(f'이미지 없음: {url}')
    # except Exception as e:
    #     logging.error(f"미디어 확인 오류: {e}")
    #     image_check_list.append(' ')

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
    save_to_csv(main_temp, f'data/raw/21.엠엘비파크/{today}/엠엘비파크_{search}.csv')
    logging.info(f'저장완료 : data/raw/21.엠엘비파크/{today}/엠엘비파크_{search}.csv')


def mlb_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/21.엠엘비파크/{today}'):
        os.makedirs(f'data/raw/21.엠엘비파크/{today}')
        print(f"폴더 생성 완료: {today}")
    else:
        print(f"해당 폴더 존재")
    logging.info(f"========================================================")
    logging.info(f"                 엠엘비파크 크롤링 시작")
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
                logging.info(f"크롤링 시작-검색어: {search}")

                url_dp1 = f'https://mlbpark.donga.com/mp/b.php?p={page_num}&m=search&b=bullpen&query={search}&select=sct&subquery=&subselect=&user='
                logging.info(f"크롤링 시작-주소: {url_dp1}")
                wd_dp1.get(url_dp1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'tbl_type01')))
                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                # 검색결과 리스트
                tr_tags = soup_dp1.find('table', class_='tbl_type01').find('tbody').find_all('tr')

                for tr in tr_tags:
                    if stop_event.is_set():
                        break
                    try:
                        date_str = tr.find('span', class_='date').text
                        date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except Exception as e:
                        logging.error("날짜 오류 발생: {e}")
                        continue
                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url = tr.find('div', class_='tit').find('a', class_='txt').get('href')
                    logging.info(f"url 찾음.")
                    mlb_crw(wd, url, search)

                if after_start_date:
                    break
                else:
                    page_num += 30  # 페이지 수 증가

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break

    wd.quit()
    wd_dp1.quit()

    if not stop_event.is_set():
        result_dir = '결과/엠엘비파크'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='엠엘비파크', subdir='21.엠엘비파크')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/엠엘비파크_raw data_{today}.csv', encoding='utf-8', index=False)
