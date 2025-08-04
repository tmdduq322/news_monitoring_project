import os
import re
import time
import random
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
    filename=f'디시인사이드_log_{today}.txt',  # 로그 파일 이름
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


def dc_crw(wd, url, search):
    try:
        wd.get(f'{url}')
        sleep_random_time = random.uniform(2, 4)
        time.sleep(sleep_random_time)
        WebDriverWait(wd, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".view_content_wrap")))

        soup = BeautifulSoup(wd.page_source, 'html.parser')

        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        image_check_list = []


        raw_title = soup.find('h3', class_='title ub-word').find('span', class_='title_subject').get_text()
        cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content_div = soup.find('div', class_='write_div')

        for og_tag in content_div.find_all('a', class_='og-wrap'):
            og_tag.decompose()
        # # 내용
        # content_tag = soup.find('div', class_='write_div')
        # if content_tag:
        #     content_strip = ' '.join(content_tag.text.split())
        #     content_list.append(content_strip)
        # else:
        #     content_list.append('')

        # <a> 태그 중 이미지가 없는 경우에만 삭제
        for a_tag in content_div.find_all('a'):
            if (
                    not a_tag.find('img') and
                    not a_tag.find('span', class_='scrap_img') and
                    not a_tag.find('video') and
                    not (a_tag.find('iframe') and 'youtube.com' in a_tag.decode_contents())
            ):
                a_tag.decompose()

        post_content = content_div.get_text(separator='\n', strip=True)
        post_content = re.sub(r'http[s]?://\S+', '', post_content)
        content_list.append(post_content)

        # 플랫폼
        search_plt_list.append('웹페이지(dcinside)')

        # 게시물url
        url_list.append(url)

        # 검색어
        search_word_list.append(search)

        # 게시물 날짜
        date_str = soup.find('span', class_='gall_date').text
        date = datetime.strptime(date_str, '%Y.%m.%d %H:%M:%S').date()
        date_list.append(date)

        # 채널명
        nickname = soup.find('span', class_='nickname').get_text()
        ip_tag = soup.find('span', class_='ip')
        if ip_tag:
            ip_address = ip_tag.get_text()
        else:
            ip_address = ''  # IP 클래스가 없는 경우
        writer = f"{nickname}{ip_address}"
        writer_list.append(writer)

        # # 이미지/비디오/유튜브 유무 확인
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
        save_to_csv(main_temp, f'data/raw/22.디시인사이드/{today}/디시인사이드_{search}.csv')
        logging.info(f"저장완료: data/raw/22.디시인사이드/{today}/디시인사이드_{search}.csv")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def dc_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/22.디시인사이드/{today}'):
        os.makedirs(f'data/raw/22.디시인사이드/{today}')
        print(f"폴더 생성 완료: {today}")
    else:
        print(f"해당 폴더 존재")
    logging.info(f"========================================================")
    logging.info(f"                 디시인사이드 크롤링 시작")
    logging.info(f"========================================================")
    wd = setup_driver()
    wd_dp1 = setup_driver()
    for search in searchs:
        page_num = 1
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        while True:
            if stop_event.is_set():
                break
            try:
                if page_num == 121:
                    break
                url_dp1 = f'https://search.dcinside.com/post/p/{page_num}/sort/latest/q/{search}'
                wd_dp1.get(url_dp1)
                sleep_random_time = random.uniform(2, 4)
                time.sleep(sleep_random_time)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 리스트
                li_tags = soup_dp1.find('ul', class_='sch_result_list').find_all('li')

                for li in li_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                    try:
                        date_str = li.find('span', class_='date_time').text
                        date = datetime.strptime(date_str, '%Y.%m.%d %H:%M').date()
                    except Exception as e:
                        logging.error("날짜 오류 발생: {e}")
                        continue

                    if date > end_date:
                        # date_flag = True
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url = li.find('a', class_='tit_txt').get('href')
                    logging.info(f"url 찾음.")
                    dc_crw(wd, url, search)

                if after_start_date:
                    break
                else:
                    page_num += 1

            except Exception as e:
                logging.error(f"오류 발생: {e}")
                break

    wd.quit()
    wd_dp1.quit()

    if not stop_event.is_set():
        result_dir = '결과/디시인사이드'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='디시인사이드', subdir='22.디시인사이드')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/디시인사이드_raw data_{today}.csv', encoding='utf-8', index=False)

