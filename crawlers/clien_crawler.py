import re
import os
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from datetime import datetime

from .utils import setup_driver, save_to_csv, clean_title,result_csv_data

# 실행날짜 변수 및 폴더 생성
today = datetime.now().strftime("%y%m%d")
if not os.path.exists(f'log'):
    os.makedirs(f'log')

logging.basicConfig(
    filename=f'클리앙_log_{today}.txt',  # 로그 파일 이름
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


# 한페이지 크롤링
def clien_crw(wd, url, search):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")

        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'post_content')))
        time.sleep(1)
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        writer_list = []

        title_list = []
        content_list = []
        url_list = []
        image_check_list = []

        search_plt_list = []
        search_word_list = []
        date_list = []
        now_date = []
        # 추출날짜 추후 삭제
        # 이미지 유무 추출
        raw_title = soup.find('h3', class_='post_subject').find('span').get_text()
        cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")
        try:
            content_div = soup.find('div', class_='post_content')
            content = content_div.get_text(separator=' ', strip=True)  # 띄어쓰기 유지

            # 본문에서 URL 제거
            content_cleaned = re.sub(r'https?://[^\s]+', '', content).strip()

            content_list.append(content_cleaned)
            logging.info("내용 추출 성공")

        except Exception as e:
            content_list.append('')
            logging.error(f"본문 추출 실패: {e}")

        search_plt_list.append('웹페이지(클리앙)')
        url_list.append(url)

        search_word_list.append(search)
        # 이미지/비디오/유튜브 유무 확인

        # image_check_list = []

        # try:
        #     content_div = soup.find('div', class_='post_content')
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
        #     else:
        #         image_check_list.append(' ')
        #         logging.info(f'이미지 없음: {url}')
        # except Exception as e:
        #     logging.error(f"미디어 확인 오류: {e}")
        #     image_check_list.append(' ')
        #
        # # 날짜 출력 (수정일 제외)
        # clien_date_str = soup.find('div', class_='post_author').find('span').text.strip()

        # if soup.find('span', class_='lastdate'):
        # "수정일" 이후의 텍스트 제거
        date_str = soup.find(class_="view_count date").text.strip()
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', date_str)
        date = date_match.group()

        # date = datetime.strptime(clien_date_str, '%Y-%m-%d %H:%M:%S')
        date_list.append(date)

        # 채널명
        writer_tag = soup.find('span', class_='nickname')

        writer_strip = ' '.join(writer_tag.text.split())
        writer_list.append(writer_strip)

        # 추출시간
        now_date.append(datetime.now().strftime('%Y-%m-%d'))
        # 임시 데이터프레임 생성
        main_temp = pd.DataFrame({

            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": now_date,
            # "이미지 유무": image_check_list
        })

        # 데이터 저장
        save_to_csv(main_temp, f'data/raw/2.클리앙/{today}/클리앙_{search}.csv')
        logging.info(f"저장완료: {search}")

    except TimeoutException as e:
        logging.error(f"페이지 로딩 시간 초과: {e}")
        return None

    except WebDriverException as e:
        logging.error(f"웹드라이버 에러: {e}")
        return None

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return None


def clien_main_crw(searchs, start_date, end_date, stop_event):
    if not os.path.exists(f'data/raw/2.클리앙/{today}'):
        os.makedirs(f'data/raw/2.클리앙/{today}')
        print(f"폴더 생성 완료: {today}")

    logging.info(f"========================================================")
    logging.info(f"                    클리앙 크롤링 시작")
    logging.info(f"========================================================")
    wd = setup_driver()
    wd_dp1 = setup_driver()

    for search in searchs:
        page_num = 1
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        while True:
            try:
                url_dp1 = f'https://www.clien.net/service/search?q={search}&sort=recency&p={page_num}&boardCd=&isBoard=false'
                wd_dp1.get(url_dp1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'nav_content')))
                time.sleep(1)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                date_flag = False
                after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                # 검색결과 리스트
                li_tags = soup_dp1.find_all('div', class_='list_item symph_row jirum')
                if not li_tags:
                    break
                for li in li_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False

                    try:
                        date_str = li.find('div', class_='list_time').find('span', class_="timestamp").get_text()
                        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').date()
                    except Exception as e:
                        logging.error(f"날짜 오류 발생: {e}")
                        continue

                    if date > end_date:
                        continue

                    if date < start_date:
                        after_start_date = True
                        break

                    # if date.day <= 7:
                    url_dp2_num = li.find('a', class_='subject_fixed').get('href')
                    url = 'https://www.clien.net' + url_dp2_num
                    logging.info(f"url 찾음.")

                    clien_crw(wd, url, search)

                if after_start_date:
                    break
                elif page_num == 50:
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
        result_dir = '결과/클리앙'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='클리앙', subdir='2.클리앙')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/클리앙_raw data_{today}.csv', encoding='utf-8', index=False)

