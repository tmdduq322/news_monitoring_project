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

from .utils import setup_driver, save_to_csv, clean_title,result_csv_data

# 실행날짜 변수 및 폴더 생성
today = datetime.now().strftime("%y%m%d")
if not os.path.exists(f'log'):
    os.makedirs(f'log')

logging.basicConfig(
    filename=f'네이트판_log_{today}.txt',
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


# 한페이지 크롤링
def pann_crw(wd, url, search):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(url)
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'posting')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 리스트 초기화 (매번 새로 초기화)
        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        current_date_list = []
        image_check_list = []

        content_div = soup.find('div', class_='posting')

        # 게시물 제목
        try:
            raw_title = soup.find('div', class_='post-tit-info').find('h1').get_text()
            cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
            title_list.append(cleaned_title)
            logging.info(f"제목 추출 성공: {cleaned_title}")
        except Exception as e:
            title_list.append('')  # 제목 없으면 빈 값으로 처리
            logging.error(f"제목 추출 실패: {e}")

        # 게시물 내용
        content_tag = soup.find('div', class_='posting')
        if content_tag:
            # 띄어쓰기 유지하며 본문 추출
            content_text = content_tag.get_text(separator=' ', strip=True)

            # URL 제거
            content_cleaned = re.sub(r'https?://[^\s]+', '', content_text).strip()

            content_list.append(content_cleaned)
            logging.info("내용 추출 성공 (URL 제거됨)")
        else:
            content_list.append('')
            logging.warning("본문 태그 없음")

        # 게시물 URL
        url_list.append(url)
        search_word_list.append(search)
        search_plt_list.append('웹페이지(네이트 판)')

        # 게시물 등록일자
        try:
            date_str = soup.find('div', class_='post-tit-info').find('span', class_='date').get_text()
            date = datetime.strptime(date_str, '%Y.%m.%d ')
            date_list.append(date.strftime('%Y-%m-%d '))
            logging.info(f"날짜 추출 성공: {date_str}")
        except Exception as e:
            date_list.append('')
            logging.error(f"날짜 추출 실패: {e}")

        # 채널명
        try:
            writer = soup.find('div', class_='post-tit-info').find('a', class_='writer').get_text()
            writer_list.append(writer)
            logging.info(f"작성자 추출 성공: {writer}")
        except Exception as e:
            writer_list.append('')
            logging.error(f"작성자 추출 실패: {e}")

        # 수집 시간
        current_date_list.append(datetime.now().strftime('%Y-%m-%d '))

        # # 이미지 유무 체크
        # try:
        #     bg_images = content_div.find_all('span', class_='scrap_img')
        #     images = content_div.find_all('img')
        #     videos = content_div.find_all('video')
        #     iframes = content_div.find_all('iframe')
        #     youtube_videos = [iframe for iframe in iframes if 'youtube.com' in iframe.get('src', '')]
        #     article_links = content_div.find_all('a', href=True)
        #     link_urls = [a['href'] for a in article_links if 'http' in a['href']]
        #     text_content = content_div.get_text()
        #     text_urls = re.findall(r'(https?://[^\s]+)', text_content)
        #
        #     # 이미지, 비디오, 유튜브 영상이 하나라도 있으면 'O', 없으면 ' '
        #     if bg_images or images or videos or youtube_videos or link_urls or text_urls:
        #         image_check_list.append('O')
        #     else:
        #         image_check_list.append(' ')
        #         logging.info(f'이미지 없음: {url}')
        # except Exception as e:
        #     image_check_list.append(' ')
        #     logging.error(f"이미지 확인 오류: {e}")

        # 데이터프레임 생성
        main_temp = pd.DataFrame({
            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            "수집시간": current_date_list,
            # "이미지 유무": image_check_list
        })

        # 데이터 저장
        save_to_csv(main_temp, f'data/raw/6.네이트판/{today}/네이트판_{search}.csv')
        logging.info(f'저장 완료: data/raw/6.네이트판/{today}/네이트판_{search}.csv')

    except TimeoutException as e:
        logging.error(f"페이지 로딩 시간 초과: {e}")
        return None

    except WebDriverException as e:
        logging.error(f"웹드라이버 에러: {e}")
        return None

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return None


def paan_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/6.네이트판/{today}'):
        os.makedirs(f'data/raw/6.네이트판/{today}')
        print(f"폴더 생성 완료: {today}")
    else:
        print(f"해당 폴더 존재")
    logging.info(f"========================================================")
    logging.info(f"                    네이트판 크롤링 시작")
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
                logging.info(f"검색어: {search}")
                url_dp1 = f'https://pann.nate.com/search/talk?q={search}&sort=DD&page={page_num}'

                logging.info(f"주소: {url_dp1}")
                wd_dp1.get(url_dp1)
                # 페이지 로딩 시간 초과 시, 넘어가고 로그남김.
                try:
                    WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'srcharea')))
                except TimeoutException:
                    logging.warning(f"==> 페이지 로딩 시간 초과 : {url_dp1}")
                    break

                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 리스트
                tr_tags = soup_dp1.find('ul', class_='s_list').find_all('li')
                logging.info(f"검색목록 찾음.")
                for tr in tr_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                    date_str = tr.find('span', class_='date').text
                    date_str = '20' + date_str
                    date = datetime.strptime(date_str, '%Y.%m.%d %H:%M').date()
                    logging.info(f"날짜 찾음")

                    if date > end_date:
                        # date_flag = True
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    # 첫째 주 게시물만
                    # if date.day <= 7:
                    url = 'https://pann.nate.com' + tr.find('div', class_='tit').find('a').get('href')
                    logging.info(f"url 찾음.")
                    pann_crw(wd, url, search)

                # 시작 날짜 이후의 게시글이 없고 기간 내 게시글도 없으면 종료
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
        result_dir = '결과/네이트판'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='네이트판', subdir='6.네이트판')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/네이트판_raw data_{today}.csv', encoding='utf-8', index=False)

