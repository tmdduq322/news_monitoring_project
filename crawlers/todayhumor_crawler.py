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
# 로그 설정
logging.basicConfig(
    filename=f'오늘의유머_log_{today}.txt',
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


# 한페이지 크롤링
def todayhumor_crw(wd, url, search):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'viewContent')))
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

        raw_title = soup.find('div', class_='viewSubjectDiv').text
        cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        # title_tag = soup.find('div', class_='viewSubjectDiv').text
        # title_strip = ' '.join(title_tag.split())
        # title_list.append(title_strip)
        # logging.info(f"제목 추출 성공: {title_strip}")

        content_tag = soup.find('div', class_='viewContent')
        if content_tag:
            # 본문 텍스트 추출 (띄어쓰기 유지)
            content_div = content_tag.get_text(separator=' ', strip=True)

            # URL 제거
            content_cleaned = re.sub(r'https?://[^\s]+', '', content_div).strip()

            content_list.append(content_cleaned)
            logging.info("내용 추출 성공 (URL 제거됨)")

        search_plt_list.append('웹페이지(오늘의유머)')
        url_list.append(url)

        search_word_list.append(search)

        # # 이미지/비디오/유튜브 유무 확인
        # try:
        #     # 1. scrap_img로 표시된 background-image 확인
        #     bg_images = content_div.find_all('div', class_='viewContent')
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
        #     text_urls = re.findall(r'(https?://[^\s]+)', text_content)
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

        divs = soup.find('div', class_='writerInfoContents').find_all('div')
        date_str = divs[6].text.strip().replace('등록시간 : ', '')
        date = datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date}")

        # 채널명
        writer_list.append(soup.find('span', id='viewPageWriterNameSpan').find('b').text)

        main_temp = pd.DataFrame({

            "검색어": search_word_list,
            "플랫폼": search_plt_list,
            "게시물 URL": url_list,
            "게시물 제목": title_list,
            "게시물 내용": content_list,
            "게시물 등록일자": date_list,
            "계정명": writer_list,
            # "이미지 유무": image_check_list,
        })

        # 데이터 저장
        save_to_csv(main_temp, f'data/raw/5.오늘의유머/{today}/오늘의유머_{search}.csv')
        logging.info(f'data/raw/5.오늘의유머/{today}/오늘의유머_{search}.csv')

    except TimeoutException as e:
        logging.error(f"페이지 로딩 시간 초과: {e}")
        return None

    except WebDriverException as e:
        logging.error(f"웹드라이버 에러: {e}")
        return None

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return None


def todayhumor_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/5.오늘의유머/{today}'):
        os.makedirs(f'data/raw/5.오늘의유머/{today}')
        print(f"폴더 생성 완료: {today}")
    else:
        print(f"해당 폴더 존재")
    logging.info(f"========================================================")
    logging.info(f"                    오늘의유머 크롤링 시작")
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

                url_dp1 = f'https://www.todayhumor.co.kr/board/list.php?table=&page={page_num}&kind=search&keyfield=subject&keyword={search}'
                # logging.info(f"크롤링 시작-주소: {url_dp1}")
                wd_dp1.get(url_dp1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'table_list')))

                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 리스트
                tr_tags = soup_dp1.find('table', class_='table_list').find_all('tr', class_='view list_tr_sisa')
                logging.info(f"검색목록 찾음.")
                if not tr_tags:
                    break  # → 다음 검색어로 이동
                after_start_date = False
                for tr in tr_tags:
                    if stop_event.is_set():
                        break
                    date_str = tr.find('td', class_='date').text
                    date_str = '20' + date_str
                    date = datetime.strptime(date_str, '%Y/%m/%d %H:%M').date()
                    logging.info(f"날짜 찾음")

                    if date > end_date:
                        # date_flag = True
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    # if date.day <= 7:
                    url = 'https://www.todayhumor.co.kr' + tr.find('td', class_='subject').find('a').get('href')
                    logging.info(f"url 찾음.")
                    todayhumor_crw(wd, url, search)

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
        result_dir = '결과/오늘의유머'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='오늘의유머', subdir='5.오늘의유머')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/오늘의유머_raw data_{today}.csv', encoding='utf-8', index=False)
