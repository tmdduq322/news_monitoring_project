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

# 실행날짜 변수 및 폴더 생성
today = datetime.now().strftime("%y%m%d")
if not os.path.exists(f'log'):
    os.makedirs(f'log')

logging.basicConfig(
    filename=f'82쿡_log_{today}.txt',  # 로그 파일 이름
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


# 한페이지 크롤링
def cook82_crw(wd, url, search):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'wrap')))
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

        raw_title = soup.find('h4', class_='title bbstitle').find('span').get_text()
        cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        # content =  soup.find('div', id='articleBody').get_text().strip()
        # content_strip = ' '.join(content.split())
        # content_list.append(content_strip)
        # logging.info("내용 추출 성공")

        # <a> 태그 중 이미지/비디오/유튜브 없는 경우만 제거
        for a_tag in content_div.find_all('a'):
            if (
                    not a_tag.find('img') and
                    not a_tag.find('span', class_='scrap_img') and
                    not a_tag.find('video') and
                    not (a_tag.find('iframe') and 'youtube.com' in a_tag.decode_contents())
            ):
                a_tag.decompose()

        # 본문 텍스트 추출 (띄어쓰기 유지)
        post_content = content_div.get_text(separator=' ', strip=True)

        # URL 제거 (텍스트에 포함된 경우만)
        post_content = re.sub(r'https?://[^\s]+', '', post_content)

        # 게시글 내용 추가
        content_list.append(post_content)
        logging.info(f"내용 추출 성공 (URL 제거 및 띄어쓰기 유지)")

        search_plt_list.append('웹페이지(82쿡)')
        url_list.append(url)

        search_word_list.append(search)

        date_text = soup.find('div', class_='readRight').get_text(strip=True)
        date_str = date_text.split()[2]
        date = datetime.strptime(date_str, '%Y-%m-%d')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date_str}")

        # 채널명
        writer_list.append(soup.find('div', class_='readLeft').find('a').get_text())

        current_date_list.append(datetime.now().strftime('%Y-%m-%d'))

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
            "수집시간": current_date_list,
            # "이미지 유무": image_check_list
        })

        # 데이터 저장
        save_to_csv(main_temp, f'data/raw/12.82쿡/{today}/82쿡_{search}.csv')
        logging.info(f"저장완료: data/raw/12.82쿡/{today}/82쿡_{search}.csv")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return pd.DataFrame()


def cook82_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/12.82쿡/{today}'):
        os.makedirs(f'data/raw/12.82쿡/{today}')
        print(f"폴더 생성 완료: {today}")
    else:
        print(f"해당 폴더 존재")
    logging.info(f"========================================================")
    logging.info(f"                    82쿡 크롤링 시작")
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
                logging.info(f"크롤링 시작-검색어: {search}")
                # 제목검색
                url = f'https://www.82cook.com/entiz/enti.php?bn=15&searchType=search&search1=1&keys={search}&page={page_num}'
                # 내용검색
                # url = f'https://www.82cook.com/entiz/enti.php?bn=15&searchType=search&search1=2&keys={search}&page={page_num}'

                wd_dp1.get(url)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'skin1')))

                time.sleep(1)

                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 리스트
                tr_tags = soup_dp1.find('div', id='bbs').find('tbody', ).find_all('tr')
                td_test = soup_dp1.find('div', id='bbs').find('tbody', ).find('tr').find('td', class_='title')
                if not td_test:
                    break
                logging.info(f"검색목록 찾음.")
                for tr in tr_tags:
                    if stop_event.is_set():
                        break
                    after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                    # 공지사항 제거
                    if 'noticeList' in tr.get('class', []):
                        continue

                    try:
                        date_str = tr.find('td', class_='regdate numbers').text
                        date = datetime.strptime(date_str, '%Y/%m/%d').date()
                        logging.info(f"날짜 찾음")
                    except Exception as e:
                        logging.error("날짜 오류 발생: {e}")
                        continue

                    if date > end_date:
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    url_str = tr.find('td', class_='title').find('a').get('href')
                    url = 'https://www.82cook.com/entiz/' + url_str
                    logging.info(f"url 찾음.")
                    cook82_crw(wd, url, search)

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
        result_dir = '결과/82쿡'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='82쿡', subdir='12.82쿡')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/82쿡_raw data_{today}.csv', encoding='utf-8', index=False)

