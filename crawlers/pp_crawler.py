import re
import os
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
    filename=f'log/뽐뿌_log_{today}.txt',  # 로그 파일 이름
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


# 한페이지 크롤링
def pp_crw(wd, url, search):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'board-contents')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 추후 수정하기
        writer_list = []

        title_list = []
        content_list = []
        url_list = []

        search_plt_list = []
        search_word_list = []
        date_list = []

        # 추출날짜 추후 삭제
        now_date = []
        image_check_list = []

        content_div = soup.find('td', class_='board-contents')
        raw_title = soup.find('div', id='topTitle').find('h1').get_text()
        cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")
        search_plt_list.append('웹페이지(뽐뿌)')
        url_list.append(url)

        # # 1. 기사 포함 내용
        # content_list.append(soup.find('td', class_='board-contents').get_text(strip=True))
        # logging.info("내용 추출 성공")

        try:
            content_div = soup.find('td', class_='board-contents')

            # 기사(div.scrap_bx) 제외
            for scrap_box in content_div.find_all('div', class_='scrap_bx'):
                scrap_box.decompose()

            # 본문 텍스트 추출 (띄어쓰기 유지)
            post_content = content_div.get_text(separator=' ', strip=True)

            # URL 제거
            post_content_cleaned = re.sub(r'https?://[^\s]+', '', post_content).strip()

            content_list.append(post_content_cleaned)
            logging.info("내용 추출 성공 (기사 제외 + URL 제거)")

        except Exception as e:
            content_list.append('')
            logging.error(f"본문 추출 실패: {e}")

        search_word_list.append(search)

        # 날짜 출력
        pp_date_str = soup.find('ul', class_='topTitle-mainbox').find_all('li')[1].get_text()
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', pp_date_str)
        date_list = date_match.group(1)

        # 채널명
        name_element = soup.find('a', class_='baseList-name')
        if name_element:
            name = name_element.get_text()
        else:
            name = soup.find('strong', class_="none").get_text()

        writer_list.append(name)

        # 추출시간
        now_date.append(datetime.now().strftime('%Y-%m-%d'))

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
            # "이미지 유무": image_check_list,
        })
        base_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', '1.뽐뿌')
        file_name = os.path.join(base_path, f'뽐뿌_{today}_{search}.csv')
        save_to_csv(main_temp, file_name)
        # 데이터 저장
        # save_to_csv(main_temp, f'../data/raw/1.뽐뿌/{today}/뽐뿌_{search}.csv')
        # logging.info(f"저장완료: ../data/raw/뽐뿌/{today}/뽐뿌_{search}.csv")

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        print(f"오류 발생: {e}")
        return pd.DataFrame()


def pp_main_crw(searchs, start_date, end_date, stop_event):
    if not os.path.exists(f'../data/raw/1.뽐뿌/{today}'):
        os.makedirs(f'../data/raw/1.뽐뿌/{today}')
        print(f"폴더 생성 완료: {today}")

    logging.info(f"========================================================")
    logging.info(f"                    뽐뿌 크롤링 시작")
    logging.info(f"========================================================")
    wd = setup_driver()
    wd_dp1 = setup_driver()

    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        page_num = 1

        while True:
            try:
                url_dp1 = f'https://www.ppomppu.co.kr/search_bbs.php?search_type=sub_memo&page_no={page_num}&keyword={search}&page_size=50&bbs_id=&order_type=date&bbs_cate=2'
                wd_dp1.get(url_dp1)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'results_board')))
                time.sleep(1)
                soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                # 검색결과 리스트
                li_tags = soup_dp1.find('div', class_='results_board').find_all('div', class_="content")

                for li in li_tags:

                    try:
                        date_str = li.find('p', class_='desc').find_all('span')[2].get_text()
                        date = datetime.strptime(date_str, '%Y.%m.%d').date()
                    except Exception as e:
                        logging.error("날짜 오류 발생: {e}")
                        continue

                    after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                    if date > end_date:
                        # date_flag = True
                        continue
                    if date < start_date:
                        after_start_date = True
                        break

                    # if date.day <= 7:
                    url_dp2_num = li.find('span', class_='title').find('a').get('href')
                    url = 'https://www.ppomppu.co.kr' + url_dp2_num
                    logging.info(f"url 찾음.")
                    pp_crw(wd, url, search)

                if after_start_date:
                    break
                else:
                    page_num += 1

            except Exception as e:
                print(f"오류 발생: {e}")
                break

            page_num += 1  # 페이지 수 증가

    wd.quit()
    wd_dp1.quit()

    result_dir = 'data/raw'
    os.makedirs(result_dir, exist_ok=True)

    all_data = pd.concat([
        result_csv_data(search, platform='뽐뿌', subdir='1.뽐뿌', base_path='data/raw')
        for search in searchs
    ])

    all_data.to_csv(f'{result_dir}/뽐뿌_raw_{today}.csv', encoding='utf-8', index=False)

