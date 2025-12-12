import os
import re
import random
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from datetime import datetime, timedelta

from .utils import setup_driver, save_to_csv, result_csv_data


def parse_date(date_str):
    try:
        # 1. '어제' 형식 처리
        if "어제" in date_str:
            return (datetime.now() - timedelta(days=1)).date()

        # 2. '몇일 전' 형식 처리
        if "일 전" in date_str:
            days_ago = int(date_str.split("일 전")[0].strip())
            return (datetime.now() - timedelta(days=days_ago)).date()

        # 3. '몇시간 전' 형식 처리
        if "시간 전" in date_str:
            hours_ago = int(date_str.split("시간 전")[0].strip())
            return (datetime.now() - timedelta(hours=hours_ago)).date()

        # 4. '몇분 전' 형식 처리
        if "분 전" in date_str:
            minutes_ago = int(date_str.split("분 전")[0].strip())
            return (datetime.now() - timedelta(minutes=minutes_ago)).date()

        # 5. '몇개월 전' 형식 처리
        if "개월 전" in date_str:
            months_ago = int(date_str.split("개월 전")[0].strip())
            return (datetime.now() - relativedelta(months=months_ago)).date()

        # 6. YYYY/MM/DD 형식 처리
        if "/" in date_str:
            return datetime.strptime(date_str.replace(" - ", "").strip(), '%Y/%m/%d').date()

        # 7. 기본 형식 처리 (YYYY. MM. DD.)
        return datetime.strptime(date_str.replace(" - ", "").strip(), '%Y. %m. %d').date()

    except Exception as e:
        logging.error(f"날짜 파싱 오류: {e} :: 원본 날짜: {date_str}")
        return None


def random_sleep(min_time=1, max_time=3):
    """지정한 범위 안에서 랜덤 대기 시간을 설정합니다."""
    sleep_time = random.uniform(min_time, max_time)
    logging.info(f"랜덤 대기 시간: {sleep_time:.2f}초")
    time.sleep(sleep_time)
# 실행날짜 변수 및 폴더 생성
today = datetime.now().strftime("%y%m%d")
if not os.path.exists(f'log'):
    os.makedirs(f'log')

# 로그 설정
logging.basicConfig(
    filename=f'인스티즈_log_{today}.txt',
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


# 한페이지 크롤링
def instiz_crw(wd, url, search, date):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")

        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'memo_content')))
        random_sleep(2, 5)

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

        content_div = soup.find('div', id='memo_content_1')

        title = soup.find('td', class_='tb_top').find('span', id='nowsubject')
        # 카테고리 제거
        info_tag = title.find('span')
        if info_tag:
            info_tag.extract()

        # '댓글 수' 제거
        cmt_tag = title.find('span', class_='cmt')
        if cmt_tag:
            cmt_tag.extract()
        # 채널명
        try:
            # 작성자 정보가 있는 div를 먼저 찾기
            tb_left_div = soup.find('div', class_='tb_left')
            writer_name = '익명'

            # 작성자 정보가 있을 때만 처리
            if tb_left_div:
                writer_tag = tb_left_div.find('a', onclick=re.compile("prlayer_print"))
                if writer_tag:
                    writer_name = writer_tag.get_text().strip()
                    logging.info(f"작성자 추출 성공: {writer_name}")
                else:
                    logging.info("작성자 정보 없음")
            else:
                logging.info("작성자 정보가 포함된 div를 찾지 못함")

            # 결과 추가
            writer_list.append(writer_name)

        except Exception as e:
            writer_list.append('익명')
            logging.error(f"채널명 추출 실패: {e}")

        # '아이콘' 제거
        icon_tag = title.find('i', class_='far fa-image fa-image-custom')
        if icon_tag:
            icon_tag.extract()
        title_list.append(title.get_text().strip())

        content_tag = soup.find('div', id='memo_content_1')
        if content_tag.find('span', class_='sorrybaby'):  # 회원 전용 글
            logging.info("회원에게만 공개된 글")
            return None
        else:
            content = content_tag.get_text(separator=' ', strip=True)

            # URL 제거
            content_cleaned = re.sub(r'https?://[^\s]+', '', content).strip()

            content_list.append(content_cleaned)
            logging.info("내용 추출 성공")

        search_plt_list.append('웹페이지(인스티즈)')
        url_list.append(url)

        search_word_list.append(search)

        # date_tag = soup.find('div', class_='tb_left').find('span', itemprop='datePublished')
        # print(f'date_tag:{date_tag}')
        # date_str = ''
        # 'content' 속성 값 추출
        # if date_tag and date_tag.has_attr('content'):
        #     date_iso = date_tag['content']
        #     date_str = date_iso.split('T')[0]
        #     logging.info(f"날짜 추출 성공: {date_str}")

        # date = datetime.strptime(date_str, '%Y-%m-%d')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date}")

        current_date_list.append(datetime.now().strftime('%Y-%m-%d '))

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
        save_to_csv(main_temp, f'data/raw/7.인스티즈/{today}/인스티즈_{search}.csv')
        logging.info(f'data/raw/7.인스티즈/{today}/인스티즈_{search}.csv')

    except TimeoutException as e:
        logging.error(f"페이지 로딩 시간 초과: {e}")
        return None

    except WebDriverException as e:
        logging.error(f"웹드라이버 에러: {e}")
        return None

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return None


# 검색결과 요소 for문
def result_soup(wd, wd_dp1, start_date, end_date, search, collected_urls, stop_event):
    soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')
    div_tags = soup_dp1.find_all('div', class_='result_search')
    logging.info(f"검색목록 찾음.")
    after_start_date = False
    for div in div_tags:
        # 공지사항 패스
        if stop_event.is_set():
            break
        after_start_date = False  # 날짜가 시작 날짜 이후인 경우

        try:
            date_str = div.find('span', class_='search_content').find('span', class_='minitext3').text
            date = parse_date(date_str)
            if date is None:
                logging.info(f"날짜 파싱 실패: {date_str}")
                continue
            logging.info(f"날짜 찾음 : {date}")
        except Exception as e:
            logging.info(f"날짜 에러 : {e} :: 원본 날짜: {date_str}")
            continue

        if date > end_date:
            continue

        if date < start_date:
            after_start_date = True
            break

        # if date.day <= 7:

        url = div.find('a').get('href')
        if url not in collected_urls:
            if stop_event.is_set():
                break
            logging.info(f"url 찾음: {url}")
            collected_urls.add(url)
            instiz_crw(wd, url, search, date)

    return after_start_date



def instiz_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/7.인스티즈/{today}'):
        os.makedirs(f'data/raw/7.인스티즈/{today}')
        print(f"폴더 생성 완료: {today}")
    else:
        print(f"해당 폴더 존재")
    logging.info(f"========================================================")
    logging.info(f"                    인스티즈 크롤링 시작")
    logging.info(f"========================================================")
    wd = setup_driver()
    wd_dp1 = setup_driver()

    # 수집할 게시판
    category = ['pt', 'name', 'name_enter']
    for search in searchs:
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        for cate in category:
            if stop_event.is_set():
                break
            collected_urls = set()  # 이미 수집한 URL을 저장

            try:
                logging.info(f"크롤링 시작-검색어: {search}")
                logging.info(f"크롤링 카테고리: {cate}")
                url = f'https://www.instiz.net/popup_search.htm?id={cate}&k={search}'
                wd_dp1.get(url)
                WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'search_container')))
                time.sleep(1)
                # soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                while True:
                    if stop_event.is_set():
                        break
                    # 검색결과 리스트
                    after_start_date = None
                    after_start_date = result_soup(wd, wd_dp1, start_date, end_date, search, collected_urls, stop_event)

                    if after_start_date:
                        break
                    else:
                        try:
                            logging.info("더보기 버튼 클릭.")
                            more_button = wd_dp1.find_element(By.CSS_SELECTOR, "div.morebutton a")
                            actions = ActionChains(wd_dp1)
                            actions.move_to_element(more_button).perform()
                            more_button.click()
                            random_sleep(2, 5)
                            result_soup(wd, wd_dp1, start_date, end_date, search, collected_urls)
                        except Exception as e:
                            logging.error(f"더보기 버튼 오류 :: 검색어: {search}, 카테고리: {cate}, 오류: {e}")
                            break

            except Exception as e:
                print(f"오류 발생: {e}")
                logging.error(f"오류 발생: {e}")
                break
    wd.quit()
    wd_dp1.quit()
    if not stop_event.is_set():
        result_dir = '결과/인스티즈'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='인스티즈', subdir='7.인스티즈')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/인스티즈_raw data_{today}.csv', encoding='utf-8', index=False)