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
    filename=f'개드립_log_{today}.txt',
    level=logging.INFO,  # 로그 레벨
    format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식
    encoding='utf-8'  # 인코딩 설정
)


# 한페이지 크롤링
def dogdrip_crw(wd, url, search):
    try:
        logging.info(f"크롤링 시작: {url}")
        wd.set_page_load_timeout(10)
        wd.get(f'{url}')
        logging.info(f"접속: {url}")
        time.sleep(1)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'ed')))
        soup = BeautifulSoup(wd.page_source, 'html.parser')

        # 추후 수정하기
        search_word_list = []
        search_plt_list = []
        writer_list = []
        url_list = []
        title_list = []
        content_list = []
        date_list = []
        image_check_list = []

        content_div = soup.find('div', class_='ed clearfix margin-vertical-large')
        raw_title = soup.find('h4', class_='ed margin-bottom-xsmall').find('a', class_='ed link text-bold').text
        cleaned_title = clean_title(raw_title)  # 제목 정리 함수 사용
        title_list.append(cleaned_title)
        logging.info(f"제목 추출 성공: {cleaned_title}")

        content = soup.find('div', class_='ed clearfix margin-vertical-large')
        # 추천 비추천 버튼 제외
        recom_btn = content.find('div', class_='wgtRv addon_addvote')
        if recom_btn:
            recom_btn.extract()

        # content = content.text.strip()
        # content_strip = ' '.join(content.split())

        # content_list.append(content_strip)

        # logging.info("내용 추출 성공")

        # 2. 기사제거
        # 모든 <a> 태그 제거 (기사, 유튜브 등 링크 제거)
        for a_tag in content_div.find_all('a'):
            a_tag.decompose()

        # URL 형태의 텍스트 제거 (http:// 또는 https://로 시작하는 모든 링크)
        post_content = content_div.get_text(separator='\n', strip=True)
        post_content = re.sub(r'http[s]?://\S+', '', post_content)

        # 본문 텍스트 추출 (띄어쓰기 유지, 불필요한 개행 제거)
        post_content = content_div.get_text(separator=' ', strip=True)
        post_content = re.sub(r'https?://[^\s]+', '', post_content)  # URL 제거
        content_list.append(post_content)

        logging.info(f"내용 추출 성공: {post_content}")

        search_plt_list.append('웹페이지(개드립)')
        url_list.append(url)

        search_word_list.append(search)

        div_tag = soup.find('div', class_='ed flex flex-wrap flex-left flex-middle title-toolbar')

        date_str = div_tag.find_all('span', class_='ed text-xsmall text-muted')[1].text
        date = datetime.strptime(date_str, '%Y.%m.%d')
        date_list.append(date)
        logging.info(f"날짜 추출 성공: {date_str}")

        # 채널명
        writer_list.append(div_tag.find_all('span', class_='ed margin-right-small')[0].text.strip())

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
            # "이미지 유무": image_check_list
        })

        # 데이터 저장
        save_to_csv(main_temp, f'data/raw/14.개드립/{today}/개드립_{search}.csv')
        logging.info(f"data/raw/14.개드립/{today}/개드립_{search}.csv")

    except TimeoutException as e:
        logging.error(f"페이지 로딩 시간 초과: {e}")
        return None

    except WebDriverException as e:
        logging.error(f"웹드라이버 에러: {e}")
        return None

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        return None


def dogdrip_main_crw(searchs, start_date, end_date,stop_event):
    if not os.path.exists(f'data/raw/14.개드립/{today}'):
        os.makedirs(f'data/raw/14.개드립/{today}')
        print(f"폴더 생성 완료: {today}")
    else:
        print(f"해당 폴더 존재")
    logging.info(f"========================================================")
    logging.info(f"                    개드립 크롤링 시작")
    logging.info(f"========================================================")
    wd = setup_driver()
    wd_dp1 = setup_driver()

    # 수집할 게시판
    category = ['dogdrip', 'userdog', 'stock', 'coin', 'free', 'sports', 'politics', 'genderissue']
    for cate in category:
        page_num = 1
        if stop_event.is_set():
            print("🛑 크롤링 중단됨")
            break
        for search in searchs:
            if stop_event.is_set():
                break
            page_num = 1
            while True:
                if stop_event.is_set():
                    break
                try:
                    logging.info(f"크롤링 시작-검색어: {search}")
                    logging.info(f"크롤링 카테고리: {cate}")
                    url = f'https://www.dogdrip.net/?_filter=search&act=&vid=&mid={cate}&category=&search_target=title_content&search_keyword={search}&page={page_num}'
                    wd_dp1.get(url)
                    WebDriverWait(wd_dp1, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'ed.board-list')))

                    time.sleep(1)

                    soup_dp1 = BeautifulSoup(wd_dp1.page_source, 'html.parser')

                    # 검색결과 리스트
                    ul_tag = soup_dp1.find('ul', class_='ed list')
                    if not ul_tag:
                        logging.warning(f"'ul.ed.list' 태그를 찾지 못함. URL: {url}")
                        break  # 혹은 continue
                    li_tags = ul_tag.find_all('li')

                    logging.info(f"검색목록 찾음.")

                    if not li_tags:
                        break

                    for li in li_tags:
                        if stop_event.is_set():
                            break
                        # 공지사항 글 발견시 continue
                        # if 'notice' in tr.get('class', []):
                        #     logging.info("공지사항 pass")
                        #     continue

                        after_start_date = False  # 날짜가 시작 날짜 이후인 경우

                        try:
                            date_str = li.find('span',
                                               class_='ed text-muted text-xxsmall margin-right-xsmall').text.strip().replace(
                                '  ', '')
                            date = datetime.strptime(date_str, '%Y.%m.%d').date()
                            logging.info(f"날짜 찾음 : {date_str}")
                        except Exception as e:
                            logging.error(f"날짜 오류 발생: {e}")
                            continue

                        if date > end_date:
                            continue

                        if date < start_date:
                            after_start_date = True
                            break

                        # if date.day <= 7:
                        url = 'https://www.dogdrip.net' + li.find('a',
                                                                  class_='ed overlay overlay-fill overlay-top').get(
                            'href')
                        logging.info(f"url 찾음.")
                        dogdrip_crw(wd, url, search)

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
        result_dir = '결과/개드립'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        all_data = pd.concat([
            result_csv_data(search, platform='개드립', subdir='14.개드립')
            for search in searchs
        ])

        all_data.to_csv(f'{result_dir}/개드립_raw data_{today}.csv', encoding='utf-8', index=False)
