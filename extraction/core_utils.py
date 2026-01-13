# extraction/core_utils.py

import re
import os
import time
import psutil
import urllib.parse
import logging
import requests
import pandas as pd

from bs4 import BeautifulSoup
from konlpy.tag import Okt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager

# 초기화
today = datetime.now().strftime("%y%m%d")

# .env 로드 (Airflow 경로)
load_dotenv(dotenv_path="/opt/airflow/.env")

# 제외 도메인 로드
excluded_domains_env = os.getenv("EXCLUDED_DOMAINS_PATH")
if excluded_domains_env:
    excluded_domains = excluded_domains_env.split(',')
else:
    excluded_domains = [] 

# 로그 저장 경로 설정 (권한 문제 방지를 위해 data 폴더 사용 권장)
log_dir = "/opt/airflow/logs/extraction" 
os.makedirs(log_dir, exist_ok=True)

# 로깅 설정
logging.basicConfig(
    filename=os.path.join(log_dir, "log.txt"),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def log(msg, index=None):
    prefix = f"[{index+1:03d}] " if index is not None else ""
    full_msg = f"{prefix}{msg}"
    print(full_msg)
    logging.info(full_msg)

def create_driver(index=None):
    try:
        options = Options()
        # [필수] Docker 환경 Headless 설정
        options.add_argument("--headless=new")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-default-apps")
        options.add_argument("--no-first-run")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--blink-settings=imagesEnabled=false")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.media_stream": 2,
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_setting_values.geolocation": 2,
            "profile.default_content_setting_values.media_stream_mic": 2,
            "profile.default_content_setting_values.media_stream_camera": 2,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.page_load_strategy = 'eager'

        # [최적화] extract_original.py에서 설정한 환경변수 경로 우선 사용
        driver_path = os.getenv("CHROMEDRIVER_PATH")
        if driver_path and os.path.exists(driver_path):
            service = Service(executable_path=driver_path)
        else:
            # 환경변수가 없으면 직접 설치 (단일 실행 시 fallback)
            service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(service=service, options=options)

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        })

        return driver
    except Exception as e:
        log(f"❌ 드라이버 생성 실패: {e}", index)
        return None

def kill_driver(driver, index=None):
    if driver:
        try:
            driver.quit()
        except Exception as e:
            pass # 조용히 넘어감

        try:
            pid = getattr(driver.service.process, 'pid', None)
            if pid and psutil.pid_exists(pid):
                parent = psutil.Process(pid)
                children = parent.children(recursive=True)
                for child in children:
                    child.kill()
                parent.kill()
        except Exception as e:
            pass

def clean_text(text):
    if not isinstance(text, str):
        text = str(text)
    if text.strip().lower() == 'nan':
        return ""

    patterns_to_remove = [
        r"Video Player", r"Video 태그를 지원하지 않는 브라우저입니다\.", 
        r"\d{2}:\d{2}", r"[01]\.\d{2}x", r"출처:\s?[^\n]+", 
        r"/\s?\d+\.?\d*", r"Your browser does not support the video tag."
    ]
    for pattern in patterns_to_remove:
        text = re.sub(pattern, "", text)

    text = text.replace("\\\"", "\"").replace("\\'", "'").replace("\\\\", "\\")
    text = re.sub(r"[ㅋㅎㅠㅜ]+", "", text)
    text = re.sub(r"[!?~\.\,\-#]{2,}", "", text)
    text = re.sub(r"&[a-z]+;|&#\d+;", "", text)
    text = re.sub(r"[\\\xa0\u200b\u3000\u200c_x000D_]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def extract_keywords(text, num_keywords=5):
    try:
        from konlpy.tag import Okt
        local_okt = Okt() 
        nouns = local_okt.nouns(text)
        return " ".join(nouns[:num_keywords])
    except Exception as e:
        # log 함수를 여기서 부르기 애매하다면 print로 대체하거나 pass
        print(f"⚠️ 키워드 추출 실패: {e}")
        return ""

def extract_first_sentences(text):
    paras = re.split(r'\n{2,}', text.strip())
    get_first = lambda p: re.split(r'(?<=[.!?])(?=\s|[가-힣])', p.strip())[0] if p else ""
    get_last = lambda p: re.split(r'(?<=[.!?])(?=\s|[가-힣])', p.strip())[-1].strip() if p else ""
    
    first = get_first(paras[0]) if len(paras) > 0 else ""
    second = get_first(paras[1]) if len(paras) > 1 else ""
    last = get_last(paras[-1]) if len(paras) > 0 else ""
    return first, second, last

def calculate_copy_ratio(article, post):
    def clean(t): return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', t)).strip()
    article, post = clean(article), clean(post)
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', article) if s.strip()]
    if not sentences: return 0.0
    scores = []
    for s in sentences:
        try:
            v = TfidfVectorizer().fit([s, post])
            tfidf = v.transform([s, post])
            scores.append(cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0])
        except:
            continue
    return round(sum(scores)/len(scores), 3) if scores else 0.0

def safe_get(driver, url, timeout=30, index=None):
    # [최적화] 타임아웃 10초로 단축
    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        return True
    except Exception as e:
        # log(f"⏰ 페이지 로딩 실패: {url}", index) 
        return False

def fallback_with_requests(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        res = requests.get(url, headers=headers, timeout=30)
        if res.status_code != 200:
            return ""
        soup = BeautifulSoup(res.text, "html.parser")
        content_div = soup.select_one("#dic_area, article")
        if content_div:
            return content_div.get_text(strip=True)
        paragraphs = soup.find_all("p")
        return "\n".join(p.get_text(strip=True) for p in paragraphs)
    except:
        return ""

def get_news_article_body(url, driver, max_retries=1, index=None):
    # [최적화] 재시도 로직 제거: 실패하면 바로 requests Fallback으로 전환 (속도 향상)
    try:
        if safe_get(driver, url, timeout=15, index=index):
            time.sleep(0.5)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            domain = urllib.parse.urlparse(url).netloc

            selector_map = {
                "n.news.naver.com": "article#dic_area",
                "m.sports.naver.com": "div._article_content",
                "m.entertain.naver.com": "article#comp_news_article div._article_content",
                "imbc.com": "div.news_txt[itemprop='articleBody']",
                "ytn.co.kr": "div#CmAdContent",
                "mt.co.kr": "div#textBody[itemprop='articleBody']",
                "heraldcorp.com": "article.article-body#articleText",
                "hankookilbo.com": "div.col-main[itemprop='articleBody']",
                "edaily.co.kr": "div.news_body[itemprop='articleBody']",
                "fnnews.com": "div#article_content",
                "seoul.co.kr": "div#articleContent .viewContent",
                "pressian.com": "div.article_body",
                "kbs.co.kr": "div#cont_newstext",
                "hani.co.kr": "div.article-text",
                "nocutnews.co.kr": "div#pnlContent",
                "asiae.co.kr": "div.article.fb-quotable#txt_area",
                "mediatoday.co.kr": "article#article-view-content-div",
                "khan.co.kr": "div#articleBody",
                "sedaily.com": "div.article_view[itemprop='articleBody']",
                "imaeil.com": "div#articlebody[itemprop='articleBody']",
                "ebn.co.kr": "article#article-view-content-div",
                "kyeongin.com": "div#article-body",
                "obsnews.co.kr": "article#article-view-content-div",
                "incheonilbo.com": "article#article-view-content-div",
                "ggilbo.com": "article#article-view-content-div",
                "ekn.kr": "div#news_body_area_contents",
            }
            selector = next((v for k, v in selector_map.items() if k in domain), None)
            
            if selector:
                div = soup.select_one(selector)
                if div:
                    body = div.get_text(separator="\n", strip=True)
                    if len(body) > 200: # 200자 이상이면 성공
                        return body, driver
            
        # Selenium 실패/Selector 불일치 시 Requests 시도
        return fallback_with_requests(url), driver

    except Exception as e:
        log(f"⚠️ 크롤링 에러 → Fallback 시도: {e}", index)
        return fallback_with_requests(url), driver

def is_excluded(url):
    return any(domain in url for domain in excluded_domains)

MAX_QUERY_LENGTH = 100

def generate_search_queries(title, first, second, last, press, index=None):
    def truncate(text):
        return text[:MAX_QUERY_LENGTH] if text else ""
    
    title_clean = truncate(clean_text(title))
    first_clean = truncate(clean_text(first))
    
    # [로그] 키워드 추출 과정 확인
    keywords = truncate(extract_keywords(title_clean))
    if index is not None:
        log(f"🔑 [키워드] {title_clean[:15]}... -> {keywords}", index)

    queries = list(set(filter(None, [
        title_clean,
        f"{keywords} {press}" if press else keywords,
        first_clean,
        # second_clean, # 쿼리가 너무 많으면 제외 가능
    ])))
    
    if index is not None:
        log(f"📜 [검색어 목록] {queries}", index)

    return queries

def extract_oid_from_naver_url(link):
    parsed = urlparse(link)
    path = parsed.path
    match = re.search(r"/article/(\d{3})/\d+", path)
    if match: return match.group(1)
    match = re.search(r"/mnews/article/(\d{3})/\d+", path)
    if match: return match.group(1)
    return None

def search_news_with_api(queries, driver, client_id, client_secret, max_results=3, index=None):
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret
    }
    results = []
    seen_links = set()

    # [로그] API 검색 시작
    log(f"🚀 API 검색 진입 (쿼리 {len(queries)}개)", index)

    for i, q in enumerate(queries):
        log(f"   🔍 [{i+1}/{len(queries)}] 검색: '{q}'", index)
        
        url = f"https://openapi.naver.com/v1/search/news.json?query={urllib.parse.quote(q)}&display={max_results}&sort=sim"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                log(f"   ⚠️ API 응답 실패: {res.status_code}", index)
                continue

            items = res.json().get("items", [])
            log(f"   ✅ 결과: {len(items)}건 발견", index)

            for item in items:
                link = item.get("link")
                title = BeautifulSoup(item.get("title", ""), "html.parser").get_text()

                if not link or link in seen_links or is_excluded(link):
                    continue
                
                if "naver.com" not in link:
                    log(f"   ⏩ 네이버 기사가 아니라 패스: {link}", index) # 필요시 주석 해제
                    continue

                if "naver.com" in link:
                    oid = extract_oid_from_naver_url(link)
                    if not oid: 
                        continue
                    # [유지] 모든 언론사 수집 (필터링 제거됨)
                
                # 본문 수집 (10초 타임아웃, 재시도 없는 고속 함수 사용)
                body, new_driver = get_news_article_body(link, driver, index=index)
                if new_driver != driver:
                    driver = new_driver

                seen_links.add(link)
                if body and len(body) > 200:
                    cleaned_body = clean_text(body)
                    results.append({"title": title, "link": link, "body": cleaned_body})

        except Exception as e:
            log(f"   ❌ API 루프 중 에러: {e}", index)
            continue

    log(f"🏁 유효 기사 확보: {len(results)}개", index)
    return results