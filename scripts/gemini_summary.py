import os
import sys
import time
import pymysql
import requests
import argparse
import google.generativeai as genai
from datetime import datetime

# 1. 환경 변수 로드
# 콤마(,)로 구분된 여러 개의 키를 리스트로 만듭니다.
keys_env = os.getenv("GEMINI_API_KEYS") # .env에 GEMINI_API_KEYS=키1,키2 형식으로 저장
if not keys_env:
    # 혹시 기존 변수명(GEMINI_API_KEY)을 쓰고 있을 경우를 대비
    keys_env = os.getenv("GEMINI_API_KEY")

API_KEYS = keys_env.split(',') if keys_env else []
current_key_index = 0

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID") 
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# 로그 출력 함수 (즉시 출력)
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

if not API_KEYS:
    log("❌ GEMINI_API_KEYS가 설정되지 않았습니다.")
    sys.exit(1)

if not NOTION_PAGE_ID:
    log("❌ NOTION_PAGE_ID가 설정되지 않았습니다.")
    sys.exit(1)

# 초기 설정
def configure_genai(key_index):
    """지정된 인덱스의 키로 Gemini를 재설정합니다."""
    global model
    try:
        current_key = API_KEYS[key_index].strip()
        genai.configure(api_key=current_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        log(f"🔑 API Key #{key_index + 1} 적용 완료 (총 {len(API_KEYS)}개)")
    except Exception as e:
        log(f"❌ API Key 설정 중 오류: {e}")
        sys.exit(1)

# 최초 1회 설정
configure_genai(current_key_index)

def get_yesterday_data(target_date):
    """DB에서 해당 날짜의 주요 기사 제목 + URL 추출"""
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME, charset='utf8mb4')
    try:
        with conn.cursor() as cursor:
            sql = f"""
                SELECT keyword, title, original_article_url
                FROM news_posts 
                WHERE DATE(crawled_at) = '{target_date}'
                ORDER BY copy_rate DESC LIMIT 50
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            formatted_data = []
            for row in results:
                keyword = row[0]
                title = row[1]
                url = row[2] if row[2] else "URL 없음"
                formatted_data.append(f"- [{keyword}] {title} (URL: {url})")
            
            return formatted_data
            
    except Exception as e:
        log(f"❌ DB 조회 실패: {e}")
        sys.exit(1)
    finally:
        conn.close()

def generate_summary(data_list):
    """제미나이를 이용한 트렌드 요약 생성 (키 로테이션 + 재시도)"""
    global current_key_index
    
    if not data_list:
        return "데이터가 없어 요약을 생성할 수 없습니다."

    context = "\n".join(data_list)
    
    prompt = f"""
    너는 뉴스 데이터 분석가야. 아래는 오늘 수집된 뉴스 기사 데이터야.
    각 항목은 '[키워드] 제목 (URL: 주소)' 형식으로 되어 있어.
    이 내용을 바탕으로 다음 형식에 맞춰 한국어로 요약해줘.
    
    [데이터]
    {context}

    [형식]
    💡 오늘의 핵심 이슈 (3가지)
    1. (이슈 1 - 2줄 이내로 간결하게)
    2. (이슈 2)
    3. (이슈 3)

    🔥 트렌드 분석
    (사람들의 관심사가 어디에 쏠려있는지 2문장으로 자연스럽게 요약)

    📰 주요 뉴스 바로가기 (3개 추천)
    (위 이슈와 가장 관련성 높은 실제 기사 3개를 골라서 아래 형식으로 작성해)
    - [기사 제목](기사 URL)
    - [기사 제목](기사 URL)
    - [기사 제목](기사 URL)

    [주의사항]
    1. **굵게**, ## 헤더 같은 마크다운 문법 사용 금지. (단, 링크 [제목](주소) 형식은 허용)
    2. 특수기호(*, #) 없이 깔끔한 줄글로 작성해.
    3. URL은 내가 제공한 [데이터]에 있는 것만 그대로 사용해야 해. 절대 지어내지 마.
    """
    
    max_retries = 3 # 키가 많으면 시도 횟수도 넉넉하게
    attempt = 0
    
    while attempt < max_retries:
        try:
            log(f"🤖 Gemini 요청 시작 (Key #{current_key_index + 1}, 시도 {attempt + 1})...")
            response = model.generate_content(prompt)
            text = response.text.replace("**", "").replace("##", "").replace("###", "")
            return text
            
        except Exception as e:
            error_msg = str(e)
            
            # 429(Too Many Requests) 또는 Quota 에러 발생 시 키 교체
            if "429" in error_msg or "Quota" in error_msg or "ResourceExhausted" in error_msg:
                log(f"⚠️ 현재 키(#{current_key_index + 1}) 한도 초과!")
                
                # 다음 키가 있는지 확인
                if len(API_KEYS) > 1:
                    # 다음 키로 인덱스 변경 (순환)
                    current_key_index = (current_key_index + 1) % len(API_KEYS)
                    log(f"♻️ 다음 키(#{current_key_index + 1})로 교체합니다...")
                    configure_genai(current_key_index) # 모델 재설정
                    time.sleep(2) # 교체 후 아주 잠깐 대기
                    # retry 카운트는 늘리지 않고 바로 다시 시도 (키 바꿨으니까)
                    continue 
                else:
                    # 키가 하나뿐이면 어쩔 수 없이 대기
                    wait_time = 60
                    log(f"⏳ 예비 키가 없습니다. {wait_time}초 대기합니다...")
                    time.sleep(wait_time)
                    attempt += 1
            else:
                log(f"⚠️ 알 수 없는 오류: {error_msg}")
                time.sleep(10)
                attempt += 1
            
    log("❌ 모든 키와 재시도 횟수를 소진했습니다. 실패.")
    sys.exit(1)

def create_summary_page_in_notion(summary_text, target_date):
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    if len(summary_text) > 2000:
        summary_text = summary_text[:2000] + "..."

    payload = {
        "parent": {"page_id": NOTION_PAGE_ID}, 
        "properties": {
            "title": { 
                "title": [
                    {"text": {"content": f"🤖 {target_date} AI 요약 리포트"}}
                ]
            }
        },
        "children": [
            {
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [{"type": "text", "text": {"content": "Gemini 2.5 Flash 뉴스 요약"}}],
                    "icon": {"emoji": "📰"},
                    "color": "gray_background"
                }
            },
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "오늘의 트렌드 분석"}}]
                }
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": summary_text}}]
                }
            }
        ]
    }
    
    url = "https://api.notion.com/v1/pages"
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        log(f"✅ 노션 리포트 생성 완료: {target_date}")
        
    except requests.exceptions.HTTPError as err:
        log(f"❌ 노션 요청 실패: {err}")
        log(f"응답 내용: {response.text}")
        sys.exit(1)

def run_gemini_pipeline(target_date):
    news_data = get_yesterday_data(target_date)
    log(f"데이터 {len(news_data)}건 조회됨.")
    
    if not news_data:
        log("데이터가 없어 종료합니다.")
        return

    summary = generate_summary(news_data)
    log("Gemini 요약 완료.")
    
    create_summary_page_in_notion(summary, target_date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="데이터 조회 대상 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.date:
        run_gemini_pipeline(args.date)
    else:
        log("⚠️ 날짜(--date) 파라미터가 필요합니다.")
        sys.exit(1)