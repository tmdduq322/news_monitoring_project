import os
import sys
import pymysql
import requests
import argparse
import google.generativeai as genai
from datetime import datetime, timedelta
from extraction.core_utils import log

# 1. 환경 변수 로드
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
# [추가] DAG에서 받지 않고 여기서 직접 가져옵니다.
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID") 

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# 필수 환경변수 확인
if not GEMINI_API_KEY:
    log("❌ GEMINI_API_KEY가 설정되지 않았습니다.")
    sys.exit(1)

if not NOTION_PAGE_ID:
    log("❌ NOTION_PAGE_ID가 설정되지 않았습니다. .env 파일을 확인해주세요.")
    sys.exit(1)

# Gemini 설정
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

def get_yesterday_data(target_date):
    """DB에서 해당 날짜의 주요 기사 제목 추출"""
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME, charset='utf8mb4')
    try:
        with conn.cursor() as cursor:
            sql = f"""
                SELECT keyword, title ,original_article_url
                FROM news_posts 
                WHERE DATE(crawled_at) = '{target_date}'
                ORDER BY copy_rate DESC LIMIT 100
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            return [f"- [{row[0]}] {row[1]}" for row in results]
    except Exception as e:
        log(f"❌ DB 조회 실패: {e}")
        sys.exit(1)
    finally:
        conn.close()

def generate_summary(data_list):
    """제미나이를 이용한 트렌드 요약 생성"""
    if not data_list:
        return "데이터가 없어 요약을 생성할 수 없습니다."

    context = "\n".join(data_list)
    
    prompt = f"""
    너는 뉴스 데이터 분석가야. 아래는 오늘 수집된 뉴스 기사 제목과 원문기사url 리스트야.
    이 내용을 바탕으로 다음 형식에 맞춰 한국어로 요약해줘.
    
    [데이터]
    {context}

    [형식]
    💡 오늘의 핵심 이슈 (3가지)
    1. (이슈 1 - 2줄 이내로 간결하게)
    2. (이슈 2)
    3. (이슈 3)

    🔥 트렌드 분석
    (사람들의 관심사가 어디에 쏠려있는지 3문장으로 자연스럽게 요약)
    
    📰 주요뉴스 확인
    (사람들이 관심있는 이슈과 가장 연관있는 기사의 url 3개 추천)

    [주의사항]
    1. **굵게**, ## 헤더 같은 마크다운(Markdown) 문법을 절대 사용하지 마.
    2. 특수기호(*, #) 없이 깔끔한 줄글(Plain Text)로만 작성해.
    3. 문장은 명확하고 간결하게 끝맺어줘.
    """
    
    try:
        response = model.generate_content(prompt)
        # 마크다운 제거
        return response.text.replace("**", "").replace("##", "").replace("###", "")
    except Exception as e:
        log(f"❌ Gemini 요약 생성 실패: {e}")
        sys.exit(1)

def create_summary_page_in_notion(summary_text, target_date):
    """
    [수정] 인자에서 parent_page_id를 제거하고 전역 변수 NOTION_PAGE_ID를 사용합니다.
    """
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    if len(summary_text) > 2000:
        summary_text = summary_text[:2000] + "..."

    payload = {
        # 환경변수에서 가져온 ID 사용
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
    # 1. 데이터 가져오기
    news_data = get_yesterday_data(target_date)
    log(f"데이터 {len(news_data)}건 조회됨.")
    
    if not news_data:
        log("데이터가 없어 종료합니다.")
        return

    # 2. 제미나이 요약 생성
    summary = generate_summary(news_data)
    log("Gemini 요약 완료.")
    
    # 3. 노션 등록
    create_summary_page_in_notion(summary, target_date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="데이터 조회 대상 날짜 (YYYY-MM-DD)")
    # [제거] --page_id 인자는 이제 안 받습니다.
    
    args = parser.parse_args()

    if args.date:
        run_gemini_pipeline(args.date)
    else:
        log("⚠️ 날짜(--date) 파라미터가 필요합니다.")
        sys.exit(1)