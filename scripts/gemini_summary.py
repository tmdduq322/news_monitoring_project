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
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# API 키 확인
if not GEMINI_API_KEY:
    log("❌ GEMINI_API_KEY가 설정되지 않았습니다.")
    sys.exit(1)

# Gemini 설정 (최신 모델)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

def get_yesterday_data(target_date):
    """DB에서 해당 날짜의 주요 기사 제목 추출"""
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME, charset='utf8mb4')
    try:
        with conn.cursor() as cursor:
            # copy_rate가 높은 순으로 데이터 조회
            sql = f"""
                SELECT keyword, title 
                FROM news_posts 
                WHERE DATE(crawled_at) = '{target_date}'
                ORDER BY copy_rate DESC LIMIT 50
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
    너는 뉴스 데이터 분석가야. 아래는 오늘 수집된 뉴스 기사 제목 리스트야.
    이 내용을 바탕으로 다음 형식에 맞춰 한국어로 요약해줘.
    
    [데이터]
    {context}

    [형식]
     💡 오늘의 핵심 이슈 (3가지)
    1. (이슈 1)
    2. (이슈 2)
    3. (이슈 3)

     🔥 트렌드 분석
    (사람들의 관심사가 어디에 쏠려있는지 3문장으로 요약)
    
    [주의사항]
    1. **굵게**, ## 헤더 같은 마크다운(Markdown) 문법을 절대 사용하지 마.
    2. 특수기호(*, #) 없이 깔끔한 줄글(Plain Text)로만 작성해.
    3. 문장은 명확하고 간결하게 끝맺어줘.
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        log(f"❌ Gemini 요약 생성 실패: {e}")
        sys.exit(1)

def create_summary_page_in_notion(parent_page_id, summary_text, target_date):
    """
    노션 페이지 생성 함수 (최종 수정 버전)
    """
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # 본문 길이 제한 방지
    if len(summary_text) > 2000:
        summary_text = summary_text[:2000] + "..."

    # Payload 설정
    payload = {
        "parent": {"database_id": parent_page_id}, 
        "properties": {
            "제목": { 
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
                    "rich_text": [{"type": "text", "text": {"content": "Gemini 1.5 Flash 뉴스 요약"}}],
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

def run_gemini_pipeline(target_date, page_id):
    # 1. 데이터 가져오기
    news_data = get_yesterday_data(target_date)
    log(f"데이터 {len(news_data)}건 조회됨.")
    
    if not news_data:
        log("데이터가 없어 종료합니다.")
        return

    # 2. 제미나이 요약 생성
    summary = generate_summary(news_data)
    log("Gemini 요약 완료.")
    
    # 3. 노션 등록 (여기서 딱 한 번만 호출합니다!)
    create_summary_page_in_notion(page_id, summary, target_date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="데이터 조회 대상 날짜 (YYYY-MM-DD)")
    parser.add_argument("--page_id", help="노션 데이터베이스 ID")
    args = parser.parse_args()

    if args.date and args.page_id:
        run_gemini_pipeline(args.date, args.page_id)
    else:
        log("⚠️ 날짜와 Page ID가 필요합니다.")
        sys.exit(1)