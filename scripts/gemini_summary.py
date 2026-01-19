import os
import pymysql
import requests
import argparse
import google.generativeai as genai
from datetime import datetime, timedelta
from dotenv import load_dotenv
from extraction.core_utils import log, clean_text  # 기존 유틸리티 활용

# 1. 환경 변수 로드
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# 제미나이 설정
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')
print("============== AVAILABLE MODELS ==============")
try:
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(f"Model Name: {m.name}")
except Exception as e:
    print(f"모델 목록 조회 실패: {e}")
print("============================================")
def get_yesterday_data(target_date):
    """DB에서 전날 수집된 주요 기사 제목과 검색어(언론사) 추출"""
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME, charset='utf8mb4')
    try:
        with conn.cursor() as cursor:
            # 유사도가 높거나 많이 수집된 상위 20개 기사 추출
            sql = f"""
                SELECT keyword, title 
                FROM news_posts 
                WHERE DATE(crawled_at) = '{target_date}'
                ORDER BY copy_rate DESC LIMIT 50
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            return [f"[{row[0]}] {row[1]}" for row in results]
    finally:
        conn.close()

def generate_summary(data_list):
    """제미나이를 이용한 트렌드 요약 생성"""
    if not data_list:
        return "조회된 데이터가 없어 요약을 생성할 수 없습니다."

    context = "\n".join(data_list)
    prompt = f"""
    너는 뉴스 데이터 분석가야. 아래 리스트는 오늘 커뮤니티에서 가장 많이 공유된 뉴스 기사 제목들이야.
    이 데이터들을 분석해서 다음 양식으로 요약해줘:

    1. 💡 오늘의 핵심 이슈 (3줄 이내)
    2. 🔥 사람들의 관심사가 집중된 이유

    [데이터 리스트]
    {context}
    """
    
    response = model.generate_content(prompt)
    return response.text

def add_summary_to_notion(page_id, summary_text):
    """노션 페이지 최상단에 요약 블록 추가"""
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # 요약 내용을 노션 '인용(Quote)' 및 '콜아웃(Callout)' 블록으로 변환
    payload = {
        "children": [
            {
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [{"type": "text", "text": {"content": "🤖 Gemini AI 트렌드 요약"}}],
                    "icon": {"emoji": "💡"},
                    "color": "blue_background"
                }
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": summary_text}}]
                }
            },
            {
                "object": "block",
                "type": "divider",
                "divider": {}
            }
        ]
    }
    
    # 페이지의 콘텐츠(blocks) 최상단에 추가하기 위해 PATCH 요청 사용
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    response = requests.patch(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        log("✅ 노션 페이지 요약 추가 완료")
    else:
        log(f"❌ 노션 업데이트 실패: {response.text}")

def run_gemini_pipeline(target_date, notion_page_id):
    """전체 요약 파이프라인 실행"""
    log(f"🚀 {target_date} 데이터 기반 AI 요약 시작")
    
    # 1. 데이터 가져오기
    news_data = get_yesterday_data(target_date)
    
    # 2. 제미나이 요약 생성
    summary = generate_summary(news_data)
    
    # 3. 노션 업데이트
    add_summary_to_notion(notion_page_id, summary)

if __name__ == "__main__":
    # [핵심 수정] 명령줄 인자 파싱 로직 추가
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="데이터 조회 대상 날짜 (YYYY-MM-DD)")
    parser.add_argument("--page_id", help="요약을 추가할 노션 페이지/데이터베이스 ID")
    args = parser.parse_args()

    if args.date and args.page_id:
        run_gemini_pipeline(args.date, args.page_id)
    else:
        # 인자가 없을 경우 기본값(어제 날짜)으로 동작 (테스트용)
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        log("⚠️ 인자가 부족하여 기본 설정을 시도합니다.")