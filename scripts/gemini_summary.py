import os
import sys
import pymysql
import requests
import argparse
import google.generativeai as genai
from datetime import datetime, timedelta
from extraction.core_utils import log  # clean_text는 안 쓰면 제거

# 1. 환경 변수 로드
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# [수정 1] 모델명 교체 (2.5 -> 1.5-flash) 및 API 키 설정 확인
if not GEMINI_API_KEY:
    log("❌ GEMINI_API_KEY가 설정되지 않았습니다.")
    sys.exit(1)

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
                ORDER BY copy_rate DESC LIMIT 30
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
    ### 💡 오늘의 핵심 이슈 (3가지)
    1. (이슈 1)
    2. (이슈 2)
    3. (이슈 3)

    ### 🔥 트렌드 분석
    (사람들의 관심사가 어디에 쏠려있는지 2문장으로 요약)
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        log(f"❌ Gemini 요약 생성 실패: {e}")
        sys.exit(1)

# scripts/gemini_summary.py

def create_summary_page_in_notion(parent_page_id, summary_text, target_date):
    """
    [수정] 데이터베이스 행이 아니라, 하위 '페이지'로 생성합니다.
    이렇게 하면 컬럼(속성) 에러에서 완전히 해방됩니다.
    """
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    if len(summary_text) > 2000:
        summary_text = summary_text[:2000] + "..."

    # 👇 [핵심] 페이지 생성 Payload (Database가 아닌 Page가 부모일 때)
    # 만약 parent_page_id가 '데이터베이스 ID'라면 자동으로 표 안에 들어갑니다.
    # 표가 싫다면 노션에서 '빈 페이지'를 하나 만들고 그 ID를 Airflow에 넣어야 합니다.
    
    payload = {
        # 부모가 데이터베이스면 "database_id", 일반 페이지면 "page_id"
        # 범용성을 위해 page_id로 시도합니다. (데이터베이스도 page_id로 취급 가능)
        "parent": {"page_id": parent_page_id}, 
        "properties": {
            "title": { # 일반 페이지는 속성 이름이 무조건 'title'입니다. (수정 불필요)
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
    
    # 만약 부모가 '데이터베이스'라면 위 payload 구조로는 에러가 날 수 있습니다.
    # 사용자가 준 ID가 '데이터베이스'인지 '페이지'인지 모르므로
    # 안전하게 "제목" 속성만 쓰는 데이터베이스 행 추가 방식을 유지하되,
    # 'Date' 같은 잡다한 속성은 절대 넣지 않겠습니다.
    
    # ---------------------------------------------------------
    # [최종 안전 버전]
    # 사용자가 준 ID가 데이터베이스 ID일 확률이 높으므로 (이미지상 표니까)
    # 아까 성공했던 방식에서 'Date'만 뺀 깔끔한 버전을 다시 드립니다.
    # ---------------------------------------------------------
    
    payload_safe = {
        "parent": {"database_id": parent_page_id}, # Airflow 변수명이 page_id라도 실제론 DB ID일 것임
        "properties": {
            "제목": { # 아까 성공한 그 이름!
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
                    "color": "blue_background"
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
        # 안전한 payload_safe로 전송
        response = requests.post(url, headers=headers, json=payload_safe)
        response.raise_for_status()
        log(f"✅ 노션 리포트 생성 완료: {target_date}")
        
    except requests.exceptions.HTTPError as err:
        log(f"❌ 노션 요청 실패: {err}")
        log(f"응답 내용: {response.text}")
        sys.exit(1)
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # 2000자 제한
    if len(summary_text) > 2000:
        summary_text = summary_text[:2000] + "..."

    # 👇 [수정] properties에서 'Date'를 완전히 제거했습니다.
    # 오직 '제목'만 보냅니다.
    payload = {
        "parent": {"database_id": database_id},
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
                    "rich_text": [{"type": "text", "text": {"content": "Gemini 1.5 Flash가 분석한 오늘의 뉴스 요약"}}],
                    "icon": {"emoji": "📰"},
                    "color": "gray_background"
                }
            },
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "요약 내용"}}]
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
        log(f"✅ 노션 페이지 생성 완료: {target_date}")
        
    except requests.exceptions.HTTPError as err:
        log(f"❌ 노션 요청 실패: {err}")
        log(f"응답 내용: {response.text}")
        sys.exit(1)
    """
    [수정 2] '블록 추가(Append)' 대신 '페이지 생성(Create Page)' 방식 사용
    데이터베이스 ID가 넘어오면 그 안에 새로운 페이지를 만듭니다.
    """
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # 3000자 제한 방지 (간단히 자르기)
    if len(summary_text) > 2000:
        summary_text = summary_text[:2000] + "..."

    # 페이지 생성 페이로드 (Parent를 Database로 설정)
    payload = {
        "parent": {"database_id": database_id},
        "properties": {
            "제목": { # 데이터베이스의 제목 컬럼명이 'Name' 또는 '제목'인지 확인 필요 (보통 기본값은 Name/title)
                "title": [
                    {"text": {"content": f"🤖 {target_date} AI 요약 리포트"}}
                ]
            },
            "Date": { # 날짜 컬럼이 있다면 추가 (없으면 에러날 수 있으니 주의. 필요시 주석 처리)
                 "date": {"start": target_date}
            }
        },
        "children": [
            {
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [{"type": "text", "text": {"content": "Gemini 1.5 Flash가 분석한 오늘의 뉴스 요약입니다."}}],
                    "icon": {"emoji": "📰"},
                    "color": "gray_background"
                }
            },
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": "요약 내용"}}]
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
    
    # [중요] Endpoint 변경: v1/pages (페이지 생성)
    url = "https://api.notion.com/v1/pages"
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status() # 400/500 에러 시 즉시 예외 발생
        log(f"✅ 노션 페이지 생성 완료: {target_date}")
        
    except requests.exceptions.HTTPError as err:
        log(f"❌ 노션 요청 실패: {err}")
        log(f"응답 내용: {response.text}")
        # [수정 3] 에러 발생 시 시스템 종료 코드 1 반환 -> Airflow Task Failed 처리
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
    
    # 3. 노션 등록
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