import pandas as pd
import requests
import os
import sys
from sqlalchemy import create_engine

# 환경 변수 로드
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
PARENT_PAGE_ID = os.getenv("NOTION_DATABASE_ID") # 부모 페이지 ID로 변경
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

def create_daily_database(target_date):
    """실행 날짜를 이름으로 하는 새 데이터베이스 생성"""
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    payload = {
        "parent": { "type": "page_id", "page_id": PARENT_PAGE_ID },
        "title": [ { "type": "text", "text": { "content": f"📅 뉴스 모니터링 결과 ({target_date})" } } ],
        "properties": {
            "제목": { "title": {} },
            "유사도": { "number": {} },
            "게시글 URL": { "url": {} },   # 기존 URL
            "원문 URL": { "url": {} },     # [추가] 뉴스 원문 URL
            "플랫폼": { "select": {} }
        }
    }
    
    response = requests.post("https://api.notion.com/v1/databases", headers=headers, json=payload)
    if response.status_code == 200:
        new_db_id = response.json().get("id")
        print(f"✅ 새 데이터베이스 생성 완료: {new_db_id}")
        return new_db_id
    else:
        print(f"❌ 데이터베이스 생성 실패: {response.text}")
        return None

def upload_from_db_to_notion(target_date):
    # 1. 일별 테이블 자동 생성
    database_id = create_daily_database(target_date)
    if not database_id: return

    # 2. DB 연결 및 조회 (실제 컬럼명 반영)
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}")
    query = f"""
        SELECT * FROM news_posts 
        WHERE copy_rate >= 0.3 
        AND DATE(crawled_at) = '{target_date}'
    """
    
    try:
        df = pd.read_sql(query, engine)
        print(f"📊 {target_date} 데이터 {len(df)}건 조회 완료")
    except Exception as e:
        print(f"❌ DB 조회 실패: {e}")
        return

    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    # 3. 데이터 업로드
    for _, row in df.iterrows():
        payload = {
            "parent": { "database_id": database_id },
            "properties": {
                "제목": { "title": [{ "text": { "content": str(row['title']) } }] },
                "유사도": { "number": round(float(row['copy_rate']), 4) },
                "게시글 URL": { "url": row['url'] }, # 커뮤니티 URL
                "원문 URL": { "url": row['original_article_url'] if row['original_article_url'] else "" }, # 뉴스 원문 URL
                "플랫폼": { "select": { "name": row['platform'] if row['platform'] else "기타" } }
            }
        }
        requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload)
    print(f"🏁 {target_date} 노션 업로드 완료!")
    print(database_id)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        upload_from_db_to_notion(sys.argv[1])