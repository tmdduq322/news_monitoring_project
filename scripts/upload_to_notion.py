import pandas as pd
import requests
import os
import sys
from sqlalchemy import create_engine

# 환경 변수 로드
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

def upload_from_db_to_notion(target_date):
    # 1. DB 연결 설정 (SQLAlchemy)
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}?charset=utf8mb4")
    
    # 2. 유사도 0.3 이상 & 특정 날짜 데이터 쿼리
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

    # 3. 노션 API 헤더
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    # 4. 루프를 돌며 노션 업로드
    for _, row in df.iterrows():
        payload = {
            "parent": { "database_id": DATABASE_ID },
            "properties": {
                "제목": { "title": [{ "text": { "content": row['title'] } }] },
                "유사도": { "number": round(float(row['copy_rate']), 4) },
                "URL": { "url": row['url'] },
                "플랫폼": { "select": { "name": row['platform'] } }
            }
        }
        requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload)

if __name__ == "__main__":
    # Airflow로부터 실행 날짜(YYYY-MM-DD)를 인자로 받음
    upload_from_db_to_notion(sys.argv[1])