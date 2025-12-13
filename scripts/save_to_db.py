# save_to_db.py
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, MetaData
from sqlalchemy.dialects.mysql import LONGTEXT, VARCHAR, FLOAT
import pymysql
import argparse
import os

# [추가] Airflow 기본 경로 설정
AIRFLOW_HOME = "/opt/airflow"

# AWS 연결 정보
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST") 
port = 3306
database = "news-monitoring-db" 

def summarize(text, limit=10000):
    if isinstance(text, str) and len(text) > limit:
        return text[:limit] + "...(이하 생략)"
    return text

def save_excel_to_mysql(filepath, table_name="news_posts"):
    # [수정] 입력된 파일 경로가 상대 경로라면 절대 경로로 변환
    if not os.path.isabs(filepath):
        filepath = os.path.join(AIRFLOW_HOME, filepath)
    
    print(f"📂 [DB 저장] 읽을 파일 경로: {filepath}")

    # 파일 존재 여부 확인 (디버깅용)
    if not os.path.exists(filepath):
        print(f"❌ 파일을 찾을 수 없습니다: {filepath}")
        # 여기서 에러를 내지 않고 리턴하거나, raise FileNotFoundError 할 수 있음
        # 명확한 에러 메시지를 위해 raise 사용 권장
        raise FileNotFoundError(f"파일이 없습니다: {filepath}")

    df = pd.read_excel(filepath) if filepath.endswith(".xlsx") else pd.read_csv(filepath)
    if "게시물 내용" in df.columns:
        df["게시물 내용"] = df["게시물 내용"].apply(summarize)

    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    )
    metadata = MetaData()

    news_posts_table = Table(
        table_name,
        metadata,
        Column("검색어", VARCHAR(100)),
        Column("플랫폼", VARCHAR(100)),
        Column("게시물 URL", VARCHAR(500)),
        Column("게시물 제목", VARCHAR(500)),
        Column("게시물 내용", LONGTEXT),
        Column("게시물 등록일자", VARCHAR(50)),
        Column("계정명", VARCHAR(100)),
        Column("수집시간", VARCHAR(50)),
        Column("원본기사", VARCHAR(500)),
        Column("복사율", FLOAT),
    )

    print(f"🧹 기존 테이블 '{table_name}' 삭제 후 재생성 중...")
    news_posts_table.drop(engine, checkfirst=True)
    news_posts_table.create(engine)

    df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
    print(f"✅ MySQL 테이블 '{table_name}'에 저장 완료!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_excel", required=True)
    parser.add_argument("--table_name", default="news_posts")
    args = parser.parse_args()

    save_excel_to_mysql(args.input_excel, args.table_name)