import pandas as pd
from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy.dialects.mysql import LONGTEXT, VARCHAR, FLOAT
import pymysql
import argparse
import os
import sys

# Airflow 경로
AIRFLOW_HOME = "/opt/airflow"

# AWS 및 DB 환경 변수 로드
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", 3306))

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def summarize(text, limit=10000):
    if isinstance(text, str) and len(text) > limit:
        return text[:limit] + "...(이하 생략)"
    return text

def save_to_rds(filepath, table_name="news_posts"):
    print(f"📂 [DB 저장 시작] 파일 경로: {filepath}")

    # 1. 파일 읽기 (S3 지원)
    storage_options = None
    
    # S3 경로인 경우
    if filepath.startswith("s3://"):
        if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
            print("❌ AWS 자격 증명(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)이 필요합니다.")
            sys.exit(1)
        storage_options = {"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    
    # 로컬 경로인 경우 절대 경로 변환 및 존재 확인
    else:
        if not os.path.isabs(filepath):
            filepath = os.path.join(AIRFLOW_HOME, filepath)
        if not os.path.exists(filepath):
            print(f"❌ 파일을 찾을 수 없습니다: {filepath}")
            sys.exit(1)

    try:
        # 파일 확장자에 따라 읽기 함수 분기
        if filepath.endswith(".xlsx"):
            df = pd.read_excel(filepath, storage_options=storage_options)
        else:
            df = pd.read_csv(filepath, storage_options=storage_options) #
        
        print(f"✅ 데이터 로드 성공: {len(df)}행")
    except Exception as e:
        print(f"❌ 파일 읽기 실패: {e}")
        sys.exit(1)

    # 2. 데이터 전처리
    if "게시물 내용" in df.columns:
        df["게시물 내용"] = df["게시물 내용"].apply(summarize)

    # 3. RDS 연결
    if not DB_HOST:
        print("❌ DB 연결 정보(DB_HOST)가 없습니다.")
        sys.exit(1)

    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    engine = create_engine(db_url)
    metadata = MetaData()

    # 4. 테이블 정의
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

    # 5. 저장 (기존 테이블 삭제 후 재생성)
    try:
        print(f"🔄 테이블 '{table_name}' 초기화 및 저장 중...")
        news_posts_table.drop(engine, checkfirst=True)
        news_posts_table.create(engine)
        
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        print("✅ DB 저장 완료.")
    except Exception as e:
        print(f"❌ DB 저장 중 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # 인자 이름을 통일했습니다 (--input_file)
    parser.add_argument("--input_file", required=True, help="입력 파일 경로 (로컬 또는 S3)")
    parser.add_argument("--table_name", default="news_posts", help="테이블 이름")

    args = parser.parse_args()
    save_to_rds(args.input_file, args.table_name)