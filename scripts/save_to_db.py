# save_to_db.py
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, MetaData
from sqlalchemy.dialects.mysql import LONGTEXT, VARCHAR, FLOAT
import pymysql
import argparse
import os

# MySQL 연결 정보
user = "root"
password = "1234"
host = "mysql"  # Docker 내부 서비스명
port = 3306
database = "article_db"

def summarize(text, limit=10000):
    if isinstance(text, str) and len(text) > limit:
        return text[:limit] + "...(이하 생략)"
    return text

def save_excel_to_mysql(filepath, table_name="news_posts"):
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
