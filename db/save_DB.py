import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, MetaData
from sqlalchemy.dialects.mysql import LONGTEXT, VARCHAR, FLOAT
from dotenv import load_dotenv
import os

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = int(os.getenv("DB_PORT", 3306))
database = os.getenv("DB_NAME")

# 💡 SQLAlchemy 엔진 및 MetaData 생성
engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
)
metadata = MetaData()

# 💾 엑셀 → MySQL 저장 함수 (개선된 버전)
def save_excel_to_mysql(filepath, table_name="news_posts"):
    # 1. 엑셀 불러오기 및 전처리
    df = pd.read_excel(filepath)
    if "게시물_내용" in df.columns:
        df["게시물_내용"] = df["게시물_내용"].apply

    # 2. SQLAlchemy를 사용하여 정확한 테이블 스키마 정의
    #    이 부분이 핵심입니다!
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
        # 만약 기본키(Primary Key)를 지정하고 싶다면 아래처럼 추가
        # Column("id", INT, primary_key=True, autoincrement=True)
    )

    # 3. 테이블 생성 (만약 존재하면 먼저 삭제)
    print(f"🔄 기존 테이블 '{table_name}'을(를) 삭제하고 새로 생성합니다.")
    # checkfirst=True: 삭제할 테이블이 없어도 오류 발생 안 함
    news_posts_table.drop(engine, checkfirst=True)
    # 테이블 생성
    news_posts_table.create(engine)

    # 4. 데이터 저장 (if_exists를 'append'로 변경)
    #    이제 pandas는 테이블을 만들지 않고, 이미 만들어진 테이블에 데이터만 넣습니다.
    df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
    print(f"✅ MySQL 테이블 '{table_name}'에 저장 완료!")

#  실행
if __name__ == "__main__":
    # 엑셀 파일 경로를 확인해주세요.
    save_excel_to_mysql("../../결과/6월 원문기사자료/웹사이트_원문기사통계_6월.xlsx")