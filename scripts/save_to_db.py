import os
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import sys

# 프로젝트 루트 경로 설정
SCRIPT_PATH = os.path.abspath(__file__)
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))

# .env 로드 (AWS RDS 접속 정보)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT_DIR, '.env'))

def save_to_mysql(input_file, table_name):
    # 1. DB 연결 문자열 생성
    db_user = os.getenv("DB_USER", "admin")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME", "airflow_db")
    
    if not db_password or not db_host:
        print("❌ .env 파일에 DB 접속 정보(DB_HOST, DB_PASSWORD)가 없습니다.")
        sys.exit(1)

    # SQLAlchemy 엔진 생성
    db_url = f"mysql+mysqldb://{db_user}:{db_password}@{db_host}:3306/{db_name}?charset=utf8mb4"
    engine = create_engine(db_url)

    # 2. 데이터 로드
    print(f"📂 데이터 로드 중: {input_file}")
    try:
        # S3 경로인 경우 storage_options 필요 (s3fs 라이브러리 필요)
        if input_file.startswith("s3://"):
            storage_options = {
                "key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY")
            }
            df = pd.read_csv(input_file, storage_options=storage_options)
        else:
            df = pd.read_csv(input_file)
            
    except Exception as e:
        print(f"❌ 파일 읽기 실패: {e}")
        sys.exit(1)

    if df.empty:
        print("⚠️ 저장할 데이터가 없습니다.")
        return

    # 3. 필요한 컬럼만 선택 및 정제
    # DB 스키마와 DataFrame 컬럼명을 맞춰주는 작업이 필요할 수 있습니다.
    # 예: '게시물 제목' -> 'title', '게시물 URL' -> 'url' 등
    # 여기서는 CSV 컬럼명을 그대로 쓴다고 가정하거나, 매핑합니다.
    column_mapping = {
        "게시물 제목": "title",
        "게시물 내용": "content",
        "게시물 URL": "url",
        "게시물 등록일자": "published_at",
        "수집시간": "crawled_at",
        "플랫폼": "platform",
        "계정명": "writer",
        "원본기사": "original_article_url",
        "복사율": "copy_rate"
    }
    
    # 존재하는 컬럼만 변경
    df = df.rename(columns=column_mapping)
    
    # DB에 없는 컬럼이 df에 있으면 에러나므로, 필요한 컬럼만 필터링하는 로직 추천
    # (여기서는 생략하고 진행)

    print(f"💾 DB 저장 시작 ({len(df)}건) -> 테이블: {table_name}")

    # 4. 데이터 저장 (INSERT IGNORE 방식 구현)
    # Pandas의 to_sql은 기본적으로 중복 처리를 못하므로, temp 테이블을 활용하거나
    # 한 줄씩 넣으면서 예외처리를 해야 합니다. 대량 데이터에는 temp 테이블 방식이 빠릅니다.
    
    try:
        with engine.connect() as conn:
            # (1) URL 컬럼에 유니크 인덱스가 없다면 생성 (최초 1회만 실행됨)
            # 포트폴리오용으로 안전하게 코드 내에서 처리
            try:
                conn.execute(text(f"ALTER TABLE {table_name} ADD UNIQUE INDEX idx_url (url(255));"))
                print("✅ URL 컬럼에 유니크 인덱스를 생성했습니다.")
            except Exception:
                pass # 이미 있으면 패스

            # (2) Pandas to_sql로 'append' (중복나면 에러 발생함)
            # 따라서 'chunksize'를 사용하여 나누어 넣거나, 
            # 가장 깔끔한 방법: 'INSERT IGNORE' 쿼리를 직접 생성해서 실행
            
            # DataFrame을 딕셔너리 리스트로 변환
            data_to_insert = df.to_dict(orient='records')
            
            success_count = 0
            
            # 쿼리문 생성 (MySQL INSERT IGNORE)
            # 컬럼 리스트 추출
            if not data_to_insert:
                return
            
            columns = data_to_insert[0].keys()
            cols_str = ", ".join([f"`{c}`" for c in columns])
            vals_str = ", ".join([f":{c}" for c in columns])
            
            sql = text(f"INSERT IGNORE INTO {table_name} ({cols_str}) VALUES ({vals_str})")
            
            # 실행
            result = conn.execute(sql, data_to_insert)
            conn.commit()
            
            print(f"✅ DB 저장 완료. (영향받은 행: {result.rowcount}개 / 전체: {len(df)}개)")
            print("   (중복된 URL은 자동으로 건너뛰었습니다)")

    except Exception as e:
        print(f"❌ DB 저장 중 오류 발생: {e}")
        # 테이블이 아예 없어서 에러난 경우라면, to_sql로 최초 생성 시도
        if "Table" in str(e) and "doesn't exist" in str(e):
            print("⚠️ 테이블이 없어서 새로 생성합니다.")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            # 생성 후 유니크 인덱스 추가
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD UNIQUE INDEX idx_url (url(255));"))
            print("✅ 테이블 생성 및 데이터 저장 완료.")
        else:
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True, help="저장할 CSV/Excel 파일 경로")
    parser.add_argument("--table_name", default="news_posts", help="저장할 테이블 이름")
    
    args = parser.parse_args()
    
    # 엑셀 파일인 경우 변환
    if args.input_file.endswith(".xlsx"):
        # 엑셀 읽기 기능이 필요하다면 pandas read_excel 사용
        # 여기서는 csv로 넘어온다고 가정하거나, 코드 상단에서 처리
        pass 

    save_to_mysql(args.input_file, args.table_name)