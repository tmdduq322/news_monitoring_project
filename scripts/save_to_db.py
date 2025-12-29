import os
import argparse
import pandas as pd
import glob
import sys
import ssl
from sqlalchemy import create_engine, text

# [필수] S3 파일 검색을 위한 라이브러리
try:
    import s3fs
except ImportError:
    print("❌ s3fs 라이브러리가 없습니다. 'pip install s3fs'를 실행해주세요.")
    sys.exit(1)

# 프로젝트 루트 경로 설정
SCRIPT_PATH = os.path.abspath(__file__)
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))

# .env 로드
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT_DIR, '.env'))

def save_to_mysql(input_file_pattern, table_name):
    # 1. DB 연결
    db_user = os.getenv("DB_USER", "admin")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME", "airflow_db")
    
    if not db_password or not db_host:
        print("❌ .env 파일에 DB 접속 정보가 없습니다.")
        sys.exit(1)

    # SSL 설정
    db_url = f"mysql+mysqldb://{db_user}:{db_password}@{db_host}:3306/{db_name}?charset=utf8mb4"
    connect_args = {
        "ssl": {    
            "check_hostname": False,
            "verify_mode": ssl.CERT_NONE
        }
    }
    engine = create_engine(db_url, connect_args=connect_args)

    # 2. 데이터 파일 검색
    print(f"🔍 파일 검색 요청: {input_file_pattern}")
    
    matched_files = []
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    # (A) S3 경로인 경우
    if input_file_pattern.startswith("s3://"):
        try:
            fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)
            if fs.exists(input_file_pattern):
                matched_files = [input_file_pattern]
            else:
                base, ext = os.path.splitext(input_file_pattern)
                s3_glob_pattern = f"{base}_part_*{ext}"
                if s3_glob_pattern.startswith("s3://"):
                    search_path = s3_glob_pattern[5:]
                else:
                    search_path = s3_glob_pattern
                files = fs.glob(search_path)
                matched_files = [f"s3://{f}" for f in files]
                
                if matched_files:
                    print(f"ℹ️ S3 분할 파일 발견 ({len(matched_files)}개): {matched_files}")
        except Exception as e:
            print(f"❌ S3 검색 중 오류 발생: {e}")
            sys.exit(1)

    # (B) 로컬 파일인 경우
    else:
        matched_files = glob.glob(input_file_pattern)
        if not matched_files:
            base, ext = os.path.splitext(input_file_pattern)
            part_pattern = f"{base}_part_*{ext}"
            matched_files = glob.glob(part_pattern)

    if not matched_files:
        print(f"⚠️ 저장할 파일을 찾지 못했습니다: {input_file_pattern}")
        # 파일이 없는 것은 정상 상황일 수 있으므로 에러 아님
        return

    # 3. 데이터 읽기 및 병합
    df_list = []
    storage_options = {"key": aws_key, "secret": aws_secret} if aws_key else None

    for file_path in matched_files:
        try:
            if file_path.startswith("s3://"):
                d = pd.read_csv(file_path, storage_options=storage_options)
            else:
                d = pd.read_csv(file_path)
            df_list.append(d)
        except Exception as e:
            print(f"❌ 파일 읽기 실패 ({file_path}): {e}")
            sys.exit(1)

    if not df_list:
        return

    df = pd.concat(df_list, ignore_index=True)
    
    # [중요] NaN 값을 NULL로 변환
    df = df.where(pd.notnull(df), None)

    # 4. 컬럼 매핑
    column_mapping = {
        "게시물 제목": "title", "게시물 내용": "content", "게시물 URL": "url",
        "게시물 등록일자": "published_at", "수집시간": "crawled_at", "플랫폼": "platform",
        "계정명": "writer", "원본기사": "original_article_url", "복사율": "copy_rate",
        "검색어": "keyword" 
    }
    # 실제 컬럼명에 맞춰서 변경 (엑셀/CSV 헤더 확인 필요)
    # 코드에서는 '검색어'가 들어가 있는지 확인 필요, 없으면 에러날 수 있음
    # df에 있는 컬럼만 rename하도록 처리
    rename_map = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    
    print(f"💾 DB 저장 시작 (총 {len(df)}건) -> 테이블: {table_name}")

    try:
        # 👇 [수정 1] engine.begin() 사용 (자동 커밋 Transaction)
        with engine.begin() as conn:
            # 유니크 인덱스 생성 (없으면)
            try:
                conn.execute(text(f"ALTER TABLE {table_name} ADD UNIQUE INDEX idx_url (url(255));"))
            except Exception:
                pass

            data_to_insert = df.to_dict(orient='records')
            if not data_to_insert: return
            
            columns = data_to_insert[0].keys()
            cols_str = ", ".join([f"`{c}`" for c in columns])
            vals_str = ", ".join([f":{c}" for c in columns])
            
            sql = text(f"INSERT IGNORE INTO {table_name} ({cols_str}) VALUES ({vals_str})")
            
            # 실행 (commit은 with 블록 나갈 때 자동 수행됨)
            result = conn.execute(sql, data_to_insert)
            
            # 👇 [수정 2] conn.commit() 삭제됨
            print(f"✅ DB 저장 완료: {result.rowcount}건 삽입됨.")
            
    except Exception as e:
        print(f"❌ DB 저장 실패: {e}")
        # 테이블 없으면 생성 시도
        if "Table" in str(e) and "doesn't exist" in str(e):
             print("⚠️ 테이블 생성 후 재시도...")
             df.to_sql(table_name, engine, if_exists='replace', index=False)
             with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD UNIQUE INDEX idx_url (url(255));"))
             print("✅ 테이블 생성 완료.")
        else:
             # 👇 [수정 3] 진짜 에러라면 Airflow가 알 수 있게 강제 종료
             sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True)
    parser.add_argument("--table_name", default="news_posts")
    args = parser.parse_args()
    save_to_mysql(args.input_file, args.table_name)