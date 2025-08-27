import os
import re
import pandas as pd
from datetime import datetime
import argparse
# 'process_file.py'의 위치가 'scripts' 폴더와 다른 'processing' 폴더에 있으므로 경로를 추가해줘야 할 수 있습니다.
# 만약 ModuleNotFoundError가 발생하면 아래 경로 설정을 활성화하세요.
# import sys
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processing.process_file import filter_untrusted_posts, filter_da

# --- 1. 프로젝트 절대 경로 설정 (이전과 동일) ---
SCRIPT_PATH = os.path.abspath(__file__)
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))

def process_data(
    input_csv_path,
    output_excel_path,
    search_excel_path,
    target_year,
    target_month
):
    os.makedirs(os.path.dirname(output_excel_path), exist_ok=True)

    pd_search = pd.read_excel(search_excel_path, sheet_name='검색어 목록')
    searchs = pd_search['검색어명']

    try:
        df = pd.read_csv(input_csv_path, encoding="utf-8")
    except UnicodeDecodeError:
        print("⚠️ UTF-8 디코딩 실패, cp949로 재시도합니다.")
        df = pd.read_csv(input_csv_path, encoding="cp949")

    df['게시물 등록일자'] = pd.to_datetime(df['게시물 등록일자'], errors='coerce')
    df["게시물 제목"] = df["게시물 제목"].fillna("").astype(str)
    df["게시물 내용"] = df["게시물 내용"].fillna("").astype(str)

    df1 = df[
        (df.apply(
            lambda x: any(s.lower() in str(x['게시물 제목']).lower() or s.lower() in str(x['게시물 내용']).lower() for s in searchs),
            axis=1
        )) &
        (~df['게시물 내용'].str.contains('신춘문예', na=False, case=False)) &
        (~df['게시물 제목'].str.contains('신춘문예', na=False, case=False)) &
        (~df['계정명'].fillna('').str.contains('뽐뿌뉴스', case=False))
    ]
    df2 = df1[
        (df1['게시물 등록일자'].dt.year == target_year) &
        (df1['게시물 등록일자'].dt.month == target_month)
    ]
    df3 = df2.drop_duplicates(subset=['게시물 URL'])

    # --- 2. config 파일 경로도 절대 경로로 수정 ---
    # 프로젝트 루트 경로와 config 파일의 상대 경로를 합쳐 절대 경로를 만듭니다.
    untrusted_file_path = os.path.join(PROJECT_ROOT_DIR, "config", "비신탁사_저작권문구+도메인주소.xlsx")
    trusted_file_path = os.path.join(PROJECT_ROOT_DIR, "config", "process_keywords.xlsx")

    df_filtered = filter_untrusted_posts(
        df3,
        untrusted_file=untrusted_file_path,
        trusted_file=trusted_file_path
    )
    # --- 여기까지 수정 ---

    filtered_df = filter_da(df_filtered)
    filtered_df.to_excel(output_excel_path, index=False)
    print(f"✅ 전처리 완료: {output_excel_path}")
    print(f"→ 입력: {len(df)}개 / 전처리 후: {len(filtered_df)}개 / 제거: {len(df) - len(filtered_df)}개")

    return filtered_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_csv", required=True)
    parser.add_argument("--output_excel", required=True)
    parser.add_argument("--search_excel", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)

    args = parser.parse_args()

    # --- 3. input/output 경로 절대 경로로 변환 (이전과 동일) ---
    input_csv = args.input_csv if os.path.isabs(args.input_csv) else os.path.join(PROJECT_ROOT_DIR, args.input_csv)
    output_excel = args.output_excel if os.path.isabs(args.output_excel) else os.path.join(PROJECT_ROOT_DIR, args.output_excel)
    search_excel = args.search_excel

    process_data(
        input_csv_path=input_csv,
        output_excel_path=output_excel,
        search_excel_path=search_excel,
        target_year=args.year,
        target_month=args.month
    )