import os
import re
import pandas as pd
from datetime import datetime
import argparse
from processing.process_file import filter_untrusted_posts, filter_da

# 1. 프로젝트 루트 절대 경로 설정
SCRIPT_PATH = os.path.abspath(__file__)
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))

def process_data(
    input_csv_path,
    output_excel_path,
    search_excel_path,
    target_year,
    target_month
):
    # 출력 폴더 자동 생성
    os.makedirs(os.path.dirname(output_excel_path), exist_ok=True)

    print(f"📂 설정 파일 로드: {search_excel_path}")
    pd_search = pd.read_excel(search_excel_path, sheet_name='검색어 목록')
    searchs = pd_search['검색어명']

    print(f"📂 데이터 로드: {input_csv_path}")
    try:
        df = pd.read_csv(input_csv_path, encoding="utf-8")
    except UnicodeDecodeError:
        print("⚠️ UTF-8 디코딩 실패, cp949로 재시도합니다.")
        df = pd.read_csv(input_csv_path, encoding="cp949")
    except FileNotFoundError:
        print(f"❌ 파일을 찾을 수 없습니다: {input_csv_path}")
        return None

    df['게시물 등록일자'] = pd.to_datetime(df['게시물 등록일자'], errors='coerce')
    df["게시물 제목"] = df["게시물 제목"].fillna("").astype(str)
    df["게시물 내용"] = df["게시물 내용"].fillna("").astype(str)

    # 필터링 로직
    df1 = df[
        (df.apply(
            lambda x: any(s.lower() in str(x['게시물 제목']).lower() or s.lower() in str(x['게시물 내용']).lower() for s in searchs),
            axis=1
        )) &
        (~df['게시물 내용'].str.contains('신춘문예', na=False, case=False)) &
        (~df['게시물 제목'].str.contains('신춘문예', na=False, case=False)) &
        (~df['계정명'].fillna('').str.contains('뽐뿌뉴스', case=False))
    ]
    
    # 날짜 필터링
    df2 = df1[
        (df1['게시물 등록일자'].dt.year == target_year) &
        (df1['게시물 등록일자'].dt.month == target_month)
    ]
    df3 = df2.drop_duplicates(subset=['게시물 URL'])

    # 비신탁사 필터링 (현재는 통과)
    df_filtered = df3 

    # DA 필터링 (process_file.py에 정의됨)
    # filtered_df = filter_da(df_filtered)
    
    df_filtered.to_excel(output_excel_path, index=False)
    print(f"✅ 전처리 완료: {output_excel_path}")
    print(f"→ 입력: {len(df)}개 / 최종: {len(df_filtered)}개")

    return df_filtered


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_csv", required=True)
    parser.add_argument("--output_excel", required=True)
    parser.add_argument("--search_excel", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)

    args = parser.parse_args()

    # 2. 경로 처리: 입력값이 절대 경로가 아니면 프로젝트 루트 기준으로 결합
    def resolve_path(path):
        if os.path.isabs(path):
            return path
        return os.path.join(PROJECT_ROOT_DIR, path)

    process_data(
        input_csv_path=resolve_path(args.input_csv),
        output_excel_path=resolve_path(args.output_excel),
        search_excel_path=resolve_path(args.search_excel),
        target_year=args.year,
        target_month=args.month
    )