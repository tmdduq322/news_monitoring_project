# process_data.py
import os
import re
import pandas as pd
from datetime import datetime
import argparse
from processing.process_file import filter_untrusted_posts, filter_da

def process_data(
    input_csv_path,
    output_excel_path,
    search_excel_path,
    target_year,
    target_month
):
    os.makedirs(os.path.dirname(output_excel_path), exist_ok=True)

    # 1. 검색어 목록 로딩
    pd_search = pd.read_excel(search_excel_path, sheet_name='검색어 목록')
    searchs = pd_search['검색어명']

    # 2. CSV 로딩
    try:
        df = pd.read_csv(input_csv_path, encoding="utf-8")
    except UnicodeDecodeError:
        print("⚠️ UTF-8 디코딩 실패, cp949로 재시도합니다.")
        df = pd.read_csv(input_csv_path, encoding="cp949")

    df['게시물 등록일자'] = pd.to_datetime(df['게시물 등록일자'], errors='coerce')
    df["게시물 제목"] = df["게시물 제목"].fillna("").astype(str)
    df["게시물 내용"] = df["게시물 내용"].fillna("").astype(str)

    # 3. 검색어 필터
    df1 = df[
        (df.apply(
            lambda x: any(s.lower() in str(x['게시물 제목']).lower() or s.lower() in str(x['게시물 내용']).lower() for s in searchs),
            axis=1
        )) &
        (~df['게시물 내용'].str.contains('신춘문예', na=False, case=False)) &
        (~df['게시물 제목'].str.contains('신춘문예', na=False, case=False)) &
        (~df['계정명'].fillna('').str.contains('뽐뿌뉴스', case=False))
    ]

    # 4. 날짜 필터
    df2 = df1[
        (df1['게시물 등록일자'].dt.year == target_year) &
        (df1['게시물 등록일자'].dt.month == target_month)
    ]

    # 5. 중복 제거
    df3 = df2.drop_duplicates(subset=['게시물 URL'])

    # 6. 비신탁사/도메인 필터
    df_filtered = filter_untrusted_posts(
        df3,
        untrusted_file="config/수집 제외 도메인 주소.xlsx",
        trusted_file="config/process_keywords.xlsx"
    )

    # 7. '다.' 필터 + 만평 제거
    filtered_df = filter_da(df_filtered)

    # 8. 저장
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

    process_data(
        input_csv_path=args.input_csv,
        output_excel_path=args.output_excel,
        search_excel_path=args.search_excel,
        target_year=args.year,
        target_month=args.month
    )
