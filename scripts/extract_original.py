
import argparse
import os
import pandas as pd
from extraction.main_script import find_original_article_multiprocess
from extraction.core_utils import log
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

today = datetime.now().strftime("%y%m%d")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="원문기사 매칭 및 복사율 계산")
    parser.add_argument("--input_excel", required=True, help="전처리된 입력 엑셀 경로")
    parser.add_argument("--output_csv", required=True, help="결과 저장 csv 경로")

    args = parser.parse_args()

    input_path = args.input_excel
    output_path = args.output_csv

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df = pd.read_excel(input_path, dtype={"게시글 등록일자": str})
    total = len(df)
    log(f"📄 전체 게시글 수: {total}개")
    if "게시물 URL" in df.columns:
        df["게시물 URL"] = df["게시물 URL"].apply(
            lambda x: f'=HYPERLINK("{x}")' if pd.notna(x) and not str(x).startswith("=HYPERLINK") else x
        )
    df["원본기사"] = ""
    df["복사율"] = 0.0
    tasks = [(i, row.to_dict(), total) for i, row in df.iterrows()]

    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(find_original_article_multiprocess, *args) for args in tasks]
        for future in as_completed(futures):
            try:
                index, link, score = future.result()
                df.at[index, "원본기사"] = link
                df.at[index, "복사율"] = score
            except Exception as e:
                log(f"❌ 결과 처리 오류: {e}")

    # 매칭 통계 계산
    matched_count = df["복사율"].gt(0).sum()
    above_80_count = df["복사율"].ge(0.8).sum()
    above_30_count = df["복사율"].ge(0.3).sum() - above_80_count

    stats_rows = pd.DataFrame([
        {"검색어": "매칭건수", "플랫폼": f"{matched_count}건"},
        {"검색어": "0.3 이상", "플랫폼": f"{above_30_count}건"},
        {"검색어": "0.8 이상", "플랫폼": f"{above_80_count}건"},
    ])
    df = pd.concat([df, stats_rows], ignore_index=True)
    df.to_csv(output_path, index=False)
    log("🎉 완료! 저장됨 → " + output_path)
