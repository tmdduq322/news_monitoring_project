import os
import glob
import pandas as pd
from datetime import datetime
import argparse


def merge_daily_raw_csv(target_date, raw_data_dir="data/raw", output_dir="data/merged"):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"merged_raw_{target_date}.csv")

    # 모든 플랫폼의 날짜별 파일을 glob으로 검색
    search_pattern = os.path.join(raw_data_dir, "1.*", f"*_{target_date}_*.csv")
    raw_files = glob.glob(search_pattern)

    if not raw_files:
        print(f"❌ 대상 파일 없음: {search_pattern}")
        return

    merged_df = pd.DataFrame()

    for file in raw_files:
        try:
            df = pd.read_csv(file, encoding="utf-8")
            # 예: 뽐뿌_250801_KBS.csv → 뽐뿌
            platform = os.path.basename(file).split("_")[0]
            df["플랫폼"] = platform
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            print(f"✅ 병합됨: {file}")
        except Exception as e:
            print(f"❌ 병합 실패: {file} ({e})")

    merged_df.to_csv(output_path, index=False)
    print(f"📦 저장 완료: {output_path}")
    return merged_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="날짜 (형식: yymmdd)")
    args = parser.parse_args()
    merge_daily_raw_csv(args.date)
