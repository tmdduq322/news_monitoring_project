import os
import glob
import pandas as pd
import argparse

SCRIPT_PATH = os.path.abspath(__file__)

PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))
# --- 여기까지 추가 ---


def merge_daily_raw_csv(target_date, raw_data_dir=None, output_dir=None):
    # --- 이 부분을 수정해주세요 ---
    # 경로를 프로젝트 루트 기준으로 설정합니다.
    if raw_data_dir is None:
        raw_data_dir = os.path.join(PROJECT_ROOT_DIR, "data", "raw")
    if output_dir is None:
        output_dir = os.path.join(PROJECT_ROOT_DIR, "data", "merged")
    # --- 여기까지 수정 ---
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"merged_raw_{target_date}.csv")

    search_pattern = os.path.join(raw_data_dir, "*", f"*_{target_date}_*.csv")
    raw_files = glob.glob(search_pattern)

    if not raw_files:
        print(f"❌ 대상 파일 없음: {search_pattern}")
        return

    # (이하 코드는 동일)
    merged_df = pd.DataFrame()
    for file in raw_files:
        try:
            platform = os.path.basename(os.path.dirname(file))
            df = pd.read_csv(file, encoding="utf-8")
            df["플랫폼"] = platform
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            print(f"✅ 병합됨: {file}")
        except Exception as e:
            print(f"❌ 병합 실패: {file} ({e})")
    
    if not merged_df.empty:
        merged_df.to_csv(output_path, index=False)
        print(f"📦 저장 완료: {output_path}")
    else:
        print("결과 파일이 비어있어 저장하지 않습니다.")
        
    return merged_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="날짜 (형식: yymmdd)")
    args = parser.parse_args()
    merge_daily_raw_csv(args.date)