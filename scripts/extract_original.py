import argparse
import os
import sys
import pandas as pd
from extraction.main_script import find_original_article_multiprocess
from extraction.core_utils import log
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
# from webdriver_manager.chrome import ChromeDriverManager # 필요 시 주석 해제

# Airflow 경로 설정
AIRFLOW_HOME = "/opt/airflow"

# AWS 인증 정보
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

today = datetime.now().strftime("%y%m%d")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="원문기사 매칭 및 복사율 계산")
    parser.add_argument("--input_excel", required=True, help="전처리된 입력 엑셀 경로 (로컬)")
    parser.add_argument("--output_csv", required=True, help="결과 저장 csv 경로 (로컬 또는 S3)")

    args = parser.parse_args()

    # 1. 입력 파일 경로 처리 (로컬 파일)
    # 입력 경로가 절대 경로가 아니라면 AIRFLOW_HOME을 붙여줌
    if not os.path.isabs(args.input_excel):
        input_path = os.path.join(AIRFLOW_HOME, args.input_excel)
    else:
        input_path = args.input_excel

    if not os.path.exists(input_path):
        log(f"❌ 입력 파일을 찾을 수 없습니다: {input_path}")
        sys.exit(1)

    log(f"📂 입력 파일 읽기: {input_path}")
    try:
        df = pd.read_excel(input_path)
    except Exception as e:
        log(f"❌ 엑셀 로드 실패: {e}")
        sys.exit(1)

    total = len(df)
    log(f"📄 전체 게시글 수: {total}개")

    # URL 하이퍼링크 처리
    if "게시물 URL" in df.columns:
        df["게시물 URL"] = df["게시물 URL"].apply(
            lambda x: f'=HYPERLINK("{x}")' if pd.notna(x) and not str(x).startswith("=HYPERLINK") else x
        )
    
    df["원본기사"] = ""
    df["복사율"] = 0.0

    # 멀티프로세싱 작업
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

    # 매칭 결과 통계
    matched_count = df["복사율"].gt(0).sum()
    log(f"✨ 매칭 완료: {matched_count}건 매칭됨")

    # 2. 결과 저장 (S3 또는 로컬)
    output_path = args.output_csv
    storage_options = None

    if output_path.startswith("s3://"):
        # S3 저장 설정
        if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
            log("❌ AWS 자격 증명이 없습니다.")
            sys.exit(1)
        storage_options = {
            "key": AWS_ACCESS_KEY,
            "secret": AWS_SECRET_KEY
        }
        log(f"☁️ S3 업로드 시작: {output_path}")
    else:
        # 로컬 저장 설정
        if not os.path.isabs(output_path):
            output_path = os.path.join(AIRFLOW_HOME, output_path)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        log(f"💾 로컬 저장 시작: {output_path}")

    try:
        df.to_csv(output_path, index=False, encoding='utf-8-sig', storage_options=storage_options)
        log("✅ 저장 완료.")
    except Exception as e:
        log(f"❌ 저장 실패: {e}")
        sys.exit(1)