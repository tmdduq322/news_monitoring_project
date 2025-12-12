import argparse
import os
import pandas as pd
from extraction.main_script import find_original_article_multiprocess
from extraction.core_utils import log
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from webdriver_manager.chrome import ChromeDriverManager

# [중요] Airflow 컨테이너의 기본 홈 디렉토리 고정
AIRFLOW_HOME = "/opt/airflow"

today = datetime.now().strftime("%y%m%d")

if __name__ == "__main__":
    #  드라이버를 미리 한 번 설치하여 캐시 생성
    print("🚗 ChromeDriver 설치 확인 중...")
    ChromeDriverManager().install()
    parser = argparse.ArgumentParser(description="원문기사 매칭 및 복사율 계산")
    parser = argparse.ArgumentParser(description="원문기사 매칭 및 복사율 계산")
    parser.add_argument("--input_excel", required=True, help="전처리된 입력 엑셀 경로")
    parser.add_argument("--output_csv", required=True, help="결과 저장 csv 경로")

    args = parser.parse_args()

    # [수정된 핵심 부분]
    # 입력받은 경로가 'data/...' 같은 상대 경로라면, 무조건 /opt/airflow를 앞에 붙입니다.
    # 이렇게 하면 실행 위치가 /tmp 든 어디든 상관없이 정확한 파일을 찾습니다.
    
    if not os.path.isabs(args.input_excel):
        input_path = os.path.join(AIRFLOW_HOME, args.input_excel)
    else:
        input_path = args.input_excel

    if not os.path.isabs(args.output_csv):
        output_path = os.path.join(AIRFLOW_HOME, args.output_csv)
    else:
        output_path = args.output_csv

    log(f"📂 [Input] 읽을 파일: {input_path}")
    log(f"📂 [Output] 저장 경로: {output_path}")

    # 저장할 폴더가 없으면 생성
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 엑셀 읽기
    try:
        df = pd.read_excel(input_path, dtype={"게시글 등록일자": str})
    except FileNotFoundError:
        log(f"❌ 파일을 찾을 수 없습니다: {input_path}")
        # 파일이 없으면 여기서 확실하게 에러를 내고 종료
        exit(1)

    total = len(df)
    log(f"📄 전체 게시글 수: {total}개")

    if "게시물 URL" in df.columns:
        df["게시물 URL"] = df["게시물 URL"].apply(
            lambda x: f'=HYPERLINK("{x}")' if pd.notna(x) and not str(x).startswith("=HYPERLINK") else x
        )
    
    df["원본기사"] = ""
    df["복사율"] = 0.0

    # 멀티프로세싱 작업 준비
    tasks = [(i, row.to_dict(), total) for i, row in df.iterrows()]

    # [참고] core_utils.log 설정 덕분에 여기서 발생하는 로그도 /opt/airflow/data/log/extraction/log.txt 에 쌓임
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
    
    # CSV 저장
    df.to_csv(output_path, index=False, encoding='utf-8-sig') 
    log(f"🎉 작업 완료! 최종 파일 저장됨: {output_path}")