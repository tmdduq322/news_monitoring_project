import os
import glob
import pandas as pd
import argparse

# 1. 프로젝트 루트 경로 계산 (어디서 실행하든 scripts 상위 폴더를 찾음)
SCRIPT_PATH = os.path.abspath(__file__)
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))

def merge_daily_raw_csv(target_date, raw_data_dir=None, output_dir=None):
    # 2. 경로가 안 들어오면 기본값 설정 (절대 경로)
    if raw_data_dir is None:
        raw_data_dir = os.path.join(PROJECT_ROOT_DIR, "data", "raw")
    if output_dir is None:
        output_dir = os.path.join(PROJECT_ROOT_DIR, "data", "merged")
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"merged_raw_{target_date}.csv")

    # [핵심 수정 1] 파일 찾는 패턴 변경
    # 수정 전: os.path.join(raw_data_dir, "*", f"*{target_date}*.csv")
    # 수정 후: raw_data_dir / 모든플랫폼(*) / 날짜폴더(target_date) / 모든csv(*)
    search_pattern = os.path.join(raw_data_dir, "*", target_date, "*.csv")
    
    print(f"🔍 검색 패턴: {search_pattern}")
    raw_files = glob.glob(search_pattern)

    if not raw_files:
        print(f"❌ 대상 파일 없음: {search_pattern}")
        # 혹시 구버전 구조(플랫폼 폴더 안에 바로 파일)일 수도 있으니 예비 검색 (안전장치)
        fallback_pattern = os.path.join(raw_data_dir, "*", f"*{target_date}*.csv")
        if glob.glob(fallback_pattern):
             print(f"⚠️ 경고: 구버전 폴더 구조의 파일이 발견되었습니다. 크롤러가 업데이트되었는지 확인하세요.")
        return

    merged_df = pd.DataFrame()
    for file in raw_files:
        try:
            # [핵심 수정 2] 플랫폼 이름 추출 방식 변경
            # 파일 경로: .../data/raw/1.뽐뿌/251222/뽐뿌_검색어.csv
            
            # dirname(file) -> .../data/raw/1.뽐뿌/251222
            # dirname(dirname(file)) -> .../data/raw/1.뽐뿌
            # basename(...) -> 1.뽐뿌
            platform = os.path.basename(os.path.dirname(os.path.dirname(file)))
            
            # (만약 구버전 구조라면 예외 처리)
            if platform == 'raw': 
                platform = os.path.basename(os.path.dirname(file))

            df = pd.read_csv(file, encoding="utf-8")
            df["플랫폼"] = platform # 데이터에 출처 표시
            
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            print(f"✅ 병합됨: {os.path.basename(file)} (플랫폼: {platform})")
            
        except Exception as e:
            print(f"❌ 병합 실패: {file} ({e})")
    
    if not merged_df.empty:
        merged_df.to_csv(output_path, index=False, encoding='utf-8-sig') # 엑셀 깨짐 방지 utf-8-sig
        print(f"📦 저장 완료: {output_path}")
        print(f"   (총 {len(merged_df)}개 데이터)")
    else:
        print("결과 파일이 비어있어 저장하지 않습니다.")
        
    return merged_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="날짜 (형식: yymmdd)")
    args = parser.parse_args()
    merge_daily_raw_csv(args.date)