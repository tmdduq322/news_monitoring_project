import argparse
import os
import sys
import pandas as pd
import numpy as np
from extraction.main_script import find_original_article_multiprocess
from extraction.core_utils import log
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# 1. 프로젝트 루트 절대 경로 설정
SCRIPT_PATH = os.path.abspath(__file__)
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_PATH))

# AWS 인증 정보
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def resolve_path(path):
    if path.startswith("s3://"): 
        return path
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT_DIR, path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="원문기사 매칭 (분산 처리용)")
    parser.add_argument("--input_excel", required=True, help="전처리된 입력 엑셀 경로")
    parser.add_argument("--output_csv", required=True, help="결과 저장 csv 경로 (자동으로 _partX 붙음)")
    # [추가] 분할 처리를 위한 인자
    parser.add_argument("--worker_id", type=int, default=0, help="현재 워커 번호 (0부터 시작)")
    parser.add_argument("--total_workers", type=int, default=1, help="총 워커 수")

    args = parser.parse_args()

    input_path = resolve_path(args.input_excel)
    # 저장 파일명 분리 (예: result.csv -> result_part_0.csv)
    base, ext = os.path.splitext(resolve_path(args.output_csv))
    part_output_path = f"{base}_part_{args.worker_id}{ext}"
    
    # 임시 저장 파일도 분리
    temp_output_path = os.path.join(PROJECT_ROOT_DIR, "data", "extracted", f"temp_progress_part_{args.worker_id}.csv")
    os.makedirs(os.path.dirname(temp_output_path), exist_ok=True)

    if not os.path.exists(input_path):
        log(f"❌ 입력 파일을 찾을 수 없습니다: {input_path}")
        sys.exit(1)

    log(f"📂 [Worker {args.worker_id}/{args.total_workers}] 파일 로드: {input_path}")
    try:
        df_all = pd.read_excel(input_path)
    except Exception as e:
        log(f"❌ 엑셀 로드 실패: {e}")
        sys.exit(1)

    # [핵심] 데이터 분할 (Partitioning)
    # 전체 데이터를 워커 수만큼 쪼개서 내 몫만 가져옴
    chunks = np.array_split(df_all, args.total_workers)
    if args.worker_id >= len(chunks):
        log("⚠️ 할당된 데이터가 없습니다. 종료합니다.")
        sys.exit(0)
        
    df = chunks[args.worker_id].copy()  # 내 할당량
    
    # 이어하기 기능 (내 파트의 임시 파일 확인)
    processed_indices = set()
    if os.path.exists(temp_output_path):
        try:
            df_temp = pd.read_csv(temp_output_path)
            if "원본기사" in df_temp.columns:
                # 인덱스를 기준으로 병합
                # 주의: df_temp는 전체가 아니라 내 파트의 일부일 수 있음
                # 우선 간단하게는 '이미 처리된 원본 df의 인덱스'를 파악
                processed_indices = set(df_temp[df_temp["원본기사"].notna()].index)
                
                # 기존 df에 덮어씌우기 (인덱스 매칭)
                df.update(df_temp)
                log(f"🔄 [Worker {args.worker_id}] 이전 작업 내역 발견: {len(processed_indices)}개 처리됨.")
        except Exception as e:
            log(f"⚠️ 임시 파일 읽기 실패 ({e}), 처음부터 시작합니다.")

    if "원본기사" not in df.columns:
        df["원본기사"] = ""
        df["복사율"] = 0.0

    total_my_task = len(df)
    log(f"🔥 [Worker {args.worker_id}] 내 할당량: {total_my_task}개 (전체 {len(df_all)}개 중)")

    # 처리해야 할 인덱스 (이미 한 거 제외)
    # df.index는 전체 데이터 프레임의 원본 인덱스를 유지하고 있음
    target_indices = [i for i in df.index if i not in processed_indices]

    # 배치 처리
    CHUNK_SIZE = 5 # 워커당 작업량이 줄었으니 더 자주 저장해도 됨
    
    for i in range(0, len(target_indices), CHUNK_SIZE):
        chunk_indices = target_indices[i : i + CHUNK_SIZE]
        log(f"🚀 [Worker {args.worker_id}] 배치 시작 ({i}/{len(target_indices)}) - {len(chunk_indices)}건")

        tasks = [(idx, df.loc[idx].to_dict(), len(df_all)) for idx in chunk_indices]

        # 각 분할 태스크 안에서는 워커 1개만 사용 (안정성)
        with ProcessPoolExecutor(max_workers=1) as executor:
            futures = [executor.submit(find_original_article_multiprocess, *args) for args in tasks]
            
            for future in as_completed(futures):
                try:
                    index, link, score = future.result()
                    df.at[index, "원본기사"] = link
                    df.at[index, "복사율"] = score
                except Exception as e:
                    log(f"❌ [Worker {args.worker_id}] 개별 오류: {e}")

        # 중간 저장
        df.to_csv(temp_output_path, index=True, encoding='utf-8-sig') # 인덱스 포함해서 저장해야 나중에 매칭 가능
        log(f"💾 [Worker {args.worker_id}] 중간 저장 완료.")

    # 최종 저장
    storage_options = None
    if args.output_csv.startswith("s3://"):
        storage_options = {"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    
    if not args.output_csv.startswith("s3://"):
        os.makedirs(os.path.dirname(part_output_path), exist_ok=True)

    try:
        # 인덱스 없이 깔끔하게 저장 (나중에 DB 저장 시 concat하면 됨)
        df.to_csv(part_output_path, index=False, encoding='utf-8-sig', storage_options=storage_options)
        
        # 임시 파일 삭제
        if os.path.exists(temp_output_path):
            os.remove(temp_output_path)
            
        log(f"✅ [Worker {args.worker_id}] 최종 저장 완료: {part_output_path}")
    except Exception as e:
        log(f"❌ [Worker {args.worker_id}] 저장 실패: {e}")
        sys.exit(1)