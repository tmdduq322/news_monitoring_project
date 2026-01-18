import sys
import os
import argparse
import pandas as pd
from datetime import datetime
import multiprocessing
import time
import glob

# [중요] Airflow 및 로컬 환경 모두에서 모듈을 찾을 수 있도록 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 각 크롤러 모듈 임포트
from crawlers.pp_crawler import pp_main_crw
from crawlers.clien_crawler import clien_main_crw
from crawlers.inven_crawler import inven_main_crw
from crawlers.todayhumor_crawler import todayhumor_main_crw
from crawlers.paan_crawler import paan_main_crw
from crawlers.instiz_crawler import instiz_main_crw
from crawlers.bobaedream_crawler import bobaedream_main_crw
from crawlers.rw_crawler import rw_main_crw
from crawlers.arca_crawler import arca_main_crw
from crawlers.ilbe_crawler import ilbe_main_crw
from crawlers.humoruniv_crawler import humoruniv_main_crw
from crawlers.cook82_crawler import cook82_main_crw
from crawlers.orbi_crawler import orbi_main_crw
from crawlers.dogdrip_crawler import dogdrip_main_crw
from crawlers.dp_crawler import dp_main_crw
from crawlers.dongsaroma_crawler import dongsaroma_main_crw
from crawlers.scline_crawler import scline_main_crw
from crawlers.fomos_crawler import fomos_main_crw
from crawlers.jjang0u_crawler import jjang0u_main_crw
from crawlers.blind_crawler import blind_main_crw
from crawlers.mlb_crawler import mlb_main_crw
from crawlers.dc_crawler import dc_main_crw
from crawlers.fm_crawler import fm_main_crw
from crawlers.dq_crawler import dq_main_crw

# 크롤러 매핑
crawlers = {
    "뽐뿌": pp_main_crw,
    "클리앙": clien_main_crw,
    "인벤": inven_main_crw,
    "오늘의유머": todayhumor_main_crw,
    "네이트판": paan_main_crw,
    "인스티즈": instiz_main_crw,
    "보배드림": bobaedream_main_crw,
    "루리웹": rw_main_crw,
    "아카라이브": arca_main_crw,
    "일간베스트": ilbe_main_crw,
    "웃긴대학": humoruniv_main_crw,
    "82쿡": cook82_main_crw,
    "오르비": orbi_main_crw,
    "개드립": dogdrip_main_crw,
    "DVD프라임": dp_main_crw,
    "동사로마닷컴": dongsaroma_main_crw,
    "사커라인": scline_main_crw,
    "포모스": fomos_main_crw,
    "짱공유닷컴": jjang0u_main_crw,
    "블라인드": blind_main_crw,
    "엠엘비파크": mlb_main_crw,
    "디시인사이드": dc_main_crw,
    "에펨코리아": fm_main_crw,
    "더쿠": dq_main_crw
}

# 프로세스 실행 래퍼 함수
def run_crawler_process(crawler_func, searchs, start_date, end_date, stop_event):
    try:
        crawler_func(searchs, start_date, end_date, stop_event)
    except Exception as e:
        print(f"Error inside process: {e}")

# [핵심] 해당 사이트의 데이터 저장 폴더 찾기
def find_data_folder(site_name, target_date_str):
    # 프로젝트 루트의 data/raw 경로
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw'))
    
    # 1.뽐뿌, 5.오늘의유머 등 폴더명이 숫자와 섞여있으므로 검색
    if not os.path.exists(base_dir):
        return None
        
    for folder in os.listdir(base_dir):
        if site_name in folder: 
            full_path = os.path.join(base_dir, folder, target_date_str)
            return full_path
    return None

# [핵심] 폴더 내에서 가장 최신 파일의 수정 시간 가져오기
def get_latest_file_mtime(folder_path):
    if not folder_path or not os.path.exists(folder_path):
        return 0
    
    list_of_files = glob.glob(os.path.join(folder_path, '*.csv'))
    if not list_of_files:
        return 0
    
    latest_file = max(list_of_files, key=os.path.getmtime)
    return os.path.getmtime(latest_file)

if __name__ == "__main__":
    multiprocessing.freeze_support()

    parser = argparse.ArgumentParser()
    parser.add_argument("--site", type=str, default="all", help="크롤링할 사이트 이름 (콤마로 구분 가능, 예: '뽐뿌,클리앙')")
    parser.add_argument("--start_date", type=str, required=True, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--search_excel", type=str, required=True, help="검색어 엑셀 파일 경로")
    
    args = parser.parse_args()

    # 1. 날짜 변환
    try:
        start_date_obj = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        target_date_str = start_date_obj.strftime("%y%m%d") # 폴더명용 (251222)
    except ValueError:
        print("❌ 날짜 형식이 올바르지 않습니다.")
        sys.exit(1)

    # 2. 검색어 로드
    if not os.path.exists(args.search_excel):
        print(f"❌ 검색어 파일을 찾을 수 없습니다: {args.search_excel}")
        sys.exit(1)
    
    df = pd.read_excel(args.search_excel, sheet_name='검색어 목록')
    searchs = df['검색어명'].tolist()

    # 3. 사이트 선택 (다중 사이트 지원 수정)
    input_site = args.site
    sites_to_crawl = []

    if input_site == "all":
        sites_to_crawl = list(crawlers.keys())
    else:
        # 콤마로 구분된 리스트 처리 (공백 제거 포함)
        potential_sites = [s.strip() for s in input_site.split(',')]
        for s in potential_sites:
            if s in crawlers:
                sites_to_crawl.append(s)
            else:
                print(f"⚠️ 경고: '{s}'는(은) 지원하지 않는 사이트이거나 오타입니다.")

    if not sites_to_crawl:
        print("❌ 실행할 유효한 사이트가 없습니다.")
        sys.exit(1)

    print(f"📋 크롤링 대상 사이트 ({len(sites_to_crawl)}개): {sites_to_crawl}")

    # [설정] 무응답 대기 시간 (5분) - 필요시 조정
    IDLE_TIMEOUT = 4 * 60 
    # [설정] 전체 최대 제한 시간 (6시간)
    MAX_TOTAL_TIMEOUT = 6 * 60 * 60

    stop_event = multiprocessing.Event()

    for site_name in sites_to_crawl:
        crawler_func = crawlers[site_name]
        print(f"\n🚀 [{site_name}] 크롤링 시작... ({IDLE_TIMEOUT}초 무응답 시 종료)")
        
        p = multiprocessing.Process(
            target=run_crawler_process, 
            args=(crawler_func, searchs, start_date_obj, end_date_obj, stop_event)
        )
        
        p.start()
        
        process_start_time = time.time()
        last_activity_time = time.time()
        
        # 감시할 폴더 경로 찾기 (초기엔 없을 수 있음)
        target_folder = None
        
        while p.is_alive():
            current_time = time.time()
            
            # 1. 전체 시간 초과 체크 (안전장치)
            if current_time - process_start_time > MAX_TOTAL_TIMEOUT:
                print(f"🛑 [{site_name}] 전체 제한 시간({MAX_TOTAL_TIMEOUT}초) 초과! 강제 종료.")
                p.terminate()
                break

            # 2. 폴더 찾기 (아직 안 만들어졌을 수 있으므로 반복 시도)
            if target_folder is None:
                target_folder = find_data_folder(site_name, target_date_str)
            
            # 3. 파일 변경 시간 확인 (Idle Check)
            latest_file_time = get_latest_file_mtime(target_folder)
            
            # 만약 파일이 수정되었거나 새로 생겼으면 -> 활동 중! 시간 갱신
            if latest_file_time > last_activity_time:
                last_activity_time = latest_file_time
                # print(f"   [{site_name}] 새 데이터 감지됨! 타이머 리셋.") 

            # 4. 무응답 시간 체크
            idle_duration = current_time - last_activity_time
            if idle_duration > IDLE_TIMEOUT:
                print(f"⏰ [{site_name}] {IDLE_TIMEOUT/60:.1f}분 동안 새 데이터 없음! (정체됨) -> 다음 사이트로 이동.")
                p.terminate()
                p.join()
                break
            
            # 5초마다 검사
            time.sleep(5)

        p.join() # 좀비 프로세스 방지

        if p.exitcode == 0:
            print(f"✅ [{site_name}] 작업 완료")
        else:
            print(f"⚠️ [{site_name}] 작업 종료됨 (Exit Code: {p.exitcode})")

    print("\n🎉 지정된 모든 사이트 크롤링 작업 종료")