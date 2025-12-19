import sys
import os
import argparse
import pandas as pd
from datetime import datetime
from threading import Event

# [중요] Airflow 및 로컬 환경 모두에서 모듈을 찾을 수 있도록 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 각 크롤러 모듈 임포트
# (다른 크롤러들도 pp_crawler처럼 수정해주셔야 날짜가 정확히 맞습니다)
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
from crawlers.scline_crawler import scline_main_crw
from crawlers.dongsaroma_crawler import dongsaroma_main_crw
from crawlers.fomos_crawler import fomos_main_crw
from crawlers.jjang0u_crawler import jjang0u_main_crw
from crawlers.blind_crawler import blind_main_crw
from crawlers.mlb_crawler import mlb_main_crw
from crawlers.dc_crawler import dc_main_crw
from crawlers.fm_crawler import fm_main_crw
from crawlers.dq_crawler import dq_main_crw

# 전역 중단 플래그
stop_event = Event()

# 사이트별 함수 매핑
crawlers = {
    "뽐뿌": pp_main_crw,
    "클리앙": clien_main_crw,
    "인벤": inven_main_crw,
    "루리웹": rw_main_crw,
    "오늘의유머": todayhumor_main_crw,
    "네이트판": paan_main_crw,
    "인스티즈": instiz_main_crw,
    "보배드림": bobaedream_main_crw,
    "아카라이브": arca_main_crw,
    "일간베스트": ilbe_main_crw,
    "웃긴대학": humoruniv_main_crw,
    "82쿡": cook82_main_crw,
    "오르비": orbi_main_crw,
    "개드립": dogdrip_main_crw,
    "DVD프라임": dp_main_crw,
    "사커라인": scline_main_crw,
    "동사로마닷컴": dongsaroma_main_crw,
    "포모스": fomos_main_crw,
    "짱공유닷컴": jjang0u_main_crw,
    "블라인드": blind_main_crw,
    "엠엘비파크": mlb_main_crw,
    "디시인사이드": dc_main_crw,
    "에펨코리아": fm_main_crw,
    "더쿠": dq_main_crw
}

def main(site, start_date, end_date, search_excel):
    print(f"🔧 [설정 확인] 시작일: {start_date}, 종료일: {end_date}")
    print(f"📂 [엑셀 경로] {search_excel}")

    # 1. 엑셀 로드
    try:
        pd_search = pd.read_excel(search_excel, sheet_name='검색어 목록')
        searchs = pd_search['검색어명']
    except Exception as e:
        print(f"❌ 엑셀 파일을 읽을 수 없습니다: {e}")
        return

    # 2. 날짜 문자열 -> datetime.date 객체로 변환
    # (이 객체가 pp_crawler.py로 넘어가서 .strftime('%y%m%d')로 변환됩니다)
    try:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        print("❌ 날짜 형식이 잘못되었습니다. YYYY-MM-DD 형식이어야 합니다.")
        return

    # 3. 크롤링 대상 사이트 선정
    sites_to_crawl = []
    if site == "all":
        sites_to_crawl = list(crawlers.keys())
        print(f"📢 [전체 모드] 총 {len(sites_to_crawl)}개 사이트 크롤링을 시작합니다.")
    elif site in crawlers:
        sites_to_crawl = [site]
    else:
        print(f"❌ 지원하지 않는 사이트입니다: {site}")
        print(f"   (사용 가능: 'all' 또는 {', '.join(crawlers.keys())})")
        return

    # 4. 크롤링 실행 루프
    for site_name in sites_to_crawl:
        crawler_func = crawlers[site_name]
        print(f"\n🚀 [{site_name}] 크롤링 시작... (Target: {start_date_obj})")
        
        try:
            # [핵심] 여기서 start_date_obj(어제 날짜 객체)를 넘겨줍니다.
            # pp_crawler.py 내부에서 이 객체를 받아 파일명을 생성합니다.
            crawler_func(searchs, start_date_obj, end_date_obj, stop_event)
            print(f"✅ [{site_name}] 수집 완료")
            
        except Exception as e:
            # 하나가 실패해도 나머지는 계속 진행 (Airflow 로그에서 확인 가능)
            print(f"❌ [{site_name}] 크롤링 중 에러 발생: {e}")
            # 필요 시 여기서 raise를 하여 Airflow Task를 실패 처리할 수도 있습니다.
            # raise e 

    print("\n🎉 모든 사이트 크롤링 작업 종료")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="사이트별 커뮤니티 크롤러 실행")
    parser.add_argument("--site", required=True, help="사이트 이름 (예: 뽐뿌, all)")
    parser.add_argument("--start_date", required=True, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end_date", required=True, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--search_excel", required=True, help="검색어 엑셀 파일 경로")

    args = parser.parse_args()
    main(args.site, args.start_date, args.end_date, args.search_excel)