# extraction/main_script.py

import os
import time
from dotenv import load_dotenv
from extraction.core_utils import (
    create_driver, kill_driver, clean_text,
    extract_first_sentences, generate_search_queries,
    calculate_copy_ratio, log, search_news_with_api
)

# [설정] Airflow 환경에 맞는 절대 경로로 .env 로드
load_dotenv(dotenv_path="/opt/airflow/.env")

def find_original_article_multiprocess(index, row_dict, total_count):
    """
    extract_original.py의 ProcessPoolExecutor에서 호출되는 작업 함수
    """
    
    # 1. API 키 로드 (프로세스별 별도 로드)
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    
    # 키가 없는 경우 조기 종료
    if not client_id or not client_secret:
        log("❌ NAVER API Key가 .env에 설정되지 않았습니다.", index)
        return index, "", 0.0

    # 2. 드라이버 생성 (core_utils.create_driver가 환경변수 경로를 감지함)
    driver = create_driver(index)
    
    # 첫 번째 워커는 브라우저 초기화 충돌 방지를 위해 살짝 대기
    if index == 0:
        time.sleep(2)

    if driver is None:
        return index, "", 0.0

    try:
        # 3. 데이터 전처리
        title = clean_text(str(row_dict.get("게시물 제목", "")))
        content = clean_text(str(row_dict.get("게시물 내용", "")))
        press = clean_text(str(row_dict.get("검색어", ""))) 

        log(f"▶️ [작업 시작] 게시물: {title[:15]}... (언론사: {press})", index)

        if not title and not content:
            return index, "", 0.0

        # 4. 검색 쿼리 생성 (로그 출력을 위해 index 전달)
        first, second, last = extract_first_sentences(content)
        queries = generate_search_queries(title, first, second, last, press, index=index)

        # 5. 네이버 뉴스 API 검색
        search_results = search_news_with_api(
            queries, driver, client_id, client_secret, 
            max_results=10, 
            index=index
        )

        if not search_results:
            return index, "", 0.0

        # 6. 유사도 비교 (복사율 계산)
        target_text = title + " " + content
        
        # 검색 결과 중 가장 유사도가 높은 기사 찾기
        best = max(search_results, key=lambda x: calculate_copy_ratio(x["body"], target_text))
        score = calculate_copy_ratio(best["body"], target_text)

        # 7. 결과 반환
        if score > 0.0:
            hyperlink = f'=HYPERLINK("{best["link"]}")'
            log(f"🎉 매칭 성공! 점수: {score}", index)
            return index, hyperlink, score
        else:
            return index, "", 0.0

    except Exception as e:
        log(f"❌ Worker Logic Error: {e}", index)
        return index, "", 0.0

    finally:
        # 8. 자원 정리 (드라이버 종료)
        kill_driver(driver, index)