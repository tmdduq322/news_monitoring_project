# extract_original.py
import os
import re
import time
import pandas as pd
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv

from core_utils import (
    create_driver, kill_driver, clean_text,
    extract_first_sentences, generate_search_queries,
    calculate_copy_ratio, log, search_news_with_api
)

def find_original_article(index, row_dict, total_count):
    load_dotenv(dotenv_path="/opt/airflow/.env")  # Airflow 내부에서 위치 조정

    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")

    driver = create_driver(index)
    if index == 0:
        time.sleep(10)

    if driver is None:
        return index, "", 0.0

    try:
        title = clean_text(str(row_dict["게시물 제목"]))
        content = clean_text(str(row_dict["게시물 내용"]))
        press = clean_text(str(row_dict["검색어"]))

        first, second, last = extract_first_sentences(content)
        queries = generate_search_queries(title, first, second, last, press)

        search_results = search_news_with_api(queries, driver, client_id, client_secret, index=index)
        if not search_results:
            return index, "", 0.0

        best = max(search_results, key=lambda x: calculate_copy_ratio(x["body"], title + " " + content))
        score = calculate_copy_ratio(best["body"], title + " " + content)

        if score > 0.0:
            hyperlink = f'=HYPERLINK("{best["link"]}")'
            return index, hyperlink, score
        else:
            return index, "", 0.0

    except Exception as e:
        return index, "", 0.0
    finally:
        kill_driver(driver, index)


def run_article_matching(input_excel, output_csv):
    df = pd.read_excel(input_excel)
    df["원본기사"] = ""
    df["복사율"] = 0.0

    tasks = [(i, row.to_dict(), len(df)) for i, row in df.iterrows()]

    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(find_original_article, *args) for args in tasks]
        for future in as_completed(futures):
            try:
                index, link, score = future.result()
                df.at[index, "원본기사"] = link
                df.at[index, "복사율"] = score
            except Exception as e:
                print(f"❌ 결과 처리 오류: {e}")

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"✅ 원문기사 결과 저장 완료: {output_csv}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_excel", required=True)
    parser.add_argument("--output_csv", required=True)
    args = parser.parse_args()

    run_article_matching(args.input_excel, args.output_csv)
