from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# S3 버킷 설정
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "news-monitoring-bucket")
# 원문 추출 워커 수 
EXTRACT_WORKER_COUNT = 2

with DAG(
    dag_id='news_copy_detection_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 11, 21),
    schedule='@daily',
    catchup=False,
    tags=['news', 'copy-detection'],
    # 전체 DAG 수준에서도 동시에 돌아가는 태스크 수 제한 (안전을 위해 2로 설정)
    max_active_tasks=2,
) as dag:
    # 사이트 디버깅 코드
    # docker compose run --rm airflow-scheduler bash -c "rm -rf ~/.wdm && python3 /opt/airflow/scripts/crawl_all_sites.py --site '뽐뿌' --start_date 2026-01-17 --end_date 2026-01-17 --search_excel /opt/airflow/config/search_keywords_2025.xlsx"
    
    # 1. 사이트 그룹별 병렬 크롤링 (2개 조로 분산 - 로드 밸런싱 적용)
    with TaskGroup("crawl_tasks", tooltip="사이트 그룹별 병렬 크롤링") as crawl_group:
        
        # [1조] 디시, 뽐뿌, 클리앙 포함 (Heavy 절반 + Light 절반)
        # 리스트: 디시인사이드, 뽐뿌, 클리앙, 보배드림, 더쿠, 인스티즈, 네이트판, 웃긴대학, 오르비, DVD프라임
        group_1_sites = "클리앙,보배드림,디시인사이드,동사로마닷컴,사커라인"
        
        crawl_1 = BashOperator(
            task_id='crawl_group_1',
            bash_command=f'export PYTHONUNBUFFERED=1; '
                         f'PYTHONPATH=/opt/airflow '
                         f'python3 /opt/airflow/scripts/crawl_all_sites.py '
                         f'--site "{group_1_sites}" '
                         f'--start_date {{{{ macros.ds_add(ds, -1) }}}} '
                         f'--end_date {{{{ macros.ds_add(ds, -1) }}}} '
                         f'--search_excel /opt/airflow/config/search_keywords_2025.xlsx',
            # 사이트 수가 늘어났으므로 타임아웃을 넉넉하게 6시간으로 잡음
            execution_timeout=timedelta(hours=6)
        )

        # [2조] 펨코, 루리웹, 인벤 포함 (Heavy 절반 + Light 절반)
        # 리스트: 에펨코리아, 루리웹, 인벤, 엠엘비파크, 아카라이브, 일간베스트, 오늘의유머, 82쿡, 개드립, 동사로마닷컴, 사커라인, 포모스, 짱공유닷컴, 블라인드
        # 짱공유닷컴 폐쇄, 포모스 일단 제외
        group_2_sites = "에펨코리아,루리웹,인벤,엠엘비파크,뽐뿌,아카라이브,오늘의유머,네이트판,82쿡,개드립,DVD프라임,일간베스트,블라인드,더쿠,인스티즈,웃긴대학,오르비,"
        
        crawl_2 = BashOperator(
            task_id='crawl_group_2',
            bash_command=f'export PYTHONUNBUFFERED=1; '
                         f'PYTHONPATH=/opt/airflow '
                         f'python3 /opt/airflow/scripts/crawl_all_sites.py '
                         f'--site "{group_2_sites}" '
                         f'--start_date {{{{ macros.ds_add(ds, -1) }}}} '
                         f'--end_date {{{{ macros.ds_add(ds, -1) }}}} '
                         f'--search_excel /opt/airflow/config/search_keywords_2025.xlsx',
            execution_timeout=timedelta(hours=6)
        )
        
    # 2. 병합 (로컬)
    merge = BashOperator(
        task_id='merge_raw_csvs',
        bash_command=f'export PYTHONUNBUFFERED=1; '
                     f'PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/merge_all_raw_csv.py '
                     f"--date {{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}}}"
    )
    

    # 3. 전처리 (로컬)
    process = BashOperator(
        task_id='process_data',
        bash_command=f'export PYTHONUNBUFFERED=1; '
                     f'PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/process_data.py '
                     f"--input_csv data/merged/merged_raw_{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}}}.csv "
                     f"--output_excel data/processed/전처리_{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}}}.xlsx "
                     f'--search_excel "/opt/airflow/config/search_keywords_2025.xlsx" '
                     f"--year {{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}}} "
                     f"--month {{{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}}}"
    )

    # 4. 원문 추출 (결과는 S3로 전송)
    with TaskGroup("extract_tasks", tooltip="원문 기사 병렬 추출") as extract_group:
        extract_tasks = []
        for i in range(EXTRACT_WORKER_COUNT):
            task = BashOperator(
                task_id=f'extract_part_{i}',
                bash_command=f'export PYTHONUNBUFFERED=1; '
                             f'PYTHONPATH=/opt/airflow '
                             f'python3 /opt/airflow/scripts/extract_original.py '
                             f"--input_excel data/processed/전처리_{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}}}.xlsx "
                             f"--output_csv s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}}}.csv "
                             f"--worker_id {i} "
                             f"--total_workers {EXTRACT_WORKER_COUNT}"
            )
            extract_tasks.append(task)

    # 5. DB 저장 (S3에서 읽어서 저장)
    save_db = BashOperator(
        task_id='save_to_mysql',
        bash_command=f'export PYTHONUNBUFFERED=1; '
                     f'PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/save_to_db.py '
                     f"--input_file s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}}}.csv "
                     f'--table_name news_posts',
        trigger_rule='all_success'
    )
    
    # 6. 노션 업로드 
    notion_upload = BashOperator(
        task_id='upload_to_notion',
        bash_command=f'export PYTHONPATH=/opt/airflow; '
                    f'python3 /opt/airflow/scripts/upload_to_notion.py '
                    f'{{{{ ds }}}}', # 수집 날짜와 동일하게 어제 날짜 전달
    )

    # 7. 제미나이 요약 (XCom에서 ID를 받아와 실행)
    gemini_summarize = BashOperator(
        task_id='gemini_summarize',
        bash_command=f'export PYTHONUNBUFFERED=1; '
                     f'export PYTHONPATH=/opt/airflow; '
                     f'python3 /opt/airflow/scripts/gemini_summary.py '
                     f'--date "{{{{ ds }}}}"', 
        trigger_rule='all_success'
    )

    # 작업 순서 연결
    crawl_group >> merge >> process >> extract_group >> save_db >> notion_upload >> gemini_summarize