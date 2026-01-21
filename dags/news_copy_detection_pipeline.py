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

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "news-monitoring-bucket")
EXTRACT_WORKER_COUNT = 2

with DAG(
    dag_id='news_copy_detection_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 11, 21),
    schedule='15 0 * * *',  # 매일 00:15 실행
    catchup=False,
    tags=['news', 'copy-detection'],
    max_active_tasks=2,
) as dag:
    
    # 1. 크롤링 (ds 그대로 사용 = 어제 날짜 수집)
    with TaskGroup("crawl_tasks", tooltip="사이트 그룹별 병렬 크롤링") as crawl_group:
        group_1_sites = "클리앙,보배드림,디시인사이드,동사로마닷컴,사커라인"
        crawl_1 = BashOperator(
            task_id='crawl_group_1',
            bash_command=f'export PYTHONUNBUFFERED=1; PYTHONPATH=/opt/airflow '
                         f'python3 /opt/airflow/scripts/crawl_all_sites.py '
                         f'--site "{group_1_sites}" '
                         f'--start_date {{{{ ds }}}} '  # [수정] -1 제거
                         f'--end_date {{{{ ds }}}} '    # [수정] -1 제거
                         f'--search_excel /opt/airflow/config/search_keywords_2025.xlsx',
            execution_timeout=timedelta(hours=6)
        )

        group_2_sites = "루리웹,에펨코리아,인벤,엠엘비파크,뽐뿌,아카라이브,오늘의유머,네이트판,82쿡,개드립,DVD프라임,일간베스트,블라인드,더쿠,인스티즈,웃긴대학,오르비"
        crawl_2 = BashOperator(
            task_id='crawl_group_2',
            bash_command=f'export PYTHONUNBUFFERED=1; PYTHONPATH=/opt/airflow '
                         f'python3 /opt/airflow/scripts/crawl_all_sites.py '
                         f'--site "{group_2_sites}" '
                         f'--start_date {{{{ ds }}}} '  # [수정] -1 제거
                         f'--end_date {{{{ ds }}}} '    # [수정] -1 제거
                         f'--search_excel /opt/airflow/config/search_keywords_2025.xlsx',
            execution_timeout=timedelta(hours=6)
        )
        
    # 2. 병합
    merge = BashOperator(
        task_id='merge_raw_csvs',
        bash_command=f'export PYTHONUNBUFFERED=1; PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/merge_all_raw_csv.py '
                     f"--date {{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}" # [수정] -1 제거
    )
    
    # 3. 전처리
    process = BashOperator(
        task_id='process_data',
        bash_command=f'export PYTHONUNBUFFERED=1; PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/process_data.py '
                     f"--input_csv data/merged/merged_raw_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.csv " # [수정]
                     f"--output_excel data/processed/전처리_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.xlsx " # [수정]
                     f'--search_excel "/opt/airflow/config/search_keywords_2025.xlsx" '
                     f"--year {{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}}} "
                     f"--month {{{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}}}"
    )

    # 4. 원문 추출
    with TaskGroup("extract_tasks", tooltip="원문 기사 병렬 추출") as extract_group:
        extract_tasks = []
        for i in range(EXTRACT_WORKER_COUNT):
            task = BashOperator(
                task_id=f'extract_part_{i}',
                bash_command=f'export PYTHONUNBUFFERED=1; PYTHONPATH=/opt/airflow '
                             f'python3 /opt/airflow/scripts/extract_original.py '
                             f"--input_excel data/processed/전처리_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.xlsx " # [수정]
                             f"--output_csv s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.csv " # [수정]
                             f"--worker_id {i} "
                             f"--total_workers {EXTRACT_WORKER_COUNT}"
            )
            extract_tasks.append(task)

    # 5. DB 저장
    save_db = BashOperator(
        task_id='save_to_mysql',
        bash_command=f'export PYTHONUNBUFFERED=1; PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/save_to_db.py '
                     f"--input_file s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.csv " # [수정]
                     f'--table_name news_posts',
        trigger_rule='all_success'
    )
    
    # 6. [삭제됨] Notion Upload 태스크는 Gemini 태스크로 흡수되었으므로 삭제

    # 7. 제미나이 요약 + 노션 리포트 생성 (통합)
    gemini_summarize = BashOperator(
        task_id='gemini_summarize',
        bash_command=f'export PYTHONUNBUFFERED=1; PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/gemini_summary.py '
                     f'--date "{{{{ ds }}}}"', # [유지] ds 사용
        trigger_rule='all_success'
    )
    
    # 8. 인스턴스 종료
    instance_stop = BashOperator(
        task_id='instance_stop_now',
        bash_command='sudo shutdown -h now', 
        trigger_rule='all_done' 
    )

    crawl_group >> merge >> process >> extract_group >> save_db >> gemini_summarize >> instance_stop