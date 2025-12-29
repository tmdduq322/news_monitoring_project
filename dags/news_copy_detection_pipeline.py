from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup  # [필수] 이 줄이 없으면 에러납니다!
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# S3 버킷 설정 (없으면 기본값 사용)
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
    max_active_tasks=3,
) as dag:

    # 1. 사이트 그룹별 병렬 크롤링 (3개 조로 분산)
    with TaskGroup("crawl_tasks", tooltip="사이트 그룹별 병렬 크롤링") as crawl_group:
        
        # [1조] 대형 커뮤니티 (데이터 양 많음)
        group_1_sites = "뽐뿌,클리앙,인벤,루리웹,보배드림,에펨코리아,디시인사이드"
        crawl_1 = BashOperator(
            task_id='crawl_group_1_heavy',
            bash_command=f'export PYTHONUNBUFFERED=1; '
                         f'PYTHONPATH=/opt/airflow '
                         f'python3 /opt/airflow/scripts/crawl_all_sites.py '
                         f'--site "{group_1_sites}" '
                         f'--start_date {{{{ ds }}}} '
                         f'--end_date {{{{ ds }}}} '
                         f'--search_excel /opt/airflow/config/search_keywords_2025.xlsx',
            execution_timeout=timedelta(hours=6)
        )

        # [2조] 중형 및 뉴스/연예 집중
        group_2_sites = "더쿠,엠엘비파크,인스티즈,네이트판,아카라이브,일간베스트,오늘의유머"
        crawl_2 = BashOperator(
            task_id='crawl_group_2_medium',
            bash_command=f'export PYTHONUNBUFFERED=1; '
                         f'PYTHONPATH=/opt/airflow '
                         f'python3 /opt/airflow/scripts/crawl_all_sites.py '
                         f'--site "{group_2_sites}" '
                         f'--start_date {{{{ ds }}}} '
                         f'--end_date {{{{ ds }}}} '
                         f'--search_excel /opt/airflow/config/search_keywords_2025.xlsx',
            execution_timeout=timedelta(hours=5)
        )

        # [3조] 기타 사이트
        group_3_sites = "웃긴대학,82쿡,오르비,개드립,DVD프라임,동사로마닷컴,사커라인,포모스,짱공유닷컴,블라인드"
        crawl_3 = BashOperator(
            task_id='crawl_group_3_light',
            bash_command=f'export PYTHONUNBUFFERED=1; '
                         f'PYTHONPATH=/opt/airflow '
                         f'python3 /opt/airflow/scripts/crawl_all_sites.py '
                         f'--site "{group_3_sites}" '
                         f'--start_date {{{{ ds }}}} '
                         f'--end_date {{{{ ds }}}} '
                         f'--search_excel /opt/airflow/config/search_keywords_2025.xlsx',
            execution_timeout=timedelta(hours=4)
        )

    # 2. 병합 (로컬)
    # 날짜 포맷: YYMMDD (예: 251229)
    merge = BashOperator(
        task_id='merge_raw_csvs',
        bash_command=f'export PYTHONUNBUFFERED=1; '
                     f'PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/merge_all_raw_csv.py '
                     f"--date {{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}"
    )

    # 3. 전처리 (로컬)
    process = BashOperator(
        task_id='process_data',
        bash_command=f'export PYTHONUNBUFFERED=1; '
                     f'PYTHONPATH=/opt/airflow '
                     f'python3 /opt/airflow/scripts/process_data.py '
                     f"--input_csv data/merged/merged_raw_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.csv "
                     f"--output_excel data/processed/전처리_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.xlsx "
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
                             f"--input_excel data/processed/전처리_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.xlsx "
                             f"--output_csv s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.csv "
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
                     f"--input_file s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%y%m%d') }}}}.csv "
                     f'--table_name news_posts',
        trigger_rule='all_success'
    )

    # 작업 순서 연결
    crawl_group >> merge >> process >> extract_group >> save_db