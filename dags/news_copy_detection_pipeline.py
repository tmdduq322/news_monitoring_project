from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "news-monitoring-bucket")

with DAG(
    dag_id='news_copy_detection_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 11, 21),
    schedule='@daily',
    catchup=False,
    tags=['news', 'copy-detection'],
) as dag:

    # 1. 크롤링: 시작일과 종료일을 모두 '어제'로 설정
    crawl = BashOperator(
        task_id='crawl_all_sites',
        bash_command='export PYTHONUNBUFFERED=1; '
                     'PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/crawl_all_sites.py '
                     '--site "all" ' # 테스트 끝나면 "all"로 변경하세요
                     '--start_date {{ macros.ds_add(ds, -1) }} '
                     '--end_date {{ macros.ds_add(ds, -1) }} '
                     '--search_excel /opt/airflow/config/search_keywords_2025.xlsx'
    )

    # 2. 병합: 파일명에 들어갈 날짜도 '어제' (YYMMDD 형식)
    merge = BashOperator(
        task_id='merge_raw_csvs',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/merge_all_raw_csv.py '
                     "--date {{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}"
    )

    # 3. 전처리: 입력/출력 파일명 모두 '어제' 날짜 사용
    process = BashOperator(
        task_id='process_data',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/process_data.py '
                     "--input_csv data/merged/merged_raw_{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}.csv "
                     "--output_excel data/processed/전처리_{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}.xlsx "
                     '--search_excel "/opt/airflow/config/search_keywords_2025.xlsx" '
                     # 년/월 정보도 어제 날짜 기준으로 추출
                     "--year {{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y') }} "
                     "--month {{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%m') }}"
    )

    # 4. 원문 추출
    extract = BashOperator(
        task_id='extract_original_articles',
        bash_command='export PYTHONUNBUFFERED=1; '
                     'PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/extract_original.py '
                     "--input_excel data/processed/전처리_{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}.xlsx "
                     f"--output_csv s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}}}.csv"
    )
    
    # 5. DB 저장
    save_db = BashOperator(
        task_id='save_to_mysql',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/save_to_db.py '
                     f"--input_file s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%y%m%d') }}}}.csv "
                     '--table_name news_posts'
    )

    crawl >> merge >> process >> extract >> save_db