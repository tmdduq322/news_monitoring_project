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

    crawl = BashOperator(
        task_id='crawl_all_sites',
        bash_command='export PYTHONUNBUFFERED=1; '
                     'PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/crawl_all_sites.py '
                     '--site "뽐뿌" '
                     '--start_date {{ ds }} '
                     '--end_date {{ ds }} '
                    '--search_excel /opt/airflow/config/search_keywords_2025.xlsx'
    )

    merge = BashOperator(
        task_id='merge_raw_csvs',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/merge_all_raw_csv.py '
                     '--date {{ ds_nodash[2:] }}'
    )

    process = BashOperator(
        task_id='process_data',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/process_data.py '
                     '--input_csv data/merged/merged_raw_{{ ds_nodash[2:] }}.csv '
                     '--output_excel data/processed/전처리_{{ ds_nodash[2:] }}.xlsx '
                     '--search_excel "/opt/airflow/config/search_keywords_2025.xlsx" '
                     '--year {{ execution_date.year }} '
                     '--month {{ execution_date.month }}'
    )

    extract = BashOperator(
        task_id='extract_original_articles',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/extract_original.py '
                     '--input_excel data/processed/전처리_{{ ds_nodash[2:] }}.xlsx '
                     f'--output_csv s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ ds_nodash[2:] }}}}.csv'
    )
    

    save_db = BashOperator(
        task_id='save_to_mysql',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/save_to_db.py '
                     f'--input_file s3://{BUCKET_NAME}/data/extracted/원문기사_{{{{ ds_nodash[2:] }}}}.csv '
                     '--table_name news_posts'
    )

    crawl >> merge >> process >> extract >> save_db
