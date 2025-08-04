from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='news_copy_detection_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule='@daily',
    catchup=True,
    tags=['news', 'copy-detection'],
) as dag:

    crawl = BashOperator(
        task_id='crawl_all_sites',
        bash_command='PYTHONPATH=/opt/airflow '
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
                     '--input_csv data/merged/merged_raw_{{ ds_nodash }}.csv '
                     '--output_excel data/processed/전처리_{{ ds_nodash }}.xlsx '
                     '--search_excel "/opt/airflow/config/search_keywords_2025.xlsx" '
                     '--year {{ execution_date.year }} '
                     '--month {{ execution_date.month }}'
    )

    extract = BashOperator(
        task_id='extract_original_articles',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/extract_original.py '
                     '--input_excel data/processed/전처리_{{ ds_nodash }}.xlsx '
                     '--output_csv data/extracted/원문기사_{{ ds_nodash }}.csv'
    )

    save_db = BashOperator(
        task_id='save_to_mysql',
        bash_command='PYTHONPATH=/opt/airflow '
                     'python3 /opt/airflow/scripts/save_to_db.py '
                     '--input_excel data/extracted/원문기사_{{ ds_nodash }}.csv '
                     '--table_name news_posts'
    )

    crawl >> merge >> process >> extract >> save_db
