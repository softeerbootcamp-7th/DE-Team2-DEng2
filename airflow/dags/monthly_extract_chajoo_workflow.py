from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
from dotenv import load_dotenv
load_dotenv("/opt/airflow/project/.env")

from utils.slack_utils import slack_failure_callback, slack_success_callback
from utils.file_validators import validate_parquet_existence

try:
    from data_pipeline.extract.extract_chajoo_dist import run_workflow
except ImportError as e:
    print(f"Import Error: {e}")

sys.path.append('/opt/airflow/project')

# ==========================================================
# 월배치 Extract: Crawling
# 차주인구분포 data (월 1회 갱신)
# ==========================================================

PROJECT_DIR = "/opt/airflow/project"
UTILS_DIR = f"{PROJECT_DIR}/data_pipeline/utils"
DATA_DIR = f"{PROJECT_DIR}/data"


default_args = {
    'owner': 'DE-Team2',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    "on_failure_callback": slack_failure_callback
}


with DAG(
    dag_id='chajoo_dist_monthly_extract',
    default_args=default_args,
    description='월배치: 자동차등록현황보고 데이터 수집',
    schedule="30 1 1 * *",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['monthly', 'chajoo', 'bronze'],
    on_success_callback=slack_success_callback,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_chajoo_dist',
        python_callable=run_workflow,
        provide_context=True
    )

    # 2. [신규] 수집 결과 데이터 검증
    validate_task_bronze = PythonOperator(
        task_id='validate_collected_data_bronze',
        python_callable=validate_parquet_existence,
        op_args=[f"{DATA_DIR}/bronze/chajoo_dist/parquet"],
        provide_context=True
    )

    # 2. [신규] 수집 결과 데이터 검증
    validate_task_gold = PythonOperator(
        task_id='validate_collected_data_gold',
        python_callable=validate_parquet_existence,
        op_args=[f"{DATA_DIR}/gold/chajoo_dist"],
        provide_context=True
    )


    # 3. S3 업로드
    upload_s3_bronze = BashOperator(
        task_id='upload_chajoo_dist_bronze_to_s3',
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/bronze/chajoo_dist "
            f"--prefix data/bronze/chajoo_dist "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
        trigger_rule='all_success'
    )

    upload_s3_gold = BashOperator(
        task_id='upload_chajoo_dist_gold_to_s3',
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/gold/chajoo_dist "
            f"--prefix data/gold/chajoo_dist "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
        trigger_rule='all_success'
    )

    extract_task >> validate_task_bronze >> validate_task_gold >> upload_s3_bronze >> upload_s3_gold
