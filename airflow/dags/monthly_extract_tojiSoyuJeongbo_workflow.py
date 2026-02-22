import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv("/opt/airflow/project/.env")

from utils.slack_utils import slack_failure_callback, slack_success_callback
from utils.file_validators import validate_parquet_existence

try:
    from data_pipeline.extract.extract_tojiSoyuJeongbo import run_workflow
except ImportError as e:
    print(f"Import Error: {e}")

sys.path.append("/opt/airflow/project")

# ==========================================================
# 월배치 Extract: Crawling
# 토지소유정보 data (월 1회 갱신)
# ==========================================================

PROJECT_DIR = "/opt/airflow/project"
UTILS_DIR = f"{PROJECT_DIR}/data_pipeline/utils"
DATA_DIR = f"{PROJECT_DIR}/data"


default_args = {
    'owner': 'DE-Team2',
    'start_date': datetime(2026, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    "on_failure_callback": slack_failure_callback
}

with DAG(
    dag_id='toji_soyu_monthly_extract',
    default_args=default_args,
    max_active_runs=1,
    schedule="0 2 1 * *",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['monthly', 'tojisoyu', 'bronze'],
    on_success_callback=slack_success_callback,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_tojisoyu',
        python_callable=run_workflow,
        provide_context=True
    )

    # 수집 결과 데이터 검증
    validate_task = PythonOperator(
        task_id='validate_collected_data',
        python_callable=validate_parquet_existence,
        op_args=[f"{DATA_DIR}/bronze/tojiSoyuJeongbo/parquet"],
        provide_context=True
    )

    # 3. S3 업로드
    upload_s3 = BashOperator(
        task_id='upload_chajoo_dist_to_s3',
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/bronze/tojiSoyuJeongbo "
            f"--prefix data/bronze/tojiSoyuJeongbo "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
        trigger_rule='all_success'
    )

    extract_task >> validate_task >> upload_s3
