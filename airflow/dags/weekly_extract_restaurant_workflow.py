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
    from data_pipeline.extract.extract_restaurant_owner import run_workflow
except ImportError as e:
    print(f"Import Error: {e}")

sys.path.append('/opt/airflow/project')

# ==========================================================
# 주배치 Extract: Crawling
# 음식점대표자 data (월 1회 갱신)
# ==========================================================

PROJECT_DIR = "/opt/airflow/project"
UTILS_DIR = f"{PROJECT_DIR}/data_pipeline/utils"
DATA_DIR = f"{PROJECT_DIR}/data"

default_args = {
    'owner': 'DE-Team2',
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
    "on_failure_callback": slack_failure_callback
}

with DAG(
    dag_id='restaurant_weekly_extract',
    default_args=default_args,
    description='주배치: 음식점 대표자 데이터 수집',
    schedule=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['weekly', 'restaurant', 'bronze'],
    params={
        "sido": "경기도",
        "addr": "",
        "start_page": 1,
        "end_page": 2,
        "auto_resume": True
    },
    on_success_callback=slack_success_callback,
) as dag:

    # 1. 수집 단계
    extract_task = PythonOperator(
        task_id='extract_restaurant_owner',
        python_callable=run_workflow,
        # 핵심: 이 설정을 통해 함수 내부에서 **kwargs로 conf에 접근 가능해집니다.
        provide_context=True, 
    )

    # 2. [신규] 수집 결과 데이터 검증
    validate_task = PythonOperator(
        task_id='validate_collected_data',
        python_callable=validate_parquet_existence,
        op_args=[f"{DATA_DIR}/bronze/restaurant/parquet"],
        provide_context=True
    )

    # 3. S3 업로드
    upload_s3 = BashOperator(
        task_id='upload_restaurant_to_s3',
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/bronze/restaurant "
            f"--prefix data/bronze/restaurant "
            f"--exclude_work --exclude .DS_Store --exclude _SUCCESS"
        ),
        trigger_rule='all_success'
    )


    extract_task >> validate_task >> upload_s3
