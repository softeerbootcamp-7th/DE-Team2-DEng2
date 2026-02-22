import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv("/opt/airflow/project/.env")

sys.path.append("/opt/airflow/project")

try:
    from data_pipeline.utils.slack_utils import SlackNotifier
except ImportError as e:
    print(f"Import Error: {e}")

from utils.sensor_helpers import check_parquet_exists, check_success_marker

# ==========================================================
# 주배치 Transform: 식당 Clean → S1 (크롤링 리스트 생성)
# 식당 대표자 데이터 (주 1회 갱신)
# ==========================================================

SPARK_SUBMIT = "docker exec spark-master spark-submit"
JOBS_DIR = "/opt/spark/jobs"
PROJECT_DIR = "/opt/airflow/project"
UTILS_DIR = f"{PROJECT_DIR}/data_pipeline/utils"
DATA_DIR = f"{PROJECT_DIR}/data"


def send_slack_alert(title, message, status="info"):
    notifier = SlackNotifier(
        webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        stage="TRANSFORM-Weekly",
        logger=None,
    )
    if status == "error":
        notifier.error(title, Exception(message))
    elif status == "success":
        notifier.success(title, message)
    else:
        notifier.info(title, message)


def slack_failure_callback(context):
    task_id = context["task_instance"].task_id
    send_slack_alert(f"[Airflow] transform_weekly / {task_id} 실패", str(context.get("exception")), status="error")


def slack_success_callback(context):
    send_slack_alert(
        "[Airflow] transform_weekly 완료",
        "주배치 Transform (Clean → S1) 모든 task 성공\n다음 단계: crawling_list 기반 크롤링 실행",
        status="success",
    )



default_args = {
    "owner": "DE-Team2",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": slack_failure_callback,
}

with DAG(
    dag_id="transform_weekly",
    default_args=default_args,
    description="주배치: 식당 Clean → S1 (크롤링 리스트 생성)",
    schedule="0 12 * * 2",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["weekly", "transform", "silver"],
) as dag:

    # Sensors: 입력 데이터 존재 확인
    sense_bronze_restaurant = PythonSensor(
        task_id="sense_bronze_restaurant",
        python_callable=check_parquet_exists,
        op_kwargs={"base_path": "bronze/restaurant_owner/parquet", "partition_type": "year_month_week"},
        poke_interval=600,
        timeout=86400,
        mode="reschedule",
    )

    sense_s0_address = PythonSensor(
        task_id="sense_s0_address",
        python_callable=check_success_marker,
        op_kwargs={"base_path": "silver/s0/address", "partition_type": "year_month"},
        poke_interval=600,
        timeout=86400,
        mode="reschedule",
    )

    sense_s0_toji_building = PythonSensor(
        task_id="sense_s0_toji_building",
        python_callable=check_success_marker,
        op_kwargs={"base_path": "silver/s0/toji_building", "partition_type": "year_month"},
        poke_interval=600,
        timeout=86400,
        mode="reschedule",
    )

    # Phase 1: Bronze → Silver Clean (식당)
    clean_restaurant = BashOperator(
        task_id="clean_restaurant",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/clean_restaurant.py",
    )

    # Phase 2: Silver Clean + S0 → Silver S1
    chk_s0_to_s1 = BashOperator(
        task_id="transform_chk_s0_to_s1",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/transform_chk_s0_to_s1.py",
    )

    s0_to_s1 = BashOperator(
        task_id="transform_s0_to_s1",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/transform_s0_to_s1.py",
    )

    # S3 업로드 - Clean
    upload_clean_restaurant = BashOperator(
        task_id="upload_clean_restaurant_to_datalake",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/clean/restaurant "
            f"--prefix data/silver/clean/restaurant "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
    )

    # S3 업로드 - S1
    upload_s1 = BashOperator(
        task_id="upload_s1_to_datalake",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/s1 "
            f"--prefix data/silver/s1 "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
        on_success_callback=slack_success_callback,
    )

    sense_bronze_restaurant >> clean_restaurant >> upload_clean_restaurant >> chk_s0_to_s1
    sense_s0_address >> chk_s0_to_s1
    chk_s0_to_s1 >> s0_to_s1
    sense_s0_toji_building >> s0_to_s1
    s0_to_s1 >> upload_s1
