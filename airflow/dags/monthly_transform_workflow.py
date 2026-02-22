import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv("/opt/airflow/project/.env")

sys.path.append("/opt/airflow/project")

try:
    from data_pipeline.utils.slack_utils import SlackNotifier
except ImportError as e:
    print(f"Import Error: {e}")

# ==========================================================
# 월배치 Transform: Bronze → Silver Clean → Silver S0
# 주소/좌표/건축물/토지 데이터 (월 1회 갱신)
# ==========================================================

SPARK_SUBMIT = "docker exec spark-master spark-submit"
JOBS_DIR = "/opt/spark/jobs"
PROJECT_DIR = "/opt/airflow/project"
UTILS_DIR = f"{PROJECT_DIR}/data_pipeline/utils"
DATA_DIR = f"{PROJECT_DIR}/data"


def send_slack_alert(title, message, status="info"):
    notifier = SlackNotifier(
        webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        stage="TRANSFORM-Monthly",
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
    send_slack_alert(f"[Airflow] transform_monthly / {task_id} 실패", str(context.get("exception")), status="error")


def slack_success_callback(context):
    send_slack_alert("[Airflow] transform_monthly 완료", "월배치 Transform (Bronze → S0) 모든 task 성공", status="success")



default_args = {
    "owner": "DE-Team2",
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": slack_failure_callback,
}

with DAG(
    dag_id="transform_monthly",
    default_args=default_args,
    description="월배치: Bronze → Clean → S0 (주소/좌표/건축물/토지)",
    schedule=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["monthly", "transform", "silver"],
) as dag:

    # Phase 1: Bronze → Silver Clean
    clean_address = BashOperator(
        task_id="clean_address",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/clean_address.py",
    )

    clean_coord = BashOperator(
        task_id="clean_coord",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/clean_coord.py",
    )

    clean_building = BashOperator(
        task_id="clean_building",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/clean_building.py",
    )

    clean_toji = BashOperator(
        task_id="clean_toji",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/clean_toji.py",
    )

    # Phase 2: Silver Clean → Silver S0
    s0_address = BashOperator(
        task_id="transform_clean_to_s0_address",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/transform_clean_to_s0_address.py",
    )

    s0_toji_building = BashOperator(
        task_id="transform_clean_to_s0_toji_building",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/transform_clean_to_s0_toji_building.py",
    )

    # S3 업로드 - Clean
    upload_clean_address = BashOperator(
        task_id="upload_clean_address_to_s3",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/clean/address "
            f"--prefix data/silver/clean/address "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
    )

    upload_clean_coord = BashOperator(
        task_id="upload_clean_coord_to_s3",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/clean/coord "
            f"--prefix data/silver/clean/coord "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
    )

    upload_clean_building = BashOperator(
        task_id="upload_clean_building_to_s3",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/clean/building "
            f"--prefix data/silver/clean/building "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
    )

    upload_clean_toji = BashOperator(
        task_id="upload_clean_toji_to_s3",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/clean/toji "
            f"--prefix data/silver/clean/toji "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
    )

    # S3 업로드 - S0
    upload_s0_address = BashOperator(
        task_id="upload_s0_address_to_s3",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/s0/address "
            f"--prefix data/silver/s0/address "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
    )

    upload_s0_toji_building = BashOperator(
        task_id="upload_s0_toji_building_to_s3",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/s0/toji_building "
            f"--prefix data/silver/s0/toji_building "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
        on_success_callback=slack_success_callback,
    )

    clean_address >> upload_clean_address >> s0_address
    clean_coord >> upload_clean_coord >> s0_address
    clean_building >> upload_clean_building >> s0_toji_building
    clean_toji >> upload_clean_toji >> s0_toji_building
    s0_address >> upload_s0_address
    s0_toji_building >> upload_s0_toji_building
