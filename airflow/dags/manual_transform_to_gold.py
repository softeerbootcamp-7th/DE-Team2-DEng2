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
# S2 → Gold
# ==========================================================

SPARK_SUBMIT = "docker exec spark-master spark-submit"
JOBS_DIR = "/opt/spark/jobs"
PROJECT_DIR = "/opt/airflow/project"
UTILS_DIR = f"{PROJECT_DIR}/data_pipeline/utils"
DATA_DIR = f"{PROJECT_DIR}/data"


def send_slack_alert(title, message, status="info"):
    notifier = SlackNotifier(
        webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        stage="TRANSFORM-Gold",
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
    send_slack_alert(f"[Airflow] transform_to_gold / {task_id} 실패", str(context.get("exception")), status="error")


def slack_success_callback(context):
    send_slack_alert("[Airflow] transform_to_gold 완료", "Gold Transform (S2 → Gold) 모든 task 성공", status="success")



default_args = {
    "owner": "DE-Team2",
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": slack_failure_callback,
}

with DAG(
    dag_id="transform_to_gold",
    default_args=default_args,
    description="S2 → Gold (ownership_inference 크롤링 완료 후 실행)",
    schedule=None, 
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["manual", "transform", "gold"],
) as dag:

    # Phase 1: S1 + ownership_inference → S2
    s1_to_s2 = BashOperator(
        task_id="transform_s1_to_s2",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/transform_s1_to_s2.py",
    )

    # Phase 2: S2 → Gold
    s2_to_gold = BashOperator(
        task_id="transform_s2_to_gold",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/transform_s2_to_gold.py",
    )

    # S3 업로드 - S2
    upload_s2 = BashOperator(
        task_id="upload_s2_to_s3",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/silver/s2 "
            f"--prefix data/silver/s2 "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
    )

    # S3 업로드 - Gold
    upload_gold = BashOperator(
        task_id="upload_gold_to_s3",
        bash_command=(
            f"python {UTILS_DIR}/upload_data_to_s3.py "
            f"--local-dir {DATA_DIR}/gold/restaurant "
            f"--prefix data/gold/restaurant "
            f"--exclude _work --exclude .DS_Store --exclude _SUCCESS"
        ),
        on_success_callback=slack_success_callback,
    )

    s1_to_s2 >> upload_s2 >> s2_to_gold >> upload_gold
