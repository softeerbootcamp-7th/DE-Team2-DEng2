# dags/utils/slack_handlers.py
from dotenv import load_dotenv
load_dotenv("/opt/airflow/project/.env")
import sys
sys.path.append("/opt/airflow/project")
import os
from data_pipeline.utils.slack_utils import SlackNotifier

def send_slack_alert(title, message, stage, status="info"):
    notifier = SlackNotifier(
        webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        stage=stage,
        logger=None,
    )
    if status == "error":
        notifier.error(title, Exception(message))
    elif status == "success":
        notifier.success(title, message)
    else:
        notifier.info(title, message)

def slack_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context["task_instance"].task_id
    error_msg = str(context.get("exception"))

    send_slack_alert(
        title=f"[Airflow Failure] {dag_id} / {task_id}",
        message=error_msg,
        stage=dag_id,
        status="error"
    )

def slack_success_callback(context):
    dag_id = context['dag'].dag_id
    send_slack_alert(
        title=f"[Airflow Success] {dag_id}",
        message="모든 task가 성공적으로 완료되었습니다.",
        stage=dag_id,
        status="success"
    )