import os
import sys
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

sys.path.append('/opt/airflow/project')

try:
    from data_pipeline.utils.slack_utils import SlackNotifier
    from data_pipeline.extract.extract_tojiSoyuJeongbo import run_workflow
except ImportError as e:
    print(f"Import Error: {e}")

def send_slack_alert(title, message, status='info'):
    notifier = SlackNotifier(
        webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        stage="EXTRACT-토지소유정보",
        logger=None # 콜백에서는 별도 로거 없어도 동작하도록 설계됨
    )
    if status == 'error':
        notifier.error(title, Exception(message))
    elif status == 'success':
        notifier.success(title, message)
    else:
        notifier.info(title, message)

# Airflow 콜백용 함수들
def on_cookie_sensor_retry(context):
    send_slack_alert("쿠키 대기 중", "아직 쿠키 파일이 감지되지 않았습니다. 로그인 후 save_cookie.py를 실행해주세요.")

def on_cookie_sensor_failure(context):
    send_slack_alert("작업 실패", "1시간 동안 쿠키가 입력되지 않아 작업을 종료합니다.", status='error')

def on_cookie_sensor_execute(context):
    send_slack_alert("작업 시작", "토지소유정보 수집을 위해 쿠키 갱신이 필요합니다. (1시간 대기 시작)")

default_args = {
    'owner': 'PHW',
    'start_date': datetime(2026, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

with DAG(
    dag_id='toji_soyu_monthly_extract',
    max_active_runs=1,
    default_args=default_args,
    schedule='0 12 1 * *',
    catchup=False,
    tags=['monthly', 'vworld']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_vworld_toji',
        python_callable=run_workflow,
        provide_context=True
    )
