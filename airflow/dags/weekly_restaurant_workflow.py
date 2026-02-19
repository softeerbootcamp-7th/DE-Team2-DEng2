from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# 1. 프로젝트 경로 추가
sys.path.append('/opt/airflow/project')

# 2. 임포트 (함수명을 run_extract로 통일했다고 가정합니다)
try:
    from data_pipeline.extract.extract_restaurant_owner import run_workflow
except ImportError as e:
    print(f"Import Error: {e}")

default_args = {
    'owner': 'PHW',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='restaurant_weekly_extract',
    default_args=default_args,
    description='매주 음식점 대표자 수집 및 분석 파이프라인',
    schedule_interval='0 9 * * 1', # 매주 월요일 오전 9시
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['weekly', 'restaurant'],
    params={
        "sido": "경기도",
        "addr": "",
        "start_page": 1,
        "end_page": 2,
        "auto_resume": True
    },
) as dag:

    # 1. 수집 단계
    extract_task = PythonOperator(
        task_id='extract_restaurant_owner',
        python_callable=run_workflow,
        # 핵심: 이 설정을 통해 함수 내부에서 **kwargs로 conf에 접근 가능해집니다.
        provide_context=True, 
    )

    extract_task