from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# 프로젝트 경로 추가
sys.path.append('/opt/airflow/project')

try:
    from data_pipeline.extract.extract_buildingLeader import run_workflow
except ImportError as e:
    print(f"Import Error: {e}")

default_args = {
    'owner': 'PHW',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='building_leader_monthly_extract',
    default_args=default_args,
    description='매달 건축물대장 표제부 데이터 수집',
    schedule='0 10 1 * *',  # 매월 1일 오전 10시
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['monthly', 'bronze'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_building_leader',
        python_callable=run_workflow,
        provide_context=True
    )