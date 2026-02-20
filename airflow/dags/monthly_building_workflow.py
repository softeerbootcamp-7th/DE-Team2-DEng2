from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
import glob

# 프로젝트 경로 추가
sys.path.append('/opt/airflow/project')

try:
    from data_pipeline.extract.extract_buildingLeader import run_workflow
except ImportError as e:
    print(f"Import Error: {e}")

# [추가] 파일 검증 함수
def validate_parquet_files(**kwargs):
    # 전월 기준 경로 계산 (수집 로직과 동일하게 맞춤)
    now = datetime.now()
    # 실행 시점의 전월 (예: 2월 실행 시 1월 폴더 확인)
    first_day_of_current_month = now.replace(day=1)
    last_day_of_prev_month = first_day_of_current_month - timedelta(days=1)

    y = last_day_of_prev_month.strftime('%Y')
    m = last_day_of_prev_month.strftime('%m')

    target_path = f"/opt/airflow/project/data/bronze/buildingLeader/parquet/year={y}/month={m}"


    print(f"🔍 검증 경로: {target_path}")

    # 1. 디렉토리 존재 확인
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"❌ 검증 실패: 디렉토리가 존재하지 않습니다 -> {target_path}")

    # 2. .parquet 파일이 하나라도 있는지 확인
    search_pattern = os.path.join(target_path, "**", "*.parquet")
    parquet_files = glob.glob(search_pattern, recursive=True)
    if not parquet_files:
        raise FileNotFoundError(f"❌ 검증 실패: {target_path} 에 Parquet 파일이 없습니다.")

    print(f"✅ 검증 완료: {len(parquet_files)}개의 파일을 확인했습니다.")

default_args = {
    'owner': 'PHW',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='building_leader_monthly_extract',
    default_args=default_args,
    description='매달 건축물대장 표제부 데이터 수집',
    schedule='0 10 1 * *',
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['monthly', 'bronze'],
) as dag:

    # 1. 데이터 수집
    extract_task = PythonOperator(
        task_id='extract_building_leader',
        python_callable=run_workflow,
        provide_context=True
    )

    # 2. [신규] 수집 결과 데이터 검증
    validate_task = PythonOperator(
        task_id='validate_collected_data',
        python_callable=validate_parquet_files,
        provide_context=True
    )

    # 3. S3 업로드
    upload_s3 = BashOperator(
        task_id='upload_to_s3',
        bash_command=(
            f"python /opt/airflow/project/data_pipeline/utils/upload_data_to_s3.py "
            f"--local-dir /opt/airflow/project/data/bronze/buildingLeader "
            f"--prefix data/bronze/buildingLeader "
            f"--exclude _work --exclude .DS_Store"
        ),
        trigger_rule='all_success' 
    )

    # 흐름: 수집 -> 검증 -> S3 업로드
    extract_task >> validate_task >> upload_s3