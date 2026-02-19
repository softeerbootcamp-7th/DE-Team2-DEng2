from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/project')

try:
    from data_pipeline.extract.extract_chajoo_dist import run_workflow
except ImportError as e:
    print(f"Import Error: {e}")

with DAG(
    dag_id='chajoo_dist_monthly_extract',
    schedule='0 11 1 * *',  # 매월 1일 오전 11시 (건축물대장 1시간 뒤)
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['monthly', 'chajoo'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_chajoo_dist',
        python_callable=run_workflow,
        provide_context=True
    )