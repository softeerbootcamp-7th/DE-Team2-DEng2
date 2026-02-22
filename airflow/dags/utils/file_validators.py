import os
import glob
from datetime import datetime, timedelta

def get_prev_month_ym():
    """실행 시점 기준 전월의 YYYY, MM 반환"""
    now = datetime.now()
    # 이번 달 1일에서 하루를 빼면 무조건 전월 마지막 날이 나옴
    first_day_of_this_month = now.replace(day=1)
    last_day_of_prev_month = first_day_of_this_month - timedelta(days=1)

    return last_day_of_prev_month.strftime('%Y'), last_day_of_prev_month.strftime('%m')

def get_monthly_path(base_dir, year, month):
    """표준화된 연/월 경로 생성"""
    return f"{base_dir}/year={year}/month={month}"

def validate_parquet_existence(base_data_dir):
    """전월 데이터 파케 파일 존재 여부 통합 검증"""
    y, m = get_prev_month_ym()
    target_path = get_monthly_path(base_data_dir, y, m)
    
    print(f"🔍 검증 시작: {target_path}")

    if not os.path.exists(target_path):
        raise FileNotFoundError(f"❌ 검증 실패: 디렉토리 없음 -> {target_path}")

    search_pattern = os.path.join(target_path, "**", "*.parquet")
    parquet_files = glob.glob(search_pattern, recursive=True)
    
    if not parquet_files:
        raise FileNotFoundError(f"❌ 검증 실패: 해당 경로에 Parquet 파일이 없습니다.")

    print(f"✅ 검증 완료: {len(parquet_files)}개 파일 확인 (기준: {y}-{m})")
    return True