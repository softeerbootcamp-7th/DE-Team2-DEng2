import os
import glob as globmod
from typing import Optional

DATA_DIR = "/opt/airflow/project/data"


def _find_latest_year_month(base_path: str) -> Optional[str]:
    """base_path/year=YYYY/month=MM 에서 최신 파티션 경로 반환."""
    if not os.path.isdir(base_path):
        return None

    years = []
    for entry in os.listdir(base_path):
        if entry.startswith("year="):
            try:
                years.append(int(entry.split("=")[1]))
            except ValueError:
                continue
    if not years:
        return None

    latest_year = max(years)
    year_path = os.path.join(base_path, f"year={latest_year}")

    months = []
    for entry in os.listdir(year_path):
        if entry.startswith("month="):
            try:
                months.append(int(entry.split("=")[1]))
            except ValueError:
                continue
    if not months:
        return None

    latest_month = max(months)
    return os.path.join(year_path, f"month={latest_month:02d}")


def _find_latest_year_month_week(base_path: str) -> Optional[str]:
    """base_path/year=YYYY/month=MM/week=WW 에서 최신 파티션 경로 반환."""
    month_path = _find_latest_year_month(base_path)
    if month_path is None:
        return None

    weeks = []
    for entry in os.listdir(month_path):
        if entry.startswith("week="):
            try:
                weeks.append(int(entry.split("=")[1]))
            except ValueError:
                continue
    if not weeks:
        return None

    latest_week = max(weeks)
    return os.path.join(month_path, f"week={latest_week:02d}")



def check_parquet_exists(base_path: str, partition_type: str = "year_month") -> bool:
    """
    최신 파티션에 .parquet 파일이 존재하는지 확인

    Parameters
    ----------
    base_path : str
        DATA_DIR 기준 상대 경로 (ex: "bronze/address/parquet")
    partition_type : str
        "year_month" 또는 "year_month_week"
    """
    full_base = os.path.join(DATA_DIR, base_path)

    if partition_type == "year_month":
        latest = _find_latest_year_month(full_base)
    elif partition_type == "year_month_week":
        latest = _find_latest_year_month_week(full_base)
    else:
        return False

    if latest is None:
        return False

    parquet_files = globmod.glob(os.path.join(latest, "**", "*.parquet"), recursive=True)
    return len(parquet_files) > 0


def check_success_marker(base_path: str, partition_type: str = "year_month") -> bool:
    """
    최신 파티션에 _SUCCESS 파일이 존재하는지 확인

    Parameters
    ----------
    base_path : str
        DATA_DIR 기준 상대 경로 (ex: "silver/s0/address")
    partition_type : str
        "year_month" 또는 "year_month_week"
    """
    full_base = os.path.join(DATA_DIR, base_path)

    if partition_type == "year_month":
        latest = _find_latest_year_month(full_base)
    elif partition_type == "year_month_week":
        latest = _find_latest_year_month_week(full_base)
    else:
        return False

    if latest is None:
        return False

    if os.path.isfile(os.path.join(latest, "_SUCCESS")):
        return True

    # region 파티션 내부 확인
    for entry in os.listdir(latest):
        entry_path = os.path.join(latest, entry)
        if os.path.isdir(entry_path) and entry.startswith("region="):
            if os.path.isfile(os.path.join(entry_path, "_SUCCESS")):
                return True

    return False
