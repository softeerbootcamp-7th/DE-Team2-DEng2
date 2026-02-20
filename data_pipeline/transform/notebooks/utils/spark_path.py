import datetime as dt
import os

def get_latest_year_month_path(spark, base_path):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    Path = spark._jvm.org.apache.hadoop.fs.Path

    year_status = fs.listStatus(Path(base_path))
    years = [
        int(str(s.getPath().getName()).split("=")[1])
        for s in year_status
        if s.getPath().getName().startswith("year=")
    ]
    if not years:
        raise ValueError(f"No year partition in {base_path}")

    latest_year = max(years)
    year_path = f"{base_path}/year={latest_year}"

    month_status = fs.listStatus(Path(year_path))
    months = [
        int(str(s.getPath().getName()).split("=")[1])
        for s in month_status
        if s.getPath().getName().startswith("month=")
    ]
    if not months:
        raise ValueError(f"No month partition in {year_path}")

    latest_month = max(months)
    return f"{base_path}/year={latest_year}/month={latest_month:02d}"

def get_current_year_month_path(base_path):
    """
    현재 날짜 기준 year=YYYY/month=MM 경로 생성

    Parameters
    ----------
    base_path : str
        parquet base path (예: /opt/spark/data/silver/s0/address)
        
    Returns
    -------
    str
        base_path/year=YYYY/month=MM
    """

    today = dt.date.today()
    year = today.year
    month = f"{today.month:02d}"

    return os.path.join(base_path, f"year={year}", f"month={month}")


def get_latest_year_week_path(spark, base_path: str) -> str:
    """
    base_path 아래에 year=YYYY/week=WW 파티션이 있을 때
    가장 최신 year, week 경로를 반환

    return:
      {base_path}/year=YYYY/week=WW
    """
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    Path = spark._jvm.org.apache.hadoop.fs.Path

    # year 파티션 목록
    year_status = fs.listStatus(Path(base_path))
    years = []
    for s in year_status:
        name = str(s.getPath().getName())
        if name.startswith("year="):
            years.append(int(name.split("=")[1]))

    if not years:
        raise ValueError(f"No year partition in {base_path}")

    latest_year = max(years)
    year_path = f"{base_path}/year={latest_year}"

    # week 파티션 목록
    week_status = fs.listStatus(Path(year_path))
    weeks = []
    for s in week_status:
        name = str(s.getPath().getName())
        if name.startswith("week="):
            weeks.append(int(name.split("=")[1]))

    if not weeks:
        raise ValueError(f"No week partition in {year_path}")

    latest_week = max(weeks)
    return f"{base_path}/year={latest_year}/week={latest_week:02d}"

def get_current_year_week_path(base_path: str) -> str:
    """
    현재 날짜 기준 year / week partition 경로 생성

    example:
    base_path = "silver/clean/restaurant"

    return:
    silver/clean/restaurant/year=2026/week=08
    """

    today = dt.date.today()

    year = today.year
    
    # ISO week (월요일 시작, 1~53)
    week = f"{today.isocalendar().week:02d}"

    return os.path.join(
        base_path,
        f"year={year}",
        f"week={week}",
    )