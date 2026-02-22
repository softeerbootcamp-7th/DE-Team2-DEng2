"""
Restaurant → 로컬 Docker PostgreSQL 적재
==============================================

data/gold/restaurant 에서 최신 year/month/week snapshot을 찾아
하위 Parquet 파일들을 로컬 PostgreSQL에 적재한다.
"""

import unicodedata
import argparse
import sys
from pathlib import Path

from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.models.restaurant import Restaurant
from api.session import create_engine_for_mode
from data_pipeline.load.parquet_loader import (
    TableConfig,
    find_latest_year_month_week,
    insert_rows,
    read_parquet_rows,
)

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

DEFAULT_DIR = "data/gold/restaurant"

RESTAURANT_CONFIG = TableConfig(
    model=Restaurant,
    column_aliases={},
    float_columns=frozenset({
        "총점",
        "영업_적합도",
        "수익성",
        "유휴부지_면적",
        "longitude",
        "latitude",
        "주차_적합도",
    }),
    auto_columns=frozenset(),
    snapshot_keys=("year", "month", "week"),
)

# ------------------------------------------------------------
# Utils
# ------------------------------------------------------------

def normalize_str(val: str) -> str:
    """모든 문자열을 NFC + strip으로 통일"""
    return unicodedata.normalize("NFC", val).strip()


def extract_region_from_path(fp: Path) -> str:
    """
    parquet 경로에서 region 추출
    예: .../region=경기/part-0000.parquet → 경기
    """
    for part in fp.parts:
        if part.startswith("region="):
            return normalize_str(part.split("=", 1)[1])
    return "미분류"


# ------------------------------------------------------------
# Args
# ------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restaurant Parquet → 로컬 PostgreSQL 적재",
    )
    parser.add_argument("--local-dir", default=DEFAULT_DIR)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main() -> None:
    args = parse_args()
    root = Path(args.local_dir).expanduser().resolve()

    # 1️⃣ 최신 snapshot 탐색
    year, month, week = find_latest_year_month_week(root)
    print(f"[INFO] 최신 snapshot: year={year}, month={month}, week={week}")

    week_dir = root / f"year={year}" / f"month={month}" / f"week={week}"
    parquet_files = sorted(week_dir.rglob("*.parquet"))

    if not parquet_files:
        print(f"[INFO] Parquet 파일이 없습니다: {week_dir}")
        return

    print(f"[INFO] {len(parquet_files)}개 파일 발견")

    if args.dry_run:
        for fp in parquet_files:
            print(f"[DRY-RUN] {fp}")
        return

    # 2️⃣ DB 연결
    engine: Engine = create_engine_for_mode(mode="local")

    all_rows: list[dict] = []

    # 3️⃣ parquet 로드
    for fp in parquet_files:
        region = extract_region_from_path(fp)

        snapshot = {
            "year": year,
            "month": month,
            "week": week,
            "region": region,
        }

        print(f"[READ] {fp.name} (region={region})")

        rows = read_parquet_rows(
            fp,
            snapshot=snapshot,
            config=RESTAURANT_CONFIG,
        )
        all_rows.extend(rows)

    print(f"[INFO] parquet 로드 완료: {len(all_rows)} rows")

    # 4️⃣ 데이터 정제 + NFC 통일 + PK 방어
    refined_rows: list[dict] = []

    for row in all_rows:
        # PK 필수값 체크
        if not row.get("업체명") or not row.get("도로명주소"):
            continue

        # 모든 문자열 NFC 정규화
        for k, v in row.items():
            if isinstance(v, str):
                row[k] = normalize_str(v)

        # 숫자형 보정
        try:
            row["수익성"] = float(row.get("수익성", 0.0))
            row["주차_적합도"] = int(float(row.get("주차_적합도", 0)))
        except (TypeError, ValueError):
            pass

        refined_rows.append(row)

    print(f"[INFO] 정제 후 row 수: {len(refined_rows)}")

    # 5️⃣ snapshot 단위 적재 (재실행 안전)
    inserted = insert_rows(
        engine,
        refined_rows,
        snapshot={
            "year": year,
            "month": month,
            "week": week,
        },
        config=RESTAURANT_CONFIG,
        batch_size=args.batch_size,
    )

    print(f"[DONE] restaurant 테이블에 {inserted}건 적재 완료")
    engine.dispose()


if __name__ == "__main__":
    main()