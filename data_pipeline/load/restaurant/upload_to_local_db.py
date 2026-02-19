"""
Restaurant → 로컬 Docker PostgreSQL 적재
==============================================

data/gold/restaurant_master에서 최신 연도/월 폴더를 자동으로 찾아
하위 Parquet 파일들을 로컬 PostgreSQL에 적재
"""
import argparse
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.models.restaurant import RestaurantMaster, RestaurantStatus
from api.session import create_engine_for_mode
from data_pipeline.load.parquet_loader import (
    TableConfig,
    find_latest_year_month,
    insert_rows,
    read_parquet_rows,
)

DEFAULT_DIR = "data/gold/restaurant_master"

RESTAURANT_CONFIG = TableConfig(
    model=RestaurantMaster,
    column_aliases={},
    float_columns=frozenset({"부번", "min_유휴부지면적", "유휴부지면적", "신뢰도점수", "longitude", "latitude"}),
    auto_columns=frozenset(),
    snapshot_keys=("year", "month"),
)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restaurant Parquet → 로컬 Docker PostgreSQL 적재",
    )
    parser.add_argument("--local-dir", default=DEFAULT_DIR, help="데이터 루트 디렉토리")
    parser.add_argument("--batch-size", type=int, default=2000, help="배치 INSERT 크기")
    parser.add_argument("--dry-run", action="store_true", help="실제 적재 없이 계획만 출력")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.local_dir).expanduser().resolve()

    # 1) 최신 연도/월 탐색
    year, month = find_latest_year_month(root)
    print(f"[INFO] 최신 snapshot: year={year}, month={month}")

    # 2) 해당 월 하위의 모든 Parquet 수집
    month_dir = root / f"year={year}" / f"month={month}"
    parquet_files = sorted(month_dir.rglob("*.parquet"))

    if not parquet_files:
        print(f"[INFO] Parquet 파일이 없습니다: {month_dir}")
        return

    print(f"[INFO] {len(parquet_files)}개 파일 발견")

    if args.dry_run:
        for fp in parquet_files:
            print(f"  [DRY-RUN] {fp}")
        return

    # 3) DB 연결 및 적재
    snapshot = {"year": year, "month": month}
    engine = create_engine_for_mode(mode="local")

    all_rows: list[dict] = []
    for fp in parquet_files:
        print(f"[READ] {fp}")
        all_rows.extend(read_parquet_rows(fp, snapshot=snapshot, config=RESTAURANT_CONFIG))

    # PK(업체명, 도로명주소)가 NULL인 행 제거
    before = len(all_rows)
    all_rows = [r for r in all_rows if r.get("업체명") and r.get("도로명주소")]
    skipped = before - len(all_rows)
    if skipped:
        print(f"[WARN] PK(업체명/도로명주소) NULL → {skipped}건 스킵")

    inserted = insert_rows(engine, all_rows, snapshot=snapshot, config=RESTAURANT_CONFIG, batch_size=args.batch_size)
    print(f"[DONE] {RESTAURANT_CONFIG.table_name}에 {inserted}건 적재 완료")

    engine.dispose()


if __name__ == "__main__":
    main()
