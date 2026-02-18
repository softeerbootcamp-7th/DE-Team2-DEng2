"""
트럭헬퍼 차고지 → AWS RDS PostgreSQL 적재
==============================================

data/output/gold_stage/truckhelper/year=YYYY/month=MM 구조의 Parquet 파일을 자동으로 찾아
AWS RDS PostgreSQL truckhelper_parking_area 테이블에 적재합니다.

사전 조건:
    - .env에 RDS_HOST, RDS_DB, RDS_USER, RDS_PASSWORD 설정 필요

사용:
    python load/truckhelper/upload_to_rds.py
    python load/truckhelper/upload_to_rds.py --rds-sslmode require
    python load/truckhelper/upload_to_rds.py --dry-run
"""
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.session import create_engine_for_mode
from data_pipeline.load.parquet_loader import (
    find_latest_year_month,
    insert_rows,
    read_parquet_rows,
)
from data_pipeline.load.truckhelper.upload_to_local_db import TRUCKHELPER_CONFIG

DEFAULT_DIR = "data/output/gold_stage/truckhelper"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="트럭헬퍼 차고지 Parquet → AWS RDS PostgreSQL 적재",
    )
    parser.add_argument("--local-dir", default=DEFAULT_DIR, help="데이터 루트 디렉토리")
    parser.add_argument("--rds-sslmode", default="require", help="RDS SSL 모드")
    parser.add_argument("--batch-size", type=int, default=2000, help="배치 INSERT 크기")
    parser.add_argument("--dry-run", action="store_true", help="실제 적재 없이 계획만 출력")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.local_dir).expanduser().resolve()

    year, month = find_latest_year_month(root)
    print(f"[INFO] 최신 snapshot: year={year}, month={month}")

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

    engine = create_engine_for_mode(mode="aws", rds_sslmode=args.rds_sslmode)

    all_rows: list[dict] = []
    for fp in parquet_files:
        print(f"[READ] {fp}")
        all_rows.extend(read_parquet_rows(fp, snapshot={}, config=TRUCKHELPER_CONFIG))

    inserted = insert_rows(engine, all_rows, snapshot={}, config=TRUCKHELPER_CONFIG, batch_size=args.batch_size)
    engine.dispose()

    print(f"[DONE] {TRUCKHELPER_CONFIG.table_name}에 {inserted}건 적재 완료")


if __name__ == "__main__":
    main()
