"""
Restaurant → 로컬 Docker PostgreSQL 적재
==============================================

data/output/gold_stage/restaurant에서 최신 연도/월 폴더를 자동으로 찾아
하위 Parquet 파일들을 로컬 PostgreSQL에 적재

restaurant_master 적재 후, 신규 (업체명, 도로명주소) 건을
restaurant_status 테이블 INSERT

사용:
    python load/restaurant/upload_to_local_db.py
    python load/restaurant/upload_to_local_db.py --local-dir data/output/gold_stage/restaurant
    python load/restaurant/upload_to_local_db.py --dry-run
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

DEFAULT_DIR = "data/output/gold_stage/restaurant"

RESTAURANT_CONFIG = TableConfig(
    model=RestaurantMaster,
    column_aliases={},
    float_columns=frozenset({"부번", "min_유휴부지면적", "유휴부지면적", "신뢰도점수", "longitude", "latitude"}),
    auto_columns=frozenset(),
    snapshot_keys=("year", "month"),
)


def sync_restaurant_status(engine: Engine, rows: list[dict[str, Any]]) -> int:
    """
    restaurant_master에 적재된 rows 중 restaurant_status에 없는
    신규 (업체명, 도로명주소) 건을 INSERT
    나머지 컬럼은 default/null로 설정
    """
    # rows에서 고유한 (업체명, 도로명주소) 추출
    new_pairs = {(r["업체명"], r["도로명주소"]) for r in rows if r.get("업체명") and r.get("도로명주소")}
    if not new_pairs:
        return 0

    with Session(engine) as session:
        # DB에 이미 존재하는 쌍 조회
        existing = session.execute(
            select(
                RestaurantStatus.__table__.c["업체명"],
                RestaurantStatus.__table__.c["도로명주소"],
            )
        ).fetchall()
        existing_set = {(row[0], row[1]) for row in existing}

        # 신규 건만 필터
        to_insert = [
            {"업체명": name, "도로명주소": addr}
            for name, addr in new_pairs
            if (name, addr) not in existing_set
        ]

        if to_insert:
            session.execute(insert(RestaurantStatus.__table__), to_insert)
            session.commit()

    if to_insert:
        print(f"[STATUS] restaurant_status에 신규 {len(to_insert)}건 INSERT")
    return len(to_insert)


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

    # 4) restaurant_status 신규 건 연동
    sync_restaurant_status(engine, all_rows)
    engine.dispose()


if __name__ == "__main__":
    main()
