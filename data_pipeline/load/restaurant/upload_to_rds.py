"""
Restaurant → AWS RDS PostgreSQL 적재
==========================================

data/gold/restaurant 에서 최신 year/month/week snapshot을 찾아
하위 Parquet 파일들을 AWS RDS PostgreSQL에 적재한다.
"""

import argparse
import sys
from pathlib import Path

from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.session import create_engine_for_mode
from data_pipeline.load.parquet_loader import (
    find_latest_year_month_week,  # week 단위 탐색으로 변경
    insert_rows,
    read_parquet_rows,
)
# 공통 Config 및 Utils 재사용
from data_pipeline.load.restaurant.upload_to_local_db import (
    RESTAURANT_CONFIG,
    DEFAULT_DIR,
    normalize_str,
    extract_region_from_path
)

# ------------------------------------------------------------
# Args
# ------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restaurant Parquet → AWS RDS PostgreSQL 적재",
    )
    parser.add_argument("--local-dir", default=DEFAULT_DIR, help="데이터 루트 디렉토리")
    parser.add_argument("--rds-sslmode", default="require", help="RDS SSL 모드")
    parser.add_argument("--batch-size", type=int, default=2000, help="배치 INSERT 크기")
    parser.add_argument("--dry-run", action="store_true", help="실제 적재 없이 계획만 출력")
    return parser.parse_args()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main() -> None:
    args = parse_args()
    root = Path(args.local_dir).expanduser().resolve()

    # 1️⃣ 최신 snapshot 탐색 (year, month, week)
    year, month, week = find_latest_year_month_week(root)
    print(f"[INFO] 최신 snapshot: year={year}, month={month}, week={week}")

    # 2️⃣ 해당 주차(week) 하위의 모든 Parquet 수집
    week_dir = root / f"year={year}" / f"month={month}" / f"week={week}"
    parquet_files = sorted(week_dir.rglob("*.parquet"))

    if not parquet_files:
        print(f"[INFO] Parquet 파일이 없습니다: {week_dir}")
        return

    print(f"[INFO] {len(parquet_files)}개 파일 발견")

    if args.dry_run:
        for fp in parquet_files:
            print(f"  [DRY-RUN] {fp}")
        return

    # 3️⃣ DB 연결 (AWS 모드)
    engine: Engine = create_engine_for_mode(mode="aws", rds_sslmode=args.rds_sslmode)

    all_rows: list[dict] = []

    # 4️⃣ parquet 로드
    for fp in parquet_files:
        region = extract_region_from_path(fp)
        
        snapshot = {
            "year": year, 
            "month": month, 
            "week": week,
            "region": region
        }

        print(f"[READ] {fp.name} (region={region})")
        
        rows = read_parquet_rows(
            fp, 
            snapshot=snapshot, 
            config=RESTAURANT_CONFIG
        )
        all_rows.extend(rows)

    print(f"[INFO] parquet 로드 완료: {len(all_rows)} rows")

    # 5️⃣ 데이터 정제 (NFC 통일 + PK 방어 + 타입 보정)
    refined_rows: list[dict] = []
    
    for row in all_rows:
        # PK 필수값 체크
        if not row.get("업체명") or not row.get("도로명주소"):
            continue

        # 모든 문자열 NFC 정규화
        for k, v in row.items():
            if isinstance(v, str):
                row[k] = normalize_str(v)

        # 숫자형 보정 (Local 코드와 동일하게 유지)
        try:
            row["수익성"] = float(row.get("수익성", 0.0))
            row["주차_적합도"] = int(float(row.get("주차_적합도", 0)))
        except (TypeError, ValueError):
            pass

        refined_rows.append(row)

    print(f"[INFO] 정제 후 row 수: {len(refined_rows)} (스킵: {len(all_rows) - len(refined_rows)}건)")

    # 6️⃣ snapshot 단위 적재
    inserted = insert_rows(
        engine, 
        refined_rows, 
        snapshot={
            "year": year, 
            "month": month, 
            "week": week
        }, 
        config=RESTAURANT_CONFIG, 
        batch_size=args.batch_size
    )

    print(f"[DONE] {RESTAURANT_CONFIG.model.__tablename__} 테이블에 {inserted}건 적재 완료")
    engine.dispose()


if __name__ == "__main__":
    main()