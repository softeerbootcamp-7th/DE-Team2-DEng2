import argparse
import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

# 프로젝트 루트를 import 경로에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.db.init_db import init_db
from api.db.session import create_engine_for_mode
from api.models.silver_stage_1 import SilverStage1Main

# ============================================================================
# Target 설정 블록
# ----------------------------------------------------------------------------
# 테이블/스키마가 바뀔 때는 아래 TARGET_PRESET만 수정하고
# 다른 함수들은 건드리지 않아도 되게 구성했습니다.
# ============================================================================
TARGET_PRESET: Dict[str, Any] = {
    # SQLAlchemy ORM 모델
    "model": SilverStage1Main,

    # "같은 시점 데이터"를 식별하는 기본 키 성격의 컬럼들(파일 저장 경로 기반)
    #  동일 조합으로 재실행될시 기존 데이터를 먼저 삭제 (변화된 열 업데이트가 아닌 전체 delete 후 전체 insert)
    "snapshot_fields": ("source_year", "source_month", "region", "sigungu"),
    # snapshot_fields 중 int로 파싱해야 하는 컬럼
    # 폴더 경로에서 문자열로 읽히므로 여기 지정한 값만 정수로 변환
    "snapshot_int_fields": {"source_year", "source_month"},

    # 파일 실제 경로와 스냅샷 컬럼명  매핑
    # 예: year=2025 도 source_year로 인식되도록 alias 허용
    "path_partitions": {
        "source_year": ("source_year", "year"),
        "source_month": ("source_month", "month"),
        "region": ("region",),
        "sigungu": ("sigungu",),
    },

    # 원본 파일 컬럼명 -> SQLAlchemy ORM 모델의 컬럼명 매핑
    "column_aliases": {
        "고유번호": "pnu_code",
        "법정동명": "legal_dong_name",
        "지번": "lot_number",
        "소유권변동원인": "ownership_change_reason",
        "소유권변동일자": "ownership_change_date",
        "토지면적": "land_area",
        "지목": "land_category",
        "공시지가": "official_land_price",
        "본번": "main_lot_no",
        "부번": "sub_lot_no",
        "관리_건축물대장_PK": "building_ledger_pk",
        "옥외자주식면적": "outdoor_self_parking_area",
        "상호명": "store_name",
        "지점명": "branch_name",
        "대표자": "owner_name",
        "도로명주소": "road_address",
        "상권업종대분류명": "biz_category_large",
        "상권업종중분류명": "biz_category_mid",
        "상권업종소분류명": "biz_category_small",
        "경도": "longitude",
        "위도": "latitude",
    },

    # 숫자형(float)인 컬럼 목록
    # 빈값/NaN/숫자형으로 변환 실패값은 None으로 저장됩니다.
    "float_columns": {
        "land_area",
        "official_land_price",
        "outdoor_self_parking_area",
        "longitude",
        "latitude",
    },

    # 테이블 컬럼 중 자동 생성/관리되는 컬럼
    # INSERT 대상 컬럼 계산 시 제외
    "auto_columns": {"id", "created_at"},
}

# CSV는 메모리 사용량을 줄이기 위해 chunk 단위로 read
CSV_CHUNK_SIZE = 50_000


def load_target_schema(target_preset: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    target 설정으로부터 실제 실행용 schema dict 설정
    기본값은 파일 상단 TARGET_PRESET을 사용합니다.
    """
    preset = target_preset or TARGET_PRESET

    model = preset["model"]
    auto_columns = set(preset.get("auto_columns", set()))
    writable_columns = {
        col.name for col in model.__table__.columns if col.name not in auto_columns
    }

    schema = {
        "table_name": model.__tablename__,
        "model": model,
        "snapshot_fields": tuple(preset["snapshot_fields"]),
        "snapshot_int_fields": set(preset.get("snapshot_int_fields", set())),
        "path_partitions": dict(preset["path_partitions"]),
        "writable_columns": writable_columns,
        "column_aliases": dict(preset.get("column_aliases", {})),
        "float_columns": set(preset.get("float_columns", set())),
    }
    validate_schema(schema)
    return schema


def validate_schema(schema: Dict[str, Any]) -> None:
    """스키마 설정 오타를 초기에 잡기 위한 검증"""
    writable_columns = schema["writable_columns"]
    snapshot_fields = schema["snapshot_fields"]

    for field in snapshot_fields:
        if field not in writable_columns:
            raise ValueError(f"snapshot field '{field}' is not in writable columns")

    for field in snapshot_fields:
        if field not in schema["path_partitions"]:
            raise ValueError(f"path partition mapping missing for snapshot field '{field}'")

    for source_col, target_col in schema["column_aliases"].items():
        if target_col not in writable_columns:
            raise ValueError(
                f"column alias target '{target_col}' (from '{source_col}') is not writable"
            )

    for col in schema["float_columns"]:
        if col not in writable_columns:
            raise ValueError(f"float column '{col}' is not writable")


def build_parser(description: str, include_rds_sslmode: bool = False) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "--local-dir",
        default="data/output/silver_stage_1/main",
        help="CSV/Parquet 파일이 있는 폴더",
    )

    parser.add_argument(
        "--file-format",
        choices=["auto", "csv", "parquet"],
        default="auto",
        help="읽을 파일 형식",
    )

    parser.add_argument("--db-url", type=str, default=None, help="직접 DB URL 지정 (선택)")
    if include_rds_sslmode:
        parser.add_argument("--rds-sslmode", type=str, default="require", help="RDS sslmode")

    parser.add_argument("--batch-size", type=int, default=2000, help="배치 INSERT 크기")
    parser.add_argument("--dry-run", action="store_true", help="실제 적재 없이 계획만 출력")
    return parser


def run_upload_common(description: str, mode: str, include_rds_sslmode: bool) -> None:
    """local/rds 엔트리포인트에서 공통으로 사용하는 실행 함수"""
    parser = build_parser(description=description, include_rds_sslmode=include_rds_sslmode)
    args = parser.parse_args()

    rds_sslmode = args.rds_sslmode if include_rds_sslmode else "disable"
    schema = load_target_schema()

    inserted = upload_files_to_postgres(
        mode=mode,
        local_dir=args.local_dir,
        db_url=args.db_url,
        rds_sslmode=rds_sslmode,
        file_format=args.file_format,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        schema=schema,
    )

    if args.dry_run:
        print("[DONE] dry-run completed")
    else:
        print(f"[DONE] table={schema['table_name']} inserted={inserted}")


def upload_files_to_postgres(
    mode: str,
    local_dir: str | Path,
    db_url: Optional[str] = None,
    rds_sslmode: str = "require",
    file_format: str = "auto",
    batch_size: int = 2000,
    dry_run: bool = False,
    schema: Optional[Dict[str, Any]] = None,
) -> int:
    """
    local-dir 안의 csv/parquet를 읽어서 target table에 넣습니다.

    처리 순서:
    1) 파일 목록 수집
    2) dry-run이면 계획 출력 후 종료
    3) 파일 경로에서 snapshot(year/month/region/sigungu) 추출
    4) 같은 snapshot은 기존 데이터 1회 삭제
    5) 파일 row를 변환해서 batch insert
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    if schema is None:
        schema = load_target_schema()

    local_root = Path(local_dir).expanduser().resolve()
    if not local_root.exists() or not local_root.is_dir():
        raise FileNotFoundError(f"Local directory not found: {local_root}")

    files = find_data_files(local_root, file_format)
    if not files:
        print(f"[INFO] No target files found under: {local_root}")
        return 0

    file_with_snapshot: list[tuple[Path, Dict[str, Any]]] = []
    for file_path in files:
        snapshot = extract_snapshot_from_path(file_path, schema)
        file_with_snapshot.append((file_path, snapshot))

    if dry_run:
        print(f"[DRY-RUN] table={schema['table_name']} files={len(files)} mode={mode}")
        snapshot_counts: Dict[tuple[int, int, str, str], int] = {}
        for _, snapshot in file_with_snapshot:
            key = snapshot_key(snapshot, schema)
            snapshot_counts[key] = snapshot_counts.get(key, 0) + 1

        for (year, month, region, sigungu), count in sorted(snapshot_counts.items()):
            print(
                f"[DRY-RUN] snapshot=({year}, {month:02d}, {region}, {sigungu}) files={count}"
            )
        return 0

    engine = create_engine_for_mode(mode=mode, db_url=db_url, rds_sslmode=rds_sslmode)
    init_db(mode=mode, db_url=db_url, rds_sslmode=rds_sslmode, engine=engine)

    inserted = 0
    with Session(engine) as session:
        deleted_snapshots: set[tuple[int, int, str, str]] = set()

        batch: list[Dict[str, Any]] = []
        for file_path, snapshot in file_with_snapshot:
            print(f"[READ] {file_path}")

            key = snapshot_key(snapshot, schema)
            if key not in deleted_snapshots:
                delete_existing_snapshot(session, snapshot, schema)
                deleted_snapshots.add(key)

            for row in iter_rows_from_file(file_path):
                batch.append(to_db_mapping(row=row, snapshot=snapshot, schema=schema))

                if len(batch) >= batch_size:
                    session.bulk_insert_mappings(schema["model"], batch)
                    session.commit()
                    inserted += len(batch)
                    batch.clear()

        if batch:
            session.bulk_insert_mappings(schema["model"], batch)
            session.commit()
            inserted += len(batch)

    engine.dispose()
    return inserted


def snapshot_key(snapshot: Dict[str, Any], schema: Dict[str, Any]) -> tuple[int, int, str, str]:
    fields = schema["snapshot_fields"]
    return tuple(snapshot[field] for field in fields)


def find_data_files(local_root: Path, file_format: str) -> list[Path]:
    """옵션에 맞는 파일만 재귀 탐색"""
    if file_format == "csv":
        allowed_ext = {".csv"}
    elif file_format == "parquet":
        allowed_ext = {".parquet"}
    else:
        allowed_ext = {".csv", ".parquet"}

    return sorted([p for p in local_root.rglob("*") if p.is_file() and p.suffix.lower() in allowed_ext])


def extract_snapshot_from_path(file_path: Path, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    파일 경로에서 스냅샷 메타를 추출합니다.

    예시:
    - year=2025/month=12/region=경기/sigungu=41461
    """
    raw: Dict[str, str] = {}
    for part in file_path.parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        raw[key.strip().lower()] = value.strip()

    snapshot: Dict[str, Any] = {}
    for field in schema["snapshot_fields"]:
        aliases = schema["path_partitions"][field]

        value: Optional[str] = None
        for alias in aliases:
            alias_key = alias.strip().lower()
            if alias_key in raw and raw[alias_key]:
                value = raw[alias_key]
                break

        if value is None:
            raise ValueError(f"Missing path partition for '{field}': {file_path}")

        if field in schema["snapshot_int_fields"]:
            try:
                snapshot[field] = int(value)
            except ValueError as exc:
                raise ValueError(f"Invalid int partition for '{field}': {file_path}") from exc
        else:
            snapshot[field] = value

    return snapshot


def iter_rows_from_file(file_path: Path) -> Iterator[Dict[str, Any]]:
    """파일 타입별 row iterator"""
    suffix = file_path.suffix.lower()

    if suffix == ".parquet":
        df = pd.read_parquet(file_path)
        if df.empty:
            return
        df = df.where(pd.notna(df), None)
        for row in df.to_dict(orient="records"):
            yield row
        return

    if suffix == ".csv":
        for chunk in read_csv_with_fallback(file_path):
            if chunk.empty:
                continue
            chunk = chunk.where(pd.notna(chunk), None)
            for row in chunk.to_dict(orient="records"):
                yield row
        return

    raise ValueError(f"Unsupported file extension: {file_path.suffix}")


def read_csv_with_fallback(file_path: Path):
    """CSV 인코딩 fallback 순서"""
    last_exc: Optional[Exception] = None
    for enc in ("utf-8", "euc-kr", "utf-8-sig", "cp949"):
        try:
            return pd.read_csv(
                file_path,
                encoding=enc,
                low_memory=False,
                chunksize=CSV_CHUNK_SIZE,
            )
        except UnicodeDecodeError as exc:
            last_exc = exc
            continue

    raise ValueError(f"CSV encoding parse failed: {file_path}") from last_exc


def to_db_mapping(row: Dict[str, Any], snapshot: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    파일 row -> DB row 매핑

    우선순위:
    1) 파일에 DB 영문 컬럼이 있으면 그대로 사용
    2) 없으면 alias(한글 컬럼)로 보강
    3) 테이블에 없는 컬럼은 버림
    """
    mapping: Dict[str, Any] = {}

    # 스냅샷 필드 우선 채움
    for field in schema["snapshot_fields"]:
        mapping[field] = snapshot[field]

    # 영문 컬럼 우선
    for key, value in row.items():
        if key in schema["writable_columns"] and key not in mapping:
            mapping[key] = None if is_missing(value) else value

    # alias 컬럼으로 보강
    for source_col, target_col in schema["column_aliases"].items():
        if target_col in mapping and not is_missing(mapping[target_col]):
            continue
        if source_col in row and not is_missing(row[source_col]):
            mapping[target_col] = row[source_col]

    # float 컬럼 안전 변환
    for col in schema["float_columns"]:
        if col in mapping:
            mapping[col] = to_float(mapping[col])

    # 실제 테이블 컬럼만 유지
    return {k: v for k, v in mapping.items() if k in schema["writable_columns"]}


def delete_existing_snapshot(session: Session, snapshot: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """동일 snapshot 재적재를 위해 기존 데이터 삭제"""
    model = schema["model"]
    filters = [getattr(model, field) == snapshot[field] for field in schema["snapshot_fields"]]

    session.execute(delete(model).where(*filters))
    session.commit()


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def to_float(value: Any) -> Optional[float]:
    if is_missing(value):
        return None

    try:
        parsed = float(value)
        if math.isnan(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def main() -> None:
    run_upload_common(
        description="Upload local CSV/Parquet files to local Docker Postgres (silver_stage_1_main).",
        mode="local",
        include_rds_sslmode=False,
    )


if __name__ == "__main__":
    main()
