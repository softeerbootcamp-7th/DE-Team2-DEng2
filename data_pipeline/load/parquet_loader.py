"""
Parquet → DB 범용 로더
======================

year=YYYY/month=MM 구조의 Parquet 데이터를 읽어서
지정된 SQLAlchemy 모델 테이블에 적재하는 공통 함수

"""
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy import delete, insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass(frozen=True)
class TableConfig:
    """
    Parquet → DB 적재에 필요한 테이블별 설정.

    Attributes:
        model: SQLAlchemy ORM 모델 클래스
        column_aliases: Parquet 컬럼 → DB 컬럼 매핑 (동일하면 빈 dict)
        float_columns: float 안전 변환이 필요한 DB 컬럼명들
        auto_columns: DB가 자동 생성하는 컬럼 (INSERT에서 제외)
        snapshot_keys: idempotent 재적재를 위한 삭제 기준 컬럼 (DB 컬럼명)
                       빈 tuple이면 전체 테이블 삭제 후 INSERT
    """
    model: Any
    column_aliases: dict[str, str] = field(default_factory=dict)
    float_columns: frozenset[str] = field(default_factory=frozenset)
    auto_columns: frozenset[str] = field(default_factory=frozenset)
    snapshot_keys: tuple[str, ...] = field(default_factory=lambda: ("year", "month"))

    @property
    def writable_columns(self) -> frozenset[str]:
        """INSERT 대상 컬럼 (auto_columns 제외). DB 컬럼명 기준"""
        return frozenset(
            col.name
            for col in self.model.__table__.columns
            if col.name not in self.auto_columns
        )

    @property
    def table_name(self) -> str:
        return self.model.__tablename__


def find_latest_year_month(root: Path) -> tuple[str, str]:
    """
    root/ 하위에서 가장 큰 year, 그 안에서 가장 큰 month를 반환
    폴더 이름 형식: year=2025 / month=1
    """
    if not root.exists():
        raise FileNotFoundError(f"디렉토리가 존재하지 않습니다: {root}")

    year = _max_partition_value(root, key="year")
    year_dir = root / f"year={year}"

    month = _max_partition_value(year_dir, key="month")
    return str(year), str(month)


def _max_partition_value(parent: Path, key: str) -> int:
    """parent/ 아래에서 '{key}=N' 형태 폴더 중 N이 가장 큰 값을 반환"""
    prefix = f"{key}="
    values: list[int] = []
    for child in parent.iterdir():
        if child.is_dir() and child.name.startswith(prefix):
            try:
                values.append(int(child.name[len(prefix):]))
            except ValueError:
                continue

    if not values:
        raise FileNotFoundError(
            f"'{key}=N' 형태의 폴더를 찾을 수 없습니다: {parent}"
        )
    return max(values)


def read_parquet_rows(
    file_path: Path,
    snapshot: dict[str, Any],
    config: TableConfig,
) -> list[dict[str, Any]]:
    """
    Parquet 파일 1개를 읽어 INSERT용 dict 리스트를 반환

    처리 순서:
    1) snapshot 값(year, month 등)을 기본으로 채움
    2) Parquet 컬럼 중 DB 컬럼과 이름이 같은 것을 그대로 매핑
    3) column_aliases로 별칭 보강
    4) float_columns에 대해 NaN/빈값→None 안전 변환
    5) writable_columns에 없는 컬럼 제거
    """
    df = pd.read_parquet(file_path)
    if df.empty:
        return []

    writable = config.writable_columns
    aliases = config.column_aliases
    floats = config.float_columns

    df = df.where(pd.notna(df), None)
    rows: list[dict[str, Any]] = []

    for raw in df.to_dict(orient="records"):
        record: dict[str, Any] = dict(snapshot)

        # DB 컬럼명과 동일한 Parquet 컬럼을 그대로 매핑
        for col, val in raw.items():
            if col in writable and col not in record:
                record[col] = None if _is_missing(val) else val

        # 별칭 매핑 보강 (Parquet 컬럼명 → DB 컬럼명이 다른 경우)
        for source, target in aliases.items():
            if target not in record or _is_missing(record.get(target)):
                if source in raw and not _is_missing(raw[source]):
                    record[target] = raw[source]

        # float 컬럼 안전 변환
        for col in floats:
            if col in record:
                record[col] = _safe_float(record[col])

        # 테이블에 없는 컬럼 제거
        rows.append({k: v for k, v in record.items() if k in writable})

    return rows


# DB 적재 
def insert_rows(
    engine: Engine,
    rows: list[dict[str, Any]],
    snapshot: dict[str, Any],
    config: TableConfig,
    batch_size: int = 2000,
) -> int:
    """
    지정된 테이블에 rows를 적재

    1) snapshot_keys에 해당하는 기존 데이터를 삭제 (idempotent 재적재).
       snapshot_keys가 빈 tuple이면 전체 테이블을 삭제
    2) batch_size 단위로 INSERT
    """
    if not rows:
        return 0

    model = config.model

    with Session(engine) as session:
        # 기존 snapshot 삭제
        stmt = delete(model)
        if config.snapshot_keys:
            for key in config.snapshot_keys:
                col = model.__table__.c[key]
                stmt = stmt.where(col == snapshot[key])

        result = session.execute(stmt)
        session.commit()

        snapshot_desc = ", ".join(f"{k}={snapshot[k]}" for k in config.snapshot_keys) if config.snapshot_keys else "전체"
        print(f"[DELETE] {config.table_name} ({snapshot_desc}) → 기존 {result.rowcount}건 삭제")

        # batch INSERT
        table = model.__table__
        inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i: i + batch_size]
            session.execute(insert(table), batch)
            session.commit()
            inserted += len(batch)

    return inserted


# 내부 유틸리티 
def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _safe_float(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None
    try:
        parsed = float(value)
        return None if math.isnan(parsed) else parsed
    except (TypeError, ValueError):
        return None
