import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import delete
from sqlalchemy.orm import Session


# Spark container에서 /opt/spark/api 패키지를 import할 수 있도록 경로 보정
PROJECT_ROOT_CANDIDATES = [
    Path("/opt/spark"),
    Path(__file__).resolve().parents[2],
]
for root in PROJECT_ROOT_CANDIDATES:
    if root.exists() and str(root) not in sys.path:
        sys.path.insert(0, str(root))

from api.db.init_db import init_db
from api.db.session import create_engine_for_mode
from api.models.silver_stage_1 import SilverStage1Main


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
        if math.isnan(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _flush_batches(
    session: Session,
    model: Any,
    mappings: Iterable[Dict[str, Any]],
    batch_size: int,
) -> int:
    batch: list[Dict[str, Any]] = []
    inserted = 0

    for mapping in mappings:
        batch.append(mapping)
        if len(batch) >= batch_size:
            session.bulk_insert_mappings(model, batch)
            session.commit()
            inserted += len(batch)
            batch.clear()

    if batch:
        session.bulk_insert_mappings(model, batch)
        session.commit()
        inserted += len(batch)

    return inserted


def store_silver_stage_1(
    main_df,
    mode: str,
    source_year: int,
    source_month: int,
    region: str,
    sigungu: str,
    db_url: Optional[str] = None,
    batch_size: int = 2000,
    rds_sslmode: str = "require",
) -> int:
    """
    Store Spark DataFrames into Postgres using SQLAlchemy ORM models.
    Existing rows for the same (year, month, region, sigungu) are deleted first.

    Spark DataFrame
        ↓ toLocalIterator() (한 행씩)
        ↓ asDict() (딕셔너리로)
        ↓ 한국어 키 → 영어 컬럼 매핑
        ↓ _to_float()로 숫자 안전 변환
        ↓ 2000건씩 배치
        ↓ bulk_insert_mappings()로 INSERT
        ↓ commit()
    PostgreSQL 테이블
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    engine = create_engine_for_mode(mode=mode, db_url=db_url, rds_sslmode=rds_sslmode)
    init_db(mode=mode, db_url=db_url, rds_sslmode=rds_sslmode, engine=engine)

    main_cols = [
        "고유번호",
        "법정동명",
        "지번",
        "소유권변동원인",
        "소유권변동일자",
        "토지면적",
        "지목",
        "공시지가",
        "본번",
        "부번",
        "관리_건축물대장_PK",
        "옥외자주식면적",
        "상호명",
        "지점명",
        "대표자",
        "도로명주소",
        "상권업종대분류명",
        "상권업종중분류명",
        "상권업종소분류명",
        "경도",
        "위도",
    ]

    with Session(engine) as session:
        session.execute(
            delete(SilverStage1Main).where(
                SilverStage1Main.source_year == source_year,
                SilverStage1Main.source_month == source_month,
                SilverStage1Main.region == region,
                SilverStage1Main.sigungu == sigungu,
            )
        )
        session.commit()

        main_iter = (
            row.asDict(recursive=True) for row in main_df.select(*main_cols).toLocalIterator()
        )
        main_mappings = (
            {
                "source_year": source_year,
                "source_month": source_month,
                "region": region,
                "sigungu": sigungu,
                "pnu_code": d.get("고유번호"),
                "legal_dong_name": d.get("법정동명"),
                "lot_number": d.get("지번"),
                "ownership_change_reason": d.get("소유권변동원인"),
                "ownership_change_date": d.get("소유권변동일자"),
                "land_area": _to_float(d.get("토지면적")),
                "land_category": d.get("지목"),
                "official_land_price": _to_float(d.get("공시지가")),
                "main_lot_no": d.get("본번"),
                "sub_lot_no": d.get("부번"),
                "building_ledger_pk": d.get("관리_건축물대장_PK"),
                "outdoor_self_parking_area": _to_float(d.get("옥외자주식면적")),
                "store_name": d.get("상호명"),
                "branch_name": d.get("지점명"),
                "owner_name": d.get("대표자"),
                "road_address": d.get("도로명주소"),
                "biz_category_large": d.get("상권업종대분류명"),
                "biz_category_mid": d.get("상권업종중분류명"),
                "biz_category_small": d.get("상권업종소분류명"),
                "longitude": _to_float(d.get("경도")),
                "latitude": _to_float(d.get("위도")),
            }
            for d in main_iter
        )
        inserted_count = _flush_batches(
            session=session,
            model=SilverStage1Main,
            mappings=main_mappings,
            batch_size=batch_size,
        )

    engine.dispose()
    return inserted_count
