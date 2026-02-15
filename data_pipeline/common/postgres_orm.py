import math
import os
from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import DateTime, Float, Integer, String, Text, create_engine, delete, func
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


load_dotenv()


class Base(DeclarativeBase):
    pass


class SilverStage1Main(Base):
    __tablename__ = "silver_stage_1_main"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    source_month: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    region: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    sigungu: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    pnu_code: Mapped[Optional[str]] = mapped_column(String(32))
    legal_dong_name: Mapped[Optional[str]] = mapped_column(String(255))
    lot_number: Mapped[Optional[str]] = mapped_column(String(64))
    ownership_change_reason: Mapped[Optional[str]] = mapped_column(String(128))
    ownership_change_date: Mapped[Optional[str]] = mapped_column(String(32))

    land_area: Mapped[Optional[float]] = mapped_column(Float)
    land_category: Mapped[Optional[str]] = mapped_column(String(32))
    official_land_price: Mapped[Optional[float]] = mapped_column(Float)
    main_lot_no: Mapped[Optional[str]] = mapped_column(String(16))
    sub_lot_no: Mapped[Optional[str]] = mapped_column(String(16))

    building_ledger_pk: Mapped[Optional[str]] = mapped_column(String(64))
    outdoor_self_parking_area: Mapped[Optional[float]] = mapped_column(Float)

    store_name: Mapped[Optional[str]] = mapped_column(String(255))
    branch_name: Mapped[Optional[str]] = mapped_column(String(255))
    owner_name: Mapped[Optional[str]] = mapped_column(String(255))
    road_address: Mapped[Optional[str]] = mapped_column(String(512))

    biz_category_large: Mapped[Optional[str]] = mapped_column(String(64))
    biz_category_mid: Mapped[Optional[str]] = mapped_column(String(64))
    biz_category_small: Mapped[Optional[str]] = mapped_column(String(128))

    longitude: Mapped[Optional[float]] = mapped_column(Float)
    latitude: Mapped[Optional[float]] = mapped_column(Float)

    created_at = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


def build_postgres_url(mode: str, db_url: Optional[str] = None, rds_sslmode: str = "require") -> str:
    if db_url:
        return db_url

    if mode == "local":
        host = os.getenv("LOCAL_PG_HOST", "postgres-local")
        port = os.getenv("LOCAL_PG_PORT", "5432")
        database = os.getenv("LOCAL_PG_DB", "deng2")
        user = os.getenv("LOCAL_PG_USER", "deng2")
        password = os.getenv("LOCAL_PG_PASSWORD", "deng2")
        return (
            f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}"
        )

    if mode == "aws":
        host = os.getenv("RDS_HOST")
        port = os.getenv("RDS_PORT", "5432")
        database = os.getenv("RDS_DB")
        user = os.getenv("RDS_USER")
        password = os.getenv("RDS_PASSWORD")
        missing = [k for k, v in {
            "RDS_HOST": host,
            "RDS_DB": database,
            "RDS_USER": user,
            "RDS_PASSWORD": password,
        }.items() if not v]
        if missing:
            raise ValueError(f"Missing required RDS env vars: {', '.join(missing)}")

        return (
            f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}?sslmode={rds_sslmode}"
        )

    raise ValueError(f"Unsupported mode: {mode}")


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
    model: Any,                             # ORM 클래스 (SilverStage1Main)
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
) -> tuple[int, int]:
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

    engine = create_engine(
        build_postgres_url(mode=mode, db_url=db_url, rds_sslmode=rds_sslmode),
        pool_pre_ping=True,  # 연결이 끊겼는지 미리 체크 (끊겼으면 재연결)
    )
    Base.metadata.create_all(engine) # 테이블이 없으면 CREATE TABLE 실행

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
        main_count = _flush_batches(session, SilverStage1Main, main_mappings, batch_size=batch_size)

    engine.dispose()
    return main_count
