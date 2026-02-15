from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import Numeric, cast, desc, func, select
from sqlalchemy.orm import Session

from api.models.silver_stage_1 import SilverStage1Main


def _build_dataset_filters(
    source_year: Optional[int],
    source_month: Optional[int],
) -> list:
    filters = []
    if source_year is not None:
        filters.append(SilverStage1Main.source_year == source_year)
    if source_month is not None:
        filters.append(SilverStage1Main.source_month == source_month)
    return filters


def get_latest_snapshot(session: Session) -> Optional[dict[str, int]]:
    source_year = SilverStage1Main.source_year.label("source_year")
    source_month = SilverStage1Main.source_month.label("source_month")
    stmt = (
        select(
            source_year,
            source_month,
        )
        .group_by(SilverStage1Main.source_year, SilverStage1Main.source_month)
        .order_by(desc(SilverStage1Main.source_year), desc(SilverStage1Main.source_month))
        .limit(1)
    )
    row = session.execute(stmt).mappings().first()
    return dict(row) if row else None


def get_region_overview(
    session: Session,
    source_year: Optional[int] = None,
    source_month: Optional[int] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    filters = _build_dataset_filters(source_year=source_year, source_month=source_month)

    row_count = func.count().label("row_count")
    lot_count = func.count(func.distinct(SilverStage1Main.pnu_code)).label("lot_count")
    store_row_count = func.count(SilverStage1Main.store_name).label("store_row_count")
    avg_official_land_price = func.round(
        cast(func.avg(SilverStage1Main.official_land_price), Numeric),
        2,
    ).label("avg_official_land_price")

    stmt = (
        select(
            SilverStage1Main.region.label("region"),
            row_count,
            lot_count,
            store_row_count,
            avg_official_land_price,
        )
        .where(*filters)
        .group_by(SilverStage1Main.region)
        .order_by(row_count.desc())
        .limit(limit)
    )
    return [dict(row) for row in session.execute(stmt).mappings().all()]


def get_sigungu_overview(
    session: Session,
    region: str,
    source_year: Optional[int] = None,
    source_month: Optional[int] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    filters = _build_dataset_filters(source_year=source_year, source_month=source_month)
    filters.append(SilverStage1Main.region == region)

    row_count = func.count().label("row_count")
    lot_count = func.count(func.distinct(SilverStage1Main.pnu_code)).label("lot_count")
    store_row_count = func.count(SilverStage1Main.store_name).label("store_row_count")

    stmt = (
        select(
            SilverStage1Main.sigungu.label("sigungu"),
            row_count,
            lot_count,
            store_row_count,
        )
        .where(*filters)
        .group_by(SilverStage1Main.sigungu)
        .order_by(row_count.desc())
        .limit(limit)
    )
    return [dict(row) for row in session.execute(stmt).mappings().all()]


def search_stores(
    session: Session,
    keyword: str,
    region: Optional[str] = None,
    source_year: Optional[int] = None,
    source_month: Optional[int] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    filters = _build_dataset_filters(source_year=source_year, source_month=source_month)
    filters.append(SilverStage1Main.store_name.is_not(None))
    filters.append(SilverStage1Main.store_name.ilike(f"%{keyword}%"))
    if region:
        filters.append(SilverStage1Main.region == region)

    stmt = (
        select(
            SilverStage1Main.source_year,
            SilverStage1Main.source_month,
            SilverStage1Main.region,
            SilverStage1Main.sigungu,
            SilverStage1Main.store_name,
            SilverStage1Main.owner_name,
            SilverStage1Main.road_address,
            SilverStage1Main.longitude,
            SilverStage1Main.latitude,
        )
        .where(*filters)
        .order_by(desc(SilverStage1Main.source_year), desc(SilverStage1Main.source_month))
        .limit(limit)
    )
    return [dict(row) for row in session.execute(stmt).mappings().all()]


def get_dashboard_points(
    session: Session,
    region: str,
    source_year: int,
    source_month: int,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    stmt = (
        select(
            SilverStage1Main.region,
            SilverStage1Main.sigungu,
            SilverStage1Main.legal_dong_name,
            SilverStage1Main.store_name,
            SilverStage1Main.owner_name,
            SilverStage1Main.official_land_price,
            SilverStage1Main.longitude,
            SilverStage1Main.latitude,
        )
        .where(
            SilverStage1Main.region == region,
            SilverStage1Main.source_year == source_year,
            SilverStage1Main.source_month == source_month,
            SilverStage1Main.longitude.is_not(None),
            SilverStage1Main.latitude.is_not(None),
        )
        .limit(limit)
    )
    return [dict(row) for row in session.execute(stmt).mappings().all()]
