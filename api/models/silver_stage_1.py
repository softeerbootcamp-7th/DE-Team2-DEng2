from typing import Optional

from sqlalchemy import DateTime, Float, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


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

