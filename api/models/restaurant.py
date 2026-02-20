"""
식당 관련 테이블 ORM 모델
"""
from typing import Optional

from sqlalchemy import BigInteger, DateTime, Float, Index, Integer, PrimaryKeyConstraint, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class RestaurantMaster(Base):
    __tablename__ = "restaurant_master"
    __table_args__ = (
        PrimaryKeyConstraint("업체명", "도로명주소"),
        Index("idx_restaurant_location", "latitude", "longitude"),
    )

    legal_dong_name: Mapped[Optional[str]] = mapped_column("법정동명", Text)
    main_lot_no: Mapped[Optional[int]] = mapped_column("본번", BigInteger)
    owner_name: Mapped[Optional[str]] = mapped_column("지주", Text)
    sub_lot_no: Mapped[Optional[float]] = mapped_column("부번", Float)
    pnu_code: Mapped[Optional[int]] = mapped_column("PNU코드", BigInteger)
    road_address: Mapped[str] = mapped_column("도로명주소", Text, nullable=False)
    business_name: Mapped[str] = mapped_column("업체명", Text, nullable=False)
    representative: Mapped[Optional[str]] = mapped_column("대표자", Text)
    min_idle_area: Mapped[Optional[float]] = mapped_column("min_유휴부지면적", Float)
    idle_area: Mapped[Optional[float]] = mapped_column("유휴부지면적", Float)
    reliability_score: Mapped[Optional[float]] = mapped_column("신뢰도점수", Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    year: Mapped[Optional[str]] = mapped_column(Text)
    month: Mapped[Optional[str]] = mapped_column(Text)
    region: Mapped[Optional[str]] = mapped_column(Text)
    sigungu: Mapped[Optional[str]] = mapped_column(Text)


class RestaurantStatus(Base):
    __tablename__ = "restaurant_status"
    __table_args__ = (
        PrimaryKeyConstraint("업체명", "도로명주소"),
    )

    business_name: Mapped[str] = mapped_column("업체명", Text, nullable=False)
    road_address: Mapped[str] = mapped_column("도로명주소", Text, nullable=False)
    large_vehicle_access: Mapped[Optional[int]] = mapped_column(Integer)
    contract_status: Mapped[Optional[str]] = mapped_column(Text, server_default=text("'후보 식당'"))
    remarks: Mapped[Optional[str]] = mapped_column(Text)
    created_at = mapped_column(DateTime, server_default=func.now())
    updated_at = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
