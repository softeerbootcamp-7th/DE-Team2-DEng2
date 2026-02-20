"""
차주 분포 데이터 (chajoo_dist) ORM 모델
"""
from typing import Optional

from sqlalchemy import BigInteger, Text, Integer, Float
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class ChajooDist(Base):
    __tablename__ = "chajoo_dist"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sido: Mapped[str] = mapped_column(Text)
    sigungu: Mapped[str] = mapped_column(Text)
    year: Mapped[str] = mapped_column(Text)
    month: Mapped[str] = mapped_column(Text)
    SHP_CD: Mapped[Optional[str]] = mapped_column(Text)

    cargo_sales_count: Mapped[Optional[int]] = mapped_column(BigInteger)


class ChajooHotspot(Base):
    __tablename__ = "cargo_zscore_hotspots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sigungu_cd: Mapped[str] = mapped_column(Text)
    oa_code: Mapped[Optional[int]] = mapped_column(BigInteger)
    lat: Mapped[Optional[float]] = mapped_column(Float)
    lon: Mapped[Optional[float]] = mapped_column(Float)
    value: Mapped[Optional[float]] = mapped_column(Float)
    z_score: Mapped[Optional[float]] = mapped_column(Float)
