"""
차주 분포 데이터 (chajoo_dist) ORM 모델
"""
from typing import Optional

from sqlalchemy import BigInteger, Text, Integer
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class ChajooDist(Base):
    __tablename__ = "chajoo_dist"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sido: Mapped[str] = mapped_column(Text)
    sigungu: Mapped[str] = mapped_column(Text)
    year: Mapped[str] = mapped_column(Text)
    month: Mapped[str] = mapped_column(Text)
    SHP_CD: Mapped[str] = mapped_column(Text)

    cargo_sales_count: Mapped[Optional[int]] = mapped_column(BigInteger)
