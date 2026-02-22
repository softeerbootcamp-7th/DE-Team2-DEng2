"""
차주 분포 데이터 (chajoo_dist) ORM 모델
"""
from typing import Optional

from sqlalchemy import BigInteger, Text, Integer, Float

from api.models.base import Base

try:
    from sqlalchemy.orm import Mapped, mapped_column

    class ChajooDist(Base):
        __tablename__ = "chajoo_dist"

        id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
        sido: Mapped[str] = mapped_column(Text)
        sigungu: Mapped[str] = mapped_column(Text)
        year: Mapped[str] = mapped_column(Text)
        month: Mapped[str] = mapped_column(Text)
        SHP_CD: Mapped[Optional[str]] = mapped_column(Text)
        score: Mapped[Optional[float]] = mapped_column("전략적_중요도", Float)
        cargo_count: Mapped[Optional[int]] = mapped_column(BigInteger)

except ImportError:
    from sqlalchemy import Column

    class ChajooDist(Base):  # type: ignore[no-redef]
        __tablename__ = "chajoo_dist"

        id = Column(Integer, primary_key=True, autoincrement=True)
        sido = Column(Text)
        sigungu = Column(Text)
        year = Column(Text)
        month = Column(Text)
        SHP_CD = Column(Text)
        score = Column("전략적_중요도", Float)
        cargo_count = Column(BigInteger)
