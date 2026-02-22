"""
트럭헬퍼 공영차고지 데이터 ORM 모델
"""
from typing import Optional

from sqlalchemy import Float, PrimaryKeyConstraint, Text

from api.models.base import Base

try:
    from sqlalchemy.orm import Mapped, mapped_column

    class TruckhelperParkingArea(Base):
        __tablename__ = "truckhelper_parking_area"
        __table_args__ = (
            PrimaryKeyConstraint("공영차고지명", "주소"),
        )

        parking_name: Mapped[str] = mapped_column("공영차고지명", Text, nullable=False)
        address: Mapped[str] = mapped_column("주소", Text, nullable=False)

        lat: Mapped[Optional[float]] = mapped_column(Float)
        lon: Mapped[Optional[float]] = mapped_column(Float)

except ImportError:
    from sqlalchemy import Column

    class TruckhelperParkingArea(Base):  # type: ignore[no-redef]
        __tablename__ = "truckhelper_parking_area"
        __table_args__ = (
            PrimaryKeyConstraint("공영차고지명", "주소"),
        )

        parking_name = Column("공영차고지명", Text, nullable=False)
        address = Column("주소", Text, nullable=False)

        lat = Column(Float)
        lon = Column(Float)
