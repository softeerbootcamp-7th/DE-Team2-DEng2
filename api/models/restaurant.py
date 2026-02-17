from typing import Optional

from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint, func, text
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class RestaurantMaster(Base):
    __tablename__ = "restaurant_master"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    month: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    restaurant_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    road_address: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    owner_name: Mapped[Optional[str]] = mapped_column(String(255))

    longitude: Mapped[Optional[float]] = mapped_column(Float)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    total_parking_area: Mapped[Optional[float]] = mapped_column(Float)
    wds: Mapped[Optional[int]] = mapped_column(int)

    created_at = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class RestaurantStatus(Base):
    __tablename__ = "restaurant_status"
    __table_args__ = (
        UniqueConstraint(
            "restaurant_name",
            "road_address",
            name="uq_restaurant_status_pair",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    restaurant_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    road_address: Mapped[str] = mapped_column(String(512), nullable=False, index=True)

    large_vehicle_access: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("3"),
    )
    contract_status: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        server_default=text("'후보 식당'"),
    )
    remarks: Mapped[Optional[str]] = mapped_column(String(2048))

    created_at = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )