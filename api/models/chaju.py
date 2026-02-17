from typing import Optional

from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint, func, text
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class RestaurantMaster(Base):
    __tablename__ = "restaurant_master"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    month: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    sido: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    sigungu: Mapped[str] = mapped_column(String(512), nullable=False, index=True)

    cargo_sales_count: Mapped[Optional[int]] = mapped_column(int)
    SHP_CD: Mapped[Optional[str]] = mapped_column(String(255))

    created_at = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )