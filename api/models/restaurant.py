from typing import Optional
from sqlalchemy import Float, Integer, PrimaryKeyConstraint, Text
from api.models.base import Base

try:
    from sqlalchemy.orm import Mapped, mapped_column

    class Restaurant(Base):
        __tablename__ = "restaurant"
        __table_args__ = (
            PrimaryKeyConstraint(
                "업체명", "도로명주소", "year", "month", "week",
                name="pk_restaurant"
            ),
        )

        # 행정 구역 정보
        sigungu: Mapped[Optional[str]] = mapped_column("sigungu", Text)
        region: Mapped[Optional[str]] = mapped_column("region", Text)

        # 핵심 분석 지표 (분석 결과 데이터)
        total_score: Mapped[Optional[float]] = mapped_column("총점", Float)
        success_probability: Mapped[Optional[float]] = mapped_column("영업_적합도", Float)
        profitability: Mapped[Optional[float]] = mapped_column("수익성", Float)

        # 기본 식당 정보 (PK 포함)
        business_name: Mapped[str] = mapped_column("업체명", Text, nullable=False)
        road_address: Mapped[str] = mapped_column("도로명주소", Text, nullable=False)

        # 부지 및 위치 정보
        idle_area: Mapped[Optional[float]] = mapped_column("유휴부지_면적", Float)
        longitude: Mapped[Optional[float]] = mapped_column("longitude", Float)
        latitude: Mapped[Optional[float]] = mapped_column("latitude", Float)

        # 시계열 정보
        year: Mapped[Optional[int]] = mapped_column("year", Integer, nullable=False)
        month: Mapped[Optional[int]] = mapped_column("month", Integer, nullable=False)
        week: Mapped[Optional[int]] = mapped_column("week", Integer, nullable=False)

        # 추가된 접근성 및 관리 컬럼
        truck_accessibility: Mapped[Optional[int]] = mapped_column("주차_적합도", Integer)
        contract_status: Mapped[str] = mapped_column(
            "contract_status",
            Text,
            server_default="'후보'",
            nullable=False,
        )
        remarks: Mapped[Optional[str]] = mapped_column("remarks", Text)

except ImportError:
    from sqlalchemy import Column

    class Restaurant(Base):  # type: ignore[no-redef]
        __tablename__ = "restaurant"
        __table_args__ = (
            PrimaryKeyConstraint(
                "업체명", "도로명주소", "year", "month", "week",
                name="pk_restaurant"
            ),
        )

        sigungu = Column("sigungu", Text)
        region = Column("region", Text)

        total_score = Column("총점", Float)
        success_probability = Column("영업_적합도", Float)
        profitability = Column("수익성", Float)

        business_name = Column("업체명", Text, nullable=False)
        road_address = Column("도로명주소", Text, nullable=False)

        idle_area = Column("유휴부지_면적", Float)
        longitude = Column("longitude", Float)
        latitude = Column("latitude", Float)

        year = Column("year", Integer, nullable=False)
        month = Column("month", Integer, nullable=False)
        week = Column("week", Integer, nullable=False)

        truck_accessibility = Column("주차_적합도", Integer)
        contract_status = Column(
            "contract_status",
            Text,
            server_default="'후보'",
            nullable=False,
        )
        remarks = Column("remarks", Text)
