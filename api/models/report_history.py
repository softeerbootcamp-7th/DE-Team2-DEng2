"""
식당 신고 이력 데이터 ORM 모델
"""
from datetime import date
from sqlalchemy import Date, PrimaryKeyConstraint, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class ReportHistory(Base):
    __tablename__ = "report_history"
    __table_args__ = (
        PrimaryKeyConstraint("업체명", "도로명주소"),
    )

    company_name: Mapped[str] = mapped_column("업체명", Text, nullable=False)
    road_address: Mapped[str] = mapped_column("도로명주소", Text, nullable=False)

    # 신고일자: 기본값으로 현재 날짜(server_default)를 사용합니다.
    report_date: Mapped[date] = mapped_column(
        "신고일자", 
        Date, 
        server_default=func.current_date(),
        nullable=False
    )