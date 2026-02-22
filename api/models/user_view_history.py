from typing import Optional
from sqlalchemy import Text, DateTime, Integer, func
from sqlalchemy.orm import Mapped, mapped_column
from api.models.base import Base

class UserViewHistory(Base):
    """
    단일 행만 유지하여 가장 최근에 조회한 지역을 저장하는 모델
    """
    __tablename__ = "user_view_history"

    # 항상 1번 행만 사용하도록 고정 ID 설정
    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)

    # 시군구 명칭 (예: "경기도 평택시")
    sigungu_nm: Mapped[str] = mapped_column("sigungu", Text, nullable=False)

    # 마지막 업데이트 시점
    updated_at: Mapped[DateTime] = mapped_column(
        "updated_at",
        DateTime,
        server_default=func.now(),
        onupdate=func.now(), # 수정될 때마다 자동으로 시간 갱신
        nullable=False
    )