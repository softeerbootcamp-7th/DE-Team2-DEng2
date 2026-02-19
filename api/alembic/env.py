import os
from logging.config import fileConfig

from dotenv import load_dotenv
from sqlalchemy import pool
from sqlalchemy import create_engine

from alembic import context


load_dotenv()

# Alembic Config 객체
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# 모든 ORM 모델 Import
from api.models.base import Base
import api.models.restaurant
import api.models.chajoo
import api.models.truckhelper
import api.models.report_history

target_metadata = Base.metadata


# DB URL 동적 생성
def _build_db_url() -> str:
    """
    ALEMBIC_DB_MODE 환경변수에 따라 DB URL을 결정
    - "local" (기본): 로컬 Docker PostgreSQL
    - "aws":          AWS RDS PostgreSQL
    """
    from api.session import build_postgres_url

    mode = os.getenv("ALEMBIC_DB_MODE", "local")
    rds_sslmode = os.getenv("ALEMBIC_RDS_SSLMODE", "require")

    return build_postgres_url(mode=mode, rds_sslmode=rds_sslmode)


def run_migrations_offline() -> None:
    """
    실제 DB에 연결하지 않고 마이그레이션 SQL 스크립트만 생성

    사용 예시:
      cd api && alembic upgrade head --sql > migration.sql
    """
    url = _build_db_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    실제 DB에 연결하여 마이그레이션을 실행

    사용 예시:
        # 로컬 DB
        cd api && alembic upgrade head

        # AWS RDS
        cd api && ALEMBIC_DB_MODE=aws alembic upgrade head
    """
    connectable = create_engine(
        _build_db_url(),
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
