import os
from typing import Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


load_dotenv()


def build_postgres_url(
    mode: str,
    db_url: Optional[str] = None,
    rds_sslmode: str = "require",
) -> str:
    if db_url:
        return db_url

    if mode == "local":
        host = os.getenv("LOCAL_PG_HOST", "postgres-local")
        port = os.getenv("LOCAL_PG_PORT", "5432")
        database = os.getenv("LOCAL_PG_DB", "deng2")
        user = os.getenv("LOCAL_PG_USER", "deng2")
        password = os.getenv("LOCAL_PG_PASSWORD", "deng2")
        return (
            f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}"
        )

    if mode == "aws":
        host = os.getenv("RDS_HOST")
        port = os.getenv("RDS_PORT", "5432")
        database = os.getenv("RDS_DB")
        user = os.getenv("RDS_USER")
        password = os.getenv("RDS_PASSWORD")
        missing = [
            key
            for key, value in {
                "RDS_HOST": host,
                "RDS_DB": database,
                "RDS_USER": user,
                "RDS_PASSWORD": password,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"Missing required RDS env vars: {', '.join(missing)}")

        return (
            f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}?sslmode={rds_sslmode}"
        )

    raise ValueError(f"Unsupported mode: {mode}")


def create_engine_for_mode(
    mode: str,
    db_url: Optional[str] = None,
    rds_sslmode: str = "require",
) -> Engine:
    return create_engine(
        build_postgres_url(mode=mode, db_url=db_url, rds_sslmode=rds_sslmode),
        pool_pre_ping=True,
    )
