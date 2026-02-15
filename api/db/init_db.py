import argparse
from typing import Optional

from sqlalchemy.engine import Engine

from api.db.session import create_engine_for_mode
from api.models.base import Base


def init_db(
    mode: str = "local",
    db_url: Optional[str] = None,
    rds_sslmode: str = "require",
    engine: Optional[Engine] = None,
) -> None:
    owns_engine = engine is None
    if engine is None:
        engine = create_engine_for_mode(mode=mode, db_url=db_url, rds_sslmode=rds_sslmode)

    try:
        Base.metadata.create_all(engine)
    finally:
        if owns_engine:
            engine.dispose()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize API database tables")
    parser.add_argument("--mode", choices=["local", "aws"], default="local")
    parser.add_argument("--db_url", type=str, default=None)
    parser.add_argument("--rds_sslmode", type=str, default="require")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    init_db(mode=args.mode, db_url=args.db_url, rds_sslmode=args.rds_sslmode)
    print(f"[SUCCESS] DB initialized for mode={args.mode}")
