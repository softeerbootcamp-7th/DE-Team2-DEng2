from sqlalchemy import create_engine
from core.settings import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
            f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
    return _engine