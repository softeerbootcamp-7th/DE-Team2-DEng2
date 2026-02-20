import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.session import create_engine_for_mode
from core.settings import DASHBOARD_DB_CONNECTION

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine_for_mode(mode=DASHBOARD_DB_CONNECTION)
    return _engine
