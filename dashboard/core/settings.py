from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parents[2]

ENV_PATH = BASE_DIR / ".env"
load_dotenv(ENV_PATH)

DASHBOARD_DB_CONNECTION = os.getenv(
    "DASHBOARD_DB_CONNECTION", "local"
)  # "local" 또는 "aws"

SHP_PATH = BASE_DIR / "dashboard/data/sigungu_shp/bnd_sigungu_00.shp"
