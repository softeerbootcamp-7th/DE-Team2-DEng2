from pathlib import Path
from dotenv import load_dotenv
import os

# ❌ parents[1] → dashboard
# BASE_DIR = Path(__file__).resolve().parents[1]

# ✅ parents[2] → 프로젝트 루트 (DE-Team2-DEng2)
BASE_DIR = Path(__file__).resolve().parents[2]

ENV_PATH = BASE_DIR / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("LOCAL_PG_HOST")
DB_PORT = os.getenv("LOCAL_PG_PORT")
DB_NAME = os.getenv("LOCAL_PG_DB")
DB_USER = os.getenv("LOCAL_PG_USER")
DB_PASSWORD = os.getenv("LOCAL_PG_PASSWORD")

SHP_PATH = BASE_DIR / "dashboard/data/sigungu_shp/bnd_sigungu_00.shp"
