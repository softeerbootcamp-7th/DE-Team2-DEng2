import os
import sys
import json
import time
import requests
import zipfile
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoAlertPresentException

from webdriver_manager.chrome import ChromeDriverManager

from dotenv import load_dotenv
load_dotenv()

# slack_utils.py를 찾기 위해 상위 경로 추가
sys.path.append(str(Path(__file__).resolve().parent.parent))
try:
    from data_pipeline.utils.slack_utils import SlackNotifier
except ImportError:
    # 파일이 없을 경우를 대비한 Mock 클래스
    class SlackNotifier:
        def __init__(self, *args, **kwargs): pass
        def info(self, *args): print(f"[INFO] {args}")
        def success(self, *args): print(f"[SUCCESS] {args}")
        def error(self, *args): print(f"[ERROR] {args}")

import logging
from logging.handlers import RotatingFileHandler

# =========================================================
# 시도 코드 및 설정
# =========================================================

SIDO_CODE = {
    "서울":"11","부산":"26","대구":"27","인천":"28",
    "광주":"29","대전":"30","울산":"31","세종":"36",
    "경기":"41","충북":"43","충남":"44","전남":"46",
    "경북":"47","경남":"48","제주":"50",
    "강원":"51","전북":"52",
}
# 시도 코드(Value)를 key로 하여 지역명(Key)을 찾는 맵
SIDO_NAME_MAP = {v: k for k, v in SIDO_CODE.items()}

@dataclass
class Config:
    ds_id: str = "12"
    cookie_path: str = "data_pipeline/extract/secrets/vworld_cookies.json"
    headless: bool = False
    work_dir: str = "data/tojiSoyuJeongbo/_work"
    out_dir: str = "data/tojiSoyuJeongbo/parquet"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    format_select: str = "CSV"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
    retries: int = 3
    retry_sleep_sec: int = 5
    timeout_sec: int = 60

# =========================================================
# 유틸리티 함수 (Logger, Date, URL, Driver)
# =========================================================

def build_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "run.log"
    logger = logging.getLogger("vworld")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def previous_month_range() -> Tuple[str, str]:
    today = dt.date.today()
    first = today.replace(day=1)
    last = first - dt.timedelta(days=1)
    return last.replace(day=1).strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d")

def build_query_url(cfg: Config, start_date: str, end_date: str) -> str:
    """
    vWorld 토지소유정보 데이터셋 조회를 위한 상세 URL 생성
    제시해주신 URL의 모든 파라미터를 포함하여 구성했습니다.
    """
    base_url = "https://www.vworld.kr/dtmk/dtmk_ntads_s002.do"

    params = {
        "pageSize": "10",
        "pageUnit": "25",
        "listPageIndex": "1",
        "gidsCd": "",
        "searchKeyword": "토지소유정보",  # URL 인코딩은 f-string이나 params 처리 시 자동 적용
        "svcCde": "NA",
        "gidmCd": "",
        "searchBrmCode": "",
        "datIde": "",
        "searchFrm": "",
        "dsId": cfg.ds_id,
        "searchSvcCde": "",
        "searchOrganization": "",
        "dataSetSeq": cfg.ds_id,  # dsId와 동일하게 12로 설정됨
        "searchTagList": "",
        "pageIndex": "1",
        "sortType": "00",
        "datPageIndex": "1",
        "datPageSize": "50",
        "startDate": start_date,
        "endDate": end_date,
        "sidoCd": "",
        "dsNm": "",
        "formatSelect": cfg.format_select
    }

    # query string 생성 (URL 인코딩 포함)
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])

    return f"{base_url}?{query_string}"



def get_driver(cfg: Config, download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--user-data-dir=/Users/apple/chrome-vworld-profile")
    if cfg.headless: opts.add_argument("--headless=new")
    prefs = {
        "download.default_directory": str(download_dir.absolute()),
        "download.prompt_for_download": False,
        "profile.default_content_setting_values.multiple_automatic_downloads": 1,
    }
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(cfg.timeout_sec)
    return driver

# =========================================================
# 쿠키 및 다운로드 로직
# =========================================================

def load_cookies(driver: webdriver.Chrome, cfg: Config) -> None:
    if not Path(cfg.cookie_path).exists():
        raise FileNotFoundError(f"쿠키 파일이 없습니다: {cfg.cookie_path}")
    
    cookies = json.loads(Path(cfg.cookie_path).read_text(encoding="utf-8"))
    for attempt in range(1, cfg.retries + 1):
        driver.get("https://www.vworld.kr/")
        time.sleep(2)
        for c in cookies:
            c = dict(c)
            for k in ["sameSite", "storeId", "hostOnly", "session"]: c.pop(k, None)
            if "expirationDate" in c:
                c["expiry"] = int(c["expirationDate"])
                c.pop("expirationDate", None)
            if "vworld.kr" in c.get("domain", ""):
                c["domain"] = ".vworld.kr"
                driver.add_cookie(c)
        driver.refresh()
        time.sleep(3)
        if "로그아웃" in driver.page_source: return
    raise RuntimeError("로그인 실패 (쿠키 만료 혹은 사이트 응답 없음)")

def wait_new_zip_created(download_dir: Path, before: set, timeout=600) -> Path:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if list(download_dir.glob("*.crdownload")):
            time.sleep(2)
            continue
        now = set(download_dir.glob("*.zip"))
        new_files = now - before
        if new_files:
            return max(new_files, key=lambda p: p.stat().st_mtime)
        time.sleep(2)
    raise TimeoutError("새 zip 파일 생성 대기 초과")

def click_each_row_download_one_by_one(driver, logger, zip_save_dir: Path, limit: int = 17):
    all_buttons = driver.find_elements(By.XPATH, "//button[normalize-space()='다운로드']")
    buttons = all_buttons[-limit:]
    logger.info(f"대상 데이터 {len(buttons)}건 다운로드 시작")
    saved = []
    for idx, btn in enumerate(buttons, start=1):
        before = set(zip_save_dir.glob("*.zip"))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        driver.execute_script("arguments[0].click();", btn)
        try:
            f = wait_new_zip_created(zip_save_dir, before)
            logger.info(f"✔️ [{idx}/{len(buttons)}] 다운로드 완료: {f.name}")
            saved.append(f)
        except Exception as e:
            logger.error(f"❌ [{idx}] 다운로드 실패: {e}")
        time.sleep(3)
    return saved

# =========================================================
# 데이터 변환 (CSV -> Parquet)
# =========================================================

def read_csv_to_table(csv_path: Path) -> pa.Table:
    df = None
    for enc in ("euc-kr", "cp949", "utf-8-sig"):
        try:
            df = pd.read_csv(csv_path, encoding=enc, low_memory=False)
            break
        except UnicodeDecodeError: continue
    if df is None: raise ValueError(f"CSV 인코딩 해석 실패: {csv_path}")
    
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype("string")
    return pa.Table.from_pandas(df, preserve_index=False)

def has_any_zip(zip_dir: Path) -> bool: return any(zip_dir.glob("*.zip"))
def has_any_csv(unzip_dir: Path) -> bool: return any(unzip_dir.glob("*.csv"))
def has_any_parquet(out_dir: Path, y: str, m: str) -> bool:
    base = out_dir / f"year={y}" / f"month={m}"
    return base.exists() and any(base.rglob("*.parquet"))

# =========================================================
# Main Execution Logic
# =========================================================

def run(cfg: Config, logger: logging.Logger) -> None:
    notifier = SlackNotifier(cfg.slack_webhook_url, "EXTRACT-토지소유정보", logger)
    start_date, end_date = (cfg.start_date, cfg.end_date) if cfg.start_date else previous_month_range()
    y, m = start_date.split("-")[:2]

    work_dir = Path(cfg.work_dir)
    zip_dir = work_dir / "per_row_zips" / f"{start_date}_to_{end_date}"
    unzip_dir = work_dir / "unzipped" / f"{start_date}_to_{end_date}"
    zip_dir.mkdir(parents=True, exist_ok=True)
    unzip_dir.mkdir(parents=True, exist_ok=True)

    driver = None
    success_count = 0
    is_skipped = False


    try:
        # [START]
        notifier.info("작업 시작", f"수집 기간: {start_date} ~ {end_date}")

        # 1️⃣ ZIP 다운로드 단계
        if has_any_zip(zip_dir):
            logger.warning("⏭ ZIP 파일이 이미 존재하여 다운로드를 건너뜁니다.")
        else:
            logger.info("🌐 드라이버 세션 시작 및 쿠키 로드 중...")
            driver = get_driver(cfg, zip_dir)
            load_cookies(driver, cfg)

            logger.info(f"🔍 데이터 조회 페이지 접속: {start_date} ~ {end_date}")
            driver.get(build_query_url(cfg, start_date, end_date))
            time.sleep(2)
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.XPATH, "//button[normalize-space()='다운로드']"))
            )

            saved_zips = click_each_row_download_one_by_one(driver, logger, zip_dir)
            logger.info(f"✅ ZIP 다운로드 완료: {len(saved_zips)}개 파일")

        # 2️⃣ UNZIP 단계
        if has_any_csv(unzip_dir):
            logger.warning("⏭ CSV 파일이 이미 존재하여 압축 해제를 건너뜁니다.")
        else:
            logger.info("🔓 압축 해제(Unzip) 시작...")
            for zp in zip_dir.glob("*.zip"):
                with zipfile.ZipFile(zp) as zf:
                    zf.extractall(unzip_dir)
            logger.info("✅ 모든 ZIP 파일 압축 해제 완료")

        # 3️⃣ PARQUET 변환 단계
        if has_any_parquet(Path(cfg.out_dir), y, m):
            logger.warning(f"⏭ {y}-{m} Parquet 결과가 이미 존재하여 변환을 건너뜁니다.")
        else:
            csv_files = list(unzip_dir.rglob("*.csv"))
            logger.info(f"📦 CSV -> Parquet 변환 시작 (총 {len(csv_files)}개)")

            success_count = 0
            for idx, csv in enumerate(csv_files, start=1):
                try:
                    sido_code = csv.stem.split("_")[2]
                    region = SIDO_NAME_MAP.get(sido_code, "Unknown")

                    out = Path(cfg.out_dir) / f"year={y}" / f"month={m}" / f"region={region}"
                    out.mkdir(parents=True, exist_ok=True)

                    target_path = out / f"{csv.stem}.parquet"
                    pq.write_table(read_csv_to_table(csv), target_path)
                    logger.info(f"   └─ [{idx}/{len(csv_files)}] {region} 완료")
                    success_count += 1

                except Exception as e:
                    logger.error(f"❌ {csv.name} 변환 중 개별 에러 발생: {e}")
                    # 개별 파일 실패는 logger에만 남기고 진행하거나, 중요하면 알림을 보냅니다.

            logger.info(f"✅ 변환 공정 종료 (성공: {success_count}/{len(csv_files)})")

        # [SUCCESS]
        notifier.success("작업 완료", f"{y}년 {m}월 데이터 적재에 성공했습니다. (성공: {success_count}건)")

    except Exception as e:
        # [CRITICAL ERROR]
        logger.error(f"🚨 파이프라인 중단됨: {str(e)}")
        notifier.error("토지소유정보 수집 중단됨", e)
        raise e

    finally:
        if driver:
            driver.quit()
            logger.info("🔒 드라이버 세션을 종료했습니다.")

def main():
    cfg = Config()
    logger = build_logger(Path(cfg.work_dir))
    logger.info("🚀 파이프라인 가동")
    run(cfg, logger)

if __name__ == "__main__":
    main()
