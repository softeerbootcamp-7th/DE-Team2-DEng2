import os
import sys
import json
import time
import shutil
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


import logging
from logging.handlers import RotatingFileHandler

# =========================================================
# 시도 코드
# =========================================================

SIDO_CODE = {
    "서울":"11","부산":"26","대구":"27","인천":"28",
    "광주":"29","대전":"30","울산":"31","세종":"36",
    "경기":"41","충북":"43","충남":"44","전남":"46",
    "경북":"47","경남":"48","제주":"50",
    "강원":"51","전북":"52",
}
SIDO_NAME_MAP = {v: k for k, v in SIDO_CODE.items()}


# =========================================================
# Config
# =========================================================

@dataclass
class Config:
    ds_id: str = "12"
    cookie_path: str = "data_pipeline/extract/secrets/vworld_cookies.json"
    headless: bool = True
    work_dir: str = "data/tojiSoyuJeongbo/_work"
    out_dir: str = "data/tojiSoyuJeongbo/parquet"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    format_select: str = "CSV"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")



# =========================================================
# Logger
# =========================================================

def build_logger(log_dir: Path) -> logging.Logger:
    # 로그 저장 폴더가 없으면 생성
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "run.log"

    logger = logging.getLogger("vworld")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    # 1. 콘솔 출력
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. 파일 저장 (work_dir/run.log 에 저장됨)
    file_handler = RotatingFileHandler(
        log_path, 
        maxBytes=10*1024*1024, 
        backupCount=5, 
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def slack_notify(webhook_url: Optional[str], text: str, logger: logging.Logger):
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set")
        return
    try:
        r = requests.post(webhook_url, json={"text": text}, timeout=10)
        if r.status_code >= 400:
            logger.error(f"Slack notify failed: {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"Slack notify exception: {e}")


# =========================================================
# Date
# =========================================================

def previous_month_range() -> Tuple[str, str]:
    today = dt.date.today()
    first = today.replace(day=1)
    last = first - dt.timedelta(days=1)
    return last.replace(day=1).strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d")


# =========================================================
# URL (네가 준 구조 그대로)
# =========================================================

def build_query_url(cfg: Config, start_date: str, end_date: str) -> str:
    return (
        "https://www.vworld.kr/dtmk/dtmk_ntads_s002.do"
        "?pageSize=10&pageUnit=10&listPageIndex=1"
        "&gidsCd=&searchKeyword=%ED%86%A0%EC%A7%80%EC%86%8C%EC%9C%A0%EC%A0%95%EB%B3%B4"
        "&svcCde=NA&gidmCd=&searchBrmCode=&datIde=&searchFrm="
        f"&dsId={cfg.ds_id}"
        "&searchSvcCde=&searchOrganization=&dataSetSeq=12"
        "&searchTagList=&pageIndex=1&sortType=00"
        "&datPageIndex=1&datPageSize=25"
        f"&startDate={start_date}&endDate={end_date}"
        "&sidoCd=&dsNm="
        f"&formatSelect={cfg.format_select}"
    )


# =========================================================
# Selenium Driver
# =========================================================
# 수정된 get_driver 함수의 일부분
def get_driver(cfg: Config, download_dir: Path) -> webdriver.Chrome:
    opts = Options()

    # Chrome 프로필 고정
    opts.add_argument("--user-data-dir=/Users/apple/chrome-vworld-profile")

    # 자동화 감지 완화
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    if cfg.headless:
        opts.add_argument("--headless=new")

    # 🔥 핵심 설정 추가
    prefs = {
        "download.default_directory": str(download_dir.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        # 아래 줄을 추가하세요. 1은 허용, 2는 차단입니다.
        "profile.default_content_setting_values.multiple_automatic_downloads": 1,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )
    driver.set_page_load_timeout(60)
    return driver


# =========================================================
# Cookies
# =========================================================

def load_cookies(driver: webdriver.Chrome, cookie_path: str) -> None:
    cookies = json.loads(Path(cookie_path).read_text(encoding="utf-8"))

    driver.get("https://www.vworld.kr/")
    time.sleep(1)

    for c in cookies:
        c = dict(c)
        for k in ["sameSite", "storeId", "hostOnly", "session"]:
            c.pop(k, None)

        if "expirationDate" in c:
            c["expiry"] = int(c["expirationDate"])
            c.pop("expirationDate", None)

        if "vworld.kr" in c.get("domain", ""):
            c["domain"] = ".vworld.kr"
            c.setdefault("path", "/")
            driver.add_cookie(c)

    driver.refresh()
    time.sleep(2)

    if "로그아웃" not in driver.page_source:
        raise RuntimeError("로그인 실패 (쿠키 만료)")

def close_login_popup_if_any(driver, timeout=10):
    """
    로그인 직후 뜨는 modal popup을 닫는다.
    vWorld는 이걸 닫아야 내부 로그인 상태가 완료된다.
    """
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            # '닫기', '확인', '오늘 하루 보지 않기' 등
            btns = driver.find_elements(
                By.XPATH,
                "//button[contains(., '닫기') or "
                "contains(., '확인') or "
                "contains(., '오늘')]"
            )
            for b in btns:
                if b.is_displayed():
                    driver.execute_script("arguments[0].click();", b)
                    time.sleep(0.5)
                    return
        except Exception:
            pass
        time.sleep(0.5)


def wait_login_session_ready(driver, timeout=20):
    """
    UI가 아니라 쿠키 기준으로 로그인 상태 판단
    """
    t0 = time.time()
    while time.time() - t0 < timeout:
        cookies = driver.get_cookies()
        cookie_names = {c["name"] for c in cookies}

        # 🔥 vWorld 로그인 시 항상 존재하는 쿠키
        if any(name.lower().startswith(("sso", "login", "session")) for name in cookie_names):
            return

        time.sleep(0.5)

    # 디버깅용 로그
    raise RuntimeError(
        f"로그인 세션 안정화 실패 - 현재 쿠키: {cookie_names}"
    )

# =========================================================
# Download helpers
# =========================================================

def wait_download_finished(download_dir: Path, timeout=600) -> Path:
    t0 = time.time()
    last_size = -1
    stable = 0

    while time.time() - t0 < timeout:
        if list(download_dir.glob("*.crdownload")):
            time.sleep(1)
            continue

        files = list(download_dir.glob("*.zip"))
        if not files:
            time.sleep(1)
            continue

        f = max(files, key=lambda p: p.stat().st_mtime)
        size = f.stat().st_size

        if size == last_size and size > 0:
            stable += 1
        else:
            stable = 0

        last_size = size
        if stable >= 2:
            return f

        time.sleep(1)

    raise TimeoutError("다운로드 완료 대기 실패")


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    for i in range(1, 10000):
        p = path.with_name(f"{path.stem}__{i}{path.suffix}")
        if not p.exists():
            return p
    raise RuntimeError("파일명 중복 과다")


# =========================================================
# 핵심: 하나씩 다운로드
# =========================================================

def wait_new_zip_created(download_dir: Path, before: set, timeout=600) -> Path:
    t0 = time.time()
    while time.time() - t0 < timeout:
        # 다운로드 중이면 대기
        if list(download_dir.glob("*.crdownload")):
            time.sleep(1)
            continue

        now = set(download_dir.glob("*.zip"))
        new_files = now - before

        if new_files:
            # 새로 생긴 것만 반환
            return max(new_files, key=lambda p: p.stat().st_mtime)

        time.sleep(1)

    raise TimeoutError("새 zip 파일이 생성되지 않음")


def is_already_prefixed(fname: str) -> bool:
    # 시도명이 이미 맨 앞에 있으면 True
    for name in SIDO_CODE.keys():
        if fname.startswith(name + "_"):
            return True
    return False


def click_each_row_download_one_by_one(
    driver,
    logger,
    zip_save_dir: Path,
):
    buttons = driver.find_elements(By.XPATH, "//button[normalize-space()='다운로드']")
    saved = []

    for idx, btn in enumerate(buttons, start=1):
        logger.info(f"[{idx}/{len(buttons)}] 다운로드 시작")

        # 🔑 다운로드 전 상태 스냅샷
        before = set(zip_save_dir.glob("*.zip"))

        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", btn
        )
        driver.execute_script("arguments[0].click();", btn)

        # 🔑 반드시 새로 생긴 파일만 잡는다
        f = wait_new_zip_created(zip_save_dir, before)

        # 🔒 이미 prefix 있으면 스킵 (이중 방어)
        if is_already_prefixed(f.name):
            logger.warning(f"이미 처리된 파일 스킵: {f.name}")
            continue

        target = zip_save_dir / f"{f.name}"

        logger.info(f"✔️ 저장 완료: {target.name}")
        saved.append(target)

        # 사람처럼 쉬기
        time.sleep(3)

    return saved


# =========================================================
# CSV → Parquet
# =========================================================

def read_csv_to_table(csv_path: Path) -> pa.Table:
    for enc in ("euc-kr", "cp949", "utf-8-sig"):
        try:
            df = pd.read_csv(csv_path, encoding=enc, low_memory=False)
            break
        except UnicodeDecodeError:
            continue

    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype("string")

    return pa.Table.from_pandas(df, preserve_index=False)

def has_any_zip(zip_dir: Path) -> bool:
    return any(zip_dir.glob("*.zip"))

def has_any_csv(unzip_dir: Path) -> bool:
    return any(unzip_dir.glob("*.csv"))

def has_any_parquet(out_dir: Path, y: str, m: str) -> bool:
    base = out_dir / f"year={y}" / f"month={m}"
    return base.exists() and any(base.rglob("*.parquet"))


# =========================================================
# Main
# =========================================================

def run(cfg: Config, logger: logging.Logger) -> None:
    start_date, end_date = (
        (cfg.start_date, cfg.end_date)
        if cfg.start_date and cfg.end_date
        else previous_month_range()
    )
    
    # 🔔 알림 1: 작업 시작
    slack_notify(cfg.slack_webhook_url, f"🚀 V-World 데이터 수집 시작 ({start_date} ~ {end_date})", logger)

    work_dir = Path(cfg.work_dir)
    zip_dir = work_dir / "per_row_zips" / f"{start_date}_to_{end_date}"
    unzip_dir = work_dir / "unzipped" / f"{start_date}_to_{end_date}"

    zip_dir.mkdir(parents=True, exist_ok=True)
    unzip_dir.mkdir(parents=True, exist_ok=True)

    y, m = start_date.split("-")[:2]

    driver = None
    try:
        # 1️⃣ ZIP 다운로드 단계
        if has_any_zip(zip_dir):
            logger.info("⏭ ZIP 파일 존재 → 다운로드 단계 스킵")
        else:
            driver = get_driver(cfg, zip_dir)
            load_cookies(driver, cfg.cookie_path)

            driver.get(build_query_url(cfg, start_date, end_date))
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.XPATH, "//button[normalize-space()='다운로드']"))
            )

            saved = click_each_row_download_one_by_one(driver, logger, zip_dir)
            # 🔔 알림 2: 다운로드 완료
            slack_notify(cfg.slack_webhook_url, f"✅ {len(saved)}개의 ZIP 파일 다운로드 완료", logger)

        # 2️⃣ UNZIP 단계
        if not has_any_csv(unzip_dir):
            logger.info("🔓 unzip 시작")
            for zp in zip_dir.glob("*.zip"):
                with zipfile.ZipFile(zp) as zf:
                    zf.extractall(unzip_dir)

        # 3️⃣ PARQUET 단계
        if has_any_parquet(Path(cfg.out_dir), y, m):
            logger.info("⏭ Parquet 파일 존재 → 변환 단계 스킵")
        else:
            csv_files = list(unzip_dir.rglob("*.csv"))
            total_count = len(csv_files)
            logger.info(f"📦 CSV → Parquet 변환 시작 (총 {total_count}개)")

            for idx, csv in enumerate(csv_files, start=1):
                try:
                    sido_code = csv.stem.split("_")[2]
                    region = SIDO_NAME_MAP.get(sido_code, "알수없음")
                    
                    out = Path(cfg.out_dir) / f"year={y}" / f"month={m}" / f"region={region}"
                    out.mkdir(parents=True, exist_ok=True)

                    target_path = out / f"{csv.stem}.parquet"
                    pq.write_table(read_csv_to_table(csv), target_path)
                    logger.info(f"   └─ ✔️ 완료: {target_path.name}")

                except Exception as e:
                    # 🔔 알림 3: 개별 파일 변환 실패 (에러)
                    error_msg = f"❌ 변환 실패: {csv.name}\n에러: {str(e)}"
                    logger.error(error_msg)
                    slack_notify(cfg.slack_webhook_url, error_msg, logger)

            # 🔔 알림 4: 전체 공정 완료
            slack_notify(cfg.slack_webhook_url, f"✨ {y}년 {m}월 데이터 Parquet 변환 및 적재 완료!", logger)

        logger.info("✨ ALL DONE")

    except Exception as e:
        # 🔔 알림 5: 치명적 시스템 에러
        critical_error = f"🚨 시스템 중단 에러 발생!\n내용: {str(e)}"
        logger.error(critical_error)
        slack_notify(cfg.slack_webhook_url, critical_error, logger)
        raise e

# =========================================================
# Entrypoint
# =========================================================

def main():
    # 1. 설정 객체 생성
    cfg = Config()
    
    # 2. 설정을 바탕으로 로거 생성 (work_dir 경로 전달)
    # Config에 정의된 'data/tojiSoyuJeongbo/_work' 폴더에 run.log가 생깁니다.
    logger = build_logger(Path(cfg.work_dir))
    
    # 3. 실행
    logger.info("프로그램을 시작합니다.")
    run(cfg, logger)


if __name__ == "__main__":
    main()