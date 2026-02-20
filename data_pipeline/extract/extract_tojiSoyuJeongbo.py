import os
import sys
import json
import time
import zipfile
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


from dotenv import load_dotenv
load_dotenv()

# slack_utils.py를 찾기 위해 상위 경로 추가
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
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
    vworld_id: str = os.getenv("VWORLD_ID")
    vworld_pw: str = os.getenv("VWORLD_PW")
    cookie_path: str = os.path.join(
        "/opt/airflow/project" if os.path.exists("/opt/airflow/project") else os.getcwd(),
        "data_pipeline/extract/secrets/vworld_cookies.json"
    )
    headless: bool = True
    work_dir: str = "data/bronze/tojiSoyuJeongbo/_work"
    out_dir: str = "data/bronze/tojiSoyuJeongbo/parquet"
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

def get_driver(download_dir: Path, cfg: Config) -> webdriver.Chrome:
    opts = Options()

    # -------------------------
    # 환경별 분기 (기존 유지)
    # -------------------------
    chrome_bin = "/usr/bin/google-chrome"
    chromium_bin = "/usr/bin/chromium"

    if os.path.exists(chrome_bin) or os.path.exists(chromium_bin):
        opts.binary_location = chrome_bin if os.path.exists(chrome_bin) else chromium_bin
        driver_path = shutil.which("chromedriver") or "/usr/bin/chromedriver"
        service = Service(driver_path)
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
    else:
        service = Service()

    # -------------------------
    # 공통 옵션 및 Headless 설정
    # -------------------------
    if cfg.headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080") # 가시성 확보를 위해 FHD로 확장

    # 봇 감지 우회 설정
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # -------------------------
    # 🔥 다중 다운로드 및 자동 저장 설정
    # -------------------------
    prefs = {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,        # 다운로드 확인창 끄기
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,                 # 세이프 브라우징 (경고창 방지)
        # 1순위 핵심: 다중 파일 다운로드 자동 허용 (1=허용, 2=차단)
        "profile.default_content_setting_values.multiple_automatic_downloads": 1,
        # 추가 보안 설정: 자동 다운로드 허용
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=service, options=opts)

    # -------------------------
    # 🔥 [중요] Headless 모드 다운로드 경로 강제 허용
    # -------------------------
    # Chrome 정책상 Headless 모드에서는 prefs의 경로를 무시하는 경우가 많아 CDP 명령으로 직접 주입합니다.
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": str(download_dir.resolve())
    })

    driver.set_page_load_timeout(60)
    return driver
# =========================================================
# 로그인 (or 쿠키) 및 다운로드 로직
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

VWORLD_MAIN = "https://www.vworld.kr/v4po_main.do"
VWORLD_LOGIN = "https://www.vworld.kr/v4po_usrlogin_a001.do"


def is_logged_in_by_myportal(driver, wait) -> bool:
    """
    팝업을 닫지 않고,
    '마이포털' 텍스트 존재 여부로 로그인 판정
    """
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[normalize-space()='마이포털']")
            )
        )
        return True
    except TimeoutException:
        return False

def login_vworld(
    driver,
    cfg,
    logger: logging.Logger
) -> None:
    if not cfg.vworld_id or not cfg.vworld_pw:
        raise ValueError(".env 파일에 VWORLD_ID 또는 VWORLD_PW가 설정되지 않았습니다.")

    wait = WebDriverWait(driver, 20)

    # 1) 메인으로 가서 이미 로그인인지 먼저 확인
    driver.get(VWORLD_MAIN)
    time.sleep(1)

    if is_logged_in_by_myportal(driver, wait):
        logger.info("이미 로그인 상태입니다. (마이포털 확인)")
        return

    logger.info("로그인을 시도합니다...")

    # 2) 로그인 페이지
    driver.get(VWORLD_LOGIN)

    try:
        # 2. 아이디 입력 (보내주신 HTML: id="loginId")
        id_input = wait.until(EC.visibility_of_element_located((By.ID, "loginId")))
        id_input.clear()
        id_input.send_keys(cfg.vworld_id)

        # 3. 비밀번호 입력 (보내주신 HTML: id="loginPwd")
        pw_input = driver.find_element(By.ID, "loginPwd")
        pw_input.clear()
        pw_input.send_keys(cfg.vworld_pw)
        logger.info("ID/PW 입력 완료")
        time.sleep(1) # JS 처리 시간 확보
        # 4) 로그인 버튼 클릭
        try:
            # bg primary 클래스를 가진 button을 찾음
            login_btn = driver.find_element(By.CSS_SELECTOR, "button.bg.primary")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", login_btn)
            time.sleep(0.5)
            # 일반 클릭 시도
            login_btn.click()
        except:
            # 방법 B: JavaScript로 강제 클릭 (가장 확실)
            logger.info("일반 클릭 실패, JS 강제 클릭 시도")
            driver.execute_script("loginFnc.login('loginId','loginPwd','loginChk');")

        # 5) 로그인 직후 화면(/null 등)은 무시하고 메인으로 이동
        time.sleep(2)
        driver.get(VWORLD_MAIN)
        time.sleep(1)

        # 6) 마이포털 기준으로 최종 판정
        if not is_logged_in_by_myportal(driver, wait):
            raise RuntimeError("로그인 실패: 메인에서 '마이포털'을 찾지 못했습니다.")

        logger.info("✅ 로그인 성공 (마이포털 확인)")


    except Exception as e:
        logger.error(f"로그인 과정 중 에러 발생: {e}")
        raise

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

def run(cfg: Config, logger: logging.Logger, start_date: str, end_date: str, base_work_dir: Path) -> None:
    notifier = SlackNotifier(cfg.slack_webhook_url, "EXTRACT-토지소유정보", logger)
    y, m = start_date.split("-")[:2]

    # 하위 디렉토리 설정
    zip_dir = base_work_dir / "per_row_zips"
    unzip_dir = base_work_dir / "unzipped"

    zip_dir.mkdir(parents=True, exist_ok=True)
    unzip_dir.mkdir(parents=True, exist_ok=True)

    # 폴더 생성 (parents=True로 상위 연/월 폴더까지 한 번에 생성)
    zip_dir.mkdir(parents=True, exist_ok=True)
    unzip_dir.mkdir(parents=True, exist_ok=True)

    driver = None
    success_count = 0

    try:
        # [START]
        notifier.info("작업 시작", f"{y}년 {m}월 데이터 추출 시작")

        # 1️⃣ ZIP 다운로드 단계
        if has_any_zip(zip_dir):
            logger.warning("⏭ ZIP 파일이 이미 존재하여 다운로드를 건너뜁니다.")
        else:
            logger.info("🌐 드라이버 세션 시작 중...")
            driver = get_driver(zip_dir, cfg)

            login_vworld(driver, cfg, logger)

            # 7) 바로 크롤링 시작 페이지로 이동
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
            logger.info(f"Skipped: {y}년 {m}월 데이터가 이미 존재합니다")
            notifier.info("작업 완료", f"Skipped: {y}년 {m}월 데이터가 이미 존재합니다")
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
            notifier.success("작업 완료", f"{y}년 {m}월 데이터 추출 완료 (성공: {success_count}건)")

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

    # 1. 날짜 결정 (Config에 있으면 쓰고, 없으면 지난달)
    start_date, end_date = (cfg.start_date, cfg.end_date) if cfg.start_date else previous_month_range()
    y, m = start_date.split("-")[:2]

    # 2. 로그 및 작업 경로 설정: _work/year=YYYY/month=MM/
    base_work_dir = Path(cfg.work_dir) / f"year={y}" / f"month={m}"
    base_work_dir.mkdir(parents=True, exist_ok=True) # 로그를 남기기 위해 폴더 먼저 생성

    # 3. 로거 빌드 (해당 경로 안에 run.log 생성)
    logger = build_logger(base_work_dir)
    logger.info(f"🚀 파이프라인 가동 (대상 기간: {start_date} ~ {end_date})")

    # 4. 실행 (결정된 경로들을 run 함수에 전달)
    run(cfg, logger, start_date, end_date, base_work_dir)

def run_workflow(**kwargs):
    main()

if __name__ == "__main__":
    main()
