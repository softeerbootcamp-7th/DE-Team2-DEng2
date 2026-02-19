import os
import sys
import time
import re
import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Iterable
from datetime import date
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

# slack_utils.py 경로 추가 (기존 구조 유지)
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from data_pipeline.utils.slack_utils import SlackNotifier


# =========================
# ENV & Config
# =========================
load_dotenv()

@dataclass
class Config:
    # 수집 대상 URL
    url: str = "https://stat.molit.go.kr/portal/cate/statView.do?hRsId=58&hFormId=5498&hSelectId=5498&sStyleNum=2&sStart=202601&sEnd=202601&hPoint=00&hAppr=1&oFileName=&rFileName=&midpath="
    project_root: str = "data/bronze/chajoo_dist"
    retries: int = 3
    timeout_sec: int = 120
    headless: bool = True  # 파일 다운로드를 위해 가급적 False 권장
    parquet_compression: str = "snappy"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
    parquet_overwrite: bool = False
    force_run: bool = False
    sigungu_mapping_csv: str = "data/bronze/chajoo_dist/_work/SHP_CD_mapping.csv"

# =========================
# Logger & Helpers
# =========================
def build_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("chajoo_extract")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    # 로그 파일 부모 디렉토리 생성 보장
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger

def init_run_dirs(cfg: Config, yyyymm: str) -> dict:
    """used_yyyymm (YYYYMM) 기반으로 파티셔닝된 경로 생성"""
    base = Path(cfg.project_root)
    year = yyyymm[:4]
    month = yyyymm[4:6]

    # 공통 파티션 경로
    partition_path = f"year={year}/month={month}"

    paths = {
        "base": base,
        "work": base / "_work" / partition_path,
        "xlsx": base / "_work" / partition_path / "xlsx",
        "log_file": base / "_work" / partition_path / "run.log",
        "parquet": base / "parquet" / partition_path,
        "gold": Path("data/gold/chajoo_dist") / partition_path,
    }

    # 필요한 모든 디렉토리 생성
    for key in ["xlsx", "parquet", "gold"]:
        paths[key].mkdir(parents=True, exist_ok=True)

    return paths

# =========================
# Data Processing Helpers (New)
# =========================
def flatten_col(col: Iterable) -> str:
    """MultiIndex 컬럼을 단일 문자열로 평탄화 및 연속 중복 제거"""
    parts: List[str] = []
    for x in col:
        s = str(x).strip()
        if s.lower() in ("nan", "none", ""):
            continue
        if not parts or parts[-1] != s:
            parts.append(s)
    return "_".join(parts)

def pick_col(cands: List[str], columns: List[str]) -> str:
    """컬럼 리스트에서 후보 키워드를 포함하는 첫 번째 컬럼 반환"""
    for key in cands:
        hit = [c for c in columns if key in c]
        if hit:
            return hit[0]
    raise KeyError(f"필수 컬럼을 찾을 수 없습니다. 후보={cands}")

# =========================
# Selenium & Download Logic
# =========================
def build_driver(download_dir: Path, cfg: Config) -> webdriver.Chrome:
    opts = Options()

    # -------------------------
    # 환경별 분기 (핵심)
    # -------------------------
    if os.path.exists("/usr/bin/chromium"):
        # ✅ 컨테이너 (Airflow / Linux)
        opts.binary_location = "/usr/bin/chromium"
        service = Service("/usr/bin/chromedriver")
    else:
        # ✅ 로컬 (macOS / Windows)
        service = Service()  # Selenium 자동 탐색

    if cfg.headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,1000")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    prefs = {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver

def is_xlsx_zip(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            sig = f.read(2)
        return sig == b"PK" and zipfile.is_zipfile(path)
    except Exception:
        return False

def set_month_and_query(
    driver,
    logger,
    yyyymm: str | None = None,
    wait_timeout: int = 30,
):


    wait = WebDriverWait(driver, wait_timeout)

    # 기존 테이블 잡아두기 (조회 후 stale 체크용)
    old_table = None
    try:
        old_table = driver.find_element(By.XPATH, "//table")
    except Exception:
        pass

    # ✅ "기간선택" 시작/끝 월 select 2개 찾기 (양식/차트 select 같은 건 제외)
    month_re = re.compile(r"^\d{6}$")

    selects = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//select")))
    month_selects = []
    for s in selects:
        try:
            sel = Select(s)
            values = [o.get_attribute("value") for o in sel.options if o.get_attribute("value")]
            month_vals = [v for v in values if month_re.match(v)]
            # 월 옵션이 충분히 많은 select만 채택
            if len(month_vals) >= 12:
                month_selects.append((s, month_vals))
        except Exception:
            continue

    if len(month_selects) < 2:
        raise RuntimeError("기간선택(시작/끝) 월 select 2개를 찾지 못했습니다. XPath/페이지 구조 확인 필요.")

    # 보통 DOM 상 앞이 시작, 뒤가 끝
    start_el, start_vals = month_selects[0]
    end_el, end_vals = month_selects[1]
    start_sel = Select(start_el)
    end_sel = Select(end_el)

    # ✅ yyyymm 없으면 최신 월(첫 옵션) 자동
    if not yyyymm:
        yyyymm = start_vals[0]
        logger.info(f"월 미지정 → 최신 월 자동 선택: {yyyymm}")
    else:
        logger.info(f"월 설정(시작/끝 동일): {yyyymm}")

    # 시작/끝 모두 같은 월로 세팅
    start_sel.select_by_value(yyyymm)
    end_sel.select_by_value(yyyymm)

    # 조회 버튼 클릭
    query_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='조회']"))
    )
    query_btn.click()

    # 테이블 갱신 대기
    if old_table is not None:
        try:
            wait.until(EC.staleness_of(old_table))
        except Exception:
            pass

    wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
    time.sleep(1.0)  # 렌더링 여유

    return yyyymm


def wait_for_download(download_dir: Path, timeout: int) -> Path:
    end = time.time() + timeout
    while time.time() < end:
        # .xlsx 파일만 필터링 (임시 파일 .crdownload 제외)
        files = list(download_dir.glob("*.xlsx"))
        files = [f for f in files if not f.name.endswith(".crdownload")]

        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            # [중요] 파일 쓰기가 완료되었는지 체크 (크기 변화 관찰)
            prev_size = -1
            for _ in range(5): 
                curr_size = latest.stat().st_size
                if curr_size > 0 and curr_size == prev_size:
                    return latest
                prev_size = curr_size
                time.sleep(0.5)
        time.sleep(1.0)

def perform_download(
    driver,
    logger,
    cfg,
    download_dir: Path,
    yyyymm: str | None = None,
) -> Path:
    wait = WebDriverWait(driver, 30)

    for attempt in range(1, cfg.retries + 1):
        try:
            logger.info(f"다운로드 시도 ({attempt}/{cfg.retries})")

            if yyyymm:
                used_yyyymm = set_month_and_query(driver, logger, yyyymm)

            start_ts = time.time()
            main_handle = driver.current_window_handle

            btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[@title='파일 다운로드' or normalize-space()='파일 다운로드']")
            ))
            btn.click()

            # 모달 표시 대기
            wait.until(EC.visibility_of_element_located(
                (By.ID, "file-download-modal")
            ))

            # 🔥 다운로드 함수 직접 실행 (가장 안정적)
            driver.execute_script("download();")

            path = wait_for_download(download_dir, cfg.timeout_sec)
            if path.stat().st_mtime >= start_ts:
                logger.info(f"다운로드 성공: {path.name}")
                return path, used_yyyymm

        except Exception as e:
            logger.warning(f"시도 {attempt} 실패: {e}")
            if attempt < cfg.retries:
                driver.refresh()
                time.sleep(3)
            else:
                raise



# =========================
# Parquet Conversion
# =========================
SIDO_CODE_MAP = {
    "11": "서울",
    "26": "부산",
    "27": "대구",
    "28": "인천",
    "29": "광주",
    "30": "대전",
    "31": "울산",
    "36": "세종",
    "41": "경기",
    "42": "강원",
    "43": "충북",
    "44": "충남",
    "45": "전북",
    "46": "전남",
    "47": "경북",
    "48": "경남",
    "50": "제주",
}

def convert_xlsx_to_parquet(
    xlsx_path: Path,
    out_dir_root: Path,
    gold_dir_root: Path,
    cfg: "Config",
    logger: logging.Logger,
    yyyymm: str,   # "YYYYMM"
) -> str:
    """
    엑셀 -> 전처리 -> (year=YYYY/month=MM)/part.parquet 저장
    - yyyymm으로 파티션 저장 위치만 결정
    - parquet가 이미 있으면 skip (force_run 아니면)
    """

    logger.info(f"📦 전처리 시작: {xlsx_path.name} (yyyymm={yyyymm})")

    # --------------------------------------------------
    # 0. 파티션 경로/파일 미리 결정 + skip
    # --------------------------------------------------
    yyyymm = str(yyyymm).strip()
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"yyyymm 형식이 이상함: {yyyymm} (예: '202601')")

    year_str = yyyymm[:4]
    month_str = yyyymm[4:6]

    partition_dir = out_dir_root / f"year={year_str}" / f"month={month_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    gold_dir = gold_dir_root / f"year={year_str}" / f"month={month_str}"
    gold_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = partition_dir / "part.parquet"
    gold_parquet_path = gold_dir / "part.parquet"

    if parquet_path.exists() and not cfg.force_run:
        logger.info(f"⏭ Parquet 이미 존재하여 변환 스킵: {parquet_path}")
        return f"Skipped: {year_str}년 {month_str}월 데이터가 이미 존재합니다."

    # --------------------------------------------------
    # 1. 엑셀 읽기
    # --------------------------------------------------
    df = pd.read_excel(xlsx_path, header=[4, 5], engine="openpyxl")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [flatten_col(col) for col in df.columns]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    col_sido = pick_col(["시도명"], list(df.columns))
    col_sigungu = pick_col(["시군구"], list(df.columns))

    cargo_sales_cols = [c for c in df.columns if ("화물" in c and "영업용" in c)]
    if not cargo_sales_cols:
        raise KeyError("'화물 영업용' 컬럼을 찾을 수 없습니다.")
    col_cargo_sales = sorted(cargo_sales_cols, key=len)[0]

    # --------------------------------------------------
    # 2. 데이터 정제
    # --------------------------------------------------
    out = df[[col_sido, col_sigungu, col_cargo_sales]].copy()

    out[col_cargo_sales] = pd.to_numeric(
        out[col_cargo_sales].astype(str).str.replace(",", "").str.strip(),
        errors="coerce"
    )

    out = out[
        out[col_sigungu].notna() &
        (out[col_sigungu].astype(str).str.strip() != "계")
    ]

    result = (
        out[[col_sido, col_sigungu, col_cargo_sales]]
        .rename(columns={
            col_sido: "sido",
            col_sigungu: "sigungu",
            col_cargo_sales: "cargo_sales_count",
        })
    )

    result["sido"] = result["sido"].astype(str).str.strip()
    result["sigungu"] = result["sigungu"].astype(str).str.strip()

    # --------------------------------------------------
    # 🔥 3. 시군구 코드 매핑 (구 포함 안정 버전)
    # --------------------------------------------------
    logger.info("🔗 시군구 코드 매핑 시작")

    # SHP_CD 컬럼을 문자열(str)로 지정해서 읽기
    mapping_df = pd.read_csv(cfg.sigungu_mapping_csv, dtype={'SHP_CD': str})

    # merge 수행
    result = result.merge(
        mapping_df,
        on=["sido", "sigungu"],
        how="left"
    )

    if result["SHP_CD"].isna().any():
        failed = result[result["SHP_CD"].isna()][["sido", "sigungu"]].drop_duplicates()
        logger.warning(f"⚠ 일부 시군구 코드 매핑 실패 존재: {len(failed)}개\n{failed.to_string(index=False)}")

    # --------------------------------------------------
    # 4. Parquet 저장 (한 달치라고 가정하고 result 전체 저장)
    # --------------------------------------------------
    result.to_parquet(
        parquet_path,
        index=False,
        compression=getattr(cfg, "parquet_compression", "snappy")
    )

    result.to_parquet(
        gold_parquet_path,
        index=False,
        compression=getattr(cfg, "parquet_compression", "snappy")
    )
    logger.info(f"💾 Parquet 저장 완료: {parquet_path} (rows={len(result)})")

    return f"{year_str}년 {month_str}월 데이터 추출 완료"


# =========================
# Main Logic
# =========================
def main():
    cfg = Config()

    # 1. 대상 월 결정 (초기 로깅용)
    base_date = date.today()
    year, month = base_date.year, base_date.month
    target_yyyymm = f"{year - 1}12" if month == 1 else f"{year}{month-1:02d}"
    year = target_yyyymm[:4]
    month = target_yyyymm[4:6]

    # 2. 경로 초기화 (데이터 기준월 기반)
    paths = init_run_dirs(cfg, target_yyyymm)
    logger = build_logger(paths["log_file"])
    notifier = SlackNotifier(cfg.slack_webhook_url, "EXTRACT-차주분포", logger)

    logger.info(f"===== EXTRACT CHAJOO START (Target: {target_yyyymm}) =====")
    driver = None

    try:
        notifier.info("작업 시작", f"{year}년 {month}월 데이터 추출 시작")

        # [STEP 1] XLSX 확보
        # 해당 월의 폴더 내에 이미 엑셀이 있는지 확인
        existing_xlsx = list(paths["xlsx"].glob("*.xlsx"))

        if existing_xlsx and not cfg.force_run:
            logger.warning(f"⏭  {target_yyyymm} 로컬 엑셀 파일 사용 (Skip Download)")
            xlsx_path = max(existing_xlsx, key=lambda p: p.stat().st_mtime)
            used_yyyymm = target_yyyymm
        else:
            driver = build_driver(paths["xlsx"], cfg)
            driver.get(cfg.url)
            # download_dir를 파티션된 xlsx 경로로 전달
            xlsx_path, used_yyyymm = perform_download(
                driver,
                logger,
                cfg,
                paths["xlsx"],
                yyyymm=target_yyyymm,
            )

        # [STEP 2] Parquet 변환
        # convert_xlsx_to_parquet 내부에서도 paths['parquet']와 paths['gold']를 사용하도록 수정 가능하나, 
        # 기존 함수 구조를 유지하면서 인자만 전달합니다.
        status_msg = convert_xlsx_to_parquet(
            xlsx_path, 
            Path(cfg.project_root) / "parquet", # root 전달 (함수 내부에서 파티션 생성)
            Path("data/gold/chajoo_dist"),      # root 전달
            cfg, 
            logger, 
            yyyymm=used_yyyymm
        )

        notifier.success("작업 완료", f"{status_msg}")
        logger.info(f"===== SUCCESS ({status_msg}) =====")

    except Exception as e:
        logger.error(f"🚨 에러: {e}", exc_info=True)
        notifier.error("차주분포 데이터 처리 실패", e)
        sys.exit(1)
    finally:
        if driver: driver.quit()

def run_workflow(**kwargs):
    cfg = Config(headless=True)
    main()

if __name__ == "__main__":
    main()