import os
import sys
import time
import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Iterable

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

# slack_utils.py 경로 추가 (기존 구조 유지)
sys.path.append(str(Path(__file__).resolve().parent.parent))
from slack_utils import SlackNotifier

# =========================
# ENV & Config
# =========================
load_dotenv()

@dataclass
class Config:
    # 수집 대상 URL
    url: str = "https://stat.molit.go.kr/portal/cate/statView.do?hRsId=58&hFormId=5498&hSelectId=5498&sStyleNum=2&sStart=202601&sEnd=202601&hPoint=00&hAppr=1&oFileName=&rFileName=&midpath="
    project_root: str = "data/chajoo_dist"
    retries: int = 3
    timeout_sec: int = 120
    headless: bool = True  # 파일 다운로드를 위해 가급적 False 권장
    parquet_compression: str = "snappy"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
    parquet_overwrite: bool = False
    force_run: bool = False
    sigungu_mapping_csv: str = "data/chajoo_dist/_work/csv/국토교통부_법정동코드_20250805.csv"

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
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    
    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger

def init_run_dirs(cfg: Config) -> dict:
    base = Path(cfg.project_root)
    paths = {
        "base": base,
        "work": base / "_work",
        "xlsx": base / "_work" / "xlsx",
        "parquet": base / "parquet",
        "log_file": base / "_work" / "run.log",
    }
    paths["xlsx"].mkdir(parents=True, exist_ok=True)
    paths["parquet"].mkdir(parents=True, exist_ok=True)
    paths["work"].mkdir(parents=True, exist_ok=True)
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

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    return driver

def is_xlsx_zip(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            sig = f.read(2)
        return sig == b"PK" and zipfile.is_zipfile(path)
    except Exception:
        return False

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

def perform_download(driver: webdriver.Chrome, logger: logging.Logger, cfg: Config, download_dir: Path) -> Path:
    wait = WebDriverWait(driver, 30)
    
    for attempt in range(1, cfg.retries + 1):
        try:
            logger.info(f"다운로드 시도 ({attempt}/{cfg.retries})")
            start_ts = time.time()
            main_handle = driver.current_window_handle

            # 1. 메인 버튼 클릭
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='파일 다운로드' or normalize-space()='파일 다운로드']")))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.5)
            btn.click()

            # 2. 새 창(팝업) 확인
            time.sleep(2)
            handles = driver.window_handles
            if len(handles) > 1:
                popup_handle = [h for h in handles if h != main_handle][0]
                driver.switch_to.window(popup_handle)
                
                # 팝업 내 다운로드 버튼 클릭
                dl_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'다운로드')] | //a[contains(.,'다운로드')]")))
                dl_btn.click()
                time.sleep(1)
                driver.close()
                driver.switch_to.window(main_handle)
            else:
                # 모달 형태일 경우
                modal_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'modal')]//button[contains(.,'다운로드')]")))
                modal_btn.click()

            # 3. 파일 대기
            path = wait_for_download(download_dir, cfg.timeout_sec)
            if path.stat().st_mtime >= start_ts:
                logger.info(f"다운로드 성공: {path.name}")
                return path
            
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
    cfg: Config,
    logger: logging.Logger
) -> str:

    logger.info(f"📦 전처리 시작: {xlsx_path.name}")

    # --------------------------------------------------
    # 1. 엑셀 읽기
    # --------------------------------------------------
    df = pd.read_excel(xlsx_path, header=[4, 5], engine="openpyxl")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [flatten_col(col) for col in df.columns]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    col_month = pick_col(["월(Monthly)", "월"], list(df.columns))
    col_sido = pick_col(["시도명"], list(df.columns))
    col_sigungu = pick_col(["시군구"], list(df.columns))

    cargo_sales_cols = [c for c in df.columns if ("화물" in c and "영업용" in c)]
    if not cargo_sales_cols:
        raise KeyError("'화물 영업용' 컬럼을 찾을 수 없습니다.")
    col_cargo_sales = sorted(cargo_sales_cols, key=len)[0]

    # --------------------------------------------------
    # 2. 데이터 정제
    # --------------------------------------------------
    out = df[[col_month, col_sido, col_sigungu, col_cargo_sales]].copy()

    out[col_cargo_sales] = pd.to_numeric(
        out[col_cargo_sales].astype(str).str.replace(",", "").str.strip(),
        errors="coerce"
    )

    out = out[
        out[col_sigungu].notna() &
        (out[col_sigungu].astype(str).str.strip() != "계")
    ]

    result = (
        out.groupby([col_month, col_sido, col_sigungu], dropna=False, as_index=False)[col_cargo_sales]
        .sum()
        .rename(columns={
            col_month: "year_month",
            col_sido: "sido",
            col_sigungu: "sigungu",
            col_cargo_sales: "cargo_sales_count",
        })
    )

    # --------------------------------------------------
    # 🔥 3. 시군구 코드 매핑 (구 포함 안정 버전)
    # --------------------------------------------------
    logger.info("🔗 시군구 코드 매핑 시작")

    mapping_df = pd.read_csv(
        cfg.sigungu_mapping_csv,
        encoding="euc-kr"
    )

    mapping_df["법정동코드"] = mapping_df["법정동코드"].astype(str)

    # 5자리 시군구 코드
    mapping_df["sigungu_code"] = mapping_df["법정동코드"].str[:5]

    # 시도 코드 추출
    mapping_df["sido_code"] = mapping_df["법정동코드"].str[:2]
    mapping_df["sido"] = mapping_df["sido_code"].map(SIDO_CODE_MAP)

    # 🔥 법정동명 분리
    name_split = mapping_df["법정동명"].str.split()

    # 시도명
    mapping_df["sido"] = mapping_df["sido"]

    # 🔥 시군구명 생성 (구 포함)
    def build_sigungu(parts):
        if len(parts) >= 3:
            return parts[1] + parts[2]  # 고양시 + 덕양구
        elif len(parts) >= 2:
            return parts[1]
        return None

    mapping_df["sigungu"] = name_split.apply(build_sigungu)

    # 🔥 공백 제거 (정규화)
    def normalize(x):
        if pd.isna(x):
            return x
        return str(x).replace(" ", "").strip()

    mapping_df["sigungu"] = mapping_df["sigungu"].apply(normalize)
    mapping_df["sido"] = mapping_df["sido"].apply(normalize)

    mapping_df = (
        mapping_df[["sigungu_code", "sido", "sigungu"]]
        .drop_duplicates()
        .dropna()
    )

    mapping_df = pd.concat([
        mapping_df,
        pd.DataFrame({
            "sigungu_code": ["36110"],
            "sido": ["세종"],
            "sigungu": ["세종특별자치시"]
        })
    ])


    # --------------------------------------------------
    # 🔥 result도 동일 정규화
    # --------------------------------------------------
    result["sido"] = result["sido"].apply(normalize)
    result["sigungu"] = result["sigungu"].apply(normalize)

    # merge
    result = result.merge(
        mapping_df,
        on=["sido", "sigungu"],
        how="left"
    )

    if result["sigungu_code"].isna().any():
        logger.warning("⚠ 일부 시군구 코드 매핑 실패 존재")

    # --------------------------------------------------
    # 4. 월별 계층 저장
    # --------------------------------------------------
    result["year_month_dt"] = pd.to_datetime(
        result["year_month"].astype(str).str.replace("/", "-")
    )

    unique_months = result["year_month_dt"].unique()
    saved_count = 0

    for target_dt in unique_months:

        ts = pd.Timestamp(target_dt)
        year_str = ts.strftime("%Y")
        month_str = ts.strftime("%m")

        partition_dir = out_dir_root / f"year={year_str}" / f"month={month_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        target_path = partition_dir / "part.parquet"

        if target_path.exists() and not cfg.parquet_overwrite and not cfg.force_run:
            continue

        monthly_df = result[result["year_month_dt"] == target_dt].copy()
        monthly_df = monthly_df.drop(columns=["year_month", "year_month_dt"])

        monthly_df.to_parquet(
            target_path,
            index=False,
            compression=cfg.parquet_compression
        )

        saved_count += 1

    logger.info(f"✅ 계층형 저장 완료: {saved_count}개 월 저장")

    return f"Processed {len(unique_months)} months, Updated {saved_count} files"

def convert_xlsx_to_parquet(
    xlsx_path: Path,
    out_dir_root: Path,
    cfg: Config,
    logger: logging.Logger
) -> str:

    logger.info(f"📦 전처리 시작: {xlsx_path.name}")

    # --------------------------------------------------
    # 1. 엑셀 읽기
    # --------------------------------------------------
    df = pd.read_excel(xlsx_path, header=[4, 5], engine="openpyxl")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [flatten_col(col) for col in df.columns]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    col_month = pick_col(["월(Monthly)", "월"], list(df.columns))
    col_sido = pick_col(["시도명"], list(df.columns))
    col_sigungu = pick_col(["시군구"], list(df.columns))

    cargo_sales_cols = [c for c in df.columns if ("화물" in c and "영업용" in c)]
    if not cargo_sales_cols:
        raise KeyError("'화물 영업용' 컬럼을 찾을 수 없습니다.")
    col_cargo_sales = sorted(cargo_sales_cols, key=len)[0]

    # --------------------------------------------------
    # 2. 데이터 정제
    # --------------------------------------------------
    out = df[[col_month, col_sido, col_sigungu, col_cargo_sales]].copy()

    out[col_cargo_sales] = pd.to_numeric(
        out[col_cargo_sales].astype(str).str.replace(",", "").str.strip(),
        errors="coerce"
    )

    out = out[
        out[col_sigungu].notna() &
        (out[col_sigungu].astype(str).str.strip() != "계")
    ]

    result = (
        out.groupby([col_month, col_sido, col_sigungu], as_index=False)[col_cargo_sales]
        .sum()
        .rename(columns={
            col_month: "year_month",
            col_sido: "sido",
            col_sigungu: "sigungu",
            col_cargo_sales: "cargo_sales_count",
        })
    )

    # 문자열 정리
    result["sido"] = result["sido"].astype(str).str.strip()
    result["sigungu"] = result["sigungu"].astype(str).str.strip()

    # --------------------------------------------------
    # 🔥 3. SHP_CD 매핑
    # --------------------------------------------------
    logger.info("🔗 SHP_CD 매핑 시작")

    shp_mapping_path = Path("data/chajoo_dist/_work/csv/SHP_CD_mapping.csv")

    shp_df = pd.read_csv(
        shp_mapping_path,
        dtype={"SHP_CD": str}
    )

    shp_df["sido"] = shp_df["sido"].astype(str).str.strip()
    shp_df["sigungu"] = shp_df["sigungu"].astype(str).str.strip()
    shp_df["SHP_CD"] = shp_df["SHP_CD"].astype(str)

    result = result.merge(
        shp_df[["sido", "sigungu", "SHP_CD"]],
        on=["sido", "sigungu"],
        how="left"
    )

    if result["SHP_CD"].isna().any():
        logger.warning("⚠ SHP_CD 매핑 실패 존재")

    # --------------------------------------------------
    # 4. year / month 분리
    # --------------------------------------------------
    result["year_month_dt"] = pd.to_datetime(
        result["year_month"].astype(str).str.replace("/", "-")
    )

    unique_months = result["year_month_dt"].unique()
    saved_count = 0

    for target_dt in unique_months:

        ts = pd.Timestamp(target_dt)
        year_str = ts.strftime("%Y")
        month_str = ts.strftime("%m")

        partition_dir = out_dir_root / f"year={year_str}" / f"month={month_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = partition_dir / "part.parquet"
        csv_path = partition_dir / "part.csv"

        monthly_df = result[result["year_month_dt"] == target_dt].copy()
        monthly_df = monthly_df.drop(columns=["year_month_dt"])

        # Parquet
        if not parquet_path.exists() or cfg.force_run:
            monthly_df.to_parquet(
                parquet_path,
                index=False,
                compression=cfg.parquet_compression
            )
            logger.info(f"💾 Parquet 저장 완료: {parquet_path}")

        saved_count += 1

    logger.info(f"✅ 계층형 저장 완료: {saved_count}개 월 처리")

    return f"Processed {len(unique_months)} months"


# =========================
# Main Logic
# =========================
def main():
    cfg = Config()
    paths = init_run_dirs(cfg)
    logger = build_logger(paths["log_file"])
    notifier = SlackNotifier(cfg.slack_webhook_url, "EXTRACT-차주분포", logger)

    logger.info("===== EXTRACT CHAJOO START =====")
    driver = None

    try:
        notifier.info("작업 시작", "국토부 차주분포 데이터 전처리 프로세스 시작")

        # [STEP 1] XLSX 확보
        existing_xlsx = list(paths["xlsx"].glob("*.xlsx"))
        if existing_xlsx and not cfg.force_run:
            logger.warning("⏭ 로컬 엑셀 파일 사용 (Skip Download)")
            xlsx_path = max(existing_xlsx, key=lambda p: p.stat().st_mtime)
        else:
            driver = build_driver(paths["xlsx"], cfg)
            driver.get(cfg.url)
            xlsx_path = perform_download(driver, logger, cfg, paths["xlsx"])

        # [STEP 2] Parquet 변환 (변경된 함수 호출)
        status_msg = convert_xlsx_to_parquet(xlsx_path, paths["parquet"], cfg, logger)

        # 완료 알림
        notifier.success("작업 완료", f"결과: {status_msg}")
        logger.info(f"===== SUCCESS ({status_msg}) =====")

    except Exception as e:
        logger.error(f"🚨 에러: {e}", exc_info=True)
        notifier.error("차주분포 데이터 처리 실패", e)
        sys.exit(1)
    finally:
        if driver: driver.quit()

if __name__ == "__main__":
    main()