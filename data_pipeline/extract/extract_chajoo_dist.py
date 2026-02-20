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

SIDO_WEIGHT = 0.7
SIGUNGU_WEIGHT = 0.3

# slack_utils.py 경로 추가 (기존 구조 유지)
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from data_pipeline.utils.slack_utils import SlackNotifier


# =========================
# ENV & Config
# =========================
load_dotenv()

@dataclass
class Config:
    # 1. 차주분포 (기존)
    url_chajoo: str = "https://stat.molit.go.kr/portal/cate/statView.do?hRsId=58&hFormId=5498&hSelectId=5498&sStyleNum=2&sStart=202601&sEnd=202601&hPoint=00&hAppr=1"
    # 2. 새로운 URL (자동차 등록현황 등)
    url_area: str = "https://stat.molit.go.kr/portal/cate/statView.do?hRsId=24&hFormId=2300&hDivEng=&month_yn="

    project_root: str = "data/bronze/chajoo_dist"
    retries: int = 3
    timeout_sec: int = 120
    headless: bool = True
    sigungu_mapping_csv: str = "data/bronze/chajoo_dist/_work/SHP_CD_mapping.csv"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
    force_run: bool = False

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
    """used_yyyymm (YYYYMM) 기반으로 파티셔닝된 경로 생성 및 세부 데이터 폴더 분리"""
    base = Path(cfg.project_root)
    year = yyyymm[:4]
    month = yyyymm[4:6]

    # 공통 파티션 경로
    partition_path = f"year={year}/month={month}"
    work_base = base / "_work" / partition_path

    paths = {
        "base": base,
        "work": work_base,
        # ✅ 요청하신 세부 경로 추가
        "chajoo_xlsx": work_base / "chajoo",
        "log_file": work_base / "run.log",
        "parquet": base / "parquet" / partition_path,
        "gold": Path("data/gold/chajoo_dist") / partition_path,
    }

    # 필요한 모든 디렉토리 생성
    # area_xlsx와 chajoo_xlsx를 포함하여 생성
    for key in ["chajoo_xlsx", "parquet", "gold"]:
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

def set_period_and_query(
    driver,
    logger,
    yyyymm: str | None = None,
    wait_timeout: int = 30,
):
    wait = WebDriverWait(driver, wait_timeout)

    # 1. 기존 테이블 및 select 엘리먼트 확보
    old_table = None
    try:
        old_table = driver.find_element(By.XPATH, "//table")
    except Exception:
        pass

    period_re = re.compile(r"^(\d{6}|\d{4})$")
    selects = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//select")))
    period_selects = []

    for s in selects:
        try:
            sel = Select(s)
            values = [o.get_attribute("value") for o in sel.options if o.get_attribute("value")]
            month_vals = [v for v in values if period_re.match(v)]
            # 월 옵션이 충분히 많은 select만 채택
            if len(month_vals) >= 10:
                period_selects.append((s, month_vals))
        except Exception:
            continue

    if len(period_selects) < 2:
        raise RuntimeError("기간선택(시작/끝) select 상자를 찾지 못했습니다.")

    start_el, start_vals = period_selects[0]
    end_el, _ = period_selects[1]
    
    # 가용 옵션 역순 정렬 (최신순)
    available_vals = sorted(start_vals, reverse=True)
    
    # 3. 데이터 형식 판별 및 요청값 가공 (YYYYMM -> YYYY)
    is_yearly = len(available_vals[0]) == 4
    requested_val = yyyymm[:4] if is_yearly and yyyymm else yyyymm

    # 4. 🔥 폴백 로직: 2026을 요청했으나 최신이 2024인 경우 대응
    if not requested_val:
        target_val = available_vals[0]
        logger.info(f"기간 미지정 -> 최신 항목 자동 선택: {target_val}")
    elif requested_val in available_vals:
        target_val = requested_val
        logger.info(f"요청 기간 설정: {target_val}")
    else:
        # 요청값보다 작거나 같은 값 중 가장 최신값 선택 (2026 요청 시 2024 선택)
        fallback_vals = [v for v in available_vals if v <= requested_val]
        target_val = fallback_vals[0] if fallback_vals else available_vals[0]
        logger.warning(f"⚠️ {requested_val} 데이터 없음 -> 가용 최신 데이터 {target_val}로 대체")

    # 5. 값 선택
    Select(start_el).select_by_value(target_val)
    Select(end_el).select_by_value(target_val)

    # 6. 조회 버튼 클릭 (Intercepted 에러 방지를 위해 JS 클릭 권장)
    query_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='조회']"))
    )

    # 화면 가림막(mu-dialog-background) 무시하고 클릭
    driver.execute_script("arguments[0].click();", query_btn)

    # 7. 테이블 갱신 대기
    if old_table:
        try:
            wait.until(EC.staleness_of(old_table))
        except Exception:
            pass

    wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
    time.sleep(1.5)

    return target_val


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
                used_yyyymm = set_period_and_query(driver, logger, yyyymm)
            else:
                used_yyyymm = yyyymm

            start_ts = time.time()

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

# def convert_xlsx_to_parquet(
#     xlsx_chajoo_path: Path,
#     xlsx_area_path:Path,
#     out_dir_root: Path,
#     gold_dir_root: Path,
#     cfg: "Config",
#     logger: logging.Logger,
#     yyyymm: str,   # "YYYYMM"
# ) -> str:
#     """
#     엑셀 -> 전처리 -> (year=YYYY/month=MM)/part.parquet 저장
#     - yyyymm으로 파티션 저장 위치만 결정
#     - parquet가 이미 있으면 skip (force_run 아니면)
#     """

#     logger.info(f"📦 전처리 시작: {xlsx_chajoo_path.name} & {xlsx_area_path.name} (yyyymm={yyyymm})")

#     # --------------------------------------------------
#     # 0. 파티션 경로/파일 미리 결정 + skip
#     # --------------------------------------------------
#     yyyymm = str(yyyymm).strip()
#     if len(yyyymm) != 6 or not yyyymm.isdigit():
#         raise ValueError(f"yyyymm 형식이 이상함: {yyyymm} (예: '202601')")

#     year_str = yyyymm[:4]
#     month_str = yyyymm[4:6]

#     partition_dir = out_dir_root
#     partition_dir.mkdir(parents=True, exist_ok=True)

#     gold_dir = gold_dir_root
#     gold_dir.mkdir(parents=True, exist_ok=True)

#     parquet_path = partition_dir / "part.parquet"
#     gold_parquet_path = gold_dir / "part.parquet"

#     if parquet_path.exists() and not cfg.force_run:
#         logger.info(f"⏭ Parquet 이미 존재하여 변환 스킵: {parquet_path}")
#         return f"Skipped: {year_str}년 {month_str}월 데이터가 이미 존재합니다."

#     # --------------------------------------------------
#     # 1. 엑셀 읽기
#     # --------------------------------------------------
#     # 1. 엑셀 읽기 및 컬럼 평탄화
#     chajoo_raw = pd.read_excel(xlsx_chajoo_path, header=[4, 5], engine="openpyxl")
#     area_raw = pd.read_excel(xlsx_area_path, header=[4, 5], engine="openpyxl")

#     for df in [chajoo_raw, area_raw]:
#         df.columns = [flatten_col(col) for col in df.columns] if isinstance(df.columns, pd.MultiIndex) else [str(c).strip() for c in df.columns]

#     # 2. 차주 데이터 전처리 (chajoo)
#     c_sido = pick_col(["시도명"], list(chajoo_raw.columns))
#     c_sigungu = pick_col(["시군구"], list(chajoo_raw.columns))
#     c_value = sorted([c for c in chajoo_raw.columns if "화물" in c and "영업용" in c], key=len)[0]

#     chajoo_df = chajoo_raw[[c_sido, c_sigungu, c_value]].copy()
#     chajoo_df.columns = ["sido", "sigungu", "cargo_count"]

#     # 3. 면적 데이터 전처리 (area)
#     a_sido = pick_col(["시도"], list(area_raw.columns))
#     a_sigungu = pick_col(["시군구"], list(area_raw.columns))
#     # '면적' 키워드가 들어간 컬럼 선택 (보통 '소계_면적' 또는 '계_면적')
#     a_value = sorted([c for c in area_raw.columns if "면적" in c], key=len)[0]

#     area_df = area_raw[[a_sido, a_sigungu, a_value]].copy()
#     area_df.columns = ["sido", "sigungu", "area_m2"]

#     # 4. 데이터 정제 (숫자 변환 및 '계' 행 제거)
#     # 4-1. 공통 처리: 공백 제거 및 문자열 변환
#     for df in [chajoo_df, area_df]:
#         df["sido"] = df["sido"].astype(str).str.strip()
#         df["sigungu"] = df["sigungu"].astype(str).str.strip()

#     # 4-2. chajoo_df: '계' 또는 '합계'가 포함된 행 제거
#     chajoo_df.drop(chajoo_df[chajoo_df["sigungu"].str.contains("계|합계")].index, inplace=True)

#     # 4-3. area_df: sido와 sigungu가 동일한 행(해당 시도 합계) 제거
#     # 추가로 '계' 단어가 들어간 행도 안전하게 제거
#     area_df.drop(area_df[
#         (area_df["sido"] == area_df["sigungu"]) | 
#         (area_df["sigungu"].str.contains("계|합계"))
#     ].index, inplace=True)

#     # 4-4. 숫자 변환
#     chajoo_df["cargo_count"] = pd.to_numeric(chajoo_df["cargo_count"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
#     area_df["area_m2"] = pd.to_numeric(area_df["area_m2"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)

#     # 4-5. Join을 위한 키 생성 및 세종시 예외 처리
#     for df in [chajoo_df, area_df]:
#         # 모든 공백 제거 (안산시상록구 등 대응)
#         df["join_key"] = df["sigungu"].str.replace(" ", "")

#         # 🔥 세종시 예외 처리: '세종'을 '세종특별자치시'로 통일
#         df.loc[df["join_key"] == "세종", "join_key"] = "세종특별자치시"

#     # 5. 데이터 병합 (Join)
#     # sido와 join_key를 모두 사용하여 병합 (다른 시도에 같은 이름의 시군구가 있을 경우 대비)
#     merged = pd.merge(
#         chajoo_df, 
#         area_df[['sido', 'join_key', 'area_m2']], 
#         on=["sido", "join_key"],
#         how="inner"
#     )
#     # 3. 임시 키 삭제 (chajoo_df의 원래 sido, sigungu만 남음)
#     merged.drop(columns=["join_key"], inplace=True)

#     # --------------------------------------------------
#     # 🔥 6. 지표 계산 (면적 대비 밀도 기반 계층적 Z-score)
#     # --------------------------------------------------
    
#     # 1) 시군구별 밀도 계산
#     merged["sig_density"] = merged["cargo_count"] / merged["area_m2"]

#     # 2) [시군구 Z-score] 같은 시도(sido) 내에서 계산
#     # groupby().transform()을 사용하여 시도별 평균/표준편차를 해당 시군구에 매칭
#     def get_group_zscore(group):
#         if len(group) > 1 and group.std() != 0:
#             return (group - group.mean()) / group.std()
#         return 0  # 시도 내 시군구가 1개뿐이거나 편차가 없으면 0 처리 (예: 세종)

#     merged["sig_zscore"] = merged.groupby("sido")["sig_density"].transform(get_group_zscore)

#     # 3) [시도 Z-score] 전국 시도끼리 비교하여 계산
#     # 시도별 전체 합계 기반 밀도 산출
#     sido_agg = merged.groupby("sido").agg({
#         "cargo_count": "sum",
#         "area_m2": "sum"
#     }).reset_index()

#     sido_agg["sido_density"] = sido_agg["cargo_count"] / sido_agg["area_m2"]

#     # 시도끼리의 분포에서 Z-score 산출
#     s_mean = sido_agg["sido_density"].mean()
#     s_std = sido_agg["sido_density"].std()
#     sido_agg["sido_zscore"] = (sido_agg["sido_density"] - s_mean) / s_std
    
#     # 계산된 시도 Z-score를 원래 merged 데이터프레임에 매핑
#     merged = merged.merge(sido_agg[["sido", "sido_zscore", "sido_density"]], on="sido", how="left")

#     # 4) 최종 점수 산출 (가중치 적용)
#     # SIDO_WEIGHT = 0.6, SIGUNGU_WEIGHT = 0.4
#     merged["final_score"] = (0.6 * merged["sido_zscore"]) + (0.4 * merged["sig_zscore"])

#     # --------------------------------------------------
#     # 🔥 3. 시군구 코드 매핑 (SHP_CD) - 직접 매칭 버전
#     # --------------------------------------------------
#     logger.info("🔗 시군구 코드 매핑 시작 (Direct Match)")
#     mapping_df = pd.read_csv(cfg.sigungu_mapping_csv, dtype={'SHP_CD': str})

#     # 직접 검증된 sido, sigungu를 기준으로 병합
#     # mapping_df에서 중복될 수 있는 sido, sigungu를 제외하고 SHP_CD만 가져옵니다.
#     final_result = merged.merge(
#         mapping_df[['sido', 'sigungu', 'SHP_CD']], 
#         on=["sido", "sigungu"],
#         how="left"
#     )

#     # 매핑 실패 확인
#     if final_result["SHP_CD"].isna().any():
#         failed = final_result[final_result["SHP_CD"].isna()][["sido", "sigungu"]].drop_duplicates()
#         logger.warning(f"⚠ 시군구 코드 매핑 실패: {len(failed)}개 구역 존재\n{failed.to_string(index=False)}")

#     # --------------------------------------------------
#     # 4. 최종 Parquet 저장
#     # --------------------------------------------------
#     # 불필요해진 임시 컬럼이 있다면 여기서 정리
#     if "join_key" in final_result.columns:
#         final_result.drop(columns=["join_key"], inplace=True)

#     final_result.to_parquet(parquet_path, index=False, compression="snappy")
#     final_result.to_parquet(gold_parquet_path, index=False, compression="snappy")

#     logger.info(f"💾 최종 Parquet 저장 완료: {gold_parquet_path} (rows={len(final_result)})")

#     return f"{year_str}년 {month_str}월 데이터 추출 완료"

def convert_xlsx_to_parquet(
    xlsx_chajoo_path: Path,
    out_dir_root: Path,
    gold_dir_root: Path,
    cfg: "Config",
    logger: logging.Logger,
    yyyymm: str,
) -> str:
    logger.info(f"📦 전처리 시작: {xlsx_chajoo_path.name} (yyyymm={yyyymm})")

    # --- 0. 경로 설정 ---
    yyyymm = str(yyyymm).strip()
    year_str, month_str = yyyymm[:4], yyyymm[4:6]
    
    parquet_path = out_dir_root / "part.parquet"
    gold_parquet_path = gold_dir_root / "part.parquet"

    if parquet_path.exists() and not cfg.force_run:
        return f"Skipped: {yyyymm} 데이터가 이미 존재합니다."

    # --- 1. 엑셀 읽기 (차주 데이터만) ---
    chajoo_raw = pd.read_excel(xlsx_chajoo_path, header=[4, 5], engine="openpyxl")
    chajoo_raw.columns = [flatten_col(col) for col in chajoo_raw.columns] if isinstance(chajoo_raw.columns, pd.MultiIndex) else [str(c).strip() for c in chajoo_raw.columns]

    # --- 2. 기본 전처리 ---
    c_sido = pick_col(["시도명"], list(chajoo_raw.columns))
    c_sigungu = pick_col(["시군구"], list(chajoo_raw.columns))
    c_value = sorted([c for c in chajoo_raw.columns if "화물" in c and "영업용" in c], key=len)[0]

    df = chajoo_raw[[c_sido, c_sigungu, c_value]].copy()
    df.columns = ["sido", "sigungu", "cargo_count"]
    
    # 공백 제거 및 계 행 제거
    df["sido"] = df["sido"].astype(str).str.strip()
    df["sigungu"] = df["sigungu"].astype(str).str.strip()
    df = df[~df["sigungu"].isin(["계", "합계"])].copy()
    
    # 숫자 변환
    df["cargo_count"] = pd.to_numeric(df["cargo_count"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)

    # --------------------------------------------------
    # 🔥 3. 계층적 Z-score 계산 (순수 차주 수 기반)
    # --------------------------------------------------
    
    # 1) [시군구 Z-score] 같은 시도(sido) 내에서 차주 수 편차 계산
    def get_group_zscore(group):
        if len(group) > 1 and group.std() != 0:
            return (group - group.mean()) / group.std()
        return 0

    df["sig_zscore"] = df.groupby("sido")["cargo_count"].transform(get_group_zscore)

    # 2) [시도 Z-score] 전국 시도끼리 비교 (시도별 총 차주 수 기준)
    sido_agg = df.groupby("sido")["cargo_count"].sum().reset_index(name="sido_cargo_sum")
    
    s_mean = sido_agg["sido_cargo_sum"].mean()
    s_std = sido_agg["sido_cargo_sum"].std()
    sido_agg["sido_zscore"] = (sido_agg["sido_cargo_sum"] - s_mean) / s_std
    
    # 원래 데이터에 시도 점수 매핑
    df = df.merge(sido_agg[["sido", "sido_zscore", "sido_cargo_sum"]], on="sido", how="left")

    # 3) 최종 점수 산출 (가중치 0.6 : 0.4)
    df["전략적_중요도"] = (0.6 * df["sido_zscore"]) + (0.4 * df["sig_zscore"])


    # 예외처리
    # 1. (경기, 부천시) 및 (경북, 군위군), (충북, 청원군) 행 제거
    # ~ 기호는 조건을 반전(not)시킵니다.
    df = df[~(
        ((df['sido'] == '경기') & (df['sigungu'] == '부천시')) | 
        ((df['sido'] == '경북') & (df['sigungu'] == '군위군')) |
        ((df['sido'] == '충북') & (df['sigungu'] == '청원군'))
    )].copy()


    # --- 4. 시군구 코드 매핑 (SHP_CD) ---
    mapping_df = pd.read_csv(cfg.sigungu_mapping_csv, dtype={'SHP_CD': str})
    final_result = df.merge(
        mapping_df[['sido', 'sigungu', 'SHP_CD']], 
        on=["sido", "sigungu"],
        how="left"
    )

    # --------------------------------------------------
    # 🔥 5. 최종 컬럼 필터링 및 저장
    # --------------------------------------------------
    # 요청하신 5개 핵심 컬럼만 선택 (year, month는 파티션 정보로 활용되므로 필요시 포함)
    target_cols = ["sido", "sigungu", "SHP_CD", "cargo_count", "전략적_중요도"]

    # 만약 DB insert를 위해 year, month도 유지해야 한다면 아래 리스트에 추가하세요.
    # target_cols += ["year", "month"] 

    final_result = final_result[target_cols].copy()

    # 저장
    final_result.to_parquet(parquet_path, index=False, compression="snappy")
    final_result.to_parquet(gold_parquet_path, index=False, compression="snappy")

    logger.info(f"💾 핵심 데이터 5개 컬럼 저장 완료: {gold_parquet_path}")
    logger.info(f"📊 저장된 컬럼: {list(final_result.columns)}")

    return f"{year_str}년 {month_str}월 데이터 전처리 완료"

# =========================
# Main Logic
# =========================
def main():
    cfg = Config()

    # 1. 대상 월 결정
    base_date = date.today()
    year_val, month_val = base_date.year, base_date.month
    target_yyyymm = f"{year_val - 1}12" if month_val == 1 else f"{year_val}{month_val-1:02d}"

    paths = init_run_dirs(cfg, target_yyyymm)
    logger = build_logger(paths["log_file"])
    notifier = SlackNotifier(cfg.slack_webhook_url, "EXTRACT-차주인구밀도", logger)

    logger.info(f"===== EXTRACT START (Target: {target_yyyymm}) =====")

    # 드라이버 변수 초기화 (finally에서 닫기 위함)
    driver_chajoo = None
    driver_area = None

    try:
        notifier.info("작업 시작", f"{target_yyyymm[:4]}년 {target_yyyymm[4:]}월 데이터 추출 시작")

        # --------------------------------------------------
        # 1. 차주 분포 데이터 다운로드 (chajoo 폴더)
        # --------------------------------------------------
        logger.info("Step 1: 차주 분포 데이터 수집 시작")
        driver_chajoo = build_driver(paths["chajoo_xlsx"], cfg)
        driver_chajoo.get(cfg.url_chajoo)

        path_chajoo, used_mm_chajoo = perform_download(
            driver_chajoo, logger, cfg, paths["chajoo_xlsx"], yyyymm=target_yyyymm
        )
        logger.info(f"Step 1 완료: {path_chajoo.name} (실제수집: {used_mm_chajoo})")


        # --------------------------------------------------
        # 2. 지역 면적 (area 폴더)
        # --------------------------------------------------
        # logger.info("Step 2: 지역 면적 데이터 수집 시작")
        # driver_area = build_driver(paths["area_xlsx"], cfg)
        # driver_area.get(cfg.url_area)

        # path_area, used_mm_area = perform_download(
        #     driver_area, logger, cfg, paths["area_xlsx"], yyyymm=target_yyyymm
        # )
        # logger.info(f"Step 2 완료: {path_area.name} (실제수집: {used_mm_area})")


        # --------------------------------------------------
        # [STEP 3] 데이터 전처리 및 병합 (이후 단계)
        # --------------------------------------------------
        status_msg = convert_xlsx_to_parquet(path_chajoo, paths["parquet"], paths["gold"], cfg, logger, target_yyyymm)
        logger.info(f"데이터 전처리 완료: {status_msg}")

        notifier.success("전체 다운로드 완료", f"{target_yyyymm[:4]}년 {target_yyyymm[4:]}월 데이터 추출 완료")

    except Exception as e:
        error_msg = f"에러 발생: {str(e)}"
        logger.error(f"🚨 {error_msg}", exc_info=True)
        notifier.error("차주인구밀도 추출 실패", error_msg)
        sys.exit(1)

    finally:
        # 에러 여부와 관계없이 드라이버 종료
        if driver_chajoo:
            driver_chajoo.quit()
            logger.info("driver_chajoo 종료")
        if driver_area:
            driver_area.quit()
            logger.info("driver_area 종료")
        logger.info("===== EXTRACT PROCESS FINISHED =====")

def run_workflow(**kwargs):
    cfg = Config(headless=True)
    main()

if __name__ == "__main__":
    main()