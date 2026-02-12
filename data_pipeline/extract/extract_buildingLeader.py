import os
import sys
import time
import re
import zipfile
import logging
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

import pandas as pd

# =========================
# ENV
# =========================
load_dotenv()

# =========================
# Config
# =========================
@dataclass
class HubConfig:
    url: str = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"

    category_label: str = "건축물대장"
    service_keyword: str = "표제부"
    usage_reason_label: str = "참고자료"

    headless: bool = True
    retries: int = 3
    retry_sleep_sec: int = 5
    timeout_ms: int = 30_000

    work_dir: str = "data/buildingLeader/_work"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")

    unzip_txt_only: bool = True


# =========================
# Logging + Slack
# =========================
def build_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("hub_extract")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
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


# =========================
# FS Helpers
# =========================
def create_run_dirs(base: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = Path(base) / f"run_{ts}"
    dirs = {
        "root": root,
        "zips": root / "zips",
        "unzipped": root / "unzipped",
        "artifacts": root / "artifacts",
        "logs": root / "logs",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def unzip_zip(zip_path: Path, out_dir: Path, txt_only: bool, logger):
    out_dir.mkdir(parents=True, exist_ok=True)
    extracted = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)
        for name in zf.namelist():
            p = out_dir / name
            if p.is_file():
                extracted.append(p)

    if txt_only:
        txts = [p for p in extracted if p.suffix.lower() == ".txt"]
        logger.info(f"Unzipped TXT files: {len(txts)}")
        return txts

    logger.info(f"Unzipped files: {len(extracted)}")
    return extracted


# =========================
# Core Helpers
# =========================
def parse_actual_data_year_month(container) -> tuple[int, int]:
    """
    '표제부 (2025년 12월)' → (2025, 12)
    """
    text = container.inner_text()
    m = re.search(r"\((\d{4})년\s*(\d{1,2})월\)", text)
    if not m:
        raise RuntimeError("실제 데이터 기준월 파싱 실패")
    return int(m.group(1)), int(m.group(2))


# ---------------------------------------------------------------------------
# 1. 허용된 지역 명칭 리스트 (이 명칭으로 시작하지 않으면 저장 안 함)
# ---------------------------------------------------------------------------
SIDO_MAP = {
    "서울": "서울", "부산": "부산", "대구": "대구", "인천": "인천",
    "광주": "광주", "대전": "대전", "울산": "울산", "세종": "세종",
    "경기": "경기", "강원": "강원", "충북": "충북", "충남": "충남",
    "전북": "전북", "전남": "전남", "경북": "경북", "경남": "경남", "제주": "제주",
    # 긴 명칭 대응
    "서울특별시": "서울", "부산광역시": "부산", "대구광역시": "대구", "인천광역시": "인천",
    "광주광역시": "광주", "대전광역시": "대전", "울산광역시": "울산", "세종특별자치시": "세종",
    "경기도": "경기", "강원도": "강원", "강원특별자치도": "강원",
    "충청북도": "충북", "충청남도": "충남", "전라북도": "전북", "전북특별자치도": "전북",
    "전라남도": "전남", "경상북도": "경북", "경상남도": "경남", "제주특별자치도": "제주"
}

def parquet_already_exists(parquet_root: Path, year: int, month: int) -> bool:
    target_dir = parquet_root / f"year={year}" / f"month={month:02d}"
    return target_dir.exists() and any(target_dir.iterdir())

# ---------------------------------------------------------------------------
# 2. 핵심 함수: TXT -> Parquet (외국 제외 + 두 글자 변환)
# ---------------------------------------------------------------------------
def txt_to_parquet_by_region(
    txt_path: Path,
    parquet_root: Path,
    year: int,
    month: int,
    chunksize: int = 500_000,
):
    columns = [
        "관리_건축물대장_PK", "대장_구분_코드", "대장_구분_코드_명", "대장_종류_코드", "대장_종류_코드_명",
        "대지_위치", "도로명_대지_위치", "건물_명", "시군구_코드", "법정동_코드", 
        "대지_구분_코드", "번", "지", "특수지_명", "블록", "로트", "외필지_수",
        "새주소_도로_코드", "새주소_법정동_코드", "새주소_지상지하_코드", "새주소_본_번", "새주소_부_번",
        "동_명", "주_부속_구분_코드", "주_부속_구분_코드_명", "대지_면적(㎡)", "건축_면적(㎡)",
        "건폐_율(%)", "연면적(㎡)", "용적_률_산정_연면적(㎡)", "용적_률(%)", "구조_코드",
        "구조_코드_명", "기타_구조", "주_용도_코드", "주_용도_코드_명", "기타_용도",
        "지붕_코드", "지붕_코드_명", "기타_지붕", "세대_수(세대)", "가구_수(가구)",
        "높이(m)", "지상_층_수", "지하_층_수", "승용_승강기_수", "비상용_승강기_수",
        "부속_건축물_수", "부속_건축물_면적(㎡)", "총_동_연면적(㎡)", "옥내_기계식_대수(대)",
        "옥내_기계식_면적(㎡)", "옥외_기계식_대수(대)", "옥외_기계식_면적(㎡)", "옥내_자주식_대수(대)",
        "옥내_자주식_면적(㎡)", "옥외_자주식_대수(대)", "옥외_자주식_면적(㎡)", "허가_일",
        "착공_일", "사용승인_일", "허가번호_년", "허가번호_기관_코드", "허가번호_기관_코드_명",
        "허가번호_구분_코드", "허가번호_구분_코드_명", "호_수(호)", "에너지효율_등급",
        "에너지절감_율", "에너지_EPI점수", "친환경_건축물_등급", "친환경_건축물_인증점수",
        "지능형_건축물_등급", "지능형_건축물_인증점수", "생성_일자", "내진_설계_적용_여부", "내진_능력"
    ]

    encodings = ["cp949", "euc-kr", "utf-8"]
    reader = None

    for enc in encodings:
        try:
            reader = pd.read_csv(
                txt_path, sep="|", encoding=enc, chunksize=chunksize, 
                names=columns, dtype=str, low_memory=False, index_col=False
            )
            next(reader) 
            reader = pd.read_csv(txt_path, sep="|", encoding=enc, chunksize=chunksize, names=columns, dtype=str, index_col=False)
            break
        except Exception:
            continue

    if reader is None: raise RuntimeError("txt read failed")

    for i, df in enumerate(reader):
        df['대지_위치'] = df['대지_위치'].fillna('')
        
        # 1. 첫 단어 추출 (ex: '서울특별시', '전북특별자치도')
        df['raw_sido'] = df['대지_위치'].str.split().str[0].str.strip()
        
        # 2. 지명 매핑 (ex: '서울특별시' -> '서울')
        # SIDO_MAP에 없는 것(외국, 번지수 등)은 NaN이 됨
        df['sido'] = df['raw_sido'].map(SIDO_MAP)
        
        # 3. 외국 및 번지수 데이터 필터링 (NaN 제거)
        valid_df = df.dropna(subset=['sido']).copy()
        
        if valid_df.empty: continue

        for sido, group in valid_df.groupby('sido'):
            target_dir = parquet_root / f"year={year}" / f"month={month:02d}" / f"region={sido}"
            target_dir.mkdir(parents=True, exist_ok=True)
            
            out_path = target_dir / f"part-{i:05d}.parquet"
            # 보조 컬럼 제거 후 저장
            save_df = group.drop(columns=['raw_sido'])
            save_df.to_parquet(out_path, index=False, engine='pyarrow')

# =========================
# Main
# =========================
def run(cfg: HubConfig):
    # 경로 설정
    base_path = Path(cfg.work_dir)
    zip_dir = base_path / "zips"
    unzip_dir = base_path / "unzipped"
    parquet_root = Path("data/buildingLeader/parquet")

    # 필요 디렉토리 생성
    for d in [zip_dir, unzip_dir, parquet_root]:
        d.mkdir(parents=True, exist_ok=True)

    console_logger = logging.getLogger("console")
    console_logger.setLevel(logging.INFO)
    console_logger.addHandler(logging.StreamHandler(sys.stdout))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=cfg.headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(cfg.timeout_ms)

        try:
            console_logger.info("Open page and search")
            page.goto(cfg.url, wait_until="domcontentloaded")

            # 검색 로직
            page.locator("select").filter(has=page.locator(f"option:has-text('{cfg.category_label}')")).select_option(label=cfg.category_label)
            page.locator("input[type='text']").first.fill(cfg.service_keyword)
            page.get_by_role("button", name="검색").click()

            # 결과 행 찾기
            page.wait_for_selector(f"text={cfg.service_keyword}")
            row = page.locator(f"text={cfg.service_keyword}").first
            container = row
            for _ in range(8):
                if "데이터제공년월" in container.inner_text(): break
                container = container.locator("xpath=..")
            
            data_year, data_month = parse_actual_data_year_month(container)
            console_logger.info(f"Target Period: {data_year}-{data_month:02d}")

            # -------------------------------------------------------
            # [단계 1] ZIP 확인 및 다운로드
            # -------------------------------------------------------
            zip_path = None
            existing_zips = list(zip_dir.glob("*.zip"))
            existing_txts = list(unzip_dir.glob("*.txt"))

            if existing_txts:
                console_logger.info(f"[SKIP] Txt files already exist in {unzip_dir}")
            elif existing_zips:
                zip_path = existing_zips[0]
                console_logger.info(f"[SKIP] Zip already exists: {zip_path}")
            else:
                console_logger.info("No local files. Downloading...")
                download_btn = None
                tmp = container
                for _ in range(6):
                    btn = tmp.locator("button", has_text="전체")
                    if btn.count() > 0:
                        download_btn = btn.first
                        break
                    tmp = tmp.locator("xpath=..")
                
                if not download_btn: raise RuntimeError("다운로드 버튼을 찾을 수 없습니다.")

                with page.expect_download() as dl_info:
                    download_btn.click()
                    page.get_by_text(cfg.usage_reason_label, exact=True).click()
                    page.get_by_role("button", name="확인").click()

                download = dl_info.value
                zip_path = zip_dir / download.suggested_filename
                download.save_as(zip_path)
                console_logger.info(f"Downloaded zip: {zip_path}")

            # -------------------------------------------------------
            # [단계 2] TXT 확인 및 압축 해제
            # -------------------------------------------------------
            if not existing_txts:
                if not zip_path or not zip_path.exists():
                    raise FileNotFoundError("압축을 풀 Zip 파일이 없습니다.")
                
                console_logger.info(f"Unzipping {zip_path.name}...")
                logger = build_logger(base_path / "run.log")
                unzip_zip(zip_path, unzip_dir, cfg.unzip_txt_only, logger)
                existing_txts = list(unzip_dir.glob("*.txt"))
            else:
                console_logger.info(f"[SKIP] Txt already exists. Skipping unzip.")

            # -------------------------------------------------------
            # [단계 3] Parquet 최종 확인 (변환 직전)
            # -------------------------------------------------------
            if parquet_already_exists(parquet_root, data_year, data_month):
                msg = f"[SKIP] {data_year}-{data_month:02d} Parquet 결과물이 이미 존재합니다."
                console_logger.info(msg)
                slack_notify(cfg.slack_webhook_url, msg, console_logger)
                browser.close()
                return

            # -------------------------------------------------------
            # [단계 4] 변환 실행
            # -------------------------------------------------------
            if not existing_txts:
                raise FileNotFoundError("변환할 Txt 파일이 없습니다.")

            logger = build_logger(base_path / "run.log")
            for txt_file in existing_txts:
                if txt_file.stat().st_size == 0: continue
                logger.info(f"Converting {txt_file.name} to Parquet...")
                txt_to_parquet_by_region(txt_file, parquet_root, data_year, data_month)

            ok_msg = f"[OK] {data_year}-{data_month:02d} 데이터 처리 및 지역별 파티셔닝 완료"
            console_logger.info(ok_msg)
            slack_notify(cfg.slack_webhook_url, ok_msg, console_logger)

            browser.close()

        except Exception as e:
            err_msg = f"[FAIL] extract_buildingLeader\n{repr(e)}"
            console_logger.error(err_msg)
            slack_notify(cfg.slack_webhook_url, err_msg, console_logger)
            if 'browser' in locals(): browser.close()
            raise

# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    cfg = HubConfig()
    run(cfg)
