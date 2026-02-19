import os
import sys
import re
import zipfile
import logging
import traceback
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()
from playwright.sync_api import sync_playwright
import pandas as pd

# slack_utils.py를 찾기 위해 상위 경로 추가
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

try:
    from data_pipeline.utils.slack_utils import SlackNotifier
except ImportError:
    class SlackNotifier:
        def __init__(self, url, title, logger): self.logger = logger
        def info(self, m, d): self.logger.info(f"[SLACK INFO] {m}: {d}")
        def success(self, m, d): self.logger.info(f"[SLACK SUCCESS] {m}: {d}")
        def error(self, m, e): self.logger.error(f"[SLACK ERROR] {m}: {e}")

# =========================
# Logging
# =========================
def build_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("hub_extract")
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

@dataclass
class HubConfig:
    url: str = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"
    category_label: str = "건축물대장"
    service_keyword: str = "표제부"
    usage_reason_label: str = "참고자료"
    headless: bool = True
    timeout_ms: int = 40_000
    # 경로 설정 변경
    base_dir: str = "data/bronze/buildingLeader"
    work_dir: str = "data/bronze/buildingLeader/_work"
    parquet_dir: str = "data/bronze/buildingLeader/parquet"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")

# =========================
# FS Helpers (year/month 구조)
# =========================
def get_target_paths(cfg: HubConfig, data_year: int, data_month: int):
    """year=YYYY/month=MM 형식으로 경로 구성"""
    # 공통 경로 부분
    partition_path = f"year={data_year}/month={data_month:02d}"

    work_root = Path(cfg.work_dir) / partition_path
    parquet_root = Path(cfg.parquet_dir) / partition_path

    paths = {
        "work_root": work_root,
        "zips": work_root / "zips",
        "unzipped": work_root / "unzipped",
        "logs": work_root / "logs",
        "parquet_root": parquet_root 
    }

    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths

def remove_old_data_runs(cfg: HubConfig, data_year: int, data_month: int, logger: logging.Logger):
    """
    M, M-1 월을 제외한 모든 year=*/month=* 폴더를 정리합니다.
    """
    target_month_str = f"month={data_month:02d}"
    target_year_str = f"year={data_year}"
    
    if data_month == 1:
        prev_month_str = "month=12"
        prev_year_str = f"year={data_year - 1}"
    else:
        prev_month_str = f"month={data_month - 1:02d}"
        prev_year_str = f"year={data_year}"

    # 유지해야 할 상대 경로 리스트
    allowed_paths = [
        f"{target_year_str}/{target_month_str}",
        f"{prev_year_str}/{prev_month_str}"
    ]
    
    logger.info(f"🧹 데이터 정리 시작 (유지 대상: {allowed_paths})")

    for base_path in [Path(cfg.work_dir), Path(cfg.parquet_dir)]:
        if not base_path.exists(): continue
        
        # 모든 month=XX 폴더 탐색
        for month_dir in base_path.glob("year=*/month=*"):
            # 기준 폴더로부터의 상대 경로 추출 (예: year=2025/month=11)
            rel_path = f"{month_dir.parent.name}/{month_dir.name}"
            
            if rel_path not in allowed_paths:
                logger.info(f"🗑️ 오래된 데이터 삭제: {base_path.name}/{rel_path}")
                shutil.rmtree(month_dir)
                
                # 만약 해당 연도(year=) 폴더가 비어있다면 연도 폴더도 삭제
                year_dir = month_dir.parent
                if not any(year_dir.iterdir()):
                    year_dir.rmdir()

# =========================
# Core ETL Helpers
# =========================
def parse_actual_data_year_month(container) -> tuple[int, int]:
    text = container.inner_text()
    m = re.search(r"\((\d{4})년\s*(\d{1,2})월\)", text)
    if not m: raise RuntimeError("실제 데이터 기준월 파싱 실패")
    return int(m.group(1)), int(m.group(2))

def parquet_already_exists(parquet_root: Path, year: int, month: int) -> bool:
    target_dir = parquet_root / f"year={year}" / f"month={month:02d}"
    # 폴더가 존재하고 내부 파일이 하나라도 있는지 확인
    return target_dir.exists() and any(target_dir.iterdir())

SIDO_MAP = {
    "서울": "서울", "부산": "부산", "대구": "대구", "인천": "인천", "광주": "광주", "대전": "대전", 
    "울산": "울산", "세종": "세종", "경기": "경기", "강원": "강원", "충북": "충북", "충남": "충남",
    "전북": "전북", "전남": "전남", "경북": "경북", "경남": "경남", "제주": "제주",
    "서울특별시": "서울", "부산광역시": "부산", "대구광역시": "대구", "인천광역시": "인천",
    "광주광역시": "광주", "대전광역시": "대전", "울산광역시": "울산", "세종특별자치시": "세종",
    "경기도": "경기", "강원도": "강원", "강원특별자치도": "강원", "충청북도": "충북", "충청남도": "충남", 
    "전라북도": "전북", "전북특별자치도": "전북", "전라남도": "전남", "경상북도": "경북", "경상남도": "경남", "제주특별자치도": "제주"
}

def txt_to_parquet_by_region(txt_path: Path, partition_root: Path):
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

    for enc in ["cp949", "euc-kr", "utf-8"]:
        try:
            reader = pd.read_csv(txt_path, sep="|", encoding=enc, chunksize=500_000, names=columns, dtype=str, index_col=False)
            for i, df in enumerate(reader):
                df['sido'] = df['대지_위치'].fillna('').str.split().str[0].str.strip().map(SIDO_MAP)
                valid_df = df.dropna(subset=['sido']).copy()
                for sido, group in valid_df.groupby('sido'):
                    out_dir = partition_root / f"region={sido}"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    group.to_parquet(out_dir / f"part-{i:05d}.parquet", index=False)
            return
        except Exception: continue

# =========================
# Main Logic
# =========================
def run(cfg: HubConfig):
    base_path = Path(cfg.work_dir)
    base_path.mkdir(parents=True, exist_ok=True)

    # 초기 부트스트랩 로거 (임시)
    temp_log_dir = base_path / "bootstrap_logs"
    temp_log_dir.mkdir(parents=True, exist_ok=True)
    logger = build_logger(temp_log_dir / "bootstrap.log")

    notifier = SlackNotifier(cfg.slack_webhook_url, "EXTRACT-건축물대장", logger)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=cfg.headless)
        page = browser.new_page(accept_downloads=True)

        try:
            logger.info("🌐 페이지 접속 중...")
            page.goto(cfg.url, wait_until="domcontentloaded", timeout=cfg.timeout_ms)

            # --- 검색 로직 ---
            page.locator("select").filter(has=page.locator(f"option:has-text('{cfg.category_label}')")).select_option(label=cfg.category_label)
            page.locator("input[type='text']").first.fill(cfg.service_keyword)
            page.get_by_role("button", name="검색").click()
            page.wait_for_selector(f"text={cfg.service_keyword}")

            # 데이터 시점 파싱을 위한 컨테이너 찾기
            container = page.locator(f"text={cfg.service_keyword}").first
            for _ in range(8):
                if "데이터제공년월" in container.inner_text(): break
                container = container.locator("xpath=..")

            data_year, data_month = parse_actual_data_year_month(container)
            datestr = f"{data_year}{data_month:02d}"

            # 1. 경로 생성 및 중복 체크
            paths = get_target_paths(cfg, data_year, data_month)

            # 이미 parquet 결과물이 있는지 확인
            if any(paths["parquet_root"].glob("region=*")):
                logger.info(f"⏭  {datestr} 데이터가 이미 존재합니다. 수집을 건너뜁니다.")
                notifier.success("작업 완료", f"{data_year}년 {data_month:02d}월 데이터가 이미 존재합니다")
            else:
                # 2. 로거 교체 및 수집 시작
                logger = build_logger(paths["logs"] / "run.log")
                logger.info(f"🚀 신규 데이터 발견! 수집 시작: {data_year}년 {data_month}월")
                notifier.info("작업 시작", f"{data_year}년 {data_month:02d}월 데이터 추출 시작")

                # 3. 다운로드 버튼 찾기 및 클릭
                download_btn = None
                tmp = container
                for _ in range(6):
                    btn = tmp.get_by_role("button", name="전체")
                    if btn.count() > 0:
                        download_btn = btn.first
                        break
                    tmp = tmp.locator("xpath=..")

                if not download_btn: raise RuntimeError("다운로드 버튼을 찾을 수 없습니다.")

                with page.expect_download() as dl_info:
                    download_btn.click()
                    page.get_by_text(cfg.usage_reason_label, exact=True).click()
                    page.get_by_role("button", name="확인").click()

                zip_path = paths["zips"] / dl_info.value.suggested_filename
                dl_info.value.save_as(zip_path)
                logger.info(f"✅ ZIP 다운로드 완료: {zip_path.name}")

                # 4. ETL (Unzip & Parquet)
                with zipfile.ZipFile(zip_path, "r") as zf:
                    zf.extractall(paths["unzipped"])

                for txt_file in paths["unzipped"].glob("*.txt"):
                    if txt_file.stat().st_size > 0:
                        logger.info(f"📦 Parquet 변환 중: {txt_file.name}")
                        # Hive 파티션 경로(paths["parquet_root"])를 그대로 전달
                        txt_to_parquet_by_region(txt_file, paths["parquet_root"])


                logger.info(f"{data_year}년 {data_month:02d}월 데이터 추출 완료")
                notifier.success("작업 완료", f"{data_year}년 {data_month:02d}월 데이터 추출 완료")

            # 5. 정리 (데이터 시점 기준으로 M, M-1 외 정리)
            # remove_old_data_runs(cfg, data_year, data_month, logger)

        except Exception as e:
            logger.error(f"🚨 에러 발생: {str(e)}\n{traceback.format_exc()}")
            if 'notifier' in locals(): notifier.error("수집 중단", str(e))
            raise
        finally:
            browser.close()
            # 임시 부트스트랩 로그 삭제
            if (temp_log_dir / "bootstrap.log").exists():
                try: shutil.rmtree(temp_log_dir)
                except: pass

if __name__ == "__main__":
    run(HubConfig())
    
    