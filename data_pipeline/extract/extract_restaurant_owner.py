import os
import sys
import csv
import logging
import random
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv  # 1. import 추가

import argparse
from datetime import datetime
import pandas as pd

# 2. .env 로드 (프로젝트 루트 경로 명시)
# 현재 파일: data_pipeline/extract/extract_restaurant_owner.py
# .env 위치: / (루트)
env_path = Path(__file__).resolve().parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# slack_utils.py를 찾기 위해 상위 경로 추가
sys.path.append(str(Path(__file__).resolve().parent.parent))
from data_pipeline.utils.slack_utils import SlackNotifier

# =========================
# Config (설정 통합)
# =========================
@dataclass
class Config:
    url: str = "https://www.foodsafetykorea.go.kr/portal/specialinfo/searchInfoCompany.do"
    headless: bool = True
    retries: int = 1
    retry_sleep_sec: int = 5
    timeout_ms: int = 30_000
    project_root: str = "data/restaurant_owner"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")

    sido_mapping: dict = field(default_factory=lambda: {
        "서울특별시": "서울특별시", "부산광역시": "부산광역시", "대구광역시": "대구광역시",
        "인천광역시": "인천광역시", "광주광역시": "광주광역시", "대전광역시": "대전광역시",
        "울산광역시": "울산광역시", "세종특별자치시": "세종특별자치시", "경기도": "경기도",
        "강원도": "강원특별자치도", "충청북도": "충청북도", "충청남도": "충청남도",
        "전라북도": "전북특별자치도", "전라남도": "전라남도", "경상북도": "경상북도",
        "경상남도": "경상남도", "제주특별자치도": "제주특별자치도",
    })

# =========================
# Logger
# =========================
def build_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("food_safety_search")
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

# =========================
# Anti-Ban Helpers
# =========================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
    {"width": 1280, "height": 720},
]


def _random_delay(min_sec: float = 1.0, max_sec: float = 3.0) -> None:
    """랜덤 딜레이"""
    time.sleep(random.uniform(min_sec, max_sec))


def _human_like_delay() -> None:
    """사람처럼 보이는 짧은 딜레이 (클릭/입력 사이)"""
    time.sleep(random.uniform(0.3, 1.2))


def _simulate_mouse_movement(page) -> None:
    """랜덤 마우스 이동으로 봇 탐지 우회"""
    for _ in range(random.randint(2, 5)):
        x = random.randint(100, 800)
        y = random.randint(100, 600)
        page.mouse.move(x, y)
        time.sleep(random.uniform(0.05, 0.15))


def _create_context(browser, logger: logging.Logger):
    ua = random.choice(USER_AGENTS)
    vp = random.choice(VIEWPORTS)

    context = browser.new_context(
        user_agent=ua,
        viewport=vp,
        locale=random.choice(["ko-KR", "ko"]),
        timezone_id="Asia/Seoul",
        extra_http_headers={
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        },
    )
    page = context.new_page()

    logger.debug(f"크롤링 컨텍스트 생성 | UA: {ua[:50]}... | VP: {vp}")
    return page

# =========================
# Main Crawler Logic
# =========================
def search_and_save_all_pages(
    sido_name: str,
    address: str,
    cfg: Config,
    logger: logging.Logger,
    output_file: Path,
) -> None:
    notifier = SlackNotifier(cfg.slack_webhook_url, "EXTRACT-식당대표자", logger)

    if sido_name not in cfg.sido_mapping:
        logger.warning(f"지원하지 않는 시도명: {sido_name}")
        return

    checkbox_label = cfg.sido_mapping[sido_name]
    HEADERS = ["번호", "인허가번호", "업체명", "업종", "대표자", "소재지", "인허가기관", "영업상태", "비고"]

    try:
        notifier.info("작업 시작", f"대상: {sido_name} {address}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=cfg.headless)

            # 컨텍스트 및 페이지 생성
            page = _create_context(browser, logger)
            logger.info(f"🌐 페이지 접속 중: {cfg.url}")
            page.goto(cfg.url, timeout=cfg.timeout_ms)
            page.wait_for_load_state("networkidle")
            _random_delay(3.0, 5.0)

            # 카테고리 및 조건 선택
            page.locator('.dsL li[val="rstrt"] a').click()
            page.locator(f'input[type="checkbox"][title="{checkbox_label}"]').first.check()

            # 주소 입력 (Human-like 타이핑)
            addr_input = page.locator('input[name="site_addr"]').first
            addr_input.click()
            for char in address:
                addr_input.type(char, delay=random.randint(50, 150))

            # 검색 및 결과 설정 (50개씩 보기)
            page.locator("#srchBtn").click()
            page.wait_for_load_state("networkidle")
            _random_delay(4.0, 6.0)

            # 결과 유무 체크
            first_row = page.locator("#tbl_bsn_list tbody tr").first
            if first_row.count() == 0 or "조회된 데이터가 없습니다" in first_row.text_content():
                logger.warning(f"검색 결과 없음: {address}")
                notifier.info("검색 결과 없음", f"{sido_name} {address} 에 해당하는 데이터가 없습니다.")
                return

            # 리스트 수 변경
            page.locator("#a_list_cnt").click()
            page.locator('a[val="50"]').click()
            page.wait_for_load_state("networkidle")
            _random_delay(4.0, 6.0)

            # 페이지 정보 파싱
            total_pages = page.evaluate("$('.pagination').pagination('getPagesCount')")
            logger.info(f"총 {total_pages} 페이지 크롤링 시작")

            # 데이터 추출 및 파일 저장
            with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(HEADERS)

                for page_num in range(1, total_pages + 1):
                    if page_num > 1:
                        page.locator('.pagination li:last-child a').click()
                        page.wait_for_load_state("networkidle")
                        time.sleep(random.uniform(4.0, 6.0))

                    rows = page.locator("#tbl_bsn_list tbody tr")
                    row_count = rows.count()

                    for i in range(row_count):
                        cells = rows.nth(i).locator("td")
                        row_data = []

                        for c in range(min(cells.count(), len(HEADERS))):
                            # 1. 일단 텍스트를 가져옵니다 (결과: "번호5527")
                            raw_text = cells.nth(c).text_content().strip()

                            # 2. 정규표현식으로 시작 부분에 있는 Header 이름을 지웁니다.
                            # 예: "번호5527" -> "5527"
                            # ^ 기호는 문자열의 시작부분을 의미합니다.
                            header_name = HEADERS[c]
                            clean_val = re.sub(f"^{header_name}", "", raw_text).strip()

                            row_data.append(clean_val)

                        writer.writerow(row_data)

                    logger.info(f"진행 중: {page_num}/{total_pages} 페이지 완료")

                    if page_num % 10 == 0:
                        _random_delay(5.0, 10.0)

            # [SUCCESS]
            notifier.success("작업 완료", f"{sido_name} {address} 데이터 수집 완료 (총 {total_pages}p)")
            browser.close()

    except Exception as e:
        # [CRITICAL ERROR]
        logger.error(f"🚨 크롤러 중단됨: {str(e)}")
        notifier.error("식품안전나라 크롤러 중단", e)
        if 'browser' in locals(): browser.close()
        raise e

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sido", type=str, default="경기도")
    parser.add_argument("--addr", type=str, default="용인시 처인구")
    args = parser.parse_args()

    # 1. 경로 및 파티션 설정
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    region = args.sido[:2] 

    base_path = Path(Config.project_root)
    work_dir = base_path / "_work"
    parquet_dir = base_path / f"parquet/year={year}/month={month}/region={region}"

    work_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    safe_addr = args.addr.replace(" ", "_")
    log_file = work_dir / f"run_{region}_{safe_addr}.log"
    csv_file = work_dir / f"{region}_{safe_addr}.csv"
    parquet_file = parquet_dir / f"{region}_{safe_addr}.parquet"

    logger = build_logger(log_file)

    # 2. 크롤링 단계 (CSV 존재 여부 체크)
    if csv_file.exists():
        logger.info(f"⏭️  이미 CSV 파일이 존재합니다. 크롤링을 건너뜁니다: {csv_file.name}")
    else:
        logger.info(f"🚀 크롤링 시작: {args.sido} {args.addr}")
        search_and_save_all_pages(
            sido_name=args.sido,
            address=args.addr,
            cfg=Config(),
            logger=logger,
            output_file=csv_file
        )

    # 3. 변환 단계 (Parquet 존재 여부 체크)
    if parquet_file.exists():
        logger.info(f"⏭️  이미 Parquet 파일이 존재합니다. 변환을 건너뜁니다: {parquet_file.name}")
    elif csv_file.exists():
        try:
            logger.info("📄 CSV를 Parquet로 변환 중...")
            df = pd.read_csv(csv_file)
            # 데이터 추출 시 발생했던 헤더 중복 이슈 등을 한 번 더 방어적으로 처리
            df.to_parquet(parquet_file, engine='pyarrow', index=False, compression='snappy')
            logger.info(f"✅ 변환 완료: {parquet_file}")
        except Exception as e:
            logger.error(f"❌ Parquet 변환 실패: {e}")
    else:
        logger.error("⚠️ 변환할 CSV 파일이 없어 프로세스를 종료합니다.")

if __name__ == "__main__":
    main()