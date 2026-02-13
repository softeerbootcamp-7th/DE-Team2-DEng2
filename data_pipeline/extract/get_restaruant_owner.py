import csv
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from playwright.sync_api import sync_playwright

def main():
    cfg = Config(headless=True)

    log_dir = Path(cfg.project_root)
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = build_logger(log_dir / "get_restaurant_owner.log")
    
    sido_name="경기도"
    address="용인시 처인구" #시 혹은 구체적 주소도 가능
    output_file = Path(cfg.project_root)  / f"{sido_name}_{address}_restaruant_owner.csv"

    search_and_save_all_pages(
        sido_name=sido_name,
        address=address,
        cfg=cfg,
        logger=logger,
        output_file=output_file,
    )


# =========================
# Config
# =========================
@dataclass
class Config:
    url: str = "https://www.foodsafetykorea.go.kr/portal/specialinfo/searchInfoCompany.do"

    headless: bool = True
    retries: int = 1
    retry_sleep_sec: int = 5
    timeout_ms: int = 30_000

    log_dir: str = "logs"
    project_root: str = "data/restaurant_owner"

    parquet_compression: str = "snappy"
    parquet_overwrite: bool = False

    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")

    sido_mapping: dict = field(default_factory=lambda: {
        "서울특별시": "서울특별시",
        "부산광역시": "부산광역시",
        "대구광역시": "대구광역시",
        "인천광역시": "인천광역시",
        "광주광역시": "광주광역시",
        "대전광역시": "대전광역시",
        "울산광역시": "울산광역시",
        "세종특별자치시": "세종특별자치시",
        "경기도": "경기도",
        "강원특별자치도": "강원특별자치도",
        "강원도": "강원특별자치도",
        "충청북도": "충청북도",
        "충청남도": "충청남도",
        "전북특별자치도": "전북특별자치도",
        "전라북도": "전북특별자치도",
        "전라남도": "전라남도",
        "경상북도": "경상북도",
        "경상남도": "경상남도",
        "제주특별자치도": "제주특별자치도",
    })


# =========================
# Logging + Slack
# =========================
def build_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("food_safety_search")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
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
# Main Crawler
# =========================
def search_and_save_all_pages(
    sido_name: str,
    address: str,
    cfg: Config,
    logger: logging.Logger,
    output_file: str,
) -> None:
    """
    식품안전나라 업체 검색 - 음식점 카테고리, 전체 페이지 크롤링 및 로컬 CSV 저장
    """
    if sido_name not in cfg.sido_mapping:
        logger.warning(f"지원하지 않는 시도명: {sido_name}")
        return

    checkbox_label = cfg.sido_mapping[sido_name]
    logger.info(f"검색 시작 | 시도: {sido_name} | 주소: {address}")

    HEADERS = ["번호", "인허가번호", "업체명", "업종", "대표자", "소재지", "인허가기관", "영업상태", "비고"]

    for attempt in range(1, cfg.retries + 1):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=cfg.headless)

            try:
                page = _create_context(browser, logger)

                # ── 1) 페이지 접속 ──
                page.goto(cfg.url, timeout=cfg.timeout_ms)
                page.wait_for_load_state("networkidle")
                _random_delay(3.0, 5.0)
                logger.info(f"페이지 로딩 완료 (attempt {attempt}/{cfg.retries})")

                # ── 2) 음식점 카테고리 선택 ──
                _simulate_mouse_movement(page)
                page.locator('.dsL li[val="rstrt"] a').click()
                _human_like_delay()
                logger.info("음식점 카테고리 선택 완료")

                # ── 3) 시도 체크박스 ──
                _simulate_mouse_movement(page)
                checkbox = page.locator(
                    f'input[type="checkbox"][title="{checkbox_label}"]'
                ).first
                checkbox.check()
                _human_like_delay()
                logger.info(f"체크박스 선택: {checkbox_label}")

                # ── 4) 주소 입력 (한 글자씩 타이핑) ──
                addr_input = page.locator('input[name="site_addr"]').first
                addr_input.click()
                _human_like_delay()
                for char in address:
                    addr_input.type(char, delay=random.randint(50, 150))
                _human_like_delay()
                logger.info(f"주소 입력: {address}")

                # ── 5) 검색 ──
                _simulate_mouse_movement(page)
                page.locator("#srchBtn").click()
                logger.info("검색 버튼 클릭")
                page.wait_for_load_state("networkidle")
                _random_delay(4.0, 6.0)

                # ── 6) 검색 결과 없음 체크 ──
                first_row = page.locator("#tbl_bsn_list tbody tr").first
                if first_row.count() == 0 or "조회된 데이터가 없습니다" in first_row.text_content():
                    logger.warning(f"검색 결과 없음 | 주소: {address}")
                    return

                # ── 7) 50개씩 보기로 변경 ──
                _simulate_mouse_movement(page)
                page.locator("#a_list_cnt").click()
                _human_like_delay()
                page.locator('a[val="50"]').click()
                logger.info("50개씩 보기로 변경")
                page.wait_for_load_state("networkidle")
                _random_delay(4.0, 6.0)

                # ── 8) 총 페이지 수 확인 ──
                total_pages = page.evaluate("$('.pagination').pagination('getPagesCount')")
                total_text = page.text_content("body")
                total_match = re.search(r"총\s*([\d,]+)\s*건", total_text)
                total_count = total_match.group(1) if total_match else "?"
                logger.info(f"총 {total_count}건 | 총 {total_pages}페이지")

                # ── 9) CSV 파일 열고 전체 페이지 순회 ──
                with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(HEADERS)

                    for page_num in range(1, total_pages + 1):
                        if page_num > 1:
                            _simulate_mouse_movement(page)
                            next_btn = page.locator('.pagination li:last-child a')
                            if next_btn.count() == 0:
                                logger.warning(f"다음페이지 버튼 없음, {page_num - 1}페이지에서 종료")
                                break
                            next_btn.click()
                            page.wait_for_load_state("networkidle")
                            base_delay = random.uniform(4.0, 5.0)
                            extra_delay = (page_num / total_pages) * random.uniform(0.5, 1.5)
                            time.sleep(base_delay + extra_delay)

                        # tbody 행 읽기
                        rows = page.locator("#tbl_bsn_list tbody tr")
                        row_count = rows.count()

                        if row_count == 0:
                            logger.warning(f"페이지 {page_num}: 행 없음, 종료")
                            break

                        # 각 행 → CSV 기록
                        for i in range(row_count):
                            cells = rows.nth(i).locator("td")
                            cell_count = cells.count()

                            row_data = []
                            for c in range(min(cell_count, len(HEADERS))):
                                cell_text = cells.nth(c).text_content().strip()
                                # 모바일 반응형 레이블 prefix 제거
                                for header in HEADERS:
                                    if cell_text.startswith(header):
                                        cell_text = cell_text[len(header):].strip()
                                        break
                                row_data.append(cell_text)

                            writer.writerow(row_data)

                        logger.info(f"페이지 {page_num}/{total_pages} 저장 완료 ({row_count}행)")

                      
                        if page_num % 10 == 0 and page_num < total_pages:
                            rest_sec = random.uniform(5.0, 10.0)
                            logger.info(f"배치 휴식 {rest_sec:.1f}초 (페이지 {page_num} 이후)")
                            time.sleep(rest_sec)

                logger.info(f"전체 크롤링 완료 | 파일: {output_file}")
                return

            except Exception as e:
                logger.error(
                    f"attempt {attempt}/{cfg.retries} 실패 | 에러: {e}",
                    exc_info=True,
                )
                if attempt < cfg.retries:
                    backoff = cfg.retry_sleep_sec * (2 ** (attempt - 1)) + random.uniform(0, 2)
                    logger.info(f"{backoff:.1f}초 후 재시도 (exponential backoff)...")
                    time.sleep(backoff)
                else:
                    logger.error(f"최대 재시도 초과 | 주소: {address}")
                    return

            finally:
                browser.close()
                logger.info("브라우저 종료")


if __name__ == "__main__":
    main()