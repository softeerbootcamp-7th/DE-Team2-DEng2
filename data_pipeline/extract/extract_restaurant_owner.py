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

from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

import argparse
from datetime import datetime
import pandas as pd

load_dotenv()

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from data_pipeline.utils.slack_utils import SlackNotifier


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
    project_root: str = "data/bronze/restaurant_owner"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")

    page_delay_min: float = 3.0
    page_delay_max: float = 6.0
    long_break_interval: int = 15   
    long_break_min: float = 15.0
    long_break_max: float = 25.0
    session_rotate_interval: int = 50  
    mouse_sim_probability: float = 0.6
    scroll_sim_probability: float = 0.5

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

VIEWPORTS = [
    {"width": 1920, "height": 1080}, {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},  {"width": 1440, "height": 900},
    {"width": 1280, "height": 720},  {"width": 1600, "height": 900},
]

REFERERS = [
    "https://www.google.com/",
    "https://search.naver.com/",
    "https://www.daum.net/",
    "https://www.foodsafetykorea.go.kr/portal/main.do",
    "",
]


def _random_delay(min_sec: float = 1.0, max_sec: float = 3.0) -> None:
    time.sleep(random.uniform(min_sec, max_sec))


def _human_like_delay() -> None:
    time.sleep(random.uniform(0.3, 1.5))


def _simulate_mouse_movement(page) -> None:
    for _ in range(random.randint(2, 5)):
        x = random.randint(100, 1200)
        y = random.randint(100, 700)
        page.mouse.move(x, y, steps=random.randint(3, 10)) 
        time.sleep(random.uniform(0.05, 0.2))


def _simulate_scroll(page) -> None:
    scroll_amount = random.randint(100, 500)
    direction = random.choice([1, -1])
    page.mouse.wheel(0, scroll_amount * direction)
    time.sleep(random.uniform(0.3, 0.8))


def _simulate_human_behavior(page, cfg: Config) -> None:
    if random.random() < cfg.mouse_sim_probability:
        _simulate_mouse_movement(page)
    if random.random() < cfg.scroll_sim_probability:
        _simulate_scroll(page)
    _human_like_delay()


def _create_context(browser, logger: logging.Logger):
    ua = random.choice(USER_AGENTS)
    vp = random.choice(VIEWPORTS)
    referer = random.choice(REFERERS)

    extra_headers = {
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    }
    if referer:
        extra_headers["Referer"] = referer

    context = browser.new_context(
        user_agent=ua,
        viewport=vp,
        locale=random.choice(["ko-KR", "ko"]),
        timezone_id="Asia/Seoul",
        extra_http_headers=extra_headers,
        java_script_enabled=True,
    )

    # WebDriver 속성 숨기기
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) =>
            parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters);
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });
        Object.defineProperty(navigator, 'languages', {
            get: () => ['ko-KR', 'ko', 'en-US', 'en'],
        });
    """)

    page = context.new_page()
    logger.debug(f"크롤링 컨텍스트 생성 | UA: {ua[:50]}... | VP: {vp}")
    return context, page


# [NEW] 특정 페이지로 직접 이동
def _navigate_to_page(page, target_page: int, logger: logging.Logger) -> None:
    logger.info(f"📄 {target_page} 페이지로 이동 중...")
    page.evaluate(f"$('.pagination').pagination('selectPage', {target_page})")
    page.wait_for_load_state("networkidle")
    _random_delay(4.0, 6.0)


# =========================
# [NEW] 진행 상태 저장/복원
# =========================
def _save_progress(progress_file: Path, last_page: int) -> None:
    progress_file.write_text(str(last_page), encoding="utf-8")


def _load_progress(progress_file: Path) -> int:
    if progress_file.exists():
        try:
            return int(progress_file.read_text(encoding="utf-8").strip())
        except ValueError:
            return 0
    return 0


# =========================
# Main Crawler Logic
# =========================
def search_and_save_all_pages(
    sido_name: str,
    address: str,
    cfg: Config,
    logger: logging.Logger,
    output_file: Path,
    start_page: int = 1,                   
    end_page: Optional[int] = None,          
    progress_file: Optional[Path] = None,    
) -> bool:
    notifier = SlackNotifier(cfg.slack_webhook_url, "EXTRACT-식당대표자", logger)

    if sido_name not in cfg.sido_mapping:
        logger.warning(f"지원하지 않는 시도명: {sido_name}")
        return

    checkbox_label = cfg.sido_mapping[sido_name]
    HEADERS = ["번호", "인허가번호", "업체명", "업종", "대표자", "소재지", "인허가기관", "영업상태", "비고"]

    # 진행 상태 기록 파일에서 시작 페이지 결정
    if progress_file:
        saved_page = _load_progress(progress_file)
        if saved_page > 0 and saved_page >= start_page:
            logger.info(f"🔄 이전 진행 상태 발견: {saved_page}페이지까지 완료. {saved_page + 1}페이지부터 재개합니다.")
            start_page = saved_page + 1

    try:
        notifier.info("작업 시작", f"대상: {sido_name} {address} (페이지 {start_page}~{end_page or '끝'})")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=cfg.headless)
            context, page = _create_context(browser, logger)

            # 검색 조건 설정 (세션 갱신 시에도 재사용)
            def _setup_search(pg):
                logger.info(f"🌐 페이지 접속 중: {cfg.url}")
                pg.goto(cfg.url, timeout=cfg.timeout_ms)
                pg.wait_for_load_state("networkidle")
                _random_delay(3.0, 5.0)

                _simulate_human_behavior(pg, cfg)  #

                pg.locator('.dsL li[val="rstrt"] a').click()
                _human_like_delay()  
                pg.locator(f'input[type="checkbox"][title="{checkbox_label}"]').first.check()
                _human_like_delay()  

                addr_input = pg.locator('input[name="site_addr"]').first
                addr_input.click()
                _human_like_delay()
                for char in address:
                    addr_input.type(char, delay=random.randint(50, 180))

                _simulate_human_behavior(pg, cfg) 

                pg.locator("#srchBtn").click()
                pg.wait_for_load_state("networkidle")
                _random_delay(5.0, 8.0)

                first_row = pg.locator("#tbl_bsn_list tbody tr").first
                if first_row.count() == 0 or "조회된 데이터가 없습니다" in first_row.text_content():
                    return 0

                _human_like_delay()
                pg.locator("#a_list_cnt").click()
                _human_like_delay()
                pg.locator('a[val="50"]').click()
                pg.wait_for_load_state("networkidle")
                _random_delay(4.0, 6.0)

                return pg.evaluate("$('.pagination').pagination('getPagesCount')")

            total_pages = _setup_search(page)
            if total_pages == 0:
                logger.warning(f"검색 결과 없음: {address}")
                notifier.error("검색 결과 없음", f"{sido_name} {address} 에 해당하는 데이터가 없습니다.")
                browser.close()
                return False

            # end_page 결정
            actual_end = min(end_page, total_pages) if end_page else total_pages
            if start_page > actual_end:
                logger.info(f"✅ 이미 모든 페이지 완료 (시작={start_page}, 끝={actual_end})")
                browser.close()
                return True

            logger.info(f"총 {total_pages} 페이지 중 {start_page}~{actual_end} 페이지 크롤링 시작")

            # 시작 페이지로 이동
            if start_page > 1:
                _navigate_to_page(page, start_page, logger)

            # 파일 모드 결정
            file_mode = "w" if start_page == 1 else "a"
            write_header = (start_page == 1) or (not output_file.exists())

            with open(output_file, file_mode, encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(HEADERS)

                consecutive_errors = 0      
                max_consecutive_errors = 3   

                for page_num in range(start_page, actual_end + 1):
                    try:
                        pages_done = page_num - start_page

                        # 세션 갱신 (N페이지마다)
                        if pages_done > 0 and pages_done % cfg.session_rotate_interval == 0:
                            logger.info(f"🔄 세션 갱신 중 ({pages_done}페이지 완료)...")
                            context.close()
                            _random_delay(10.0, 20.0)
                            context, page = _create_context(browser, logger)
                            total_pages = _setup_search(page)
                            if total_pages == 0:
                                logger.error("세션 갱신 후 검색 결과를 찾지 못했습니다.")
                                break
                            _navigate_to_page(page, page_num, logger)
                            logger.info(f"✅ 세션 갱신 완료. {page_num}페이지부터 계속합니다.")

                        elif page_num > start_page:
                            _simulate_human_behavior(page, cfg)  # 
                            page.locator('.pagination li:last-child a').click()
                            page.wait_for_load_state("networkidle")
                            _random_delay(cfg.page_delay_min, cfg.page_delay_max)

                        _simulate_human_behavior(page, cfg)  # 

                        # 데이터 추출
                        rows = page.locator("#tbl_bsn_list tbody tr")
                        row_count = rows.count()

                        # 빈 결과 → 밴 의심
                        if row_count == 0:
                            logger.warning(f"⚠️ {page_num}페이지에 데이터가 없습니다. 밴 가능성 확인 필요.")
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                logger.error(f"🚨 연속 {max_consecutive_errors}회 빈 결과 → IP 밴 의심. 중단합니다.")
                                notifier.error(
                                    "IP 밴 의심 - 크롤러 중단",
                                    f"{sido_name} {address}\n"
                                    f"마지막 성공 페이지: {page_num - 1}\n"
                                    f"재개 명령어: --start-page {page_num} --end-page {actual_end}"
                                )
                                break
                            _random_delay(15.0, 30.0)
                            continue

                        consecutive_errors = 0

                        for i in range(row_count):
                            cells = rows.nth(i).locator("td")
                            row_data = []
                            for c in range(min(cells.count(), len(HEADERS))):
                                raw_text = cells.nth(c).text_content().strip()
                                header_name = HEADERS[c]
                                clean_val = re.sub(f"^{header_name}", "", raw_text).strip()
                                row_data.append(clean_val)
                            writer.writerow(row_data)

                        f.flush()

                        # 진행 상태 저장
                        if progress_file:
                            _save_progress(progress_file, page_num)

                        logger.info(f"✅ 진행 중: {page_num}/{actual_end} 페이지 완료 ({row_count}건)")

                        # 긴 휴식
                        if pages_done > 0 and pages_done % cfg.long_break_interval == 0:
                            break_sec = random.uniform(cfg.long_break_min, cfg.long_break_max)
                            logger.info(f"☕ {cfg.long_break_interval}페이지 완료 → {break_sec:.0f}초 휴식")
                            time.sleep(break_sec)

                    # 페이지별 에러 처리 + 복구
                    except Exception as page_error:
                        consecutive_errors += 1
                        logger.error(f"⚠️ {page_num}페이지 처리 중 오류: {page_error}")

                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(
                                f"🚨 연속 {max_consecutive_errors}회 오류. 크롤러를 중단합니다."
                            )
                            notifier.error(
                                "크롤러 오류로 중단",
                                f"{sido_name} {address}\n오류 페이지: {page_num}\n"
                                f"에러: {page_error}\n"
                                f"재개 명령어: --start-page {page_num} --end-page {actual_end}"
                            )
                            break

                        logger.info(f"🔧 복구 시도 중... ({consecutive_errors}/{max_consecutive_errors})")
                        _random_delay(20.0, 40.0)

                        try:
                            context.close()
                        except Exception:
                            pass

                        context, page = _create_context(browser, logger)
                        retry_total = _setup_search(page)
                        if retry_total > 0:
                            _navigate_to_page(page, page_num, logger)
                            logger.info(f"🔧 복구 성공. {page_num}페이지 재시도합니다.")
                            continue
                        else:
                            logger.error("🔧 복구 실패.")
                            break

            last_done = _load_progress(progress_file) if progress_file else actual_end
            is_complete = (last_done >= actual_end)  
            if is_complete:
                notifier.success(
                    "작업 완료 ✅",
                    f"{sido_name} {address} 전체 수집 완료\n"
                    f"범위: {start_page}~{actual_end}p / 전체 {total_pages}p"
                )
            else:
                notifier.error(
                    "부분 완료 ⚠️",
                    f"{sido_name} {address}\n"
                    f"완료: {last_done}p / 목표: {actual_end}p\n"
                    f"재개: --auto-resume"
                )
            context.close()
            browser.close()
            return is_complete

    except Exception as e:
        logger.error(f"🚨 크롤러 중단됨: {str(e)}")
        notifier.error("식품안전나라 크롤러 중단",
                        f"{e}\n재개: --start-page {start_page} --end-page {end_page or '끝'}")
        if 'browser' in locals():
            browser.close()
        raise e


def main():
    parser = argparse.ArgumentParser(description="식품안전나라 식당 대표자 크롤러")
    parser.add_argument("--sido", type=str, default="경기도")
    parser.add_argument("--addr", type=str, default="")
    parser.add_argument("--start-page", type=int, default=1, help="시작 페이지 (기본: 1)")           
    parser.add_argument("--end-page", type=int, default=None, help="끝 페이지 (기본: 전체)")          
    parser.add_argument("--auto-resume", action="store_true", help="이전 진행 상태에서 자동 재개")  
    args = parser.parse_args()

    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    region = args.sido[:2]

    base_path = Path(Config.project_root)
    work_dir = base_path / f"_work/year={year}/month={month}/region={region}"
    parquet_dir = base_path / f"parquet/year={year}/month={month}/region={region}"

    work_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    safe_addr = args.addr.replace(" ", "_")
    log_file = work_dir / f"run_{region}_{safe_addr}.log"
    csv_file = work_dir / f"{region}_{safe_addr}.csv"
    parquet_file = parquet_dir / f"{region}_{safe_addr}.parquet"
    progress_file = work_dir / f"progress_{region}_{safe_addr}.txt"  # 

    logger = build_logger(log_file)

    start_page = args.start_page
    end_page = args.end_page

    if args.auto_resume and progress_file.exists():
        saved = _load_progress(progress_file)
        if saved > 0:
            start_page = max(start_page, saved + 1)
            logger.info(f"🔄 자동 재개 모드: {saved}페이지까지 완료 → {start_page}페이지부터 시작")

    crawl_complete = False

    # 크롤링 단계
    if start_page == 1 and csv_file.exists() and not args.auto_resume:
        logger.info(f"⏭️  이미 CSV 파일이 존재합니다. 크롤링을 건너뜁니다: {csv_file.name}")
        logger.info(f"   (재개하려면 --auto-resume 또는 --start-page N 옵션을 사용하세요)")
        crawl_complete = True
    else:
        logger.info(f"🚀 크롤링 시작: {args.sido} {args.addr} (페이지 {start_page}~{end_page or '끝'})")
        crawl_complete = search_and_save_all_pages(
            sido_name=args.sido,
            address=args.addr,
            cfg=Config(),
            logger=logger,
            output_file=csv_file,
            start_page=start_page,
            end_page=end_page,
            progress_file=progress_file if args.auto_resume else None,
        )

    # 변환 단계: 크롤링이 전체 완료된 경우에만 실행
    if not crawl_complete:
        logger.info("⏸️  크롤링이 아직 완료되지 않았습니다. Parquet 변환을 건너뜁니다.")
        logger.info(f"   재개하려면: --auto-resume 또는 --start-page {start_page}")
        return

    if parquet_file.exists():
        logger.info(f"⏭️  이미 Parquet 파일이 존재합니다: {parquet_file.name}")
    elif csv_file.exists():
        try:
            logger.info("📄 CSV를 Parquet로 변환 중...")
            df = pd.read_csv(csv_file)
            df.to_parquet(parquet_file, engine='pyarrow', index=False, compression='snappy')
            logger.info(f"✅ 변환 완료: {parquet_file}")

            # 완료 후 progress 파일 정리
            if progress_file and progress_file.exists():
                progress_file.unlink()
                logger.info("🧹 progress 파일 삭제 완료")
        except Exception as e:
            logger.error(f"❌ Parquet 변환 실패: {e}")
    else:
        logger.error("⚠️ 변환할 CSV 파일이 없어 프로세스를 종료합니다.")

def run_workflow(**kwargs):
    # 1. UI(conf)에서 값 가져오기 (기본값 설정)
    conf = kwargs.get('dag_run').conf or {}

    sido = conf.get('sido', '경기도')
    addr = conf.get('addr', '')
    start_page = conf.get('start_page', 1)  # 변수명 통일 (Pythonic)
    end_page = conf.get('end_page', None)    # None이면 전체 크롤링
    auto_resume = conf.get('auto_resume', True)

    # 2. sys.argv 조작 (main의 argparse가 읽을 수 있도록)
    import sys
    sys.argv = [sys.argv[0], "--sido", sido, "--addr", addr]

    # 숫자형 인자들 추가
    sys.argv.extend(["--start-page", str(start_page)])
    if end_page:
        sys.argv.extend(["--end-page", str(end_page)])
    if auto_resume:
        sys.argv.append("--auto-resume")

    # 3. 메인 로직 실행
    print(f"🚀 Airflow에서 전달받은 설정으로 실행: {sys.argv}")
    main()

if __name__ == "__main__":
    main()
