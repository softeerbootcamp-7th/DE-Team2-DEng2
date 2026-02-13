# ============================================================
# CONFIG
# ============================================================

WAIT_SHORT = 3
WAIT_NORMAL = 10

OUTPUT_DIR = "./data/tojidaejang"
ORIGINAL_FILE_NAME = "정부24 - 토지(임야)대장 등본 발급(열람) _ 문서출력.pdf"

# ============================================================
# IMPORTS
# ============================================================

import os
import re
import sys
import time
import logging
import json
import argparse
import urllib.request
import pandas as pd
from typing import Optional, Tuple
from datetime import datetime


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains


# ============================================================
# UTILITIES
# ============================================================

def slack_notify(text: str, webhook_url: Optional[str] = None):
    webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logging.info("⚠ Slack webhook 없음:", text)
        return

    payload = {"text": text}
    data = json.dumps(payload).encode("utf-8")

    try:
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        urllib.request.urlopen(req, timeout=10)
        logging.info("✅ Slack 알림 전송 완료")

    except Exception as e:
        logging.info("⚠ Slack 전송 실패:", e)

def setup_logging(output_dir):
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"{timestamp}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # 파일 로그
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)

    # 콘솔 로그
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logging.info(f"📄 로그 파일 생성: {log_file}")

    return log_file

def build_full_address(addr: dict) -> str:
    base = addr["base"]
    main = addr["main"]
    sub = addr["sub"]

    if sub:
        return f"{base} {main}-{sub}"
    return f"{base} {main}"

def sanitize_filename(name: str) -> str:
    # 파일명에 못 쓰는 문자 제거
    invalid = r'<>:"/\\|?*'
    for ch in invalid:
        name = name.replace(ch, "_")

    # 공백 정리
    name = name.strip().replace(" ", "_")

    return name

def rename_pdf_to_address(address: str) -> str:
    safe_name = sanitize_filename(address) + ".pdf"
    pdf_path = os.path.join(OUTPUT_DIR, ORIGINAL_FILE_NAME)

    new_path = os.path.join(
        os.path.dirname(pdf_path),
        safe_name
    )
    os.rename(pdf_path, new_path)

    return new_path

def load_addresses_from_parquet_dir(dir_path, start_idx=0, end_idx=None):
    addresses = []

    parquet_files = []

    # 모든 하위 parquet 탐색
    for root, _, files in os.walk(dir_path):
        for f in files:
            if f.lower().endswith(".parquet"):
                parquet_files.append(os.path.join(root, f))

    if not parquet_files:
        raise RuntimeError("❌ parquet 파일을 찾지 못했습니다")

    logging.info(f"📂 parquet 파일 {len(parquet_files)}개 발견")

    # parquet 하나씩 읽기
    for file in sorted(parquet_files):
        logging.info(f"→ 읽는 중: {file}")

        df = pd.read_parquet(file)

        for _, row in df.iterrows():

            base_addr = str(row["법정동명"]).strip()
            bonbun = str(row["본번"]).strip()
            bubun_list = re.findall(r'\d+', str(row["부번_리스트"]))

            if len(bubun_list) == 0:
                sub_no = ""
            else:
                sub_no = bubun_list[0]

            addresses.append({
                "base": base_addr,
                "main": bonbun,
                "sub": sub_no
            })

    # 인덱스 범위 처리
    if end_idx is None:
        end_idx = len(addresses)

    sliced = addresses[start_idx:end_idx]

    logging.info(f"✅ 총 주소 {len(addresses)}개 → 선택 {len(sliced)}개")

    return sliced


# ============================================================
# BROWSER
# ============================================================

def build_chrome_driver(headless=False):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    opts = ChromeOptions()

    prefs = {
        "printing.print_preview_sticky_settings.appState": json.dumps({
            "recentDestinations": [{
                "id": "Save as PDF",
                "origin": "local",
                "account": ""
            }],
            "selectedDestinationId": "Save as PDF",
            "version": 2
        }),
        "savefile.default_directory": os.path.abspath(OUTPUT_DIR)
    }

    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("--kiosk-printing")  # 인쇄 자동 실행

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    if headless:
        opts.add_argument("--headless=new")

    driver = webdriver.Chrome(options=opts)
    driver.implicitly_wait(0)

    return driver

def is_error_page(driver):
    try:
        error_box = driver.find_elements(
            By.CSS_SELECTOR,
            "div.error-box"
        )

        return len(error_box) > 0

    except:
        return False

def force_navigation(driver, url):
    driver.execute_script(f"window.location.href='{url}'")

# ============================================================
# GOV24 WORKFLOW — NAVIGATION
# ============================================================

def go_to_land_register(driver):
    wait = WebDriverWait(driver, WAIT_SHORT)

    link = wait.until(EC.presence_of_element_located((
        By.XPATH,
        "//a[contains(@href,'CappBizCD=13100000026')]"
    )))

    driver.execute_script("arguments[0].click();", link)
    wait.until(EC.url_contains("CappBizCD"))

    logging.info("# 토지대장 페이지 이동 완료")


def click_issue_button(driver):
    wait = WebDriverWait(driver, WAIT_NORMAL)

    btn = wait.until(EC.presence_of_element_located((
        By.XPATH,
        "//a[contains(.,'발급하기')]"
    )))

    driver.execute_script("arguments[0].click();", btn)

def detect_login_page(driver):
    try:
        WebDriverWait(driver, WAIT_SHORT).until(
            EC.presence_of_element_located((By.NAME, "btn_end"))
        )

        return False

    except TimeoutException:
        logging.info("🚨 로그인 필요")
        return True
    

# ============================================================
# GOV24 WORKFLOW — ADDRESS POPUP
# ============================================================

def click_search_popup(driver):
    wait = WebDriverWait(driver, WAIT_NORMAL)

    btn = wait.until(EC.element_to_be_clickable((By.ID, "btnAddress")))
    driver.execute_script("arguments[0].click();", btn)

def search_address_popup(driver, base_addr):
    wait = WebDriverWait(driver, WAIT_NORMAL)
    main_window = driver.current_window_handle

    driver.switch_to.window(driver.window_handles[-1])

    addr_input = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "input[type='text']")
    ))

    addr_input.clear()
    addr_input.send_keys(base_addr)

    search_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//button[contains(., '검색')]")
    ))

    driver.execute_script("arguments[0].click();", search_btn)

    wait.until(EC.presence_of_element_located((By.ID, "resultList")))

    target = base_addr.split()[-1]
    addrs = driver.find_elements(By.CSS_SELECTOR, "#resultList a")

    for a in addrs:
        if target in a.text:
            driver.execute_script("arguments[0].click();", a)
            break

    driver.switch_to.window(main_window)

def close_modal_popup(driver):

    try:
        buttons = driver.find_elements(
            By.CSS_SELECTOR,
            ".btn-close-modal"
        )

        for btn in buttons:
            if btn.is_displayed():
                driver.execute_script(
                    "arguments[0].click();",
                    btn
                )
                logging.info("✅ Gov24 모달 팝업 닫음")
                time.sleep(0.3)

    except Exception as e:
        logging.warning(f"팝업 종료 실패: {e}")


# ============================================================
# GOV24 WORKFLOW — FORM
# ============================================================

def select_form_options(driver):
    wait = WebDriverWait(driver, WAIT_NORMAL)

    # 대장 구분 = 토지 대장 
    land_radio = wait.until(EC.element_to_be_clickable(( 
        By.XPATH, 
        "//input[@type='radio' and contains(@name,'대장구분') and @value='1']" 
    ))) 
    driver.execute_script("arguments[0].click();", land_radio) 
    
    # 토지이동연혁 인쇄 유무 = 인쇄함 
    history_radio = wait.until(EC.element_to_be_clickable(( 
        By.XPATH, 
        "//input[@type='radio' and contains(@name,'토지연혁구분') and @value='Y']" 
    ))) 
    driver.execute_script("arguments[0].click();", history_radio) 
    
    # 소유권연혁 인쇄 유무 = 인쇄함 
    ownership_radio = wait.until(EC.element_to_be_clickable(( 
        By.XPATH, 
        "//input[@type='radio' and contains(@name,'소유권연혁') and @value='Y']" 
    ))) 
    driver.execute_script("arguments[0].click();", ownership_radio) 
    
    # 폐쇄 대장 구분 = 일반(N) 
    normal_radio = wait.until(EC.element_to_be_clickable(( 
        By.XPATH, 
        "//input[@type='radio' and contains(@name,'폐쇄') and @value='N']" 
    ))) 
    driver.execute_script("arguments[0].click();", normal_radio) 
    
    # 특정 소유자 유무 = 없음(02) 
    no_owner_radio = wait.until(EC.element_to_be_clickable(( 
        By.XPATH, 
        "//input[@type='radio' and contains(@name,'특정소유자선택') and @value='02']" 
    ))) 
    driver.execute_script("arguments[0].click();", no_owner_radio) 
    
    # 수령 방법 = 온라인 발급 
    delivery_radio = wait.until(EC.presence_of_element_located(( 
        By.ID, "chk01" 
    )))

    driver.execute_script(""" 
        arguments[0].scrollIntoView({block:'center'}); 
        arguments[0].checked = true; 
        if (typeof fnFtChoose === 'function') { 
            fnFtChoose(arguments[0]); 
        } 
    """, delivery_radio)

def fill_form(driver, address):
    wait = WebDriverWait(driver, WAIT_NORMAL)

    base, main_no, sub_no = address["base"], address["main"], address["sub"]

    # 기본 주소 입력
    click_search_popup(driver)
    search_address_popup(driver, base)

    # 선택 주소 출력
    addr_field = wait.until(EC.presence_of_element_located(( 
        By.ID, "토지임야대장신청서_IN-토지임야대장신청서_신청토지소재지_주소정보_지역정보_지역명" 
    ))) 
    selected_addr = addr_field.get_attribute("value") 
    
    logging.info("선택된 주소:", selected_addr)

    # 본 번지 입력
    main_input = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//input[@title='본번지']"
    )))

    main_input.clear()
    main_input.send_keys(main_no)

    # 부 번지 입력
    sub_input = wait.until(EC.element_to_be_clickable((
        By.XPATH, "//input[@title='부번지']"
    )))

    sub_input.clear()
    sub_input.send_keys(sub_no)

    # 옵션 선택
    select_form_options(driver)

    apply_btn = wait.until(EC.presence_of_element_located((By.ID, "btn_end")))
    driver.execute_script("arguments[0].click();", apply_btn)

    logging.info(f"✅ 신청 완료 ({build_full_address(address)})")


# ============================================================
# GOV24 WORKFLOW - PDF
# ============================================================

def get_pdf(driver):
    wait = WebDriverWait(driver, WAIT_NORMAL)

    # 첫 번째 "문서출력" 버튼 클릭
    print_btn = wait.until(EC.element_to_be_clickable((
        By.XPATH,
        "(//button[normalize-space()='문서출력'])[1]"
    )))
    driver.execute_script("arguments[0].click();", print_btn)

    # 팝업 전환
    main_window = driver.current_window_handle

    WebDriverWait(driver, WAIT_SHORT).until(
        lambda d: len(d.window_handles) > 1
    )
    driver.switch_to.window(driver.window_handles[-1])

    time.sleep(2)

    # 인쇄 버튼 클릭
    print_btn = WebDriverWait(driver, WAIT_NORMAL).until(
        EC.element_to_be_clickable((By.ID, "btnPrint"))
    )

    ActionChains(driver)\
        .move_to_element(print_btn)\
        .pause(0.3)\
        .click()\
        .perform()
    
    time.sleep(2)

    WebDriverWait(driver, WAIT_NORMAL).until(
        EC.element_to_be_clickable((By.ID, "btnPrint"))
    )

    driver.close()
    driver.switch_to.window(main_window)
    

# ============================================================
# MAIN WORKFLOW
# ============================================================

def run_land_register(driver, address):
    if os.path.exists(ORIGINAL_FILE_NAME):
        raise FileExistsError(f"알 수 없는 PDF 파일: {ORIGINAL_FILE_NAME}")

    force_navigation(driver, "https://plus.gov.kr/")

    while True:
        try:
            go_to_land_register(driver)
            break
        except:
            force_navigation(driver, "https://plus.gov.kr/")

    click_issue_button(driver)

    if detect_login_page(driver):
        slack_notify("🚨 정부24 로그인 필요")

        while detect_login_page(driver):
            time.sleep(1)

    fill_form(driver, address)
    get_pdf(driver)
    rename_pdf_to_address(build_full_address(address))


# ============================================================
# MAIN ENTRY
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="parquet 주소 로더"
    )

    parser.add_argument(
        "path",
        help="parquet 상위 폴더 경로"
    )

    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="시작 인덱스 (옵션)"
    )

    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="끝 인덱스 (옵션)"
    )

    parser.add_argument(
        "--output_dir",
        default="./data/tojidaejang",
        help="토지대장 pdf가 저장될 폴더 경로 (기본 경로: ../../data/jogidaejang)"
    )

    args = parser.parse_args()

    OUTPUT_DIR = args.output_dir

    setup_logging(OUTPUT_DIR)

    address_list = load_addresses_from_parquet_dir(
        args.path,
        args.start,
        args.end
    )

    driver = build_chrome_driver(headless=False)

    for idx, addr in enumerate(address_list, start=args.start):
        try:
            run_land_register(driver, addr)
            logging.info(f"✅ 완료 idx:{idx} → {build_full_address(addr)}")

            time.sleep(1)
            close_modal_popup(driver)
            
        except Exception as e:
            try:
                run_land_register(driver, addr)
                logging.info(f"✅ 완료 idx:{idx} → {build_full_address(addr)}")
            
                time.sleep(1)
                close_modal_popup(driver)

            except Exception as e:
                slack_notify(f"❌ 실패 idx:{idx} {build_full_address(addr)}\n{e}")
                logging.error(f"에러 발생: {e}")
                break

    driver.quit()
