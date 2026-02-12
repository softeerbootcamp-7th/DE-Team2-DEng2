# ============================================================
# CONFIG
# ============================================================

WAIT_SHORT = 5
WAIT_NORMAL = 20
WEBHOOK_URL = ""
DOWNLOAD_DIR = "./pdf"
ORIGINAL_FILE_NAME = "정부24 - 토지(임야)대장 등본 발급(열람) _ 문서출력.pdf"

# ============================================================
# IMPORTS
# ============================================================

import os
import re
import sys
import time
import json
import csv
import urllib.request
from typing import Optional, Tuple

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
    webhook_url = webhook_url or WEBHOOK_URL or os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("⚠ Slack webhook 없음:", text)
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
        print("✅ Slack 알림 전송 완료")

    except Exception as e:
        print("⚠ Slack 전송 실패:", e)


def parse_jibun(jibun_address: str) -> Tuple[str, str, str]:
    """
    "경기도 시흥시 대야동 41-3"
    → ("경기도 시흥시 대야동", "41", "3")
    """
    m = re.match(r"(.+?)\s+(\d+)(?:-(\d+))?$", jibun_address.strip())

    if not m:
        raise ValueError(f"지번주소 형식 오류: {jibun_address}")

    return m.group(1), m.group(2), m.group(3) or ""

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
    pdf_path = os.path.join(DOWNLOAD_DIR, ORIGINAL_FILE_NAME)

    new_path = os.path.join(
        os.path.dirname(pdf_path),
        safe_name
    )
    os.rename(pdf_path, new_path)

    return new_path

def load_addresses_from_csv(csv_path, start_idx, end_idx=None):
    addresses = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        if end_idx is None:
            end_idx = len(rows)

        for i in range(start_idx, min(end_idx, len(rows))):
            row = rows[i]

            dong = row["법정동명"].strip()
            jibun_raw = row["지번 목록"].strip()

            first_jibun = jibun_raw.split(",")[0].strip()

            address = f"{dong} {first_jibun}"
            addresses.append(address)

    return addresses


# ============================================================
# BROWSER
# ============================================================

def build_chrome_driver(headless=False):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
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
        "savefile.default_directory": os.path.abspath(DOWNLOAD_DIR)
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


# ============================================================
# GOV24 WORKFLOW — NAVIGATION
# ============================================================

def go_to_land_register(driver):
    wait = WebDriverWait(driver, WAIT_NORMAL)

    link = wait.until(EC.presence_of_element_located((
        By.XPATH,
        "//a[contains(@href,'CappBizCD=13100000026')]"
    )))

    driver.execute_script("arguments[0].click();", link)
    wait.until(EC.url_contains("CappBizCD"))

    print("# 토지대장 페이지 이동 완료")


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
        print("🚨 로그인 필요")
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

def fill_form(driver, jibun_address):
    wait = WebDriverWait(driver, WAIT_NORMAL)

    base, main_no, sub_no = parse_jibun(jibun_address)

    # 기본 주소 입력
    click_search_popup(driver)
    search_address_popup(driver, base)

    # 선택 주소 출력
    addr_field = wait.until(EC.presence_of_element_located(( 
        By.ID, "토지임야대장신청서_IN-토지임야대장신청서_신청토지소재지_주소정보_지역정보_지역명" 
    ))) 
    selected_addr = addr_field.get_attribute("value") 
    
    print("선택된 주소:", selected_addr)

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

    print(f"✅ 신청 완료 ({jibun_address})")


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

    driver.get("https://plus.gov.kr/")

    go_to_land_register(driver)
    click_issue_button(driver)

    if detect_login_page(driver):
        slack_notify("🚨 정부24 로그인 필요")

        while detect_login_page(driver):
            time.sleep(1)

    fill_form(driver, address)
    get_pdf(driver)
    rename_pdf_to_address(address)


# ============================================================
# MAIN ENTRY
# ============================================================

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("사용법:")
        print("python gov24.py <csv_path> <start_idx> [end_idx]")
        sys.exit(1)

    csv_path = sys.argv[1]
    start_idx = int(sys.argv[2])

    end_idx = None
    if len(sys.argv) >= 4:
        end_idx = int(sys.argv[3])

    address_list = load_addresses_from_csv(csv_path, start_idx, end_idx)

    driver = build_chrome_driver(headless=False)

    for idx, addr in enumerate(address_list, start=start_idx):
        try:
            run_land_register(driver, addr)
            print(f"✅ 완료 idx:{idx} → {addr}")

        except Exception as e:
            slack_notify(f"❌ 실패 idx:{idx} {addr}\n{e}")
            print("에러:", e)
            break

    driver.quit()