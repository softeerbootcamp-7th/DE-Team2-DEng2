import json
from pathlib import Path

from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

COOKIE_PATH = Path("data_pipeline/extract/secrets/vworld_cookies.json")
START_URL = "https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?svcCde=NA&dsId=12"

def main():
    COOKIE_PATH.parent.mkdir(parents=True, exist_ok=True)

    opts = Options()
    opts.add_argument("--window-size=1400,900")
    # 수동 로그인할 거니까 headless는 끄는 게 맞음
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    try:
        driver.get(START_URL)
        print("\n브라우저에서 직접 로그인 완료 후 Enter를 누르세요.")
        input()

        cookies = driver.get_cookies()
        COOKIE_PATH.write_text(json.dumps(cookies, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"✅ 쿠키 저장 완료: {COOKIE_PATH.resolve()}")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
