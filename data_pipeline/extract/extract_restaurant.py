import os
import sys
import re
import time
import logging
import requests

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List
from zipfile import ZipFile
from urllib.parse import urlencode

from dotenv import load_dotenv

# =========================
# ENV
# =========================
load_dotenv()


# =========================
# Config
# =========================
@dataclass
class Config:
    detail_url: str = "https://www.data.go.kr/data/15083033/fileData.do"
    download_base_url: str = "https://www.data.go.kr/cmm/cmm/fileDownload.do"

    project_root: str = "data/restaurant"

    retries: int = 3
    retry_sleep_sec: int = 5
    timeout_sec: int = 60

    parquet_compression: str = "snappy"
    parquet_overwrite: bool = False

    skip_if_latest_quarter_exists: bool = True
    force_run: bool = False

    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")


# =========================
# Logging + Slack
# =========================
def build_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("restaurant_extract")
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
# Path setup (Modified)
# =========================
def init_run_dirs(cfg: Config) -> dict:
    # run 구분 없이 _work 밑에 바로 생성
    base = Path(cfg.project_root) / "_work"

    paths = {
        "base": base,
        "zips": base / "zips",
        "unzipped": base / "unzipped",
        "log_file": base / "run.log",  # logs 폴더 대신 run.log 파일 직접 지정
        "parquet_root": Path(cfg.project_root) / "parquet",
    }

    # 디렉토리 생성 (zips, unzipped)
    paths["zips"].mkdir(parents=True, exist_ok=True)
    paths["unzipped"].mkdir(parents=True, exist_ok=True)

    return paths


# =========================
# Quarter helpers
# =========================
def yyyymmdd_to_quarter(tag: str) -> str:
    if not re.fullmatch(r"\d{8}", tag):
        raise ValueError(f"Invalid YYYYMMDD: {tag}")

    year = tag[:4]
    mmdd = tag[4:]

    if mmdd == "0331": q = "Q1"
    elif mmdd == "0630": q = "Q2"
    elif mmdd == "0930": q = "Q3"
    elif mmdd == "1231": q = "Q4"
    else:
        raise ValueError(f"Invalid quarter date (0331/0630/0930/1231): {tag}")

    return f"{year}{q}"


def quarter_success_marker(parquet_root: Path, quarter_tag: str) -> Path:
    return parquet_root / f"dataset={quarter_tag}" / "_SUCCESS"


def latest_quarter_already_processed(cfg: Config, paths: dict, quarter_tag: str) -> bool:
    return quarter_success_marker(paths["parquet_root"], quarter_tag).exists()


# =========================
# Download helpers
# =========================
def fetch_detail_html(cfg: Config) -> str:
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.data.go.kr/", "Accept": "text/html,*/*"}
    r = requests.get(cfg.detail_url, headers=headers, timeout=cfg.timeout_sec)
    r.raise_for_status()
    return r.text


def resolve_latest_dataset_yyyymmdd(cfg: Config, logger: logging.Logger) -> str:
    logger.info(f"Fetching detail page: {cfg.detail_url}")
    html = fetch_detail_html(cfg)
    m = re.search(r"소상공인시장진흥공단_상가\(상권\)정보_(\d{8})", html)
    if not m:
        m = re.search(r"_(\d{8})", html)
    if not m:
        raise RuntimeError("Failed to parse latest dataset YYYYMMDD")

    yyyymmdd = m.group(1)
    logger.info(f"Latest dataset YYYYMMDD on portal: {yyyymmdd}")
    return yyyymmdd


def resolve_download_params(cfg: Config, logger: logging.Logger) -> Tuple[str, str]:
    html = fetch_detail_html(cfg)
    m1 = re.search(r"atchFileId=?(FILE_[0-9]+)", html)
    m2 = re.search(r"fileDetailSn=([0-9]+)", html)
    if not (m1 and m2):
        raise RuntimeError("Failed to parse atchFileId / fileDetailSn")
    return m1.group(1), m2.group(1)


def is_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 4:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PK\x03\x04"


def download_zip(cfg: Config, paths: dict, logger: logging.Logger) -> Path:
    atch, sn = resolve_download_params(cfg, logger)
    url = cfg.download_base_url + "?" + urlencode({"atchFileId": atch, "fileDetailSn": sn})
    zip_path = paths["zips"] / "sme_store.zip"
    headers = {"User-Agent": "Mozilla/5.0", "Referer": cfg.detail_url, "Accept": "*/*"}

    for attempt in range(1, cfg.retries + 1):
        try:
            logger.info(f"Downloading ({attempt}/{cfg.retries})")
            with requests.get(url, headers=headers, stream=True, timeout=cfg.timeout_sec) as r:
                r.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        if chunk: f.write(chunk)

            if not is_zip(zip_path):
                raise ValueError("Downloaded file is not a ZIP")
            return zip_path
        except Exception as e:
            logger.error(f"Download failed: {e}")
            if attempt < cfg.retries: time.sleep(cfg.retry_sleep_sec)
            else: raise


def extract_zip(zip_path: Path, paths: dict, logger: logging.Logger):
    logger.info(f"Extracting: {zip_path}")
    with ZipFile(zip_path, "r") as z:
        z.extractall(paths["unzipped"])


# =========================
# Parquet
# =========================
def find_dataset_dir(unzipped_dir: Path) -> Path:
    children = list(unzipped_dir.iterdir())
    if len(children) == 1 and children[0].is_dir():
        return children[0]
    return unzipped_dir


def list_csv_files(dataset_dir: Path) -> List[Path]:
    return sorted([p for p in dataset_dir.rglob("*.csv") if p.is_file()])


def parse_region_from_filename(name: str) -> str:
    m = re.search(r"_([가-힣]+)_\d{6}\.csv$", name)
    return m.group(1) if m else "unknown"


def write_success_marker(parquet_root: Path, quarter_tag: str):
    d = parquet_root / f"dataset={quarter_tag}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "_SUCCESS").write_text(f"ok {time.strftime('%Y-%m-%d %H:%M:%S')}\n", encoding="utf-8")


def convert_csvs_to_parquet(cfg: Config, paths: dict, logger: logging.Logger, quarter_tag: str) -> int:
    dataset_dir = find_dataset_dir(paths["unzipped"])
    csv_files = list_csv_files(dataset_dir)

    if not csv_files:
        logger.warning("No CSV files found to convert.")
        return 0

    parquet_base = paths["parquet_root"] / f"dataset={quarter_tag}"
    parquet_base.mkdir(parents=True, exist_ok=True)

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    written = 0
    total = len(csv_files)

    for idx, csv_path in enumerate(csv_files, 1):
        region = parse_region_from_filename(csv_path.name)
        out_dir = parquet_base / f"region={region}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "part.parquet"

        # 파일 존재 시 스킵 로그
        if out_path.exists() and not cfg.parquet_overwrite:
            logger.info(f"[{idx}/{total}] Skipping (already exists): region={region}")
            written += 1
            continue

        logger.info(f"[{idx}/{total}] Converting: {csv_path.name} -> region={region}")
        
        writer = None
        try:
            # chunk 단위로 읽어서 메모리 효율화
            reader = pd.read_csv(
                csv_path, 
                encoding="utf-8-sig", 
                chunksize=300_000, 
                dtype="string", 
                low_memory=False
            )
            
            for chunk in reader:
                table = pa.Table.from_pandas(chunk.astype("string"), preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(out_path, table.schema, compression=cfg.parquet_compression)
                writer.write_table(table)
            
            written += 1
            logger.info(f"Successfully saved: {out_path}")

        except Exception as e:
            logger.error(f"Failed to convert {csv_path.name}: {e}")
            raise # 하나라도 실패하면 중단 (필요에 따라 continue로 변경 가능)
        finally:
            if writer:
                writer.close()

    # 완료 후 _SUCCESS 파일 생성
    write_success_marker(paths["parquet_root"], quarter_tag)
    logger.info(f"Parquet conversion complete. Total {written} regions processed.")
    
    return written
# =========================
# Main
# =========================
def main():
    cfg = Config()
    paths = init_run_dirs(cfg)
    logger = build_logger(paths["log_file"])
    
    logger.info("===== EXTRACT START =====")

    try:
        # 0. 포털 데이터 날짜 및 분기 확인
        latest_yyyymmdd = resolve_latest_dataset_yyyymmdd(cfg, logger)
        latest_quarter = yyyymmdd_to_quarter(latest_yyyymmdd)

        # [STEP 1] ZIP 확인 및 다운로드
        zip_path = paths["zips"] / "sme_store.zip"
        if zip_path.exists() and is_zip(zip_path) and not cfg.force_run:
            logger.info(f">>> [CHECK 1] ZIP 파일이 이미 존재합니다. 다운로드 스킵.")
        else:
            zip_path = download_zip(cfg, paths, logger)

        # [STEP 2] CSV 확인 및 압축 해제
        existing_csvs = list_csv_files(paths["unzipped"])
        if existing_csvs and not cfg.force_run:
            logger.info(f">>> [CHECK 2] CSV 파일이 이미 존재합니다. 압축 해제 스킵.")
        else:
            extract_zip(zip_path, paths, logger)

        # [STEP 3] Parquet 확인 및 변환
        if (cfg.skip_if_latest_quarter_exists and not cfg.force_run 
            and latest_quarter_already_processed(cfg, paths, latest_quarter)):
            logger.info(f">>> [CHECK 3] 최종 Parquet가 이미 존재합니다. 변환 스킵.")
            n_parquet = "Existing"
        else:
            n_parquet = convert_csvs_to_parquet(cfg, paths, logger, latest_quarter)

        logger.info(f"===== SUCCESS (dataset={latest_quarter}, status={n_parquet}) =====")
        slack_notify(cfg.slack_webhook_url, f":white_check_mark: 상가정보 처리 완료 ({latest_quarter})", logger)

    except Exception as e:
        logger.exception("===== EXTRACT FAILED =====")
        slack_notify(cfg.slack_webhook_url, f":x: 실패\n{e}", logger)
        sys.exit(1)

if __name__ == "__main__":
    main()