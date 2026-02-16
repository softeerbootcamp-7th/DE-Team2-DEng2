import argparse
import re
import sys
from pathlib import Path
from typing import Optional

# 프로젝트 루트를 import 경로에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.script._upload_to_s3_runner import (
    add_upload_args,
    ensure_upload_script,
    run_extractor_script,
    upload_existing_dirs,
)


EXTRACT_SCRIPT = PROJECT_ROOT / "data_pipeline" / "extract" / "extract_restaurant.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run extract_restaurant.py and upload written output directories to S3 "
            "using upload_data_to_s3.py when extract finishes successfully."
        )
    )
    return add_upload_args(parser).parse_args()


def find_latest_quarter_dataset_dir(parquet_root: Path) -> Optional[Path]:
    candidates: list[tuple[int, int, Path]] = []

    for d in parquet_root.glob("dataset=*"):
        if not d.is_dir():
            continue
        tag = d.name.split("=", 1)[-1]
        m = re.fullmatch(r"(\d{4})Q([1-4])", tag)
        if not m:
            continue
        candidates.append((int(m.group(1)), int(m.group(2)), d))

    if not candidates:
        return None

    return max(candidates, key=lambda x: (x[0], x[1]))[2]


def resolve_output_dirs() -> list[Path]:
    base = PROJECT_ROOT / "data" / "restaurant"
    dirs = [
        base / "_work" / "zips",
        base / "_work" / "unzipped",
    ]

    latest_dataset = find_latest_quarter_dataset_dir(base / "parquet")
    if latest_dataset is not None:
        dirs.append(latest_dataset)

    return dirs


def main() -> None:
    args = parse_args()
    ensure_upload_script()

    # extract 단계가 예외 없이 끝나야만 upload 단계로 넘어갑니다.
    run_extractor_script(EXTRACT_SCRIPT, extractor_args=[])
    print("[INFO] extract_restaurant.py finished successfully")

    output_dirs = resolve_output_dirs()
    uploaded_count = upload_existing_dirs(output_dirs, args)
    print(f"[DONE] uploaded_directories={uploaded_count}/{len(output_dirs)}")


if __name__ == "__main__":
    main()
