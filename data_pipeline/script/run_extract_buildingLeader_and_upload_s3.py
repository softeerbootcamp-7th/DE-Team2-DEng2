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


EXTRACT_SCRIPT = PROJECT_ROOT / "data_pipeline" / "extract" / "extract_buildingLeader.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run extract_buildingLeader.py and upload written output directories to S3 "
            "using upload_data_to_s3.py when extract finishes successfully."
        )
    )
    return add_upload_args(parser).parse_args()


def parse_partition_num(name: str, prefix: str) -> Optional[int]:
    m = re.fullmatch(rf"{prefix}=(\d+)", name)
    if not m:
        return None
    return int(m.group(1))


def find_latest_month_partition_dir(parquet_root: Path) -> Optional[Path]:
    candidates: list[tuple[int, int, Path]] = []

    for year_dir in parquet_root.glob("year=*"):
        if not year_dir.is_dir():
            continue
        year = parse_partition_num(year_dir.name, "year")
        if year is None:
            continue

        for month_dir in year_dir.glob("month=*"):
            if not month_dir.is_dir():
                continue
            month = parse_partition_num(month_dir.name, "month")
            if month is None:
                continue
            candidates.append((year, month, month_dir))

    if not candidates:
        return None

    return max(candidates, key=lambda x: (x[0], x[1]))[2]


def resolve_output_dirs() -> list[Path]:
    base = PROJECT_ROOT / "data" / "buildingLeader"
    dirs = [
        base / "_work" / "zips",
        base / "_work" / "unzipped",
    ]

    latest_month_dir = find_latest_month_partition_dir(base / "parquet")
    if latest_month_dir is not None:
        dirs.append(latest_month_dir)

    return dirs


def main() -> None:
    args = parse_args()
    ensure_upload_script()

    # extract 단계가 예외 없이 끝나야만 upload 단계로 넘어갑니다.
    run_extractor_script(EXTRACT_SCRIPT, extractor_args=[])
    print("[INFO] extract_buildingLeader.py finished successfully")

    output_dirs = resolve_output_dirs()
    uploaded_count = upload_existing_dirs(output_dirs, args)
    print(f"[DONE] uploaded_directories={uploaded_count}/{len(output_dirs)}")


if __name__ == "__main__":
    main()
