import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UPLOAD_SCRIPT = PROJECT_ROOT / "data_pipeline" / "utils" / "upload_data_to_s3.py"


def add_upload_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"), required=os.getenv("S3_BUCKET") is None, help="Destination S3 bucket name")
    parser.add_argument("--prefix", default="data", help="Base S3 key prefix (default: data)")
    parser.add_argument("--profile", default=None, help="AWS profile name (optional)")
    parser.add_argument("--region", default="ap-northeast-2", help="AWS region")
    parser.add_argument("--mfa-serial", default=os.getenv("AWS_MFA_SERIAL"), help="MFA device ARN (optional)")
    parser.add_argument("--mfa-token-code", default=None, help="Current MFA token code (optional)")
    parser.add_argument(
        "--mfa-duration-seconds",
        type=int,
        default=3600,
        help="STS session duration when MFA is used (default: 3600).",
    )
    parser.add_argument(
        "--prompt-mfa",
        action="store_true",
        help="Prompt for MFA token code in terminal when needed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run extractor normally, but make upload step dry-run only.",
    )
    return parser


def ensure_upload_script() -> None:
    if not UPLOAD_SCRIPT.exists():
        raise FileNotFoundError(f"upload script not found: {UPLOAD_SCRIPT}")


def build_upload_prefix(base_prefix: str, local_dir: Path) -> str:
    clean_base = base_prefix.strip("/")
    try:
        rel = local_dir.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        rel = local_dir.name

    if clean_base:
        return f"{clean_base}/{rel}"
    return rel


def count_files(local_dir: Path) -> int:
    return sum(1 for p in local_dir.rglob("*") if p.is_file())


def run_upload_for_dir(local_dir: Path, args: argparse.Namespace) -> None:
    upload_prefix = build_upload_prefix(args.prefix, local_dir)
    cmd = [
        sys.executable,
        str(UPLOAD_SCRIPT),
        "--local-dir",
        str(local_dir),
        "--bucket",
        args.bucket,
        "--prefix",
        upload_prefix,
        "--region",
        args.region,
        "--mfa-duration-seconds",
        str(args.mfa_duration_seconds),
    ]

    if args.profile:
        cmd.extend(["--profile", args.profile])
    if args.mfa_serial:
        cmd.extend(["--mfa-serial", args.mfa_serial])
    if args.mfa_token_code:
        cmd.extend(["--mfa-token-code", args.mfa_token_code])
    if args.prompt_mfa:
        cmd.append("--prompt-mfa")
    if args.dry_run:
        cmd.append("--dry-run")

    print(f"[UPLOAD] local_dir={local_dir} prefix={upload_prefix}")
    subprocess.run(cmd, check=True)


def upload_existing_dirs(dirs: Iterable[Path], args: argparse.Namespace) -> int:
    uploaded_count = 0

    for local_dir in dirs:
        if not local_dir.exists() or not local_dir.is_dir():
            print(f"[SKIP] directory not found: {local_dir}")
            continue

        n_files = count_files(local_dir)
        if n_files == 0:
            print(f"[SKIP] no files in directory: {local_dir}")
            continue

        run_upload_for_dir(local_dir, args)
        uploaded_count += 1

    return uploaded_count


def run_extractor_script(extract_script: Path, extractor_args: List[str]) -> None:
    if not extract_script.exists():
        raise FileNotFoundError(f"extract script not found: {extract_script}")

    cmd = [sys.executable, str(extract_script), *extractor_args]
    subprocess.run(cmd, check=True)
