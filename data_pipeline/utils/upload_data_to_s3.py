import argparse
import os
import sys
from pathlib import Path
from typing import Iterable
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from botocore.exceptions import ClientError

from data_pipeline.utils.aws_auth import authenticate_aws_session

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload local data directory to S3 while preserving directory structure."
    )
    parser.add_argument("--local-dir", default="data", help="Local directory to upload (default: data)")
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"), required=os.getenv("S3_BUCKET") is None, help="Destination S3 bucket name")
    parser.add_argument("--prefix", default="data", help="Destination S3 key prefix (default: data)")
    parser.add_argument("--profile", default=None, help="AWS profile name (optional)")
    parser.add_argument("--region", default="ap-northeast-2", help="AWS region (optional)")
    parser.add_argument(
        "--mfa-serial",
        default=os.getenv("AWS_MFA_SERIAL"),
        help="MFA device ARN (optional). If set, temporary session credentials are used.",
    )
    parser.add_argument(
        "--mfa-token-code",
        default=None,
        help="Current MFA token code (optional).",
    )
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
        "--exclude",
        action="append",
        default=[],
        help=(
            "Path/name pattern to exclude recursively (repeatable). "
            "Examples: --exclude _work --exclude .DS_Store"
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Print upload plan only")

    return parser.parse_args()


def s3_key(prefix: str, relative_path: Path) -> str:
    clean_prefix = prefix.strip("/")
    rel = relative_path.as_posix()
    if clean_prefix:
        return f"{clean_prefix}/{rel}"
    return rel


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            return False
        raise


def _normalize_excludes(raw_patterns: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for value in raw_patterns:
        clean = value.strip()
        if not clean:
            continue
        normalized.append(clean.replace("\\", "/").strip("/"))
    return normalized


def _is_excluded(relative_path: Path, exclude_patterns: list[str]) -> bool:
    rel = relative_path.as_posix().strip("/")
    if not rel:
        return False
    rel_parts = relative_path.parts

    for pattern in exclude_patterns:
        if not pattern:
            continue

        if "/" in pattern:
            if rel == pattern or rel.startswith(f"{pattern}/") or f"/{pattern}/" in f"/{rel}/":
                return True
        else:
            if pattern in rel_parts:
                return True
            if relative_path.name == pattern:
                return True

    return False


def _collect_upload_files(local_root: Path, exclude_patterns: list[str]) -> tuple[list[Path], int]:
    files: list[Path] = []
    excluded_count = 0

    for current_root, dir_names, file_names in os.walk(local_root, topdown=True):
        current_path = Path(current_root)
        relative_dir = current_path.relative_to(local_root)

        kept_dirs: list[str] = []
        for dir_name in dir_names:
            rel_dir = Path(dir_name) if relative_dir == Path(".") else relative_dir / dir_name
            if _is_excluded(rel_dir, exclude_patterns):
                excluded_count += 1
                continue
            kept_dirs.append(dir_name)
        dir_names[:] = kept_dirs

        for file_name in file_names:
            rel_file = Path(file_name) if relative_dir == Path(".") else relative_dir / file_name
            if _is_excluded(rel_file, exclude_patterns):
                excluded_count += 1
                continue
            files.append(current_path / file_name)

    return files, excluded_count


def main() -> None:
    args = parse_args()
    local_root = Path(args.local_dir).resolve()

    if not local_root.exists() or not local_root.is_dir():
        raise FileNotFoundError(f"Local directory not found: {local_root}")

    exclude_patterns = _normalize_excludes(args.exclude)

    session = authenticate_aws_session(
        profile_name=args.profile,
        region_name=args.region,
        mfa_serial=args.mfa_serial,
        mfa_token_code=args.mfa_token_code,
        mfa_duration_seconds=args.mfa_duration_seconds,
        prompt_for_mfa=args.prompt_mfa,
    )
    s3 = session.client("s3")

    files, excluded_count = _collect_upload_files(local_root, exclude_patterns)
    if not files:
        print(f"[INFO] No files found under: {local_root} (excluded={excluded_count})")
        return

    uploaded = 0
    skipped = 0

    for file_path in files:
        rel = file_path.relative_to(local_root)
        key = s3_key(args.prefix, rel)

        if object_exists(s3, args.bucket, key):
            print(f"[SKIP] s3://{args.bucket}/{key}")
            skipped += 1
            continue

        if args.dry_run:
            print(f"[DRY-RUN] {file_path} -> s3://{args.bucket}/{key}")
            uploaded += 1
            continue

        s3.upload_file(str(file_path), args.bucket, key)
        print(f"[UPLOADED] {file_path} -> s3://{args.bucket}/{key}")
        uploaded += 1

    print(f"[DONE] uploaded={uploaded}, skipped={skipped}, excluded={excluded_count}, total={len(files)}")


if __name__ == "__main__":
    main()
