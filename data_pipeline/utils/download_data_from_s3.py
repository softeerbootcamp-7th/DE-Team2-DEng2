import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.utils.aws_auth import authenticate_aws_session

load_dotenv()

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download S3 prefix to local directory while preserving directory structure."
    )
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"), required=os.getenv("S3_BUCKET") is None, help="Destination S3 bucket name")
    parser.add_argument("--prefix", default="data", help="Source S3 key prefix (default: data)")
    parser.add_argument("--local-dir", default="data", help="Local target directory (default: data)")
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
    parser.add_argument("--dry-run", action="store_true", help="Print download plan only")
    return parser.parse_args()


def normalize_prefix(prefix: str) -> str:
    return prefix.strip("/")


def list_object_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    paginate_args = {"Bucket": bucket}
    if prefix:
        paginate_args["Prefix"] = prefix

    keys: list[str] = []
    for page in paginator.paginate(**paginate_args):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            keys.append(key)
    return keys


def relative_path_from_key(key: str, prefix: str) -> Path:
    if prefix and key.startswith(f"{prefix}/"):
        relative_key = key[len(prefix) + 1 :]
    else:
        relative_key = key

    relative_path = Path(relative_key)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"Unsafe S3 key for local path mapping: {key}")
    return relative_path


def main() -> None:
    args = parse_args()
    clean_prefix = normalize_prefix(args.prefix)
    local_root = Path(args.local_dir).resolve()
    local_root.mkdir(parents=True, exist_ok=True)

    session = authenticate_aws_session(
        profile_name=args.profile,
        region_name=args.region,
        mfa_serial=args.mfa_serial,
        mfa_token_code=args.mfa_token_code,
        mfa_duration_seconds=args.mfa_duration_seconds,
        prompt_for_mfa=args.prompt_mfa,
    )
    s3 = session.client("s3")

    keys = list_object_keys(s3, args.bucket, clean_prefix)
    if not keys:
        target = f"s3://{args.bucket}/{clean_prefix}" if clean_prefix else f"s3://{args.bucket}"
        print(f"[INFO] No objects found under: {target}")
        return

    downloaded = 0
    skipped = 0

    for key in keys:
        rel = relative_path_from_key(key, clean_prefix)
        local_path = local_root / rel

        if local_path.exists():
            print(f"[SKIP] s3://{args.bucket}/{key} -> {local_path}")
            skipped += 1
            continue

        if args.dry_run:
            print(f"[DRY-RUN] s3://{args.bucket}/{key} -> {local_path}")
            downloaded += 1
            continue

        local_path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(args.bucket, key, str(local_path))
        print(f"[DOWNLOADED] s3://{args.bucket}/{key} -> {local_path}")
        downloaded += 1

    print(f"[DONE] downloaded={downloaded}, skipped={skipped}, total={len(keys)}")


if __name__ == "__main__":
    main()
