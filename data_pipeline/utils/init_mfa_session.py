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
        description="Initialize and cache an MFA-backed AWS session for subsequent scripts."
    )
    parser.add_argument("--profile", default=None, help="AWS profile name (optional)")
    parser.add_argument("--region", default="ap-northeast-2", help="AWS region (optional)")
    parser.add_argument(
        "--mfa-serial",
        default=os.getenv("AWS_MFA_SERIAL"),
        required=os.getenv("AWS_MFA_SERIAL") is None,
        help="MFA device ARN",
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
    return parser.parse_args()


def mfa_cache_path() -> Path:
    configured = os.getenv("AWS_MFA_CACHE_FILE")
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".aws" / "mfa_session_cache.json"


def main() -> None:
    args = parse_args()
    session = authenticate_aws_session(
        profile_name=args.profile,
        region_name=args.region,
        mfa_serial=args.mfa_serial,
        mfa_token_code=args.mfa_token_code,
        mfa_duration_seconds=args.mfa_duration_seconds,
        prompt_for_mfa=args.prompt_mfa,
    )

    identity = session.client("sts").get_caller_identity()
    print("[OK] MFA session is ready.")
    print(f"[ARN] {identity.get('Arn')}")
    print(f"[ACCOUNT] {identity.get('Account')}")
    print(f"[CACHE] {mfa_cache_path()}")


if __name__ == "__main__":
    main()
