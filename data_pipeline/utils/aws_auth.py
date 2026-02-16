import os
from typing import Optional

import boto3


def authenticate_aws_session(
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    mfa_serial: Optional[str] = None,
    mfa_token_code: Optional[str] = None,
    mfa_duration_seconds: int = 3600,
    prompt_for_mfa: bool = False,
):
    """
    Build a boto3 session with optional MFA-based temporary credentials.

    - If MFA serial is not provided, this returns a normal profile-based session.
    - If MFA serial is provided, STS get_session_token() is called and the returned
      temporary credentials are used to build a new session.
    """
    base_session = boto3.Session(profile_name=profile_name, region_name=region_name)

    serial = mfa_serial or os.getenv("AWS_MFA_SERIAL")
    token_code = mfa_token_code or os.getenv("AWS_MFA_TOKEN_CODE")

    if serial and not token_code and prompt_for_mfa:
        token_code = input(f"Enter MFA token code for {serial}: ").strip()

    if not serial:
        return base_session

    if not token_code:
        raise ValueError(
            "MFA serial is set but token code is missing. "
            "Use --mfa-token-code, set AWS_MFA_TOKEN_CODE, or use --prompt-mfa."
        )

    if not token_code.isdigit():
        raise ValueError("MFA token code must be numeric.")

    if not (900 <= mfa_duration_seconds <= 129600):
        raise ValueError("mfa_duration_seconds must be between 900 and 129600.")

    sts = base_session.client("sts")
    creds = sts.get_session_token(
        SerialNumber=serial,
        TokenCode=token_code,
        DurationSeconds=mfa_duration_seconds,
    )["Credentials"]

    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region_name or base_session.region_name,
    )

