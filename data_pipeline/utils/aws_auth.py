import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError


_MFA_CACHE_SKEW_SECONDS = 60
_DEFAULT_MFA_CACHE_FILE = Path.home() / ".aws" / "mfa_session_cache.json"


def _cache_path() -> Path:
    configured = os.getenv("AWS_MFA_CACHE_FILE")
    if configured:
        return Path(configured).expanduser()
    return _DEFAULT_MFA_CACHE_FILE


def _cache_key(profile_name: Optional[str], serial: str, region_name: Optional[str]) -> str:
    return f"{profile_name or 'default'}|{serial}|{region_name or ''}"


def _load_cache(path: Path) -> Dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError:
        return {}

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}

    if not isinstance(parsed, dict):
        return {}
    return parsed


def _save_cache(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        # Best-effort file permission hardening.
        pass


def _parse_expiration(value: str) -> Optional[datetime]:
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_not_expired(expiration: datetime) -> bool:
    return expiration > datetime.now(timezone.utc) + timedelta(seconds=_MFA_CACHE_SKEW_SECONDS)


def _build_session_from_creds(
    access_key_id: str,
    secret_access_key: str,
    session_token: str,
    region_name: Optional[str],
    fallback_region: Optional[str],
):
    return boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
        region_name=region_name or fallback_region,
    )


def _session_has_token(session: boto3.Session) -> bool:
    credentials = session.get_credentials()
    if credentials is None:
        return False
    frozen = credentials.get_frozen_credentials()
    return bool(frozen.token)


def _is_session_valid(session: boto3.Session) -> bool:
    try:
        session.client("sts").get_caller_identity()
        return True
    except (ClientError, BotoCoreError):
        return False


def _load_cached_mfa_session(
    profile_name: Optional[str],
    region_name: Optional[str],
    serial: str,
    fallback_region: Optional[str],
):
    cache_file = _cache_path()
    cache = _load_cache(cache_file)
    entry = cache.get(_cache_key(profile_name, serial, region_name))

    if not isinstance(entry, dict):
        return None

    access_key_id = entry.get("AccessKeyId")
    secret_access_key = entry.get("SecretAccessKey")
    session_token = entry.get("SessionToken")
    expiration_raw = entry.get("Expiration")

    if not all(isinstance(v, str) and v for v in [access_key_id, secret_access_key, session_token, expiration_raw]):
        return None

    expiration = _parse_expiration(expiration_raw)
    if expiration is None or not _is_not_expired(expiration):
        return None

    session = _build_session_from_creds(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region_name=region_name,
        fallback_region=fallback_region,
    )

    if _is_session_valid(session):
        return session
    return None


def _save_cached_mfa_session(
    profile_name: Optional[str],
    region_name: Optional[str],
    serial: str,
    creds: Dict[str, Any],
) -> None:
    expiration_value = creds.get("Expiration")
    if isinstance(expiration_value, datetime):
        expiration = expiration_value.astimezone(timezone.utc).isoformat()
    elif isinstance(expiration_value, str):
        expiration_dt = _parse_expiration(expiration_value)
        if expiration_dt is None:
            return
        expiration = expiration_dt.isoformat()
    else:
        return

    access_key_id = creds.get("AccessKeyId")
    secret_access_key = creds.get("SecretAccessKey")
    session_token = creds.get("SessionToken")
    if not all(isinstance(v, str) and v for v in [access_key_id, secret_access_key, session_token]):
        return

    cache_file = _cache_path()
    cache = _load_cache(cache_file)
    cache[_cache_key(profile_name, serial, region_name)] = {
        "AccessKeyId": access_key_id,
        "SecretAccessKey": secret_access_key,
        "SessionToken": session_token,
        "Expiration": expiration,
    }
    _save_cache(cache_file, cache)


def authenticate_aws_session(
    profile_name: Optional[str] = None,
    region_name: Optional[str] = None,
    mfa_serial: Optional[str] = None,
    mfa_token_code: Optional[str] = None,
    mfa_duration_seconds: int = 10800,
    prompt_for_mfa: bool = False,
):
    """
    Build a boto3 session with optional MFA-based temporary credentials.

    - If MFA serial is not provided, this returns a normal profile-based session.
    - If MFA serial is provided, an unexpired cached MFA session is reused first.
    - If no valid cached session exists, STS get_session_token() is called and the
      returned temporary credentials are used to build a new session.
    """
    base_session = boto3.Session(profile_name=profile_name, region_name=region_name)

    serial = mfa_serial or os.getenv("AWS_MFA_SERIAL")
    token_code = mfa_token_code or os.getenv("AWS_MFA_TOKEN_CODE")

    if not serial:
        return base_session

    if not (900 <= mfa_duration_seconds <= 129600):
        raise ValueError("mfa_duration_seconds must be between 900 and 129600.")

    if not token_code:
        cached_session = _load_cached_mfa_session(
            profile_name=profile_name,
            region_name=region_name,
            serial=serial,
            fallback_region=base_session.region_name,
        )
        if cached_session is not None:
            return cached_session

        if _session_has_token(base_session) and _is_session_valid(base_session):
            return base_session

    if serial and not token_code and prompt_for_mfa:
        token_code = input(f"Enter MFA token code for {serial}: ").strip()

    if not token_code:
        raise ValueError(
            "MFA serial is set but token code is missing. "
            "Use --mfa-token-code, set AWS_MFA_TOKEN_CODE, or use --prompt-mfa."
        )

    if not token_code.isdigit():
        raise ValueError("MFA token code must be numeric.")

    sts = base_session.client("sts")
    creds = sts.get_session_token(
        SerialNumber=serial,
        TokenCode=token_code,
        DurationSeconds=mfa_duration_seconds,
    )["Credentials"]

    _save_cached_mfa_session(
        profile_name=profile_name,
        region_name=region_name,
        serial=serial,
        creds=creds,
    )

    return _build_session_from_creds(
        access_key_id=creds["AccessKeyId"],
        secret_access_key=creds["SecretAccessKey"],
        session_token=creds["SessionToken"],
        region_name=region_name,
        fallback_region=base_session.region_name,
    )
