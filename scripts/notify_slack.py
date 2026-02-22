#!/usr/bin/env python3
"""
Shell Script에서 Slack 알림을 보내기 위한 CLI 래퍼

USE CASE:
    python notify_slack.py success "제목" "메시지"
    python notify_slack.py error   "제목" "에러 내용"
    python notify_slack.py info    "제목" "정보"
"""
import argparse
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.utils.slack_utils import SlackNotifier

logger = logging.getLogger("notify_slack")
logging.basicConfig(level=logging.INFO, format="%(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Slack 알림 CLI")
    parser.add_argument(
        "level",
        choices=["success", "error", "info"],
        help="알림 수준 (success | error | info)",
    )
    parser.add_argument("title", help="알림 제목")
    parser.add_argument("message", nargs="?", default="", help="알림 본문")
    parser.add_argument(
        "--stage",
        default="TRANSFORM",
        help="작업 단계 (기본: TRANSFORM)",
    )
    args = parser.parse_args()

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.info("[SLACK] SLACK_WEBHOOK_URL 미설정 — 알림 생략")
        return

    notifier = SlackNotifier(webhook_url, args.stage, logger)

    if args.level == "success":
        notifier.success(args.title, args.message)
    elif args.level == "info":
        notifier.info(args.title, args.message)
    elif args.level == "error":
        notifier.error(args.title)
    else:
        logger.error(f"알 수 없는 level: {args.level}")
        sys.exit(1)

    logger.info(f"[SLACK] {args.level}: {args.title}")


if __name__ == "__main__":
    main()
