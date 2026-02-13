import requests
import logging
import traceback
from typing import Optional

class SlackNotifier:
    def __init__(self, webhook_url: Optional[str], stage: str, logger: logging.Logger):
        """
        :param stage: 'EXTRACT', 'TRANSFORM', 'LOAD' 등 작업 단계 명칭
        """
        self.webhook_url = webhook_url
        self.stage = stage
        self.logger = logger

    def _send_slack(self, color: str, title: str, message: str, emoji: str):
        if not self.webhook_url:
            self.logger.warning(f"[{self.stage}] Slack Webhook URL이 설정되지 않아 알림을 건너뜁니다.")
            return

        # Slack Attachment 형식을 사용하면 더 눈에 띄고 깔끔합니다.
        payload = {
            "attachments": [
                {
                    "fallback": f"[{self.stage}] {title}",
                    "color": color,
                    "fields": [
                        {
                            "title": f"{emoji} {self.stage} - {title}",
                            "value": message,
                            "short": False
                        }
                    ],
                    "footer": "Data Pipeline Monitor",
                    "ts": None # 자동으로 현재 시간 표시
                }
            ]
        }

        try:
            r = requests.post(self.webhook_url, json=payload, timeout=10)
            r.raise_for_status()
        except Exception as e:
            self.logger.error(f"Slack 알림 전송 실패: {e}")

    def info(self, title: str, message: str):
        self._send_slack("#36a64f", title, message, "ℹ️")

    def success(self, title: str, message: str):
        self._send_slack("#2eb886", title, message, "✅")

    def error(self, title: str, exception: Optional[Exception] = None):
        """
        예외 발생 시 호출. exception 객체를 넘기면 Traceback의 마지막 줄을 포함합니다.
        """
        err_msg = str(exception) if exception else "알 수 없는 오류 발생"
        if exception:
            # 전체 Traceback 중 마지막 3줄만 추출하여 가독성 확보
            tb_lines = traceback.format_exc().splitlines()
            trace_summary = "\n".join(tb_lines[-3:])
            err_msg = f"*Error*: {err_msg}\n```python\n{trace_summary}\n```"

        self._send_slack("#ff0000", "Critical Error", err_msg, "🚨")