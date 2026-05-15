from __future__ import annotations

import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import WebhookConfig

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(self, cfg: WebhookConfig) -> None:
        self._cfg = cfg

    @retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3), reraise=True)
    async def send(self, title: str, body: str) -> None:
        if not self._cfg.enabled or not self._cfg.url:
            logger.info("notify.skip title=%s body=%s", title, body)
            return

        # Lark custom bot expects msg_type/content schema.
        payload = {
            "msg_type": "text",
            "content": {
                "text": f"{title}\n{body}",
            },
        }
        async with httpx.AsyncClient(timeout=5.0, proxy=self._cfg.proxy_url) as client:
            resp = await client.post(self._cfg.url, json=payload)
            resp.raise_for_status()
            try:
                data = resp.json()
            except ValueError:
                logger.info("notify.sent title=%s status=%s body=<non-json>", title, resp.status_code)
                return
            if isinstance(data, dict):
                code = data.get("code")
                if code not in (None, 0):
                    msg = data.get("msg") or data.get("message") or data
                    raise RuntimeError(f"lark webhook rejected message code={code} msg={msg}")
            logger.info("notify.sent title=%s status=%s code=%s", title, resp.status_code, data.get("code") if isinstance(data, dict) else None)
