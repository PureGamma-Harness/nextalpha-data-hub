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

        payload = {"title": title, "text": body}
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(self._cfg.url, json=payload)
            resp.raise_for_status()
