from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

import time

from prometheus_client import Counter, Gauge, Histogram, start_http_server

if TYPE_CHECKING:
    from .config import AppConfig

logger = logging.getLogger(__name__)

KLINES_RECEIVED = Counter(
    "watcher_klines_received_total",
    "Closed klines received from Binance",
    ("interval", "source"),
)
KLINES_PUBLISHED_1H = Counter(
    "watcher_klines_published_1h_total",
    "1h closed klines written to Redis stream after idempotency check",
)
KLINES_SKIPPED_DUPLICATE_1H = Counter(
    "watcher_klines_skipped_duplicate_1h_total",
    "1h closed klines skipped because already published (idempotency)",
)
WS_CONNECTIONS = Counter(
    "watcher_ws_connections_total",
    "WebSocket connections opened",
    ("interval",),
)
WS_KLINE_FAST_SKIPPED = Counter(
    "watcher_ws_kline_fast_skipped_total",
    "WebSocket kline frames skipped before JSON decoding because they are not closed klines",
    ("interval",),
)
BACKFILL_KLINES = Counter(
    "watcher_backfill_klines_total",
    "Klines loaded via REST backfill",
    ("interval",),
)
BACKFILL_ERRORS = Counter(
    "watcher_backfill_errors_total",
    "REST backfill failures",
    ("interval",),
)
EXCHANGE_TO_SERVICE_MS = Histogram(
    "watcher_exchange_to_service_ms",
    "Milliseconds from Binance kline event time to service ingest",
    ("interval", "source"),
    buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
)
LAST_INGEST_UNIX = Gauge(
    "watcher_last_ingest_unix_seconds",
    "Unix time (seconds) when a closed kline was last ingested",
    ("interval",),
)


def touch_ingest(interval: str) -> None:
    LAST_INGEST_UNIX.labels(interval).set(time.time())


def start_metrics_server(cfg: AppConfig) -> threading.Thread | None:
    if not cfg.metrics.enabled:
        return None

    def _run() -> None:
        start_http_server(cfg.metrics.port, addr=cfg.metrics.host)
        logger.info("metrics.listen host=%s port=%s", cfg.metrics.host, cfg.metrics.port)

    thread = threading.Thread(target=_run, name="prometheus-metrics", daemon=True)
    thread.start()
    return thread
