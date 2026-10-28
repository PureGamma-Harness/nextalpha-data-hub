from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import orjson
import redis.asyncio as redis
from fastapi import WebSocket
from websockets.server import WebSocketServerProtocol

from .alerts import AlertEngine, AlertSignal
from .binance_client import BinanceClient
from .config import AppConfig, WebhookConfig
from .latency import LatencyEvent
from .metrics import start_metrics_server
from .models import ClosedKline
from .notifier import Notifier
from .ws_push import WsBroadcaster

logger = logging.getLogger(__name__)


class App:
    def __init__(
        self,
        cfg: AppConfig,
        alerts_ws: object | None = None,
        klines_ws: object | None = None,
    ) -> None:
        self._cfg = cfg
        self._notifier = Notifier(cfg.webhook)
        alerts_webhook_url = cfg.webhook.alerts_url.strip() or cfg.webhook.url
        self._alerts_notifier = Notifier(
            WebhookConfig(
                enabled=cfg.webhook.enabled,
                url=alerts_webhook_url,
                proxy_url=cfg.webhook.proxy_url,
                alerts_url=cfg.webhook.alerts_url,
            ),
        )
        self._alerts: AlertEngine | None = None
        if cfg.service_mode in ("alerts", "all"):
            self._alerts = AlertEngine(cfg, self._alerts_notifier, self._get_alert_clients_count)
        self._binance = BinanceClient(cfg)
        self._alerts_ws: object | None = alerts_ws
        self._klines_ws: object | None = klines_ws
        replay_maxlen = max(0, cfg.websocket.klines_replay_count)
        self._klines_recent: deque[list[dict]] = deque(maxlen=replay_maxlen)
        self._klines_recent_lock = asyncio.Lock()
        if self._alerts_ws is None and cfg.service_mode in ("alerts", "all"):
            self._alerts_ws = WsBroadcaster(cfg.websocket.host, cfg.websocket.alerts_port, name="alerts")
        if self._klines_ws is None and cfg.service_mode in ("klines", "all"):
            self._klines_ws = WsBroadcaster(
                cfg.websocket.host,
                cfg.websocket.klines_port,
                name="klines",
                on_client_connected=self._on_klines_client_connected,
            )
        self._ingest_1m = 0
        self._ingest_1h = 0
        self._push_alerts = 0
        self._push_1h = 0
        self._expected_symbols: set[str] = set()
        self._pending_by_close: dict[int, dict[str, dict]] = {}
        self._pending_has_ws: dict[int, bool] = {}
        self._pending_first_ingest_ms: dict[int, int] = {}
        self._published_close_times: set[int] = set()
        self._published_close_order: deque[int] = deque()
        self._published_close_max = 32
        self._flush_grace_ms = max(1_000, self._cfg.websocket.klines_flush_grace_ms)
        self._full_batch_only = self._cfg.websocket.klines_full_batch_only
        self._min_publish_ratio = min(1.0, max(0.0, self._cfg.websocket.klines_min_publish_ratio))
        self._full_batch_max_wait_ms = max(1_000, self._cfg.websocket.klines_full_batch_max_wait_ms)
        self._max_missing_symbols = max(0, self._cfg.websocket.klines_max_missing_symbols)
        self._pending_lock = asyncio.Lock()
        self._klines_interval = self._cfg.websocket.klines_interval.strip() or "1h"
        self._klines_source = self._cfg.websocket.klines_source
        self._klines_ws_batch_size = max(1, self._cfg.websocket.klines_ws_batch_size)
        self._klines_passthrough_mode = bool(self._cfg.websocket.klines_passthrough_mode)
        self._rest_repair_delay_ms = self._resolve_rest_repair_delay_ms(self._klines_interval)
        self._rest_repairing_close_times: set[int] = set()
        self._log_full_missing_symbols = self._cfg.websocket.klines_log_full_missing_symbols
        self._pending_latency: dict[str, LatencyEvent] = {}
        self._pending_latency_lock = asyncio.Lock()
        self._latency_redis = redis.from_url(cfg.redis.url, decode_responses=True)
        self._event_close_time: dict[str, int] = {}
        self._ack_service_to_downstream_by_close: dict[int, list[int]] = {}
        self._ack_ingest_to_downstream_by_close: dict[int, list[int]] = {}
        self._batch_done_to_ack_by_close: dict[int, list[int]] = {}
        self._batch_done_to_integrated_ack_by_close: dict[int, list[int]] = {}
        self._batch_done_to_compute_ack_by_close: dict[int, list[int]] = {}
        self._batch_done_to_order_ack_by_close: dict[int, list[int]] = {}
        self._ack_expected_by_close: dict[int, int] = {}
        self._ack_clients_by_close: dict[int, set[int]] = {}
        self._batch_ack_clients_by_close: dict[int, set[int]] = {}
        self._batch_integrated_ack_clients_by_close: dict[int, set[int]] = {}
        self._batch_compute_ack_clients_by_close: dict[int, set[int]] = {}
        self._batch_order_ack_clients_by_close: dict[int, set[int]] = {}
        self._ack_notify_scheduled: set[int] = set()
        self._batch_summary_by_close: dict[int, dict[str, str | int]] = {}
        self._batch_metric_values_by_close: dict[int, dict[str, int | list[int]]] = {}
        self._rest_compare_summary_by_close: dict[int, dict[str, object]] = {}
        self._rest_compare_snapshots_by_close: dict[int, dict[str, dict[str, object]]] = {}
        self._rest_compare_events_by_close: dict[int, asyncio.Event] = {}
        self._passthrough_shards_sent_by_close: dict[int, set[str]] = {}
        self._passthrough_total_shards_by_close: dict[int, int] = {}
        self._batch_done_publish_ms_by_close: dict[int, int] = {}
        self._publish_queue: asyncio.Queue[tuple[int, dict[str, dict], int | None]] = asyncio.Queue(maxsize=4096)
        self._latency_write_queue: asyncio.Queue[dict[str, str]] = asyncio.Queue(maxsize=200_000)
        self._passthrough_shard_lock = asyncio.Lock()
        self._passthrough_shard_buffers: dict[tuple[int, str], dict[str, dict]] = {}
        # [expected_symbol_count, first_wall_ms, min_ingest_ms]
        self._passthrough_shard_meta: dict[tuple[int, str], list[int]] = {}
        self._passthrough_shard_max_wait_ms = max(50, self._cfg.websocket.klines_passthrough_shard_max_wait_ms)
        self._pending_symbol_refresh: list[str] | None = None
        self._symbol_refresh_apply_event = asyncio.Event()

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop_event = stop if stop is not None else asyncio.Event()
        start_metrics_server(self._cfg)
        logger.info("service.start mode=%s", self._cfg.service_mode)
        syms = await self._fetch_symbols_with_retry()
        logger.info("symbols.loaded count=%s", len(syms))

        ingest_task: asyncio.Task[None] | None = None
        refresh_task: asyncio.Task[None] | None = None
        heartbeat_task: asyncio.Task[None] | None = None
        stale_flush_task: asyncio.Task[None] | None = None
        passthrough_flush_task: asyncio.Task[None] | None = None
        publish_worker_task: asyncio.Task[None] | None = None
        latency_writer_task: asyncio.Task[None] | None = None
        symbol_refresh_apply_task: asyncio.Task[bool] | None = None
        ws_tasks: list[asyncio.Task[None]] = []
        if self._alerts_ws is not None:
            ws_tasks.append(asyncio.create_task(self._alerts_ws.serve_until_stop(stop_event)))
        if self._klines_ws is not None:
            ws_tasks.append(asyncio.create_task(self._klines_ws.serve_until_stop(stop_event)))
        try:
            ingest_task = asyncio.create_task(self._run_ingestion(syms))
            refresh_task = asyncio.create_task(
                asyncio.sleep(self._cfg.binance.symbols_refresh_seconds),
            )
            heartbeat_task = asyncio.create_task(self._run_heartbeat(stop_event))
            publish_worker_task = asyncio.create_task(self._run_publish_worker(stop_event))
            latency_writer_task = asyncio.create_task(self._run_latency_writer(stop_event))
            symbol_refresh_apply_task = asyncio.create_task(self._symbol_refresh_apply_event.wait())
            if self._cfg.service_mode in ("klines", "all") and not self._klines_passthrough_mode:
                stale_flush_task = asyncio.create_task(self._run_klines_stale_flush(stop_event))
            if self._cfg.service_mode in ("klines", "all") and self._klines_passthrough_mode:
                passthrough_flush_task = asyncio.create_task(self._run_passthrough_shard_flush(stop_event))
            stop_wait = asyncio.create_task(stop_event.wait())

            while True:
                wait_set = {ingest_task, refresh_task, stop_wait}
                if symbol_refresh_apply_task is not None:
                    wait_set.add(symbol_refresh_apply_task)
                done, _pending = await asyncio.wait(
                    wait_set,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if stop_wait in done:
                    if ingest_task and not ingest_task.done():
                        ingest_task.cancel()
                    if refresh_task and not refresh_task.done():
                        refresh_task.cancel()
                    if symbol_refresh_apply_task and not symbol_refresh_apply_task.done():
                        symbol_refresh_apply_task.cancel()
                    if ingest_task:
                        await asyncio.gather(ingest_task, return_exceptions=True)
                    if refresh_task:
                        await asyncio.gather(refresh_task, return_exceptions=True)
                    if symbol_refresh_apply_task:
                        await asyncio.gather(symbol_refresh_apply_task, return_exceptions=True)
                    if heartbeat_task:
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
                    if stale_flush_task:
                        await asyncio.gather(stale_flush_task, return_exceptions=True)
                    if passthrough_flush_task:
                        await asyncio.gather(passthrough_flush_task, return_exceptions=True)
                    if publish_worker_task:
                        await asyncio.gather(publish_worker_task, return_exceptions=True)
                    if latency_writer_task:
                        await asyncio.gather(latency_writer_task, return_exceptions=True)
                    break

                if ingest_task in done:
                    if refresh_task and not refresh_task.done():
                        refresh_task.cancel()
                        await asyncio.gather(refresh_task, return_exceptions=True)
                    exc = ingest_task.exception()
                    if exc is not None and not isinstance(exc, asyncio.CancelledError):
                        raise exc
                    break

                if symbol_refresh_apply_task in done:
                    self._symbol_refresh_apply_event.clear()
                    pending_symbols = self._pending_symbol_refresh
                    self._pending_symbol_refresh = None
                    symbol_refresh_apply_task = asyncio.create_task(self._symbol_refresh_apply_event.wait())
                    if pending_symbols is not None and set(pending_symbols) != set(syms):
                        logger.info(
                            "symbols.refresh.apply current=%s new=%s",
                            len(syms),
                            len(pending_symbols),
                        )
                        syms = pending_symbols
                        if self._alerts is not None:
                            self._alerts.prune_to_symbols(set(syms))
                        if ingest_task and not ingest_task.done():
                            ingest_task.cancel()
                            await asyncio.gather(ingest_task, return_exceptions=True)
                        ingest_task = asyncio.create_task(self._run_ingestion(syms))
                    continue

                if refresh_task in done:
                    try:
                        new_syms = await self._binance.fetch_symbols()
                    except Exception:
                        # Keep running on the current universe; retry next cycle.
                        logger.exception("symbols.refresh.failed keep_current=%s", len(syms))
                        new_syms = list(syms)
                    if not new_syms:
                        logger.warning("symbols.refresh.empty keep_current=%s", len(syms))
                        new_syms = list(syms)
                    if set(new_syms) != set(syms):
                        if self._should_defer_symbol_refresh_to_kline_batch():
                            self._queue_symbol_refresh(syms, new_syms)
                        else:
                            logger.info("symbols.refresh.apply current=%s new=%s", len(syms), len(new_syms))
                            if self._alerts is not None:
                                self._alerts.prune_to_symbols(set(new_syms))
                            syms = new_syms
                            if ingest_task and not ingest_task.done():
                                ingest_task.cancel()
                                await asyncio.gather(ingest_task, return_exceptions=True)
                            ingest_task = asyncio.create_task(self._run_ingestion(syms))
                    else:
                        self._pending_symbol_refresh = None
                        self._symbol_refresh_apply_event.clear()
                    refresh_task = asyncio.create_task(
                        asyncio.sleep(self._cfg.binance.symbols_refresh_seconds),
                    )

        finally:
            stop_event.set()
            to_cancel = [
                ingest_task,
                refresh_task,
                heartbeat_task,
                stale_flush_task,
                passthrough_flush_task,
                publish_worker_task,
                latency_writer_task,
                symbol_refresh_apply_task,
                *ws_tasks,
            ]
            for t in to_cancel:
                if t is not None and not t.done():
                    t.cancel()
            await asyncio.gather(
                *[x for x in to_cancel if x is not None],
                return_exceptions=True,
            )
            if self._alerts is not None:
                # Give in-flight alert webhooks a brief chance to finish.
                await self._alerts.drain_notifications(timeout=5.0)
            await self._latency_redis.aclose()

    async def _fetch_symbols_with_retry(self, attempts: int = 5, base_delay_s: float = 2.0) -> list[str]:
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                syms = await self._binance.fetch_symbols()
                if syms:
                    return syms
                last_exc = RuntimeError("No symbols found for Binance U perpetual.")
            except Exception as exc:
                last_exc = exc
            if attempt < attempts:
                delay = min(30.0, base_delay_s * (2 ** (attempt - 1)))
                logger.warning(
                    "symbols.load.retry attempt=%s/%s delay_s=%.1f error=%s",
                    attempt,
                    attempts,
                    delay,
                    last_exc,
                )
                await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc

    def _should_defer_symbol_refresh_to_kline_batch(self) -> bool:
        return self._cfg.service_mode in ("klines", "all") and self._klines_ws is not None

    def _queue_symbol_refresh(self, current_symbols: list[str], new_symbols: list[str]) -> None:
        current_set = set(current_symbols)
        new_set = set(new_symbols)
        added = sorted(new_set - current_set)
        removed = sorted(current_set - new_set)
        self._pending_symbol_refresh = list(new_symbols)
        logger.info(
            "symbols.refresh.detected current=%s new=%s added=%s removed=%s added_sample=%s removed_sample=%s apply=after_kline_batch",
            len(current_symbols),
            len(new_symbols),
            len(added),
            len(removed),
            ",".join(added[:20]) if added else "-",
            ",".join(removed[:20]) if removed else "-",
        )

    async def _maybe_signal_symbol_refresh_after_batch(self, close_time_ms: int) -> None:
        if self._pending_symbol_refresh is None or self._symbol_refresh_apply_event.is_set():
            return
        rest_event = self._rest_compare_events_by_close.get(close_time_ms)
        if rest_event is not None and not rest_event.is_set():
            wait_seconds = self._symbol_refresh_rest_compare_wait_seconds()
            logger.info(
                "symbols.refresh.wait_rest_compare close_time_ms=%s timeout_seconds=%.1f",
                close_time_ms,
                wait_seconds,
            )
            try:
                await asyncio.wait_for(rest_event.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                logger.warning(
                    "symbols.refresh.rest_compare_wait_timeout close_time_ms=%s timeout_seconds=%.1f",
                    close_time_ms,
                    wait_seconds,
                )
        logger.info(
            "symbols.refresh.ready close_time_ms=%s pending=%s",
            close_time_ms,
            len(self._pending_symbol_refresh),
        )
        self._symbol_refresh_apply_event.set()

    def _symbol_refresh_rest_compare_wait_seconds(self) -> float:
        compare_delay = max(0.0, self._cfg.websocket.aggtrade_rest_compare_delay_seconds)
        rest_timeout = max(1.0, float(self._cfg.backfill.rest_timeout_seconds))
        return max(60.0, compare_delay + rest_timeout * 4.0)

    async def _run_ingestion(self, symbols: list[str]) -> None:
        tasks: list[asyncio.Task[None]] = []
        if self._cfg.service_mode in ("klines", "all"):
            tasks.append(asyncio.create_task(self._run_1h_publish(symbols)))
        if self._cfg.service_mode in ("alerts", "all"):
            if self._alerts is None:
                raise RuntimeError("alerts ingestion requires service_mode alerts or all")
            tasks.append(asyncio.create_task(self._run_1m_alerts(symbols)))
        if not tasks:
            raise RuntimeError(f"No ingestion tasks for service_mode={self._cfg.service_mode}")

        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for t in done:
                exc = t.exception()
                if exc is not None and not isinstance(exc, asyncio.CancelledError):
                    raise exc
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _passthrough_row_dict(self, kline: ClosedKline) -> dict:
        return {
            "event_id": f"{kline.symbol}:{kline.interval}:{kline.open_time_ms}",
            "symbol": kline.symbol,
            "interval": kline.interval,
            "open_time_ms": kline.open_time_ms,
            "close_time_ms": kline.close_time_ms,
            "open": str(kline.open_price),
            "high": str(kline.high_price),
            "low": str(kline.low_price),
            "close": str(kline.close_price),
            "volume": str(kline.volume),
            "quote_volume": str(kline.quote_volume),
            "exchange_event_ms": kline.event_time_ms,
            "service_ingest_ms": kline.ingest_time_ms,
            "service_region": self._cfg.service_region,
            "source": kline.source,
            "ws_shard_id": kline.ws_shard_id,
            "ws_shard_symbol_count": kline.ws_shard_symbol_count,
            "ws_total_shards": kline.ws_total_shards,
        }

    async def _passthrough_buffer_add(self, kline: ClosedKline) -> None:
        row = self._passthrough_row_dict(kline)
        if kline.ws_shard_symbol_count <= 0 or not kline.ws_shard_id:
            await self._enqueue_publish(kline.close_time_ms, {kline.symbol: row}, kline.ingest_time_ms)
            return
        key = (kline.close_time_ms, kline.ws_shard_id)
        maybe_flush: tuple[int, dict[str, dict], int | None] | None = None
        async with self._passthrough_shard_lock:
            buf = self._passthrough_shard_buffers.setdefault(key, {})
            buf[kline.symbol] = row
            meta = self._passthrough_shard_meta.setdefault(
                key,
                [kline.ws_shard_symbol_count, int(time.time() * 1000), kline.ingest_time_ms],
            )
            meta[2] = min(meta[2], kline.ingest_time_ms)
            if len(buf) >= meta[0]:
                maybe_flush = (key[0], dict(buf), meta[2])
                del self._passthrough_shard_buffers[key]
                del self._passthrough_shard_meta[key]
        if maybe_flush is not None:
            close_ms, rows, first_ingest = maybe_flush
            await self._enqueue_publish(close_ms, rows, first_ingest)

    async def _run_passthrough_shard_flush(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(0.05)
            now_ms = int(time.time() * 1000)
            flush_items: list[tuple[int, dict[str, dict], int | None]] = []
            async with self._passthrough_shard_lock:
                for key in list(self._passthrough_shard_buffers.keys()):
                    buf = self._passthrough_shard_buffers.get(key)
                    if not buf:
                        continue
                    meta = self._passthrough_shard_meta.get(key)
                    if not meta:
                        continue
                    _exp, first_wall, min_ingest = meta
                    if now_ms - first_wall >= self._passthrough_shard_max_wait_ms:
                        flush_items.append((key[0], dict(buf), min_ingest))
                        del self._passthrough_shard_buffers[key]
                        del self._passthrough_shard_meta[key]
            for close_ms, rows, first_ingest in flush_items:
                await self._enqueue_publish(close_ms, rows, first_ingest)

    def _publish_kline_iter(self, symbols: list[str]):
        if self._klines_source == "kline":
            return self._binance.iter_closed_klines(
                symbols,
                interval=self._klines_interval,
                batch_size=self._klines_ws_batch_size,
            )
        return self._binance.iter_aggtrade_klines(
            symbols,
            interval=self._klines_interval,
            batch_size=self._klines_ws_batch_size,
            emit_grace_ms=max(0, self._cfg.websocket.aggtrade_emit_grace_ms),
            compare_with_rest_kline=self._cfg.websocket.aggtrade_compare_with_kline,
            rest_compare_delay_seconds=max(
                0.0,
                self._cfg.websocket.aggtrade_rest_compare_delay_seconds,
            ),
            rest_compare_concurrency=max(
                1,
                self._cfg.websocket.aggtrade_rest_compare_concurrency,
            ),
            rest_diagnostic_enabled=self._cfg.websocket.aggtrade_rest_diagnostic_enabled,
            rest_diagnostic_sample_size=max(
                1,
                self._cfg.websocket.aggtrade_rest_diagnostic_sample_size,
            ),
            rest_diagnostic_offsets_ms=tuple(
                max(0, int(offset_ms))
                for offset_ms in self._cfg.websocket.aggtrade_rest_diagnostic_offsets_ms
            ),
            price_tolerance=self._decimal_config(
                self._cfg.websocket.aggtrade_price_tolerance,
                Decimal("0"),
            ),
            volume_tolerance=self._decimal_config(
                self._cfg.websocket.aggtrade_volume_tolerance,
                Decimal("0.00000001"),
            ),
            quote_tolerance=self._decimal_config(
                self._cfg.websocket.aggtrade_quote_tolerance,
                Decimal("0.000001"),
            ),
            rest_compare_summary_callback=self._on_aggtrade_rest_compare_summary,
        )

    async def _on_aggtrade_rest_compare_summary(self, summary: dict[str, object]) -> None:
        try:
            close_time_ms = int(summary.get("close_time_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        snapshot_label = str(summary.get("snapshot_label") or "")
        if snapshot_label:
            snapshots = self._rest_compare_snapshots_by_close.setdefault(close_time_ms, {})
            snapshots[snapshot_label] = summary
            if not bool(summary.get("diagnostic_final")):
                return
            summary = dict(summary)
            summary["snapshot_summaries"] = tuple(
                snapshots[label]
                for label in sorted(
                    snapshots,
                    key=lambda item: int(snapshots[item].get("snapshot_offset_ms") or 0),
                )
            )
        self._rest_compare_summary_by_close[close_time_ms] = summary
        event = self._rest_compare_events_by_close.setdefault(close_time_ms, asyncio.Event())
        event.set()
        asyncio.create_task(self._notify_rest_compare_summary(summary))

    def _decimal_config(self, value: str, default: Decimal) -> Decimal:
        try:
            return Decimal(str(value))
        except Exception:
            logger.warning("config.decimal_invalid value=%s default=%s", value, default)
            return default

    def _is_live_ws_source(self, source: str) -> bool:
        return source in ("ws", "aggtrade")

    async def _run_1h_publish(self, symbols: list[str]) -> None:
        if self._klines_ws is None:
            async for _ in self._publish_kline_iter(symbols):
                self._ingest_1h += 1
            return

        self._expected_symbols = set(symbols)
        expected_symbols = len(self._expected_symbols)
        # In full-batch mode, always prefer full batch for immediate publish.
        # Tolerant (missing <= N) publish is handled by max-wait path.
        if self._full_batch_only:
            min_required = max(1, expected_symbols)
        else:
            min_required = max(1, math.ceil(expected_symbols * self._min_publish_ratio))
        self._pending_by_close.clear()
        self._pending_has_ws.clear()
        self._pending_first_ingest_ms.clear()
        self._published_close_times.clear()
        self._published_close_order.clear()
        latest_close_by_symbol: dict[str, int] = {}

        logger.info(
            "klines.publish.source interval=%s source=%s compare_with_rest_kline=%s",
            self._klines_interval,
            self._klines_source,
            self._cfg.websocket.aggtrade_compare_with_kline if self._klines_source == "aggtrade" else False,
        )

        async for kline in self._publish_kline_iter(symbols):
            prev_close = latest_close_by_symbol.get(kline.symbol)
            if prev_close is not None and kline.close_time_ms <= prev_close:
                continue
            latest_close_by_symbol[kline.symbol] = kline.close_time_ms
            self._ingest_1h += 1
            if self._klines_passthrough_mode:
                await self._passthrough_buffer_add(kline)
                continue
            complete_rows: dict[str, dict] | None = None
            async with self._pending_lock:
                if kline.close_time_ms in self._published_close_times:
                    continue
                rows_by_symbol = self._pending_by_close.setdefault(kline.close_time_ms, {})
                self._pending_first_ingest_ms.setdefault(kline.close_time_ms, kline.ingest_time_ms)
                rows_by_symbol[kline.symbol] = {
                    "event_id": f"{kline.symbol}:{kline.interval}:{kline.open_time_ms}",
                    "symbol": kline.symbol,
                    "interval": kline.interval,
                    "open_time_ms": kline.open_time_ms,
                    "close_time_ms": kline.close_time_ms,
                    "open": str(kline.open_price),
                    "high": str(kline.high_price),
                    "low": str(kline.low_price),
                    "close": str(kline.close_price),
                    "volume": str(kline.volume),
                    "quote_volume": str(kline.quote_volume),
                    "exchange_event_ms": kline.event_time_ms,
                    "service_ingest_ms": kline.ingest_time_ms,
                    "service_region": self._cfg.service_region,
                    "source": kline.source,
                    "ws_shard_id": kline.ws_shard_id,
                    "ws_shard_symbol_count": kline.ws_shard_symbol_count,
                    "ws_total_shards": kline.ws_total_shards,
                }
                if self._is_live_ws_source(kline.source):
                    self._pending_has_ws[kline.close_time_ms] = True
                if len(rows_by_symbol) >= min_required:
                    complete_rows = self._pending_by_close.pop(kline.close_time_ms, {})
                    self._pending_has_ws.pop(kline.close_time_ms, None)
                    first_ingest_ms = self._pending_first_ingest_ms.pop(kline.close_time_ms, None)
                    self._mark_close_time_published_locked(kline.close_time_ms)

            if complete_rows:
                await self._enqueue_publish(close_time_ms=kline.close_time_ms, rows_by_symbol=complete_rows, first_ingest_ms=first_ingest_ms)

        async with self._pending_lock:
            pending_items = sorted(self._pending_by_close.items())
            self._pending_by_close = {}
            self._pending_has_ws = {}
            pending_first = self._pending_first_ingest_ms
            self._pending_first_ingest_ms = {}
            for close_time_ms, _ in pending_items:
                self._mark_close_time_published_locked(close_time_ms)

        for close_time_ms, rows_by_symbol in pending_items:
            missing = len(self._expected_symbols) - len(rows_by_symbol)
            if missing > 0:
                logger.warning(
                    "klines.batch.incomplete close_time_ms=%s got=%s expected=%s missing=%s",
                    close_time_ms,
                    len(rows_by_symbol),
                    len(self._expected_symbols),
                    missing,
                )
            await self._enqueue_publish(
                close_time_ms=close_time_ms,
                rows_by_symbol=rows_by_symbol,
                first_ingest_ms=pending_first.get(close_time_ms),
            )

    async def _flush_close_time(
        self,
        close_time_ms: int,
        rows_by_symbol: dict[str, dict],
        first_ingest_ms: int | None = None,
    ) -> None:
        if not rows_by_symbol or self._klines_ws is None:
            return
        publish_ms = int(time.time() * 1000)
        payload = [dict(row, service_publish_ms=publish_ms) for row in rows_by_symbol.values()]
        payload.sort(key=lambda row: row["symbol"])
        await self._record_published_latency_state(payload)
        if self._klines_source == "aggtrade" and self._cfg.websocket.aggtrade_compare_with_kline:
            compare_symbols = {
                str(row.get("symbol") or "")
                for row in payload
                if self._is_live_ws_source(str(row.get("source") or ""))
            }
            compare_symbols.discard("")
            if compare_symbols:
                try:
                    compare_queued = await self._binance.request_aggtrade_rest_compare(
                        self._klines_interval,
                        close_time_ms,
                        compare_symbols,
                    )
                    if compare_queued:
                        self._rest_compare_events_by_close.setdefault(close_time_ms, asyncio.Event())
                except Exception:
                    logger.exception("aggtrade.rest_compare.request_failed close_time_ms=%s", close_time_ms)
        if self._klines_source == "aggtrade":
            self._binance.mark_aggtrade_close_published(self._klines_interval, close_time_ms)
        if self._klines_recent.maxlen and self._klines_recent.maxlen > 0:
            async with self._klines_recent_lock:
                self._klines_recent.append(payload)
        batch_done = False
        batch_sent_shards = 0
        batch_total_shards = 0
        if self._klines_passthrough_mode and payload:
            shard_id = str(payload[0].get("ws_shard_id") or "")
            if shard_id:
                total_from_payload = int(payload[0].get("ws_total_shards") or 0)
                sent = self._passthrough_shards_sent_by_close.setdefault(close_time_ms, set())
                sent.add(shard_id)
                total = self._passthrough_total_shards_by_close.get(close_time_ms, 0)
                total = max(total, total_from_payload)
                self._passthrough_total_shards_by_close[close_time_ms] = total
                batch_sent_shards = len(sent)
                batch_total_shards = total
                batch_done = total > 0 and batch_sent_shards >= total
            for row in payload:
                row["batch_sent_shards"] = batch_sent_shards
                row["batch_total_shards"] = batch_total_shards
                row["batch_done"] = batch_done
        elif payload:
            # 聚合模式：一次 flush 即整批对外推送，便于下游 batch_ack 对齐 batch_push_done_ms
            batch_done = True
            batch_sent_shards = 1
            batch_total_shards = 1
            for row in payload:
                row["batch_sent_shards"] = batch_sent_shards
                row["batch_total_shards"] = batch_total_shards
                row["batch_done"] = True
        batch_push_done_ms = int(time.time() * 1000)
        for row in payload:
            row["batch_push_done_ms"] = batch_push_done_ms
        pushed_clients = await self._klines_ws.broadcast_json(payload)
        self._enqueue_latency_events(self._build_published_latency_events(payload))
        push_done_ms = int(time.time() * 1000)
        if batch_done:
            self._batch_done_publish_ms_by_close[close_time_ms] = batch_push_done_ms
        service_to_client_no_ack_ms = max(0, push_done_ms - publish_ms)
        if pushed_clients > 0:
            self._push_1h += pushed_clients * len(payload)
        got_symbols = len(rows_by_symbol)
        if self._klines_passthrough_mode:
            expected_symbols = self._ack_expected_by_close.get(close_time_ms, got_symbols)
            missing_symbols = 0
            got_ratio = 1.0
            missing_list: list[str] = []
        else:
            expected_symbols = len(self._expected_symbols)
            missing_symbols = expected_symbols - got_symbols
            got_ratio = (got_symbols / expected_symbols) if expected_symbols > 0 else 0.0
            missing_list = sorted(self._expected_symbols - set(rows_by_symbol.keys()))
        missing_sample = ",".join(missing_list[:20]) if missing_list else "-"
        ws_count = sum(1 for row in payload if self._is_live_ws_source(str(row.get("source") or "")))
        rest_count = len(payload) - ws_count
        full_batch_wait_ms = None
        if first_ingest_ms is not None:
            full_batch_wait_ms = max(0, publish_ms - first_ingest_ms)
        min_required_for_log = 1 if self._klines_passthrough_mode else max(
            1,
            math.ceil(expected_symbols * self._min_publish_ratio),
        )
        logger.info(
            "klines.batch.published close_time_ms=%s batch_size=%s got_symbols=%s expected_symbols=%s got_ratio=%.3f ws_count=%s rest_count=%s min_required=%s missing_symbols=%s missing_sample=%s full_batch_wait_ms=%s full_batch_only=%s min_publish_ratio=%.3f pushed_clients=%s pushed_rows_total=%s",
            close_time_ms,
            len(payload),
            got_symbols,
            expected_symbols,
            got_ratio,
            ws_count,
            rest_count,
            min_required_for_log,
            max(0, missing_symbols),
            missing_sample,
            full_batch_wait_ms,
            self._full_batch_only,
            self._min_publish_ratio,
            pushed_clients,
            self._push_1h,
        )
        if self._log_full_missing_symbols and missing_list:
            logger.info(
                "klines.batch.missing_full close_time_ms=%s missing_symbols=%s missing_list=%s",
                close_time_ms,
                len(missing_list),
                ",".join(missing_list),
            )
        await self._notify_kline_batch(
            payload,
            expected_symbols,
            missing_symbols,
            ws_count,
            rest_count,
            service_to_client_no_ack_ms,
            pushed_clients,
            publish_ms,
            push_done_ms,
        )
        if batch_done:
            await self._maybe_signal_symbol_refresh_after_batch(close_time_ms)

    async def _flush_stale_batches(self, now_ms: int) -> None:
        if self._klines_ws is None:
            return
        async with self._pending_lock:
            stale_close_times = [
                close_time_ms
                for close_time_ms in self._pending_by_close.keys()
                if self._pending_has_ws.get(close_time_ms, False)
                and close_time_ms + self._flush_grace_ms <= now_ms
            ]
            stale_batches: list[tuple[int, dict[str, dict]]] = []
            for close_time_ms in sorted(stale_close_times):
                rows_by_symbol = self._pending_by_close.pop(close_time_ms, {})
                self._pending_has_ws.pop(close_time_ms, None)
                if rows_by_symbol:
                    first_ingest_ms = self._pending_first_ingest_ms.pop(close_time_ms, None)
                    self._mark_close_time_published_locked(close_time_ms)
                    stale_batches.append((close_time_ms, rows_by_symbol, first_ingest_ms))

        for close_time_ms, rows_by_symbol, first_ingest_ms in stale_batches:
            missing = len(self._expected_symbols) - len(rows_by_symbol)
            if missing > 0:
                logger.warning(
                    "klines.batch.timeout_flush close_time_ms=%s got=%s expected=%s missing=%s grace_ms=%s",
                    close_time_ms,
                    len(rows_by_symbol),
                    len(self._expected_symbols),
                    missing,
                    self._flush_grace_ms,
                )
            await self._enqueue_publish(close_time_ms=close_time_ms, rows_by_symbol=rows_by_symbol, first_ingest_ms=first_ingest_ms)

    async def _run_klines_stale_flush(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(1)
            now_ms = int(time.time() * 1000)
            if self._full_batch_only:
                await self._flush_waited_tolerant_batches(now_ms)
                await self._try_rest_repair_for_waiting_batches(now_ms)
                await self._log_full_batch_waiting(now_ms)
            else:
                await self._flush_stale_batches(now_ms)

    async def _flush_waited_tolerant_batches(self, now_ms: int) -> None:
        if self._klines_ws is None:
            return
        if self._max_missing_symbols <= 0:
            return
        expected_symbols = len(self._expected_symbols)
        tolerant_required = max(1, expected_symbols - self._max_missing_symbols)
        flush_items: list[tuple[int, dict[str, dict], int | None]] = []
        async with self._pending_lock:
            candidate_close_times = []
            for close_time_ms, rows_by_symbol in self._pending_by_close.items():
                if not self._pending_has_ws.get(close_time_ms, False):
                    continue
                first_ingest_ms = self._pending_first_ingest_ms.get(close_time_ms)
                if first_ingest_ms is None:
                    continue
                wait_ms = max(0, now_ms - first_ingest_ms)
                got = len(rows_by_symbol)
                if wait_ms >= self._full_batch_max_wait_ms and got >= tolerant_required:
                    candidate_close_times.append(close_time_ms)
            for close_time_ms in sorted(candidate_close_times):
                rows_by_symbol = self._pending_by_close.pop(close_time_ms, {})
                self._pending_has_ws.pop(close_time_ms, None)
                first_ingest_ms = self._pending_first_ingest_ms.pop(close_time_ms, None)
                if rows_by_symbol:
                    self._mark_close_time_published_locked(close_time_ms)
                    flush_items.append((close_time_ms, rows_by_symbol, first_ingest_ms))

        for close_time_ms, rows_by_symbol, first_ingest_ms in flush_items:
            missing = max(0, expected_symbols - len(rows_by_symbol))
            logger.warning(
                "klines.batch.max_wait_tolerant_flush close_time_ms=%s got=%s expected=%s missing=%s max_missing_symbols=%s max_wait_ms=%s",
                close_time_ms,
                len(rows_by_symbol),
                expected_symbols,
                missing,
                self._max_missing_symbols,
                self._full_batch_max_wait_ms,
            )
            await self._enqueue_publish(close_time_ms=close_time_ms, rows_by_symbol=rows_by_symbol, first_ingest_ms=first_ingest_ms)

    async def _try_rest_repair_for_waiting_batches(self, now_ms: int) -> None:
        if self._klines_ws is None:
            return
        if self._rest_repair_delay_ms <= 0:
            return

        min_required = max(1, math.ceil(len(self._expected_symbols) * self._min_publish_ratio))
        repair_targets: list[tuple[int, int]] = []
        async with self._pending_lock:
            for close_time_ms, rows_by_symbol in self._pending_by_close.items():
                if close_time_ms in self._rest_repairing_close_times:
                    continue
                first_ingest_ms = self._pending_first_ingest_ms.get(close_time_ms)
                if first_ingest_ms is None:
                    continue
                if len(rows_by_symbol) >= min_required:
                    continue
                wait_ms = max(0, now_ms - first_ingest_ms)
                if wait_ms < self._rest_repair_delay_ms:
                    continue
                self._rest_repairing_close_times.add(close_time_ms)
                repair_targets.append((close_time_ms, wait_ms))

        for close_time_ms, wait_ms in repair_targets:
            try:
                await self._repair_close_time_with_rest(close_time_ms, wait_ms, min_required)
            finally:
                async with self._pending_lock:
                    self._rest_repairing_close_times.discard(close_time_ms)

    async def _repair_close_time_with_rest(self, close_time_ms: int, wait_ms: int, min_required: int) -> None:
        async with self._pending_lock:
            rows_by_symbol = self._pending_by_close.get(close_time_ms)
            if not rows_by_symbol:
                return
            missing_symbols = sorted(self._expected_symbols - set(rows_by_symbol.keys()))
            first_ingest_ms = self._pending_first_ingest_ms.get(close_time_ms)
        if not missing_symbols:
            return
        expected_symbols = len(self._expected_symbols)
        publish_required = min_required
        if self._max_missing_symbols > 0 and wait_ms >= self._full_batch_max_wait_ms:
            tolerant_required = max(1, expected_symbols - self._max_missing_symbols)
            publish_required = min(publish_required, tolerant_required)

        logger.warning(
            "klines.batch.rest_repair.start close_time_ms=%s missing_symbols=%s wait_ms=%s repair_delay_ms=%s publish_required=%s",
            close_time_ms,
            len(missing_symbols),
            wait_ms,
            self._rest_repair_delay_ms,
            publish_required,
        )

        timeout_seconds = max(5.0, self._cfg.backfill.rest_timeout_seconds)
        repaired = 0
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            proxy=self._cfg.binance.rest_proxy_url,
        ) as client:
            for symbol in missing_symbols:
                async with self._pending_lock:
                    rows_now = self._pending_by_close.get(close_time_ms)
                    if not rows_now:
                        return
                    if len(rows_now) >= publish_required:
                        break
                    if symbol in rows_now:
                        continue
                try:
                    kline = await self._binance.fetch_exact_closed_kline_rest(
                        client,
                        symbol,
                        self._klines_interval,
                        close_time_ms,
                    )
                except Exception as exc:
                    logger.warning(
                        "klines.batch.rest_repair.fail close_time_ms=%s symbol=%s error=%s",
                        close_time_ms,
                        symbol,
                        exc,
                    )
                    continue
                if kline is None:
                    continue
                self._ingest_1h += 1
                got_after_repair = 0
                async with self._pending_lock:
                    rows_now = self._pending_by_close.get(close_time_ms)
                    if rows_now is None or symbol in rows_now:
                        continue
                    rows_now[symbol] = {
                        "event_id": f"{kline.symbol}:{kline.interval}:{kline.open_time_ms}",
                        "symbol": kline.symbol,
                        "interval": kline.interval,
                        "open_time_ms": kline.open_time_ms,
                        "close_time_ms": kline.close_time_ms,
                        "open": str(kline.open_price),
                        "high": str(kline.high_price),
                        "low": str(kline.low_price),
                        "close": str(kline.close_price),
                        "volume": str(kline.volume),
                        "quote_volume": str(kline.quote_volume),
                        "exchange_event_ms": kline.event_time_ms,
                        "service_ingest_ms": kline.ingest_time_ms,
                        "service_region": self._cfg.service_region,
                        "source": "rest",
                    }
                    got_after_repair = len(rows_now)
                repaired += 1
                if got_after_repair >= publish_required:
                    break

        maybe_flush_rows: dict[str, dict] | None = None
        async with self._pending_lock:
            rows_now = self._pending_by_close.get(close_time_ms, {})
            got = len(rows_now)
            if got >= publish_required:
                maybe_flush_rows = self._pending_by_close.pop(close_time_ms, {})
                self._pending_has_ws.pop(close_time_ms, None)
                first_ingest_ms = self._pending_first_ingest_ms.pop(close_time_ms, first_ingest_ms)
                self._mark_close_time_published_locked(close_time_ms)
        logger.warning(
            "klines.batch.rest_repair.done close_time_ms=%s repaired=%s got_symbols=%s min_required=%s publish_required=%s",
            close_time_ms,
            repaired,
            len(maybe_flush_rows) if maybe_flush_rows is not None else got,
            min_required,
            publish_required,
        )
        if maybe_flush_rows:
            await self._enqueue_publish(close_time_ms=close_time_ms, rows_by_symbol=maybe_flush_rows, first_ingest_ms=first_ingest_ms)

    async def _log_full_batch_waiting(self, now_ms: int) -> None:
        if not self._full_batch_only:
            return
        async with self._pending_lock:
            waiting_rows = [
                (
                    close_time_ms,
                    len(rows_by_symbol),
                    self._pending_first_ingest_ms.get(close_time_ms),
                )
                for close_time_ms, rows_by_symbol in self._pending_by_close.items()
                if self._pending_has_ws.get(close_time_ms, False)
            ]
        for close_time_ms, got, first_ingest_ms in waiting_rows:
            if first_ingest_ms is None:
                continue
            wait_ms = max(0, now_ms - first_ingest_ms)
            if wait_ms >= self._full_batch_max_wait_ms:
                expected_symbols = len(self._expected_symbols)
                got_ratio = (got / expected_symbols) if expected_symbols > 0 else 0.0
                logger.warning(
                    "klines.batch.waiting_full close_time_ms=%s got=%s expected=%s got_ratio=%.3f min_required=%s wait_ms=%s max_wait_ms=%s repair_delay_ms=%s min_publish_ratio=%.3f",
                    close_time_ms,
                    got,
                    expected_symbols,
                    got_ratio,
                    max(1, math.ceil(expected_symbols * self._min_publish_ratio)),
                    wait_ms,
                    self._full_batch_max_wait_ms,
                    self._rest_repair_delay_ms,
                    self._min_publish_ratio,
                )

    def _resolve_rest_repair_delay_ms(self, interval: str) -> int:
        configured = self._cfg.websocket.klines_rest_repair_delay_ms
        if configured > 0:
            return configured
        if interval == "5m":
            return 120_000
        if interval == "1h":
            return 600_000
        # default fallback for other intervals: 2 minutes
        return 120_000

    def _mark_close_time_published_locked(self, close_time_ms: int) -> None:
        if close_time_ms in self._published_close_times:
            return
        self._published_close_times.add(close_time_ms)
        self._published_close_order.append(close_time_ms)
        while len(self._published_close_order) > self._published_close_max:
            expired = self._published_close_order.popleft()
            self._published_close_times.discard(expired)

    async def _on_klines_client_connected(self, ws: WebSocketServerProtocol | WebSocket) -> None:
        if self._klines_recent.maxlen == 0:
            return
        async with self._klines_recent_lock:
            replay_payloads = list(self._klines_recent)
        if not replay_payloads:
            return
        sent = 0
        for payload in replay_payloads:
            publish_ms = int(time.time() * 1000)
            replay_payload = [
                {
                    **item,
                    "replay": True,
                    "replay_reason": "cold_start",
                    "service_publish_ms": publish_ms,
                }
                for item in payload
            ]
            try:
                await ws.send(orjson.dumps(replay_payload).decode("utf-8"))
                sent += 1
            except Exception:
                break
        logger.info("ws.client.replay name=klines replayed=%s", sent)

    async def _enqueue_publish(self, close_time_ms: int, rows_by_symbol: dict[str, dict], first_ingest_ms: int | None) -> None:
        if not rows_by_symbol:
            return
        await self._publish_queue.put((close_time_ms, rows_by_symbol, first_ingest_ms))

    async def _run_publish_worker(self, stop: asyncio.Event) -> None:
        while True:
            if stop.is_set() and self._publish_queue.empty():
                break
            try:
                close_time_ms, rows_by_symbol, first_ingest_ms = await asyncio.wait_for(self._publish_queue.get(), timeout=0.5)
            except TimeoutError:
                continue
            try:
                await self._flush_close_time(close_time_ms, rows_by_symbol, first_ingest_ms)
            except Exception:
                logger.exception("klines.publish_worker.failed close_time_ms=%s", close_time_ms)
            finally:
                self._publish_queue.task_done()

    async def _record_published_latency_state(self, payload: list[dict]) -> None:
        if not payload:
            return
        cap = self._cfg.redis.pending_latency_max
        for row in payload:
            event_id = str(row.get("event_id") or "")
            if not event_id:
                continue
            try:
                close_time_ms = int(row.get("close_time_ms") or 0)
            except (TypeError, ValueError):
                close_time_ms = 0
            try:
                ev = LatencyEvent(
                    event_id=event_id,
                    symbol=str(row.get("symbol") or ""),
                    interval=str(row.get("interval") or ""),
                    exchange_event_ms=int(row.get("exchange_event_ms") or 0),
                    service_ingest_ms=int(row.get("service_ingest_ms") or 0),
                    service_publish_ms=int(row.get("service_publish_ms") or 0),
                    service_region=self._cfg.service_region,
                )
            except (TypeError, ValueError):
                continue
            async with self._pending_latency_lock:
                while len(self._pending_latency) >= cap:
                    drop = next(iter(self._pending_latency))
                    self._pending_latency.pop(drop, None)
                self._pending_latency[event_id] = ev
                if close_time_ms > 0:
                    self._event_close_time[event_id] = close_time_ms
        if payload:
            try:
                close_time_ms = int(payload[0].get("close_time_ms") or 0)
            except (TypeError, ValueError):
                close_time_ms = 0
            if close_time_ms > 0:
                self._ack_expected_by_close[close_time_ms] = self._ack_expected_by_close.get(close_time_ms, 0) + len(payload)
                self._ack_service_to_downstream_by_close.setdefault(close_time_ms, [])
                if close_time_ms not in self._ack_notify_scheduled:
                    self._ack_notify_scheduled.add(close_time_ms)
                    delay_seconds = 20
                    if self._klines_source == "aggtrade":
                        delay_seconds = max(
                            1,
                            math.ceil(self._cfg.websocket.aggtrade_rest_compare_delay_seconds),
                        )
                    asyncio.create_task(self._notify_ack_summary_later(close_time_ms, delay_seconds=delay_seconds))

    def _build_published_latency_events(self, payload: list[dict]) -> list[dict[str, str]]:
        events: list[dict[str, str]] = []
        for row in payload:
            event_id = str(row.get("event_id") or "")
            if not event_id:
                continue
            interval = str(row.get("interval") or "")
            symbol = str(row.get("symbol") or "")
            try:
                exchange_event_ms = int(row.get("exchange_event_ms") or 0)
                service_ingest_ms = int(row.get("service_ingest_ms") or 0)
            except (TypeError, ValueError):
                continue
            exchange_to_service_ms = max(0, service_ingest_ms - exchange_event_ms)
            events.append(
                {
                    "event_id": event_id,
                    "symbol": symbol,
                    "interval": interval,
                    "exchange_to_service_ms": str(exchange_to_service_ms),
                    "service_region": self._cfg.service_region,
                    "phase": "published",
                },
            )
        return events

    def _enqueue_latency_events(self, events: list[dict[str, str]]) -> None:
        if not events:
            return
        dropped = 0
        for event in events:
            try:
                self._latency_write_queue.put_nowait(event)
            except asyncio.QueueFull:
                dropped += 1
        if dropped > 0:
            logger.warning("latency.queue.full dropped=%s", dropped)

    async def _run_latency_writer(self, stop: asyncio.Event) -> None:
        stream = self._cfg.redis.stream_latency_events
        maxlen = self._cfg.redis.stream_maxlen
        while True:
            if stop.is_set() and self._latency_write_queue.empty():
                break
            try:
                first = await asyncio.wait_for(self._latency_write_queue.get(), timeout=0.5)
            except TimeoutError:
                continue
            batch = [first]
            while len(batch) < 512:
                try:
                    batch.append(self._latency_write_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                pipe = self._latency_redis.pipeline(transaction=False)
                for fields in batch:
                    pipe.xadd(stream, fields=fields, maxlen=maxlen, approximate=True)
                await pipe.execute()
            except Exception:
                logger.exception("latency.writer.persist_failed batch=%s", len(batch))
            finally:
                for _ in batch:
                    self._latency_write_queue.task_done()

    async def _on_klines_client_message(self, _ws: WebSocket, message: str) -> None:
        try:
            payload = orjson.loads(message)
        except Exception:
            return

        ack_items: list[dict] = []
        batch_ack_item: dict | None = None
        batch_integrated_ack_item: dict | None = None
        batch_compute_ack_item: dict | None = None
        batch_order_ack_item: dict | None = None
        if isinstance(payload, dict):
            if payload.get("type") == "batch_ack":
                batch_ack_item = payload
            elif payload.get("type") == "batch_integrated_ack":
                batch_integrated_ack_item = payload
            elif payload.get("type") == "batch_compute_ack":
                batch_compute_ack_item = payload
            elif payload.get("type") == "batch_order_ack":
                batch_order_ack_item = payload
            elif "event_id" in payload:
                ack_items = [payload]
            elif isinstance(payload.get("acks"), list):
                ack_items = [x for x in payload["acks"] if isinstance(x, dict)]
        elif isinstance(payload, list):
            ack_items = [x for x in payload if isinstance(x, dict)]

        sender_client_id = id(_ws)
        if batch_ack_item is not None:
            await self._apply_batch_ack(batch_ack_item, sender_client_id)
        if batch_integrated_ack_item is not None:
            await self._apply_batch_integrated_ack(batch_integrated_ack_item, sender_client_id)
        if batch_compute_ack_item is not None:
            await self._apply_batch_compute_ack(batch_compute_ack_item, sender_client_id)
        if batch_order_ack_item is not None:
            await self._apply_batch_order_ack(batch_order_ack_item, sender_client_id)
        if not ack_items:
            return
        for item in ack_items:
            await self._apply_downstream_ack(item, sender_client_id)

    async def _apply_batch_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        try:
            close_time_ms = int(item.get("close_time_ms") or 0)
            downstream_receive_ms = int(item.get("downstream_receive_ms") or 0)
            echoed_push_done_ms = int(item.get("batch_push_done_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        done_publish_ms = self._batch_done_publish_ms_by_close.get(close_time_ms)
        if done_publish_ms is None or done_publish_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        if echoed_push_done_ms > 0 and echoed_push_done_ms == done_publish_ms:
            done_to_ack_ms = max(0, server_ingress_ms - echoed_push_done_ms)
        elif downstream_receive_ms > 0:
            done_to_ack_ms = max(0, downstream_receive_ms - done_publish_ms)
        else:
            return
        self._batch_done_to_ack_by_close.setdefault(close_time_ms, []).append(done_to_ack_ms)
        if sender_client_id is not None:
            self._batch_ack_clients_by_close.setdefault(close_time_ms, set()).add(sender_client_id)

    async def _apply_batch_integrated_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        try:
            close_time_ms = int(item.get("close_time_ms") or 0)
            integrated_ack_ms = int(item.get("downstream_integrated_ms") or 0)
            echoed_push_done_ms = int(item.get("batch_push_done_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        done_publish_ms = self._batch_done_publish_ms_by_close.get(close_time_ms)
        if done_publish_ms is None or done_publish_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        if echoed_push_done_ms > 0 and echoed_push_done_ms == done_publish_ms:
            done_to_integrated_ack_ms = max(0, server_ingress_ms - echoed_push_done_ms)
        elif integrated_ack_ms > 0:
            done_to_integrated_ack_ms = max(0, integrated_ack_ms - done_publish_ms)
        else:
            return
        self._batch_done_to_integrated_ack_by_close.setdefault(close_time_ms, []).append(done_to_integrated_ack_ms)
        if sender_client_id is not None:
            self._batch_integrated_ack_clients_by_close.setdefault(close_time_ms, set()).add(sender_client_id)

    async def _apply_batch_compute_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        try:
            close_time_ms = int(item.get("close_time_ms") or 0)
            compute_ack_ms = int(item.get("downstream_compute_ms") or 0)
            echoed_push_done_ms = int(item.get("batch_push_done_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        done_publish_ms = self._batch_done_publish_ms_by_close.get(close_time_ms)
        if done_publish_ms is None or done_publish_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        if echoed_push_done_ms > 0 and echoed_push_done_ms == done_publish_ms:
            done_to_compute_ack_ms = max(0, server_ingress_ms - echoed_push_done_ms)
        elif compute_ack_ms > 0:
            done_to_compute_ack_ms = max(0, compute_ack_ms - done_publish_ms)
        else:
            return
        self._batch_done_to_compute_ack_by_close.setdefault(close_time_ms, []).append(done_to_compute_ack_ms)
        if sender_client_id is not None:
            self._batch_compute_ack_clients_by_close.setdefault(close_time_ms, set()).add(sender_client_id)

    async def _apply_batch_order_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        try:
            close_time_ms = int(item.get("close_time_ms") or 0)
            order_ack_ms = int(item.get("downstream_order_ms") or 0)
            echoed_push_done_ms = int(item.get("batch_push_done_ms") or 0)
        except (TypeError, ValueError):
            return
        if close_time_ms <= 0:
            return
        done_publish_ms = self._batch_done_publish_ms_by_close.get(close_time_ms)
        if done_publish_ms is None or done_publish_ms <= 0:
            return
        server_ingress_ms = int(time.time() * 1000)
        if echoed_push_done_ms > 0 and echoed_push_done_ms == done_publish_ms:
            done_to_order_ack_ms = max(0, server_ingress_ms - echoed_push_done_ms)
        elif order_ack_ms > 0:
            done_to_order_ack_ms = max(0, order_ack_ms - done_publish_ms)
        else:
            return
        self._batch_done_to_order_ack_by_close.setdefault(close_time_ms, []).append(done_to_order_ack_ms)
        if sender_client_id is not None:
            self._batch_order_ack_clients_by_close.setdefault(close_time_ms, set()).add(sender_client_id)

    async def _apply_downstream_ack(self, item: dict, sender_client_id: int | None = None) -> None:
        event_id = str(item.get("event_id") or "")
        if not event_id:
            return
        try:
            downstream_receive_ms = int(item.get("downstream_receive_ms"))
        except (TypeError, ValueError):
            return
        downstream_region = str(item.get("downstream_region") or "")

        async with self._pending_latency_lock:
            ev = self._pending_latency.pop(event_id, None)
            close_time_ms = self._event_close_time.pop(event_id, None)
        if ev is None:
            logger.info("ack.unknown event_id=%s", event_id)
            return

        server_ingress_ms = int(time.time() * 1000)
        ev.downstream_receive_ms = downstream_receive_ms
        ev.downstream_region = downstream_region or None
        svc_to_downstream = max(0, server_ingress_ms - ev.service_publish_ms)
        e2e = max(0, server_ingress_ms - ev.exchange_event_ms)
        region_ok = ev.region_aligned()
        logger.info(
            "latency.final event_id=%s symbol=%s e2e_ms=%s exchange_to_service_ms=%s "
            "service_to_downstream_ms=%s service_region=%s downstream_region=%s region_aligned=%s",
            ev.event_id,
            ev.symbol,
            e2e,
            ev.exchange_to_service_ms(),
            svc_to_downstream,
            ev.service_region,
            ev.downstream_region,
            region_ok,
        )
        if close_time_ms is not None:
            self._ack_service_to_downstream_by_close.setdefault(close_time_ms, []).append(int(svc_to_downstream))
            ingest_to_downstream = max(0, server_ingress_ms - ev.service_ingest_ms)
            self._ack_ingest_to_downstream_by_close.setdefault(close_time_ms, []).append(int(ingest_to_downstream))
            if sender_client_id is not None:
                self._ack_clients_by_close.setdefault(close_time_ms, set()).add(sender_client_id)
        self._enqueue_latency_events(
            [
                {
                    "event_id": ev.event_id,
                    "symbol": ev.symbol,
                    "interval": ev.interval,
                    "e2e_ms": str(e2e),
                    "exchange_to_service_ms": str(ev.exchange_to_service_ms()),
                    "service_to_downstream_ms": str(svc_to_downstream),
                    "service_region": ev.service_region,
                    "downstream_region": ev.downstream_region or "",
                    "region_aligned": str(region_ok),
                    "phase": "ack",
                },
            ],
        )

    async def _notify_kline_batch(
        self,
        payload: list[dict],
        expected_symbols: int,
        missing_symbols: int,
        ws_count: int,
        rest_count: int,
        service_to_client_no_ack_ms: int,
        connected_clients: int,
        publish_ms: int,
        push_done_ms: int,
    ) -> None:
        if not payload:
            return
        b2s_values: list[int] = []
        s2p_values: list[int] = []
        for row in payload:
            try:
                exchange_event_ms = int(row.get("exchange_event_ms") or 0)
                service_ingest_ms = int(row.get("service_ingest_ms") or 0)
                service_publish_ms = int(row.get("service_publish_ms") or 0)
            except (TypeError, ValueError):
                continue
            if exchange_event_ms and service_ingest_ms:
                b2s_values.append(service_ingest_ms - exchange_event_ms)
            if service_ingest_ms and service_publish_ms:
                s2p_values.append(service_publish_ms - service_ingest_ms)
        close_time_ms = payload[0].get("close_time_ms")
        try:
            close_time_key = int(close_time_ms or 0)
        except (TypeError, ValueError):
            close_time_key = 0
        if close_time_key <= 0:
            return
        metric_values = self._batch_metric_values_by_close.setdefault(
            close_time_key,
            {
                "batch_size": 0,
                "expected_symbols": 0,
                "missing_symbols": 0,
                "ws_count": 0,
                "rest_count": 0,
                "connected_clients": 0,
                "b2s_values": [],
                "s2p_values": [],
                "s2c_no_ack_values": [],
                "push_round_sizes": [],
                "push_round_no_ack_ms": [],
                "push_round_b2s_ms": [],
                "first_publish_ms": 0,
                "last_push_done_ms": 0,
                "exchange_event_min_ms": 0,
                "service_ingest_min_ms": 0,
                "service_ingest_max_ms": 0,
                "shard_exchange_event_min_ms": {},
                "shard_service_ingest_min_ms": {},
                "shard_service_ingest_max_ms": {},
                "shard_symbol_counts": {},
                "delayed_tokens": [],
            },
        )
        metric_values["batch_size"] = int(metric_values["batch_size"]) + len(payload)
        metric_values["expected_symbols"] = max(int(metric_values["expected_symbols"]), int(expected_symbols))
        metric_values["missing_symbols"] = max(int(metric_values["missing_symbols"]), max(0, missing_symbols))
        metric_values["ws_count"] = int(metric_values["ws_count"]) + ws_count
        metric_values["rest_count"] = int(metric_values["rest_count"]) + rest_count
        metric_values["connected_clients"] = max(int(metric_values["connected_clients"]), max(0, connected_clients))
        cast_b2s = metric_values["b2s_values"]
        cast_s2p = metric_values["s2p_values"]
        cast_s2c = metric_values["s2c_no_ack_values"]
        cast_round_sizes = metric_values["push_round_sizes"]
        cast_round_no_ack = metric_values["push_round_no_ack_ms"]
        cast_round_b2s = metric_values["push_round_b2s_ms"]
        cast_delayed_tokens = metric_values["delayed_tokens"]
        first_publish_ms = int(metric_values["first_publish_ms"])
        last_push_done_ms = int(metric_values["last_push_done_ms"])
        exchange_event_min_ms = int(metric_values["exchange_event_min_ms"])
        service_ingest_min_ms = int(metric_values["service_ingest_min_ms"])
        service_ingest_max_ms = int(metric_values["service_ingest_max_ms"])
        shard_exchange_min = metric_values["shard_exchange_event_min_ms"]
        shard_ingest_min = metric_values["shard_service_ingest_min_ms"]
        shard_ingest_max = metric_values["shard_service_ingest_max_ms"]
        shard_symbol_counts = metric_values["shard_symbol_counts"]
        if isinstance(cast_b2s, list):
            cast_b2s.extend(b2s_values)
        if isinstance(cast_s2p, list):
            cast_s2p.extend(s2p_values)
        if isinstance(cast_s2c, list):
            cast_s2c.extend([service_to_client_no_ack_ms] * len(payload))
        if isinstance(cast_round_sizes, list):
            cast_round_sizes.append(len(payload))
        if isinstance(cast_round_no_ack, list):
            cast_round_no_ack.append(service_to_client_no_ack_ms)
        if isinstance(cast_round_b2s, list):
            round_exchange_min = 0
            round_ingest_max = 0
            for row in payload:
                try:
                    row_exchange_event_ms = int(row.get("exchange_event_ms") or 0)
                    row_service_ingest_ms = int(row.get("service_ingest_ms") or 0)
                except (TypeError, ValueError):
                    continue
                if row_exchange_event_ms > 0:
                    round_exchange_min = (
                        row_exchange_event_ms
                        if round_exchange_min <= 0
                        else min(round_exchange_min, row_exchange_event_ms)
                    )
                round_ingest_max = max(round_ingest_max, row_service_ingest_ms)
            round_b2s_ms = (
                max(0, round_ingest_max - round_exchange_min)
                if round_ingest_max > 0 and round_exchange_min > 0
                else 0
            )
            cast_round_b2s.append(round_b2s_ms)
        if isinstance(cast_delayed_tokens, list):
            for row in payload:
                try:
                    symbol = str(row.get("symbol") or "")
                    row_close_time_ms = int(row.get("close_time_ms") or 0)
                    row_service_ingest_ms = int(row.get("service_ingest_ms") or 0)
                    row_service_publish_ms = int(row.get("service_publish_ms") or 0)
                    row_exchange_event_ms = int(row.get("exchange_event_ms") or 0)
                except (TypeError, ValueError):
                    continue
                if not symbol or row_service_ingest_ms <= 0 or row_service_publish_ms <= 0:
                    continue
                wait_ms = max(0, row_service_publish_ms - row_service_ingest_ms)
                cast_delayed_tokens.append(
                    {
                        "symbol": symbol,
                        "wait_ms": wait_ms,
                        "close_time_ms": row_close_time_ms,
                    },
                )
                if row_exchange_event_ms > 0:
                    if exchange_event_min_ms <= 0:
                        exchange_event_min_ms = row_exchange_event_ms
                    else:
                        exchange_event_min_ms = min(exchange_event_min_ms, row_exchange_event_ms)
                if service_ingest_min_ms <= 0:
                    service_ingest_min_ms = row_service_ingest_ms
                else:
                    service_ingest_min_ms = min(service_ingest_min_ms, row_service_ingest_ms)
                service_ingest_max_ms = max(service_ingest_max_ms, row_service_ingest_ms)
                shard_id = str(row.get("ws_shard_id") or "")
                if shard_id:
                    try:
                        row_shard_symbol_count = int(row.get("ws_shard_symbol_count") or 0)
                    except (TypeError, ValueError):
                        row_shard_symbol_count = 0
                    prior_ex_min = int(shard_exchange_min.get(shard_id, 0)) if isinstance(shard_exchange_min, dict) else 0
                    prior_in_min = int(shard_ingest_min.get(shard_id, 0)) if isinstance(shard_ingest_min, dict) else 0
                    prior_in_max = int(shard_ingest_max.get(shard_id, 0)) if isinstance(shard_ingest_max, dict) else 0
                    if isinstance(shard_exchange_min, dict) and row_exchange_event_ms > 0:
                        shard_exchange_min[shard_id] = (
                            row_exchange_event_ms
                            if prior_ex_min <= 0
                            else min(prior_ex_min, row_exchange_event_ms)
                        )
                    if isinstance(shard_ingest_min, dict):
                        shard_ingest_min[shard_id] = (
                            row_service_ingest_ms
                            if prior_in_min <= 0
                            else min(prior_in_min, row_service_ingest_ms)
                        )
                    if isinstance(shard_ingest_max, dict):
                        shard_ingest_max[shard_id] = max(prior_in_max, row_service_ingest_ms)
                    if isinstance(shard_symbol_counts, dict):
                        shard_symbol_counts[shard_id] = max(
                            int(shard_symbol_counts.get(shard_id, 0)),
                            row_shard_symbol_count,
                        )
        if first_publish_ms <= 0:
            metric_values["first_publish_ms"] = publish_ms
        else:
            metric_values["first_publish_ms"] = min(first_publish_ms, publish_ms)
        metric_values["last_push_done_ms"] = max(last_push_done_ms, push_done_ms)
        metric_values["exchange_event_min_ms"] = exchange_event_min_ms
        metric_values["service_ingest_min_ms"] = service_ingest_min_ms
        metric_values["service_ingest_max_ms"] = service_ingest_max_ms

        final_push_done_ms = int(metric_values["last_push_done_ms"])
        close_to_push_done_ms = final_push_done_ms - close_time_key
        period_boundary_ms = close_time_key + 1
        period_boundary_to_push_done_ms = final_push_done_ms - period_boundary_ms
        first_publish_final = int(metric_values["first_publish_ms"])
        ingest_done_to_publish_start_ms = (
            max(0, first_publish_final - service_ingest_max_ms)
            if first_publish_final > 0 and service_ingest_max_ms > 0
            else 0
        )
        ingest_done_to_push_done_ms = (
            max(0, final_push_done_ms - service_ingest_max_ms)
            if final_push_done_ms > 0 and service_ingest_max_ms > 0
            else 0
        )

        shard_push_done_ms = max(
            0,
            final_push_done_ms - first_publish_final,
        )
        full_arrival_ms = (
            max(0, service_ingest_max_ms - exchange_event_min_ms)
            if service_ingest_max_ms > 0 and exchange_event_min_ms > 0
            else 0
        )
        ingest_spread_ms = (
            max(0, service_ingest_max_ms - service_ingest_min_ms)
            if service_ingest_max_ms > 0 and service_ingest_min_ms > 0
            else 0
        )
        shard_arrival_lines: list[str] = []
        if isinstance(shard_ingest_max, dict) and isinstance(shard_exchange_min, dict):
            sorted_shards = sorted(shard_ingest_max.keys())
            for idx, shard_id in enumerate(sorted_shards):
                try:
                    shard_ingest_max_ms = int(shard_ingest_max.get(shard_id, 0))
                    shard_exchange_min_ms = int(shard_exchange_min.get(shard_id, 0))
                except (TypeError, ValueError):
                    continue
                if shard_ingest_max_ms <= 0 or shard_exchange_min_ms <= 0:
                    continue
                shard_symbols = 0
                if isinstance(shard_symbol_counts, dict):
                    try:
                        shard_symbols = int(shard_symbol_counts.get(shard_id, 0))
                    except (TypeError, ValueError):
                        shard_symbols = 0
                shard_arrival_lines.append(
                    f"第{idx + 1}路一次性推送：symbols={shard_symbols} 到齐耗时={max(0, shard_ingest_max_ms - shard_exchange_min_ms)}ms",
                )
        shard_arrival_text = "；".join(shard_arrival_lines) if shard_arrival_lines else "n=0"
        round_b2s_text = "n=0"
        if isinstance(cast_round_sizes, list) and isinstance(cast_round_b2s, list):
            total_rounds = min(len(cast_round_sizes), len(cast_round_b2s))
            round_b2s_lines: list[str] = []
            for idx in range(total_rounds):
                try:
                    round_sz = int(cast_round_sizes[idx])
                    round_ms = int(cast_round_b2s[idx])
                except (TypeError, ValueError):
                    continue
                round_b2s_lines.append(f"第{idx + 1}次：symbols={round_sz} 到达耗时={round_ms}ms")
            if round_b2s_lines:
                round_b2s_text = self._format_round_lines(round_b2s_lines)
        delayed_top_text = "n=0"
        if isinstance(cast_delayed_tokens, list) and cast_delayed_tokens:
            delayed_sorted = sorted(
                [x for x in cast_delayed_tokens if isinstance(x, dict)],
                key=lambda x: int(x.get("wait_ms", 0)),
                reverse=True,
            )[:10]
            delayed_parts: list[str] = []
            for item in delayed_sorted:
                symbol = str(item.get("symbol") or "")
                wait_ms = int(item.get("wait_ms") or 0)
                readable_close = self._format_close_time(item.get("close_time_ms"))
                delayed_parts.append(f"{symbol}:等待{wait_ms}ms(close={readable_close})")
            if delayed_parts:
                delayed_top_text = "；".join(delayed_parts)

        self._batch_summary_by_close[close_time_key] = {
            "close_time_ms": close_time_key,
            "batch_size": int(metric_values["batch_size"]),
            "expected_symbols": int(metric_values["expected_symbols"]),
            "missing_symbols": int(metric_values["missing_symbols"]),
            "ws_count": int(metric_values["ws_count"]),
            "rest_count": int(metric_values["rest_count"]),
            "binance_to_service": self._format_stats(cast_b2s if isinstance(cast_b2s, list) else []),
            "service_ingest_to_publish": self._format_stats(cast_s2p if isinstance(cast_s2p, list) else []),
            "service_to_client_no_ack": self._format_stats(cast_s2c if isinstance(cast_s2c, list) else []),
            "connected_clients": int(metric_values["connected_clients"]),
            "service_ws_shards_push_done_ms": shard_push_done_ms,
            "binance_to_service_full_arrival_ms": full_arrival_ms,
            "service_ingest_spread_ms": ingest_spread_ms,
            "binance_to_service_shard_arrival_ms": shard_arrival_text,
            "binance_to_service_round_arrival_ms": round_b2s_text,
            "delayed_tokens_top10": delayed_top_text,
            "last_batch_push_done_ms": final_push_done_ms,
            "close_time_to_push_done_ms": close_to_push_done_ms,
            "period_boundary_to_push_done_ms": period_boundary_to_push_done_ms,
            "ingest_done_to_publish_start_ms": ingest_done_to_publish_start_ms,
            "ingest_done_to_push_done_ms": ingest_done_to_push_done_ms,
        }

    async def _notify_ack_summary_later(self, close_time_ms: int, delay_seconds: int) -> None:
        await asyncio.sleep(max(1, delay_seconds))
        expected = self._ack_expected_by_close.get(close_time_ms, 0)
        batch_done_to_ack_values = self._batch_done_to_ack_by_close.get(close_time_ms, [])
        batch_done_to_compute_ack_values = self._batch_done_to_compute_ack_by_close.get(close_time_ms, [])
        batch_done_to_order_ack_values = self._batch_done_to_order_ack_by_close.get(close_time_ms, [])
        batch_ack_clients = len(self._batch_ack_clients_by_close.get(close_time_ms, set()))
        batch_integrated_ack_clients = len(self._batch_integrated_ack_clients_by_close.get(close_time_ms, set()))
        batch_compute_ack_clients = len(self._batch_compute_ack_clients_by_close.get(close_time_ms, set()))
        batch_order_ack_clients = len(self._batch_order_ack_clients_by_close.get(close_time_ms, set()))
        batch = self._batch_summary_by_close.get(close_time_ms, {})
        raw_metrics = self._batch_metric_values_by_close.get(close_time_ms, {})
        s2c_values = raw_metrics.get("s2c_no_ack_values", [])
        s2c_text = (
            self._format_stats_compact(s2c_values)
            if isinstance(s2c_values, list)
            else str(batch.get("service_to_client_no_ack", "n=0"))
        )
        mode_text = "多轮直推" if self._klines_passthrough_mode else "服务端整批"
        count_text = (
            f"{batch.get('batch_size', expected)}/{batch.get('expected_symbols', expected)}"
            f" 缺失={batch.get('missing_symbols', 0)}"
        )
        local_done_label = "收盘边界→本地聚合完成" if self._klines_source == "aggtrade" else "首事件→末入站"
        push_done_label = (
            "本地聚合完成→WS写出完成" if self._klines_source == "aggtrade" else "末条入站→WS写出完成"
        )
        connected_clients = int(batch.get("connected_clients", 0) or 0)
        close_to_push_done_ms = batch.get("close_time_to_push_done_ms")
        lines = [
            "【K线批次与ACK汇总】",
            f"收盘时间：{self._format_close_time(batch.get('close_time_ms', close_time_ms))}",
            f"数据源/模式：{self._klines_source} / {mode_text}",
            f"推送：{count_text} 客户端={connected_clients}",
            (
                f"服务端链路：{local_done_label}={self._format_ms_value(batch.get('binance_to_service_full_arrival_ms'))}；"
                f"{push_done_label}={self._format_ms_value(batch.get('ingest_done_to_push_done_ms'))}；"
                f"收盘→WS写出完成={self._format_ms_value(close_to_push_done_ms, signed=True)}"
            ),
            f"接收跨度：{self._format_ms_value(batch.get('service_ingest_spread_ms'))}；服务端→客户端(不含ACK)：{s2c_text}",
        ]
        ack_client_text = (
            f"接收={batch_ack_clients} 整合={batch_integrated_ack_clients} "
            f"计算={batch_compute_ack_clients} 下单={batch_order_ack_clients}"
        )
        lines.extend(
            [
                f"ACK客户端：{ack_client_text}",
                self._format_batch_ack_latency_line("客户端接收ACK耗时", close_to_push_done_ms, batch_done_to_ack_values),
                self._format_batch_ack_latency_line("计算ACK耗时", close_to_push_done_ms, batch_done_to_compute_ack_values),
                self._format_batch_ack_latency_line("下单ACK耗时", close_to_push_done_ms, batch_done_to_order_ack_values),
            ]
        )
        body = "\n".join(lines)
        try:
            await self._notifier.send("K线批次与ACK汇总", body)
        except Exception:
            logger.exception("notify.ack_summary_failed close_time_ms=%s", close_time_ms)
        finally:
            self._ack_service_to_downstream_by_close.pop(close_time_ms, None)
            self._ack_ingest_to_downstream_by_close.pop(close_time_ms, None)
            self._batch_done_to_ack_by_close.pop(close_time_ms, None)
            self._batch_done_to_integrated_ack_by_close.pop(close_time_ms, None)
            self._batch_done_to_compute_ack_by_close.pop(close_time_ms, None)
            self._batch_done_to_order_ack_by_close.pop(close_time_ms, None)
            self._ack_expected_by_close.pop(close_time_ms, None)
            self._ack_clients_by_close.pop(close_time_ms, None)
            self._batch_ack_clients_by_close.pop(close_time_ms, None)
            self._batch_integrated_ack_clients_by_close.pop(close_time_ms, None)
            self._batch_compute_ack_clients_by_close.pop(close_time_ms, None)
            self._batch_order_ack_clients_by_close.pop(close_time_ms, None)
            self._batch_summary_by_close.pop(close_time_ms, None)
            self._batch_metric_values_by_close.pop(close_time_ms, None)
            self._rest_compare_summary_by_close.pop(close_time_ms, None)
            self._rest_compare_events_by_close.pop(close_time_ms, None)
            self._passthrough_shards_sent_by_close.pop(close_time_ms, None)
            self._passthrough_total_shards_by_close.pop(close_time_ms, None)
            self._batch_done_publish_ms_by_close.pop(close_time_ms, None)
            self._ack_notify_scheduled.discard(close_time_ms)

    async def _notify_rest_compare_summary(self, summary: dict[str, object]) -> None:
        try:
            close_time_ms = int(summary.get("close_time_ms") or 0)
        except (TypeError, ValueError):
            close_time_ms = 0
        detail_path = ""
        if close_time_ms > 0:
            try:
                detail_path = await asyncio.to_thread(self._write_rest_compare_detail_file, summary)
                if detail_path:
                    summary["mismatch_detail_file"] = detail_path
            except Exception:
                logger.exception("aggtrade.rest_compare.detail_write_failed close_time_ms=%s", close_time_ms)
        body = self._format_rest_compare_notification_body(summary, close_time_ms, detail_path)
        try:
            await self._notifier.send("REST验证结果", body)
        except Exception:
            logger.exception("notify.rest_compare_summary_failed close_time_ms=%s", close_time_ms)
        finally:
            if close_time_ms > 0 and close_time_ms not in self._ack_expected_by_close:
                self._rest_compare_summary_by_close.pop(close_time_ms, None)
                self._rest_compare_snapshots_by_close.pop(close_time_ms, None)
                self._rest_compare_events_by_close.pop(close_time_ms, None)

    def _write_rest_compare_detail_file(self, summary: dict[str, object]) -> str:
        try:
            close_time_ms = int(summary.get("close_time_ms") or 0)
        except (TypeError, ValueError):
            close_time_ms = 0
        interval = str(summary.get("interval") or self._klines_interval or "kline")
        safe_interval = "".join(ch if ch.isalnum() else "_" for ch in interval) or "kline"
        if close_time_ms > 0:
            stamp = datetime.fromtimestamp(close_time_ms // 1000, tz=timezone.utc).strftime("%Y%m%d_%H%M%SZ")
        else:
            stamp = str(int(time.time() * 1000))
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        path = logs_dir / f"rest_compare_{safe_interval}_{stamp}_{close_time_ms}.md"
        snapshots = self._rest_compare_snapshot_summaries(summary)
        multiple_snapshots = len(snapshots) > 1
        lines: list[str] = [
            "# REST验证结果明细",
            "",
            f"数据源：{self._klines_source}",
            f"周期：{interval}",
            f"收盘时间：{self._format_close_time(close_time_ms)}",
            f"close_time_ms：{close_time_ms}",
        ]
        for item in snapshots:
            if multiple_snapshots:
                lines.extend(["", f"## {self._format_rest_compare_snapshot_title(item)}", ""])
                lines.extend(self._format_rest_compare_detail_section(item, heading_level=3))
            else:
                lines.extend([""])
                lines.extend(self._format_rest_compare_detail_section(item, heading_level=2))
        path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(
            "aggtrade.rest_compare.detail_written close_time_ms=%s path=%s",
            close_time_ms,
            path,
        )
        return str(path)

    def _format_rest_compare_notification_body(
        self,
        summary: dict[str, object],
        close_time_ms: int,
        detail_path: str,
    ) -> str:
        blocks = [
            self._format_rest_compare_notification_block(item, close_time_ms, detail_path)
            for item in self._rest_compare_snapshot_summaries(summary)
        ]
        return "\n\n".join(block for block in blocks if block)

    def _format_rest_compare_notification_block(
        self,
        summary: dict[str, object],
        close_time_ms: int,
        detail_path: str,
    ) -> str:
        summary_text = (
            self._format_rest_compare_summary(summary)
            if bool(summary.get("skipped"))
            else self._format_rest_compare_summary_line(summary)
        )
        lines = [
            f"【{self._format_rest_compare_snapshot_title(summary)}】",
            f"数据源：{self._klines_source}",
            f"收盘时间：{self._format_close_time(close_time_ms)}",
            summary_text,
        ]
        business_line = self._format_rest_compare_business_line(summary)
        if business_line:
            lines.append(business_line)
        if detail_path:
            lines.append(f"明细文件：{detail_path}")
        if not bool(summary.get("skipped")):
            lines.extend(
                [
                    "",
                    self._format_rest_compare_bucket_table(str(summary.get("mismatch_error_buckets") or "-")),
                    "",
                    self._format_rest_compare_stats_table(str(summary.get("mismatch_error_stats") or "-")),
                ]
            )
        return "\n".join(lines)

    def _rest_compare_snapshot_summaries(self, summary: dict[str, object]) -> list[dict[str, object]]:
        raw_snapshots = summary.get("snapshot_summaries")
        if isinstance(raw_snapshots, (list, tuple)):
            snapshots = [item for item in raw_snapshots if isinstance(item, dict)]
            if snapshots:
                return sorted(snapshots, key=self._rest_compare_snapshot_sort_key)
        return [summary]

    def _rest_compare_snapshot_sort_key(self, summary: dict[str, object]) -> int:
        try:
            return int(summary.get("snapshot_offset_ms") or 0)
        except (TypeError, ValueError):
            return 0

    def _format_rest_compare_snapshot_title(self, summary: dict[str, object]) -> str:
        label = str(summary.get("snapshot_label") or "").strip()
        if label:
            return f"REST验证结果{label}"
        return "REST验证结果"

    def _format_rest_compare_business_line(self, summary: dict[str, object]) -> str:
        has_business_fields = (
            "price_mismatch_symbols" in summary
            or "vq_gt_1pct_symbols" in summary
            or "business_passed" in summary
        )
        if not has_business_fields:
            return ""
        scope = "抽样" if bool(summary.get("diagnostic_sample")) else "全量"
        if bool(summary.get("diagnostic_sample")) and summary.get("sample_size"):
            scope = f"{scope}{summary.get('sample_size')}"
        status = "通过" if bool(summary.get("business_passed")) else "关注"
        return (
            f"业务口径({scope})：{status} "
            f"price_mismatch_symbols={summary.get('price_mismatch_symbols', 0)} "
            f"vq_gt_1pct_symbols={summary.get('vq_gt_1pct_symbols', 0)}"
        )

    def _format_rest_compare_detail_section(
        self,
        summary: dict[str, object],
        *,
        heading_level: int,
    ) -> list[str]:
        heading = "#" * max(1, heading_level)
        lines = [
            f"{heading} 汇总",
            "",
            (
                self._format_rest_compare_summary(summary)
                if bool(summary.get("skipped"))
                else self._format_rest_compare_summary_line(summary)
            ),
        ]
        business_line = self._format_rest_compare_business_line(summary)
        if business_line:
            lines.append(business_line)
        if bool(summary.get("skipped")):
            return lines + [""]
        lines.extend(
            [
                "",
                self._format_rest_compare_bucket_table(str(summary.get("mismatch_error_buckets") or "-")),
                "",
                self._format_rest_compare_stats_table(str(summary.get("mismatch_error_stats") or "-")),
                "",
                f"{heading} 失败样本",
                "",
                str(summary.get("mismatch_sample") or "-"),
                "",
                f"{heading} REST错误样本",
                "",
                str(summary.get("rest_error_sample") or "-"),
                "",
                f"{heading} 缺失/额外样本",
                "",
                f"missing_agg={summary.get('missing_agg_sample', '-')}",
                f"missing_rest={summary.get('missing_rest_sample', '-')}",
                f"extra={summary.get('extra_sample', '-')}",
                "",
                self._format_rest_compare_detail_table(
                    str(summary.get("mismatch_detail_all") or "-"),
                    heading_level=heading_level,
                ),
                "",
            ]
        )
        return lines

    async def _wait_for_rest_compare_summary(
        self,
        close_time_ms: int,
    ) -> dict[str, object] | None:
        if self._klines_source != "aggtrade" or not self._cfg.websocket.aggtrade_compare_with_kline:
            return None
        existing = self._rest_compare_summary_by_close.get(close_time_ms)
        if existing is not None:
            return existing
        event = self._rest_compare_events_by_close.setdefault(close_time_ms, asyncio.Event())
        timeout_seconds = max(1.0, self._cfg.websocket.aggtrade_rest_compare_lark_timeout_seconds)
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
        except TimeoutError:
            logger.warning(
                "aggtrade.rest_compare.lark_wait_timeout close_time_ms=%s timeout_seconds=%s",
                close_time_ms,
                timeout_seconds,
            )
            return None
        return self._rest_compare_summary_by_close.get(close_time_ms)

    def _format_rest_compare_summary_line(self, summary: dict[str, object]) -> str:
        status = "通过" if bool(summary.get("passed")) else "失败"
        parts = [
            status,
            f"expected={summary.get('expected', 0)}",
            f"rest={summary.get('rest_kline', 0)}",
            f"agg={summary.get('aggtrade', 0)}",
            f"matched={summary.get('matched', 0)}",
            f"mismatch={summary.get('mismatched', 0)}",
            f"missing_rest={summary.get('missing_rest', 0)}",
            f"rest_errors={summary.get('rest_errors', 0)}",
            f"delay={summary.get('validation_delay_seconds', 0)}s",
            f"fetch={summary.get('validation_fetch_elapsed_ms', 0)}ms",
        ]
        return " ".join(parts)

    def _format_rest_compare_bucket_table(self, buckets_text: str) -> str:
        buckets = self._parse_rest_compare_buckets(buckets_text)
        if not buckets:
            return "误差分桶\n无误差"
        lines = [
            "误差分桶",
            "| 字段 | ≤0.01% | ≤0.1% | ≤1% | >1% |",
            "|---|---:|---:|---:|---:|",
        ]
        for label in self._ordered_rest_compare_labels(buckets):
            counts = buckets[label]
            lines.append(
                f"| {self._rest_compare_field_label(label)} | {counts[0]} | {counts[1]} | {counts[2]} | {counts[3]} |"
            )
        return "\n".join(lines)

    def _format_rest_compare_stats_table(self, stats_text: str) -> str:
        stats = self._parse_rest_compare_stats(stats_text)
        if not stats:
            return "误差统计\n无误差"
        lines = [
            "误差统计",
            "| 字段 | 数量 | 平均绝对误差 | 最大绝对误差 | 平均百分比 | P95百分比 | 最大百分比 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
        for label in self._ordered_rest_compare_labels(stats):
            row = stats[label]
            lines.append(
                "| "
                f"{self._rest_compare_field_label(label)} | "
                f"{row.get('n', '0')} | "
                f"{self._format_rest_compare_decimal(row.get('abs_avg', '0'))} | "
                f"{self._format_rest_compare_decimal(row.get('abs_max', '0'))} | "
                f"{self._format_rest_compare_percent(row.get('pct_avg', '0'))} | "
                f"{self._format_rest_compare_percent(row.get('pct_p95', '0'))} | "
                f"{self._format_rest_compare_percent(row.get('pct_max', '0'))} |"
            )
        return "\n".join(lines)

    def _format_rest_compare_detail_table(self, detail_text: str, *, heading_level: int = 2) -> str:
        rows = self._parse_rest_compare_details(detail_text)
        heading = "#" * max(1, heading_level)
        if not rows:
            return f"{heading} REST验证失败OHLCV/Q明细(agg->rest)\n\n无明细"
        lines = [
            f"{heading} REST验证失败OHLCV/Q明细(agg->rest)",
            "",
            "| Symbol | 字段 | Agg | REST | 绝对误差 | 百分比 |",
            "|---|---|---:|---:|---:|---:|",
        ]
        for row in rows:
            lines.append(
                "| "
                f"{self._markdown_cell(row['symbol'])} | "
                f"{self._markdown_cell(self._rest_compare_field_label(row['label']))} | "
                f"{self._format_rest_compare_decimal(row['agg'])} | "
                f"{self._format_rest_compare_decimal(row['rest'])} | "
                f"{self._format_rest_compare_decimal(row['abs'])} | "
                f"{self._format_rest_compare_percent(row['pct'])} |"
            )
        return "\n".join(lines)

    def _parse_rest_compare_details(self, text: str) -> list[dict[str, str]]:
        if not text or text == "-":
            return []
        rows: list[dict[str, str]] = []
        for segment in text.split("；"):
            segment = segment.strip()
            if not segment or "[" not in segment or not segment.endswith("]"):
                continue
            symbol, raw_fields = segment.split("[", 1)
            symbol = symbol.strip()
            raw_fields = raw_fields[:-1]
            if not symbol:
                continue
            for raw_field in raw_fields.split(";"):
                raw_field = raw_field.strip()
                if not raw_field:
                    continue
                parsed = self._parse_rest_compare_detail_field(raw_field)
                if parsed is None:
                    continue
                parsed["symbol"] = symbol
                rows.append(parsed)
        return rows

    def _parse_rest_compare_detail_field(self, text: str) -> dict[str, str] | None:
        parts = text.split()
        if not parts or "=" not in parts[0] or "->" not in parts[0]:
            return None
        label, values = parts[0].split("=", 1)
        agg, rest = values.split("->", 1)
        row = {
            "label": label.strip(),
            "agg": agg.strip(),
            "rest": rest.strip(),
            "abs": "",
            "pct": "",
        }
        for part in parts[1:]:
            if part.startswith("abs="):
                row["abs"] = part.split("=", 1)[1].strip()
            elif part.startswith("pct="):
                row["pct"] = part.split("=", 1)[1].strip()
        if not row["label"] or not row["agg"] or not row["rest"] or not row["abs"] or not row["pct"]:
            return None
        return row

    def _markdown_cell(self, value: object) -> str:
        return str(value).replace("|", "\\|").replace("\n", " ").strip()

    def _parse_rest_compare_buckets(self, text: str) -> dict[str, list[int]]:
        if not text or text == "-":
            return {}
        result: dict[str, list[int]] = {}
        bucket_indexes = {"<=0.01%": 0, "<=0.1%": 1, "<=1%": 2, ">1%": 3}
        for segment in text.split("；"):
            if ":" not in segment:
                continue
            label, raw_counts = segment.split(":", 1)
            label = label.strip()
            counts = [0, 0, 0, 0]
            for token in raw_counts.split():
                if "/" not in token:
                    continue
                bucket, value = token.rsplit("/", 1)
                idx = bucket_indexes.get(bucket)
                if idx is None:
                    continue
                try:
                    counts[idx] = int(value)
                except ValueError:
                    counts[idx] = 0
            if label and sum(counts) > 0:
                result[label] = counts
        return result

    def _parse_rest_compare_stats(self, text: str) -> dict[str, dict[str, str]]:
        if not text or text == "-":
            return {}
        result: dict[str, dict[str, str]] = {}
        for segment in text.split("；"):
            if ":" not in segment:
                continue
            label, raw_values = segment.split(":", 1)
            values: dict[str, str] = {}
            for token in raw_values.split():
                if "=" not in token:
                    continue
                key, value = token.split("=", 1)
                values[key] = value
            label = label.strip()
            if label and values:
                result[label] = values
        return result

    def _ordered_rest_compare_labels(self, data: dict[str, object]) -> list[str]:
        ordered = [label for label in ("O", "H", "L", "C", "V", "Q") if label in data]
        ordered.extend(sorted(label for label in data if label not in {"O", "H", "L", "C", "V", "Q"}))
        return ordered

    def _rest_compare_field_label(self, label: str) -> str:
        labels = {
            "O": "开盘价 O",
            "H": "最高价 H",
            "L": "最低价 L",
            "C": "收盘价 C",
            "V": "成交量 V",
            "Q": "成交额 Q",
        }
        return labels.get(label, label)

    def _format_rest_compare_decimal(self, value: object, places: int = 6) -> str:
        try:
            decimal_value = Decimal(str(value).strip().rstrip("%"))
            quant = Decimal(1).scaleb(-places)
            text = format(decimal_value.quantize(quant), "f")
        except Exception:
            return str(value)
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    def _format_rest_compare_percent(self, value: object, places: int = 6) -> str:
        try:
            decimal_value = Decimal(str(value).strip().rstrip("%"))
            quant = Decimal(1).scaleb(-places)
            text = format(decimal_value.quantize(quant), "f")
        except Exception:
            text = str(value)
            return text if text.endswith("%") else f"{text}%"
        return f"{text}%"

    def _format_rest_compare_summary(self, summary: dict[str, object] | None) -> str:
        if self._klines_source != "aggtrade":
            return "n/a"
        if not self._cfg.websocket.aggtrade_compare_with_kline:
            return "已关闭"
        if summary is None:
            return "异步验证中/暂无结果"
        if bool(summary.get("skipped")):
            open_time_ms = summary.get("open_time_ms")
            min_validate_open_time_ms = summary.get("min_validate_open_time_ms")
            open_time_text = (
                self._format_close_time(open_time_ms)
                if isinstance(open_time_ms, int)
                else str(open_time_ms or "n/a")
            )
            min_validate_text = (
                self._format_close_time(min_validate_open_time_ms)
                if isinstance(min_validate_open_time_ms, int)
                else str(min_validate_open_time_ms or "n/a")
            )
            reason = str(summary.get("skip_reason") or "unknown")
            return (
                f"跳过 reason={reason} expected={summary.get('expected', 0)} "
                f"agg={summary.get('aggtrade', 0)} "
                f"本根open={open_time_text} 首个验证open>={min_validate_text}"
            )
        line = self._format_rest_compare_summary_line(summary)
        detail_file = str(summary.get("mismatch_detail_file") or "")
        if detail_file:
            return f"{line} 明细文件={detail_file}"
        return line

    def _format_stats(self, values: list[int]) -> str:
        if not values:
            return "n=0"
        current = values[-1]
        values_sorted = sorted(values)
        avg = sum(values_sorted) / len(values_sorted)
        p95 = self._percentile(values_sorted, 95.0)
        return (
            f"n={len(values_sorted)} 本次={current}ms "
            f"avg={avg:.2f}ms p95={p95:.2f}ms min={values_sorted[0]}ms max={values_sorted[-1]}ms"
        )

    def _format_stats_compact(self, values: list[int]) -> str:
        if not values:
            return "n=0"
        values_sorted = sorted(values)
        p95 = self._percentile(values_sorted, 95.0)
        return f"n={len(values_sorted)} 本次={values[-1]}ms p95={p95:.0f}ms max={values_sorted[-1]}ms"

    def _format_ack_stats_compact(self, values: list[int]) -> str:
        if not values:
            return "n=0"
        return self._format_stats_compact(values)

    def _format_batch_ack_latency_line(self, label: str, close_to_push_done_ms: object, values: list[int]) -> str:
        if not values:
            return f"{label}：n=0"
        return (
            f"{label}：周期结束={self._format_offset_stats_compact(close_to_push_done_ms, values)}；"
            f"WS写出后={self._format_ack_stats_compact(values)}"
        )

    def _format_offset_stats_compact(self, base_ms: object, values: list[int]) -> str:
        if not values:
            return "n=0"
        if not isinstance(base_ms, int):
            return "n/a"
        return self._format_stats_compact([max(0, base_ms + value) for value in values])

    def _format_ms_value(self, value: object, *, signed: bool = False) -> str:
        if isinstance(value, int):
            return f"{value:+d}ms" if signed else f"{value}ms"
        if value is None:
            return "n/a"
        return str(value)

    def _format_round_lines(self, lines: list[str], head: int = 12, tail: int = 5) -> str:
        if not lines:
            return "n=0"
        head = max(1, head)
        tail = max(0, tail)
        if len(lines) <= head + tail:
            return "；".join(lines)
        omitted = len(lines) - head - tail
        return "；".join([*lines[:head], f"... 省略{omitted}轮 ...", *lines[-tail:]])

    def _format_close_time(self, close_time_ms: object) -> str:
        try:
            ts_ms = int(close_time_ms)
        except (TypeError, ValueError):
            return str(close_time_ms)
        if ts_ms <= 0:
            return str(close_time_ms)
        return datetime.fromtimestamp(ts_ms // 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _percentile(self, values_sorted: list[int], pct: float) -> float:
        if not values_sorted:
            return 0.0
        rank = (len(values_sorted) - 1) * (pct / 100.0)
        lo = int(rank)
        hi = min(lo + 1, len(values_sorted) - 1)
        if lo == hi:
            return float(values_sorted[lo])
        frac = rank - lo
        return values_sorted[lo] + (values_sorted[hi] - values_sorted[lo]) * frac

    async def _run_1m_alerts(self, symbols: list[str]) -> None:
        if self._alerts is None:
            return
        async for kline in self._binance.iter_closed_klines(symbols, interval="1m"):
            self._ingest_1m += 1
            signals = await self._alerts.on_closed_1m(kline)
            if not signals or self._alerts_ws is None:
                continue
            for sig in signals:
                pushed = await self._alerts_ws.broadcast_json(self._alert_payload(sig))
                if pushed > 0:
                    self._push_alerts += pushed

    async def _run_heartbeat(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(15)
            alerts_clients = await self._alerts_ws.client_count() if self._alerts_ws is not None else 0
            klines_clients = await self._klines_ws.client_count() if self._klines_ws is not None else 0
            ws_activity = self._binance.get_ws_activity_snapshot()
            ws_shard_health = self._binance.get_ws_shard_health_snapshot()
            ws_ago_ms = ws_activity.get(self._klines_interval)
            interval_shards = ws_shard_health.get(self._klines_interval, {})
            logger.info(
                "service.heartbeat mode=%s ingest_1m=%s ingest_1h=%s pushed_alerts=%s pushed_1h=%s clients_alerts=%s clients_klines=%s klines_interval=%s klines_source=%s ws_last_event_ago_ms=%s ws_stale_shards=%s ws_total_shards=%s ws_worst_shard_ago_ms=%s",
                self._cfg.service_mode,
                self._ingest_1m,
                self._ingest_1h,
                self._push_alerts,
                self._push_1h,
                alerts_clients,
                klines_clients,
                self._klines_interval,
                self._klines_source,
                ws_ago_ms,
                interval_shards.get("stale_shards"),
                interval_shards.get("total_shards"),
                interval_shards.get("worst_shard_ago_ms"),
            )

    def _alert_payload(self, signal: AlertSignal) -> dict:
        return {
            "type": "price_alert",
            "symbol": signal.symbol,
            "interval": signal.interval,
            "direction": signal.direction,
            "threshold_pct": signal.threshold_pct,
            "move_pct": signal.move_pct,
            "previous_close": signal.reference_close_price,
            "current_close": signal.current_close_price,
            "open_time_ms": signal.open_time_ms,
            "close_time_ms": signal.close_time_ms,
            "exchange_event_ms": signal.event_time_ms,
            "service_ingest_ms": signal.ingest_time_ms,
            "service_region": self._cfg.service_region,
        }

    async def _get_alert_clients_count(self) -> int:
        if self._alerts_ws is None:
            return 0
        client_count = getattr(self._alerts_ws, "client_count", None)
        if callable(client_count):
            try:
                return int(await client_count())
            except Exception:
                return 0
        return 0
