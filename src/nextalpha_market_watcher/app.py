from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque

import httpx
import orjson
from fastapi import WebSocket
from websockets.server import WebSocketServerProtocol

from .alerts import AlertEngine, AlertSignal
from .binance_client import BinanceClient
from .config import AppConfig
from .metrics import start_metrics_server
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
        self._alerts: AlertEngine | None = None
        if cfg.service_mode in ("alerts", "all"):
            self._alerts = AlertEngine(cfg, self._notifier)
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
        self._flush_grace_ms = max(1_000, self._cfg.websocket.klines_flush_grace_ms)
        self._full_batch_only = self._cfg.websocket.klines_full_batch_only
        self._min_publish_ratio = min(1.0, max(0.0, self._cfg.websocket.klines_min_publish_ratio))
        self._full_batch_max_wait_ms = max(
            self._flush_grace_ms,
            self._cfg.websocket.klines_full_batch_max_wait_ms,
        )
        self._pending_lock = asyncio.Lock()
        self._klines_interval = self._cfg.websocket.klines_interval.strip() or "1h"
        self._rest_repair_delay_ms = self._resolve_rest_repair_delay_ms(self._klines_interval)
        self._rest_repairing_close_times: set[int] = set()
        self._log_full_missing_symbols = self._cfg.websocket.klines_log_full_missing_symbols

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop_event = stop if stop is not None else asyncio.Event()
        start_metrics_server(self._cfg)
        logger.info("service.start mode=%s", self._cfg.service_mode)
        syms = await self._binance.fetch_symbols()
        logger.info("symbols.loaded count=%s", len(syms))
        if not syms:
            raise RuntimeError("No symbols found for Binance U perpetual.")

        ingest_task: asyncio.Task[None] | None = None
        refresh_task: asyncio.Task[None] | None = None
        heartbeat_task: asyncio.Task[None] | None = None
        stale_flush_task: asyncio.Task[None] | None = None
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
            if self._cfg.service_mode in ("klines", "all"):
                stale_flush_task = asyncio.create_task(self._run_klines_stale_flush(stop_event))
            stop_wait = asyncio.create_task(stop_event.wait())

            while True:
                wait_set = {ingest_task, refresh_task, stop_wait}
                done, _pending = await asyncio.wait(
                    wait_set,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if stop_wait in done:
                    if ingest_task and not ingest_task.done():
                        ingest_task.cancel()
                    if refresh_task and not refresh_task.done():
                        refresh_task.cancel()
                    if ingest_task:
                        await asyncio.gather(ingest_task, return_exceptions=True)
                    if refresh_task:
                        await asyncio.gather(refresh_task, return_exceptions=True)
                    if heartbeat_task:
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
                    if stale_flush_task:
                        await asyncio.gather(stale_flush_task, return_exceptions=True)
                    break

                if ingest_task in done:
                    if refresh_task and not refresh_task.done():
                        refresh_task.cancel()
                        await asyncio.gather(refresh_task, return_exceptions=True)
                    exc = ingest_task.exception()
                    if exc is not None and not isinstance(exc, asyncio.CancelledError):
                        raise exc
                    break

                if refresh_task in done:
                    new_syms = await self._binance.fetch_symbols()
                    if set(new_syms) != set(syms):
                        logger.info("symbols.refresh %s->%s", len(syms), len(new_syms))
                        if self._alerts is not None:
                            self._alerts.prune_to_symbols(set(new_syms))
                        syms = new_syms
                        if ingest_task and not ingest_task.done():
                            ingest_task.cancel()
                            await asyncio.gather(ingest_task, return_exceptions=True)
                        ingest_task = asyncio.create_task(self._run_ingestion(syms))
                    refresh_task = asyncio.create_task(
                        asyncio.sleep(self._cfg.binance.symbols_refresh_seconds),
                    )

        finally:
            stop_event.set()
            to_cancel = [ingest_task, refresh_task, heartbeat_task, stale_flush_task, *ws_tasks]
            for t in to_cancel:
                if t is not None and not t.done():
                    t.cancel()
            await asyncio.gather(
                *[x for x in to_cancel if x is not None],
                return_exceptions=True,
            )

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

    async def _run_1h_publish(self, symbols: list[str]) -> None:
        if self._klines_ws is None:
            async for _ in self._binance.iter_closed_klines(symbols, interval=self._klines_interval):
                self._ingest_1h += 1
            return

        self._expected_symbols = set(symbols)
        min_required = max(1, math.ceil(len(self._expected_symbols) * self._min_publish_ratio))
        self._pending_by_close.clear()
        self._pending_has_ws.clear()
        self._pending_first_ingest_ms.clear()
        latest_close_by_symbol: dict[str, int] = {}

        async for kline in self._binance.iter_closed_klines(symbols, interval=self._klines_interval):
            prev_close = latest_close_by_symbol.get(kline.symbol)
            if prev_close is not None and kline.close_time_ms <= prev_close:
                continue
            latest_close_by_symbol[kline.symbol] = kline.close_time_ms
            self._ingest_1h += 1
            complete_rows: dict[str, dict] | None = None
            async with self._pending_lock:
                rows_by_symbol = self._pending_by_close.setdefault(kline.close_time_ms, {})
                self._pending_first_ingest_ms.setdefault(kline.close_time_ms, kline.ingest_time_ms)
                rows_by_symbol[kline.symbol] = {
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
                }
                if kline.source == "ws":
                    self._pending_has_ws[kline.close_time_ms] = True
                if len(rows_by_symbol) >= min_required:
                    complete_rows = self._pending_by_close.pop(kline.close_time_ms, {})
                    self._pending_has_ws.pop(kline.close_time_ms, None)
                    first_ingest_ms = self._pending_first_ingest_ms.pop(kline.close_time_ms, None)

            if complete_rows:
                await self._flush_close_time(kline.close_time_ms, complete_rows, first_ingest_ms)

        async with self._pending_lock:
            pending_items = sorted(self._pending_by_close.items())
            self._pending_by_close = {}
            self._pending_has_ws = {}
            pending_first = self._pending_first_ingest_ms
            self._pending_first_ingest_ms = {}

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
            await self._flush_close_time(close_time_ms, rows_by_symbol, pending_first.get(close_time_ms))

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
        if self._klines_recent.maxlen and self._klines_recent.maxlen > 0:
            async with self._klines_recent_lock:
                self._klines_recent.append(payload)
        pushed_clients = await self._klines_ws.broadcast_json(payload)
        if pushed_clients > 0:
            self._push_1h += pushed_clients * len(payload)
        got_symbols = len(rows_by_symbol)
        expected_symbols = len(self._expected_symbols)
        missing_symbols = expected_symbols - got_symbols
        got_ratio = (got_symbols / expected_symbols) if expected_symbols > 0 else 0.0
        missing_list = sorted(self._expected_symbols - set(rows_by_symbol.keys()))
        missing_sample = ",".join(missing_list[:20]) if missing_list else "-"
        ws_count = sum(1 for row in payload if row.get("source") == "ws")
        rest_count = len(payload) - ws_count
        full_batch_wait_ms = None
        if first_ingest_ms is not None:
            full_batch_wait_ms = max(0, publish_ms - first_ingest_ms)
        logger.info(
            "klines.batch.published close_time_ms=%s batch_size=%s got_symbols=%s expected_symbols=%s got_ratio=%.3f ws_count=%s rest_count=%s min_required=%s missing_symbols=%s missing_sample=%s full_batch_wait_ms=%s full_batch_only=%s min_publish_ratio=%.3f pushed_clients=%s pushed_rows_total=%s",
            close_time_ms,
            len(payload),
            got_symbols,
            expected_symbols,
            got_ratio,
            ws_count,
            rest_count,
            max(1, math.ceil(expected_symbols * self._min_publish_ratio)),
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
            await self._flush_close_time(close_time_ms, rows_by_symbol, first_ingest_ms)

    async def _run_klines_stale_flush(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(1)
            now_ms = int(time.time() * 1000)
            if self._full_batch_only:
                await self._try_rest_repair_for_waiting_batches(now_ms)
                await self._log_full_batch_waiting(now_ms)
            else:
                await self._flush_stale_batches(now_ms)

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

        logger.warning(
            "klines.batch.rest_repair.start close_time_ms=%s missing_symbols=%s wait_ms=%s repair_delay_ms=%s",
            close_time_ms,
            len(missing_symbols),
            wait_ms,
            self._rest_repair_delay_ms,
        )

        timeout_seconds = max(5.0, self._cfg.backfill.rest_timeout_seconds)
        repaired = 0
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds)) as client:
            for symbol in missing_symbols:
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
                async with self._pending_lock:
                    rows_now = self._pending_by_close.get(close_time_ms)
                    if rows_now is None or symbol in rows_now:
                        continue
                    rows_now[symbol] = {
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
                repaired += 1

        maybe_flush_rows: dict[str, dict] | None = None
        async with self._pending_lock:
            rows_now = self._pending_by_close.get(close_time_ms, {})
            got = len(rows_now)
            if got >= min_required:
                maybe_flush_rows = self._pending_by_close.pop(close_time_ms, {})
                self._pending_has_ws.pop(close_time_ms, None)
                first_ingest_ms = self._pending_first_ingest_ms.pop(close_time_ms, first_ingest_ms)
        logger.warning(
            "klines.batch.rest_repair.done close_time_ms=%s repaired=%s got_symbols=%s min_required=%s",
            close_time_ms,
            repaired,
            len(maybe_flush_rows) if maybe_flush_rows is not None else got,
            min_required,
        )
        if maybe_flush_rows:
            await self._flush_close_time(close_time_ms, maybe_flush_rows, first_ingest_ms)

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
                "service.heartbeat mode=%s ingest_1m=%s ingest_1h=%s pushed_alerts=%s pushed_1h=%s clients_alerts=%s clients_klines=%s klines_interval=%s ws_last_event_ago_ms=%s ws_stale_shards=%s ws_total_shards=%s ws_worst_shard_ago_ms=%s",
                self._cfg.service_mode,
                self._ingest_1m,
                self._ingest_1h,
                self._push_alerts,
                self._push_1h,
                alerts_clients,
                klines_clients,
                self._klines_interval,
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
            "open_time_ms": signal.open_time_ms,
            "close_time_ms": signal.close_time_ms,
            "exchange_event_ms": signal.event_time_ms,
            "service_ingest_ms": signal.ingest_time_ms,
            "service_region": self._cfg.service_region,
        }
