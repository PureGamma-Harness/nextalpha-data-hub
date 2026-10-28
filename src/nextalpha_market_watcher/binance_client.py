from __future__ import annotations

import asyncio
import contextlib
import http.client
import logging
import base64
import math
import os
import random
import socket
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import urlparse, urlencode

import aiohttp
import httpx
import orjson
import websockets
import socks

from .config import AppConfig
from .metrics import (
    BACKFILL_ERRORS,
    BACKFILL_KLINES,
    EXCHANGE_TO_SERVICE_MS,
    KLINES_RECEIVED,
    WS_KLINE_FAST_SKIPPED,
    WS_CONNECTIONS,
    touch_ingest,
)
from .models import ClosedKline

logger = logging.getLogger(__name__)

AggTradeCompareSummaryCallback = Callable[[dict[str, int | float | str | bool]], Awaitable[None]]

_JSON_WHITESPACE = b" \t\r\n"
_CLOSED_KLINE_KEY = b'"x"'
_CONTROL_FRAME_MARKERS = (b'"code"', b'"msg"', b'"result"', b'"id"')


def _raw_has_closed_kline_marker(raw: bytes) -> bool:
    """Return True when a raw Binance frame contains the kline close flag x=true."""
    start = 0
    while True:
        idx = raw.find(_CLOSED_KLINE_KEY, start)
        if idx < 0:
            return False
        pos = idx + len(_CLOSED_KLINE_KEY)
        while pos < len(raw) and raw[pos] in _JSON_WHITESPACE:
            pos += 1
        if pos < len(raw) and raw[pos] == ord(":"):
            pos += 1
            while pos < len(raw) and raw[pos] in _JSON_WHITESPACE:
                pos += 1
            if raw.startswith(b"true", pos):
                return True
        start = idx + len(_CLOSED_KLINE_KEY)


def _raw_maybe_control_frame(raw: bytes) -> bool:
    return any(marker in raw for marker in _CONTROL_FRAME_MARKERS)


@dataclass(slots=True)
class _AggTradeBar:
    symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    first_trade_time_ms: int
    last_trade_time_ms: int
    event_time_ms: int

    @classmethod
    def from_trade(
        cls,
        symbol: str,
        interval: str,
        open_time_ms: int,
        close_time_ms: int,
        price: Decimal,
        quantity: Decimal,
        trade_time_ms: int,
        event_time_ms: int,
    ) -> "_AggTradeBar":
        return cls(
            symbol=symbol,
            interval=interval,
            open_time_ms=open_time_ms,
            close_time_ms=close_time_ms,
            open_price=price,
            high_price=price,
            low_price=price,
            close_price=price,
            volume=quantity,
            quote_volume=price * quantity,
            first_trade_time_ms=trade_time_ms,
            last_trade_time_ms=trade_time_ms,
            event_time_ms=event_time_ms,
        )

    def update(self, price: Decimal, quantity: Decimal, trade_time_ms: int, event_time_ms: int) -> None:
        if trade_time_ms < self.first_trade_time_ms:
            self.first_trade_time_ms = trade_time_ms
            self.open_price = price
        if trade_time_ms >= self.last_trade_time_ms:
            self.last_trade_time_ms = trade_time_ms
            self.close_price = price
        self.high_price = max(self.high_price, price)
        self.low_price = min(self.low_price, price)
        self.volume += quantity
        self.quote_volume += price * quantity
        self.event_time_ms = max(self.event_time_ms, event_time_ms)

    def to_closed_kline(
        self,
        ingest_time_ms: int,
        shard_id: str,
        shard_symbol_count: int,
        total_shards: int,
    ) -> ClosedKline:
        return ClosedKline(
            symbol=self.symbol,
            interval=self.interval,
            open_time_ms=self.open_time_ms,
            close_time_ms=self.close_time_ms,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            close_price=self.close_price,
            volume=self.volume,
            quote_volume=self.quote_volume,
            # Local aggTrade bars don't have a Binance kline event timestamp.
            # Use the candle close boundary so existing latency reports show
            # close-to-ingest for this synthetic source.
            event_time_ms=self.close_time_ms,
            ingest_time_ms=ingest_time_ms,
            source="aggtrade",
            ws_shard_id=shard_id,
            ws_shard_symbol_count=shard_symbol_count,
            ws_total_shards=total_shards,
        )


@dataclass(slots=True)
class _AggTradeRestCompareRequest:
    close_time_ms: int
    symbols: tuple[str, ...]
    snapshot_offset_ms: int
    snapshot_due_ms: int
    snapshot_label: str
    sample: bool
    final: bool


class _AggTradeKlineComparator:
    def __init__(
        self,
        *,
        interval: str,
        interval_ms: int,
        expected_symbols: set[str],
        expected_shards: set[str],
        price_tolerance: Decimal,
        volume_tolerance: Decimal,
        quote_tolerance: Decimal,
        rest_compare_queue: asyncio.Queue[_AggTradeRestCompareRequest] | None = None,
        summary_callback: AggTradeCompareSummaryCallback | None = None,
        min_validate_open_time_ms: int = 0,
        diagnostic_enabled: bool = True,
        diagnostic_sample_size: int = 50,
        diagnostic_offsets_ms: tuple[int, ...] = (800, 1800, 3000, 5000),
        final_compare_delay_ms: int = 10_000,
        mismatch_log_limit_per_close: int = 20,
        retain_close_times: int = 12,
    ) -> None:
        self._interval = interval
        self._interval_ms = interval_ms
        self._expected_symbols = expected_symbols
        self._expected_shards = expected_shards
        self._price_tolerance = price_tolerance
        self._volume_tolerance = volume_tolerance
        self._quote_tolerance = quote_tolerance
        self._rest_compare_queue = rest_compare_queue
        self._summary_callback = summary_callback
        self._min_validate_open_time_ms = min_validate_open_time_ms
        self._diagnostic_enabled = diagnostic_enabled
        self._diagnostic_sample_size = max(1, diagnostic_sample_size)
        self._diagnostic_offsets_ms = tuple(
            sorted({max(0, int(offset_ms)) for offset_ms in diagnostic_offsets_ms})
        )
        self._final_compare_delay_ms = max(0, final_compare_delay_ms)
        self._mismatch_log_limit_per_close = max(0, mismatch_log_limit_per_close)
        self._retain_close_times = max(2, retain_close_times)
        self._live_bars_by_close: dict[int, dict[str, _AggTradeBar]] = {}
        self._agg_by_close: dict[int, dict[str, ClosedKline]] = {}
        self._kline_by_close: dict[int, dict[str, ClosedKline]] = {}
        self._missing_reference_by_close: dict[int, set[str]] = {}
        self._reference_errors_by_close: dict[int, list[str]] = {}
        self._compared: set[tuple[int, str]] = set()
        self._matched_by_close: dict[int, int] = {}
        self._mismatch_by_close: dict[int, list[str]] = {}
        self._mismatch_details_by_close: dict[int, list[dict[str, object]]] = {}
        self._target_symbols_by_close: dict[int, set[str]] = {}
        self._queued_rest_compare: set[tuple[int, str]] = set()
        self._summary_done: set[int] = set()
        self._close_order: deque[int] = deque()
        self._shard_connected_ms: dict[str, int] = {}
        self._min_validate_reason = "startup_partial_period"

    def mark_shard_connected(self, shard_id: str, connected_ms: int) -> None:
        if shard_id not in self._expected_shards:
            return
        was_connected = shard_id in self._shard_connected_ms
        self._shard_connected_ms[shard_id] = connected_ms
        if len(self._shard_connected_ms) < len(self._expected_shards):
            return
        max_connected_ms = max(self._shard_connected_ms.values())
        min_validate_open_time_ms = ((max_connected_ms // self._interval_ms) + 1) * self._interval_ms
        if min_validate_open_time_ms <= self._min_validate_open_time_ms:
            return
        self._min_validate_open_time_ms = min_validate_open_time_ms
        self._min_validate_reason = "shard_reconnect_partial_period" if was_connected else "startup_partial_period"
        logger.info(
            "aggtrade.rest_compare.ready interval=%s shards=%s min_validate_open_time_ms=%s reason=%s",
            self._interval,
            len(self._expected_shards),
            self._min_validate_open_time_ms,
            self._min_validate_reason,
        )

    async def record_agg(self, kline: ClosedKline) -> None:
        self._remember_close(kline.close_time_ms)
        agg_bucket = self._agg_by_close.setdefault(kline.close_time_ms, {})
        agg_bucket[kline.symbol] = kline
        strict = self._kline_by_close.get(kline.close_time_ms, {}).get(kline.symbol)
        if strict is not None:
            self._compare(kline, strict)
        self._cleanup()

    def record_trade(
        self,
        *,
        symbol: str,
        open_time_ms: int,
        close_time_ms: int,
        price: Decimal,
        quantity: Decimal,
        trade_time_ms: int,
        event_time_ms: int,
    ) -> None:
        self._remember_close(close_time_ms)
        bucket = self._live_bars_by_close.setdefault(close_time_ms, {})
        bar = bucket.get(symbol)
        if bar is None:
            bucket[symbol] = _AggTradeBar.from_trade(
                symbol=symbol,
                interval=self._interval,
                open_time_ms=open_time_ms,
                close_time_ms=close_time_ms,
                price=price,
                quantity=quantity,
                trade_time_ms=trade_time_ms,
                event_time_ms=event_time_ms,
            )
        else:
            bar.update(price, quantity, trade_time_ms, event_time_ms)
        self._cleanup()

    async def record_kline(self, kline: ClosedKline) -> None:
        self._remember_close(kline.close_time_ms)
        self._kline_by_close.setdefault(kline.close_time_ms, {})[kline.symbol] = kline
        agg = self._agg_by_close.get(kline.close_time_ms, {}).get(kline.symbol)
        if agg is not None:
            self._compare(agg, kline)
        self._cleanup()

    async def record_reference_missing(self, close_time_ms: int, symbol: str) -> None:
        self._remember_close(close_time_ms)
        self._missing_reference_by_close.setdefault(close_time_ms, set()).add(symbol)

    async def record_reference_error(self, close_time_ms: int, symbol: str, error: str) -> None:
        self._remember_close(close_time_ms)
        self._reference_errors_by_close.setdefault(close_time_ms, []).append(f"{symbol}({error})")

    async def finish_close(
        self,
        close_time_ms: int,
        *,
        validation_delay_seconds: float = 0.0,
        validation_fetch_elapsed_ms: int = 0,
        validation_total_elapsed_ms: int = 0,
    ) -> None:
        await self._maybe_log_summary(
            close_time_ms,
            force=True,
            validation_delay_seconds=validation_delay_seconds,
            validation_fetch_elapsed_ms=validation_fetch_elapsed_ms,
            validation_total_elapsed_ms=validation_total_elapsed_ms,
        )
        self._cleanup()

    def snapshot_agg(
        self,
        close_time_ms: int,
        symbols: tuple[str, ...],
        *,
        snapshot_ms: int,
    ) -> dict[str, ClosedKline]:
        return self._snapshot_agg_bucket(close_time_ms, symbols, snapshot_ms=snapshot_ms)

    def _snapshot_agg_bucket(
        self,
        close_time_ms: int,
        symbols: tuple[str, ...] | list[str],
        *,
        snapshot_ms: int | None = None,
    ) -> dict[str, ClosedKline]:
        now_ms = snapshot_ms if snapshot_ms is not None else int(time.time() * 1000)
        live_bucket = self._live_bars_by_close.get(close_time_ms, {})
        emitted_bucket = self._agg_by_close.get(close_time_ms, {})
        out: dict[str, ClosedKline] = {}
        for symbol in symbols:
            bar = live_bucket.get(symbol)
            if bar is not None:
                out[symbol] = bar.to_closed_kline(
                    ingest_time_ms=now_ms,
                    shard_id="aggtrade:diagnostic",
                    shard_symbol_count=len(symbols),
                    total_shards=0,
                )
                continue
            emitted = emitted_bucket.get(symbol)
            if emitted is not None:
                out[symbol] = emitted
        return out

    async def finish_compare_request(
        self,
        request: _AggTradeRestCompareRequest,
        *,
        local_snapshot: dict[str, ClosedKline],
        rest_snapshot: dict[str, ClosedKline],
        rest_missing: set[str],
        rest_errors: list[str],
        validation_delay_seconds: float = 0.0,
        validation_fetch_elapsed_ms: int = 0,
        validation_total_elapsed_ms: int = 0,
        snapshot_started_ms: int = 0,
    ) -> None:
        summary = self._build_compare_summary(
            request,
            local_snapshot=local_snapshot,
            rest_snapshot=rest_snapshot,
            rest_missing=rest_missing,
            rest_errors=rest_errors,
            validation_delay_seconds=validation_delay_seconds,
            validation_fetch_elapsed_ms=validation_fetch_elapsed_ms,
            validation_total_elapsed_ms=validation_total_elapsed_ms,
            snapshot_started_ms=snapshot_started_ms,
        )
        if request.final:
            self._summary_done.add(request.close_time_ms)
        if self._summary_callback is not None:
            try:
                await self._summary_callback(summary)
            except Exception:
                logger.exception(
                    "aggtrade.rest_compare.summary_callback_failed interval=%s close_time_ms=%s snapshot=%s",
                    self._interval,
                    request.close_time_ms,
                    request.snapshot_label,
                )
        self._cleanup()

    def _build_compare_summary(
        self,
        request: _AggTradeRestCompareRequest,
        *,
        local_snapshot: dict[str, ClosedKline],
        rest_snapshot: dict[str, ClosedKline],
        rest_missing: set[str],
        rest_errors: list[str],
        validation_delay_seconds: float,
        validation_fetch_elapsed_ms: int,
        validation_total_elapsed_ms: int,
        snapshot_started_ms: int,
    ) -> dict[str, int | float | str | bool]:
        target_symbols = set(request.symbols)
        strict_symbols = set(rest_snapshot.keys())
        agg_symbols = set(local_snapshot.keys())
        missing_agg = sorted(target_symbols - agg_symbols)
        missing_reference = sorted((target_symbols - strict_symbols) | rest_missing)
        mismatches: list[str] = []
        mismatch_details: list[dict[str, object]] = []
        matched = 0
        for symbol in sorted(target_symbols & agg_symbols & strict_symbols):
            agg = local_snapshot[symbol]
            strict = rest_snapshot[symbol]
            diffs: list[str] = []
            detail_fields: list[dict[str, object]] = []
            self._append_decimal_diff(
                diffs, detail_fields, "open", "O", agg.open_price, strict.open_price, self._price_tolerance
            )
            self._append_decimal_diff(
                diffs, detail_fields, "high", "H", agg.high_price, strict.high_price, self._price_tolerance
            )
            self._append_decimal_diff(
                diffs, detail_fields, "low", "L", agg.low_price, strict.low_price, self._price_tolerance
            )
            self._append_decimal_diff(
                diffs, detail_fields, "close", "C", agg.close_price, strict.close_price, self._price_tolerance
            )
            self._append_decimal_diff(
                diffs, detail_fields, "volume", "V", agg.volume, strict.volume, self._volume_tolerance
            )
            self._append_decimal_diff(
                diffs, detail_fields, "quote", "Q", agg.quote_volume, strict.quote_volume, self._quote_tolerance
            )
            if diffs:
                mismatches.append(f"{symbol}({','.join(diffs)})")
                mismatch_details.append({"symbol": symbol, "fields": detail_fields})
            else:
                matched += 1
        mismatch_error_stats = self._format_mismatch_error_stats(mismatch_details)
        mismatch_error_buckets = self._format_mismatch_error_buckets(mismatch_details)
        mismatch_detail_all = self._format_mismatch_detail_all(mismatch_details)
        price_mismatch_symbols, vq_gt_1pct_symbols = self._business_mismatch_counts(mismatch_details)
        expected_count = len(target_symbols)
        summary: dict[str, int | float | str | bool] = {
            "interval": self._interval,
            "close_time_ms": request.close_time_ms,
            "expected": expected_count,
            "rest_kline": len(rest_snapshot),
            "aggtrade": len(local_snapshot),
            "matched": matched,
            "mismatched": len(mismatches),
            "missing_agg": len(missing_agg),
            "missing_rest": len(missing_reference),
            "rest_errors": len(rest_errors),
            "extra_agg": 0,
            "missing_agg_sample": ",".join(missing_agg[:20]) if missing_agg else "-",
            "missing_rest_sample": ",".join(missing_reference[:20]) if missing_reference else "-",
            "mismatch_sample": "；".join(mismatches[:10]) if mismatches else "-",
            "mismatch_error_stats": mismatch_error_stats,
            "mismatch_error_buckets": mismatch_error_buckets,
            "mismatch_detail_all": mismatch_detail_all,
            "rest_error_sample": "；".join(rest_errors[:10]) if rest_errors else "-",
            "extra_sample": "-",
            "validation_delay_seconds": validation_delay_seconds,
            "validation_fetch_elapsed_ms": validation_fetch_elapsed_ms,
            "validation_total_elapsed_ms": validation_total_elapsed_ms,
            "snapshot_label": request.snapshot_label,
            "snapshot_offset_ms": request.snapshot_offset_ms,
            "snapshot_started_ms": snapshot_started_ms,
            "diagnostic_sample": request.sample,
            "diagnostic_final": request.final,
            "sample_size": expected_count if request.sample else 0,
            "price_mismatch_symbols": price_mismatch_symbols,
            "vq_gt_1pct_symbols": vq_gt_1pct_symbols,
            "business_passed": (
                price_mismatch_symbols == 0
                and vq_gt_1pct_symbols == 0
                and not missing_agg
                and not missing_reference
                and not rest_errors
            ),
            "passed": (
                expected_count > 0
                and len(local_snapshot) >= expected_count
                and len(rest_snapshot) >= expected_count
                and matched >= expected_count
                and not mismatches
                and not missing_agg
                and not missing_reference
                and not rest_errors
            ),
        }
        logger.info(
            "aggtrade.rest_compare.summary interval=%s close_time_ms=%s snapshot=%s expected=%s rest_kline=%s aggtrade=%s matched=%s mismatched=%s missing_agg=%s missing_rest=%s rest_errors=%s price_mismatch_symbols=%s vq_gt_1pct_symbols=%s",
            summary["interval"],
            summary["close_time_ms"],
            summary["snapshot_label"],
            summary["expected"],
            summary["rest_kline"],
            summary["aggtrade"],
            summary["matched"],
            summary["mismatched"],
            summary["missing_agg"],
            summary["missing_rest"],
            summary["rest_errors"],
            summary["price_mismatch_symbols"],
            summary["vq_gt_1pct_symbols"],
        )
        return summary

    def _business_mismatch_counts(self, mismatch_details: list[dict[str, object]]) -> tuple[int, int]:
        price_symbols: set[str] = set()
        vq_gt_1pct_symbols: set[str] = set()
        for item in mismatch_details:
            symbol = str(item.get("symbol") or "")
            for field in self._iter_detail_fields(item):
                label = str(field.get("label") or "?")
                pct_error = field.get("pct_error")
                if label in {"O", "H", "L", "C"}:
                    price_symbols.add(symbol)
                elif label in {"V", "Q"} and isinstance(pct_error, Decimal) and pct_error > Decimal("1"):
                    vq_gt_1pct_symbols.add(symbol)
        return len(price_symbols), len(vq_gt_1pct_symbols)

    def _remember_close(self, close_time_ms: int) -> None:
        if (
            close_time_ms in self._live_bars_by_close
            or close_time_ms in self._agg_by_close
            or close_time_ms in self._kline_by_close
            or close_time_ms in self._close_order
        ):
            return
        self._close_order.append(close_time_ms)

    def _compare(self, agg: ClosedKline, strict: ClosedKline) -> None:
        key = (strict.close_time_ms, strict.symbol)
        if key in self._compared:
            return
        self._compared.add(key)
        diffs: list[str] = []
        detail_fields: list[dict[str, object]] = []
        self._append_decimal_diff(
            diffs,
            detail_fields,
            "open",
            "O",
            agg.open_price,
            strict.open_price,
            self._price_tolerance,
        )
        self._append_decimal_diff(
            diffs,
            detail_fields,
            "high",
            "H",
            agg.high_price,
            strict.high_price,
            self._price_tolerance,
        )
        self._append_decimal_diff(
            diffs,
            detail_fields,
            "low",
            "L",
            agg.low_price,
            strict.low_price,
            self._price_tolerance,
        )
        self._append_decimal_diff(
            diffs,
            detail_fields,
            "close",
            "C",
            agg.close_price,
            strict.close_price,
            self._price_tolerance,
        )
        self._append_decimal_diff(
            diffs,
            detail_fields,
            "volume",
            "V",
            agg.volume,
            strict.volume,
            self._volume_tolerance,
        )
        self._append_decimal_diff(
            diffs,
            detail_fields,
            "quote",
            "Q",
            agg.quote_volume,
            strict.quote_volume,
            self._quote_tolerance,
        )
        if diffs:
            mismatches = self._mismatch_by_close.setdefault(strict.close_time_ms, [])
            mismatches.append(f"{strict.symbol}({','.join(diffs)})")
            self._mismatch_details_by_close.setdefault(strict.close_time_ms, []).append(
                {
                    "symbol": strict.symbol,
                    "fields": detail_fields,
                },
            )
            mismatch_count = len(mismatches)
            if mismatch_count <= self._mismatch_log_limit_per_close:
                logger.warning(
                    "aggtrade.rest_compare.mismatch interval=%s close_time_ms=%s symbol=%s diffs=%s",
                    self._interval,
                    strict.close_time_ms,
                    strict.symbol,
                    ",".join(diffs),
                )
            elif mismatch_count == self._mismatch_log_limit_per_close + 1:
                logger.warning(
                    "aggtrade.rest_compare.mismatch_suppressed interval=%s close_time_ms=%s limit=%s",
                    self._interval,
                    strict.close_time_ms,
                    self._mismatch_log_limit_per_close,
                )
            return
        self._matched_by_close[strict.close_time_ms] = self._matched_by_close.get(strict.close_time_ms, 0) + 1

    def _append_decimal_diff(
        self,
        diffs: list[str],
        detail_fields: list[dict[str, object]],
        name: str,
        label: str,
        agg_value: Decimal,
        strict_value: Decimal,
        tolerance: Decimal,
    ) -> None:
        abs_error = abs(agg_value - strict_value)
        if abs_error <= tolerance:
            return
        pct_error = self._percent_error(abs_error, strict_value)
        diffs.append(f"{name}:agg={agg_value},rest={strict_value}")
        detail_fields.append(
            {
                "name": name,
                "label": label,
                "agg": agg_value,
                "rest": strict_value,
                "abs_error": abs_error,
                "pct_error": pct_error,
            },
        )

    def _percent_error(self, abs_error: Decimal, strict_value: Decimal) -> Decimal:
        denominator = abs(strict_value)
        if denominator == 0:
            return Decimal("0") if abs_error == 0 else Decimal("Infinity")
        return (abs_error / denominator) * Decimal("100")

    async def request_rest_compare(self, close_time_ms: int, symbols: set[str]) -> bool:
        if self._rest_compare_queue is None:
            return False
        if close_time_ms in self._summary_done:
            return False
        live_bucket = self._live_bars_by_close.get(close_time_ms, {})
        agg_bucket = self._agg_by_close.get(close_time_ms, {})
        target_symbols = {
            symbol
            for symbol in symbols
            if symbol in self._expected_symbols and (symbol in live_bucket or symbol in agg_bucket)
        }
        if not target_symbols:
            return False
        sample = next((agg_bucket.get(symbol) for symbol in sorted(target_symbols)), None)
        if sample is None:
            sample = self._snapshot_agg_bucket(close_time_ms, sorted(target_symbols)).get(
                next(iter(sorted(target_symbols)))
            )
        if sample is None:
            return False
        if sample.open_time_ms < self._min_validate_open_time_ms:
            await self._skip_rest_compare(
                close_time_ms,
                target_symbols=target_symbols,
                open_time_ms=sample.open_time_ms,
                reason=self._min_validate_reason,
            )
            return True
        self._target_symbols_by_close[close_time_ms] = target_symbols
        requests = self._build_rest_compare_requests(close_time_ms, target_symbols)
        queued = 0
        for request in requests:
            queue_key = (close_time_ms, request.snapshot_label)
            if queue_key in self._queued_rest_compare:
                continue
            try:
                self._rest_compare_queue.put_nowait(request)
            except asyncio.QueueFull:
                logger.warning(
                    "aggtrade.rest_compare.queue_full interval=%s close_time_ms=%s queued=%s",
                    self._interval,
                    close_time_ms,
                    len(self._queued_rest_compare),
                )
                break
            self._queued_rest_compare.add(queue_key)
            queued += 1
        if queued <= 0:
            return False
        logger.info(
            "aggtrade.rest_compare.queued interval=%s close_time_ms=%s symbols=%s requests=%s",
            self._interval,
            close_time_ms,
            len(target_symbols),
            queued,
        )
        return True

    def _build_rest_compare_requests(
        self,
        close_time_ms: int,
        target_symbols: set[str],
    ) -> list[_AggTradeRestCompareRequest]:
        close_boundary_ms = close_time_ms + 1
        sorted_symbols = tuple(sorted(target_symbols))
        requests: list[_AggTradeRestCompareRequest] = []
        if self._diagnostic_enabled:
            sample_size = min(self._diagnostic_sample_size, len(sorted_symbols))
            sample_symbols = tuple(
                sorted(random.Random(close_time_ms).sample(list(sorted_symbols), sample_size))
            )
            for offset_ms in self._diagnostic_offsets_ms:
                requests.append(
                    _AggTradeRestCompareRequest(
                        close_time_ms=close_time_ms,
                        symbols=sample_symbols,
                        snapshot_offset_ms=offset_ms,
                        snapshot_due_ms=close_boundary_ms + offset_ms,
                        snapshot_label=f"{offset_ms}ms",
                        sample=True,
                        final=False,
                    )
                )
        final_label = f"{self._final_compare_delay_ms}ms"
        requests.append(
            _AggTradeRestCompareRequest(
                close_time_ms=close_time_ms,
                symbols=sorted_symbols,
                snapshot_offset_ms=self._final_compare_delay_ms,
                snapshot_due_ms=close_boundary_ms + self._final_compare_delay_ms,
                snapshot_label=final_label,
                sample=False,
                final=True,
            )
        )
        return requests

    async def _skip_rest_compare(
        self,
        close_time_ms: int,
        *,
        target_symbols: set[str],
        open_time_ms: int,
        reason: str,
    ) -> None:
        if close_time_ms in self._summary_done:
            return
        expected_count = len(target_symbols)
        summary: dict[str, int | float | str | bool] = {
            "interval": self._interval,
            "close_time_ms": close_time_ms,
            "expected": expected_count,
            "rest_kline": 0,
            "aggtrade": expected_count,
            "matched": 0,
            "mismatched": 0,
            "missing_agg": 0,
            "missing_rest": 0,
            "rest_errors": 0,
            "extra_agg": 0,
            "missing_agg_sample": "-",
            "missing_rest_sample": "-",
            "mismatch_sample": "-",
            "mismatch_error_stats": "-",
            "mismatch_error_buckets": "-",
            "mismatch_detail_all": "-",
            "rest_error_sample": "-",
            "extra_sample": "-",
            "validation_delay_seconds": 0.0,
            "validation_fetch_elapsed_ms": 0,
            "validation_total_elapsed_ms": 0,
            "open_time_ms": open_time_ms,
            "min_validate_open_time_ms": self._min_validate_open_time_ms,
            "skip_reason": reason,
            "skipped": True,
            "passed": False,
        }
        self._summary_done.add(close_time_ms)
        logger.info(
            "aggtrade.rest_compare.skipped interval=%s close_time_ms=%s reason=%s open_time_ms=%s min_validate_open_time_ms=%s symbols=%s expected=%s",
            self._interval,
            close_time_ms,
            reason,
            open_time_ms,
            self._min_validate_open_time_ms,
            expected_count,
            expected_count,
        )
        if self._summary_callback is not None:
            try:
                await self._summary_callback(summary)
            except Exception:
                logger.exception(
                    "aggtrade.rest_compare.summary_callback_failed interval=%s close_time_ms=%s",
                    self._interval,
                    close_time_ms,
                )

    async def _maybe_log_summary(
        self,
        close_time_ms: int,
        *,
        force: bool = False,
        validation_delay_seconds: float = 0.0,
        validation_fetch_elapsed_ms: int = 0,
        validation_total_elapsed_ms: int = 0,
    ) -> None:
        if close_time_ms in self._summary_done:
            return
        target_symbols = self._target_symbols_by_close.get(close_time_ms)
        if target_symbols is None:
            target_symbols = set(self._expected_symbols)
        strict_bucket_all = self._kline_by_close.get(close_time_ms, {})
        agg_bucket_all = self._agg_by_close.get(close_time_ms, {})
        strict_bucket = {
            symbol: kline for symbol, kline in strict_bucket_all.items() if symbol in target_symbols
        }
        agg_bucket = {
            symbol: kline for symbol, kline in agg_bucket_all.items() if symbol in target_symbols
        }
        expected_count = len(target_symbols)
        if not force and expected_count > 0 and len(strict_bucket) < expected_count:
            return

        strict_symbols = set(strict_bucket.keys())
        agg_symbols = set(agg_bucket.keys())
        missing_agg = sorted(target_symbols - agg_symbols)
        missing_reference = sorted(target_symbols - strict_symbols)
        extra_agg: list[str] = []
        mismatches = self._mismatch_by_close.get(close_time_ms, [])
        mismatch_details = self._mismatch_details_by_close.get(close_time_ms, [])
        matched = self._matched_by_close.get(close_time_ms, 0)
        reference_errors = self._reference_errors_by_close.get(close_time_ms, [])
        missing_agg_sample = ",".join(missing_agg[:20]) if missing_agg else "-"
        missing_reference_sample = ",".join(missing_reference[:20]) if missing_reference else "-"
        mismatch_sample = "；".join(mismatches[:10]) if mismatches else "-"
        reference_error_sample = "；".join(reference_errors[:10]) if reference_errors else "-"
        extra_sample = ",".join(extra_agg[:20]) if extra_agg else "-"
        mismatch_error_stats = self._format_mismatch_error_stats(mismatch_details)
        mismatch_error_buckets = self._format_mismatch_error_buckets(mismatch_details)
        mismatch_detail_all = self._format_mismatch_detail_all(mismatch_details)
        summary: dict[str, int | float | str | bool] = {
            "interval": self._interval,
            "close_time_ms": close_time_ms,
            "expected": expected_count,
            "rest_kline": len(strict_bucket),
            "aggtrade": len(agg_bucket),
            "matched": matched,
            "mismatched": len(mismatches),
            "missing_agg": len(missing_agg),
            "missing_rest": len(missing_reference),
            "rest_errors": len(reference_errors),
            "extra_agg": len(extra_agg),
            "missing_agg_sample": missing_agg_sample,
            "missing_rest_sample": missing_reference_sample,
            "mismatch_sample": mismatch_sample,
            "mismatch_error_stats": mismatch_error_stats,
            "mismatch_error_buckets": mismatch_error_buckets,
            "mismatch_detail_all": mismatch_detail_all,
            "rest_error_sample": reference_error_sample,
            "extra_sample": extra_sample,
            "validation_delay_seconds": validation_delay_seconds,
            "validation_fetch_elapsed_ms": validation_fetch_elapsed_ms,
            "validation_total_elapsed_ms": validation_total_elapsed_ms,
            "passed": (
                expected_count > 0
                and len(agg_bucket) >= expected_count
                and len(strict_bucket) >= expected_count
                and matched >= expected_count
                and not mismatches
                and not missing_agg
                and not missing_reference
                and not reference_errors
                and not extra_agg
            ),
        }
        logger.info(
            "aggtrade.rest_compare.summary interval=%s close_time_ms=%s expected=%s rest_kline=%s aggtrade=%s matched=%s mismatched=%s missing_agg=%s missing_rest=%s rest_errors=%s extra_agg=%s missing_agg_sample=%s missing_rest_sample=%s mismatch_sample=%s rest_error_sample=%s extra_sample=%s",
            summary["interval"],
            summary["close_time_ms"],
            summary["expected"],
            summary["rest_kline"],
            summary["aggtrade"],
            summary["matched"],
            summary["mismatched"],
            summary["missing_agg"],
            summary["missing_rest"],
            summary["rest_errors"],
            summary["extra_agg"],
            summary["missing_agg_sample"],
            summary["missing_rest_sample"],
            summary["mismatch_sample"],
            summary["rest_error_sample"],
            summary["extra_sample"],
        )
        self._summary_done.add(close_time_ms)
        if self._summary_callback is not None:
            try:
                await self._summary_callback(summary)
            except Exception:
                logger.exception(
                    "aggtrade.rest_compare.summary_callback_failed interval=%s close_time_ms=%s",
                    self._interval,
                    close_time_ms,
                )

    def _cleanup(self) -> None:
        while len(self._close_order) > self._retain_close_times:
            expired = self._close_order.popleft()
            self._live_bars_by_close.pop(expired, None)
            self._agg_by_close.pop(expired, None)
            self._kline_by_close.pop(expired, None)
            self._missing_reference_by_close.pop(expired, None)
            self._reference_errors_by_close.pop(expired, None)
            self._matched_by_close.pop(expired, None)
            self._mismatch_by_close.pop(expired, None)
            self._mismatch_details_by_close.pop(expired, None)
            self._target_symbols_by_close.pop(expired, None)
            self._queued_rest_compare = {key for key in self._queued_rest_compare if key[0] != expired}
            self._summary_done.discard(expired)
            self._compared = {key for key in self._compared if key[0] != expired}

    def _format_mismatch_error_stats(self, mismatch_details: list[dict[str, object]]) -> str:
        field_values: dict[str, dict[str, list[Decimal]]] = {}
        for item in mismatch_details:
            for field in self._iter_detail_fields(item):
                label = str(field.get("label") or "?")
                bucket = field_values.setdefault(label, {"abs": [], "pct": []})
                abs_error = field.get("abs_error")
                pct_error = field.get("pct_error")
                if isinstance(abs_error, Decimal):
                    bucket["abs"].append(abs_error)
                if isinstance(pct_error, Decimal) and pct_error.is_finite():
                    bucket["pct"].append(pct_error)
        if not field_values:
            return "-"
        parts: list[str] = []
        for label in ("O", "H", "L", "C", "V", "Q"):
            values = field_values.get(label)
            if not values:
                continue
            abs_values = values["abs"]
            pct_values = values["pct"]
            parts.append(
                f"{label}:n={len(abs_values)} "
                f"abs_avg={self._decimal_mean(abs_values)} "
                f"abs_max={self._decimal_max(abs_values)} "
                f"abs_var={self._decimal_variance(abs_values)} "
                f"pct_avg={self._decimal_mean(pct_values)}% "
                f"pct_p95={self._decimal_percentile(pct_values, 95)}% "
                f"pct_max={self._decimal_max(pct_values)}% "
                f"pct_var={self._decimal_variance(pct_values)}"
            )
        return "；".join(parts) if parts else "-"

    def _format_mismatch_error_buckets(self, mismatch_details: list[dict[str, object]]) -> str:
        labels = ("O", "H", "L", "C", "V", "Q")
        buckets: dict[str, list[int]] = {label: [0, 0, 0, 0] for label in labels}
        for item in mismatch_details:
            for field in self._iter_detail_fields(item):
                label = str(field.get("label") or "?")
                if label not in buckets:
                    continue
                pct_error = field.get("pct_error")
                if not isinstance(pct_error, Decimal) or not pct_error.is_finite():
                    buckets[label][3] += 1
                elif pct_error <= Decimal("0.01"):
                    buckets[label][0] += 1
                elif pct_error <= Decimal("0.1"):
                    buckets[label][1] += 1
                elif pct_error <= Decimal("1"):
                    buckets[label][2] += 1
                else:
                    buckets[label][3] += 1
        parts = [
            f"{label}:<=0.01%/{counts[0]} <=0.1%/{counts[1]} <=1%/{counts[2]} >1%/{counts[3]}"
            for label, counts in buckets.items()
            if sum(counts) > 0
        ]
        return "；".join(parts) if parts else "-"

    def _format_mismatch_detail_all(self, mismatch_details: list[dict[str, object]]) -> str:
        if not mismatch_details:
            return "-"
        parts: list[str] = []
        for item in sorted(mismatch_details, key=lambda x: str(x.get("symbol") or "")):
            symbol = str(item.get("symbol") or "")
            field_parts: list[str] = []
            for field in self._iter_detail_fields(item):
                label = str(field.get("label") or "?")
                agg = field.get("agg")
                rest = field.get("rest")
                abs_error = field.get("abs_error")
                pct_error = field.get("pct_error")
                field_parts.append(
                    f"{label}={self._decimal_short(agg)}->{self._decimal_short(rest)} "
                    f"abs={self._decimal_short(abs_error)} "
                    f"pct={self._decimal_short(pct_error)}%"
                )
            text = f"{symbol}[{';'.join(field_parts)}]"
            parts.append(text)
        return "；".join(parts)

    def _iter_detail_fields(self, item: dict[str, object]) -> list[dict[str, object]]:
        fields = item.get("fields")
        if not isinstance(fields, list):
            return []
        return [field for field in fields if isinstance(field, dict)]

    def _decimal_short(self, value: object) -> str:
        if not isinstance(value, Decimal):
            return str(value)
        if not value.is_finite():
            return str(value)
        normalized = value.normalize()
        text = format(normalized, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    def _decimal_mean(self, values: list[Decimal]) -> str:
        if not values:
            return "0"
        return self._decimal_short(sum(values) / Decimal(len(values)))

    def _decimal_max(self, values: list[Decimal]) -> str:
        if not values:
            return "0"
        return self._decimal_short(max(values))

    def _decimal_variance(self, values: list[Decimal]) -> str:
        if not values:
            return "0"
        mean = sum(values) / Decimal(len(values))
        variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values))
        return self._decimal_short(variance)

    def _decimal_percentile(self, values: list[Decimal], pct: int) -> str:
        if not values:
            return "0"
        values_sorted = sorted(values)
        idx = max(0, min(len(values_sorted) - 1, math.ceil((pct / 100) * len(values_sorted)) - 1))
        return self._decimal_short(values_sorted[idx])


def _no_proxy_matches_host(host: str, no_proxy: str) -> bool:
    host = host.lower().strip(".")
    for raw in no_proxy.split(","):
        pat = raw.strip().lower()
        if not pat:
            continue
        if pat == "*":
            return True
        if pat.startswith("."):
            pat = pat[1:]
        if host == pat or host.endswith("." + pat):
            return True
    return False


def _ws_proxy_from_env(dest_host: str) -> str | None:
    no_proxy = (os.getenv("NO_PROXY") or os.getenv("no_proxy") or "").strip()
    if no_proxy and _no_proxy_matches_host(dest_host, no_proxy):
        return None
    return (
        (os.getenv("BINANCE_HTTP_PROXY") or "").strip()
        or (os.getenv("HTTPS_PROXY") or "").strip()
        or (os.getenv("HTTP_PROXY") or "").strip()
        or None
    )


def _kline_from_rest_row(symbol: str, interval: str, row: list) -> ClosedKline:
    ingest_ms = int(time.time() * 1000)
    open_t = int(row[0])
    close_t = int(row[6])
    return ClosedKline(
        symbol=symbol,
        interval=interval,
        open_time_ms=open_t,
        close_time_ms=close_t,
        open_price=Decimal(row[1]),
        high_price=Decimal(row[2]),
        low_price=Decimal(row[3]),
        close_price=Decimal(row[4]),
        volume=Decimal(row[5]),
        quote_volume=Decimal(row[7]),
        event_time_ms=close_t,
        ingest_time_ms=ingest_ms,
        source="rest",
        ws_shard_id="",
        ws_shard_symbol_count=0,
        ws_total_shards=0,
    )


def next_silence_repair_backoff_ms(current_ms: int, max_ms: int) -> int:
    """Exponential backoff step for WS-silence REST repair, capped at max_ms."""
    return min(max(1, current_ms) * 2, max(1, max_ms))


def watchdog_should_rebuild(
    *,
    now_ms: int,
    stale_ms: int,
    stale_threshold_ms: int,
    alerted: bool,
    last_rebuild_ms: int,
) -> bool:
    """Decide whether the watchdog should (re)trigger a shard rebuild.

    A shard is rebuilt once it is stale. If it is still stale after another
    full stale threshold since the last rebuild (i.e. the rebuild did not bring
    data back), the watchdog re-arms and triggers again.
    """
    if stale_ms < stale_threshold_ms:
        return False
    if not alerted:
        return True
    return now_ms - last_rebuild_ms >= stale_threshold_ms


class BinanceClient:
    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg
        self._ws_rebuild_generation: dict[str, int] = {}
        self._ws_last_event_ms: dict[str, int] = {}
        self._ws_watchdog_alerted: dict[str, bool] = {}
        self._ws_shard_last_event_ms: dict[str, dict[str, int]] = {}
        self._ws_shard_watchdog_alerted: dict[str, dict[str, bool]] = {}
        self._aggtrade_comparators: dict[str, _AggTradeKlineComparator] = {}
        self._aggtrade_published_close_ms: dict[str, int] = {}
        # Debug: print first kline payload structure per interval/shard.
        self._ws_parse_debug_logged: set[str] = set()

    def mark_aggtrade_close_published(self, interval: str, close_time_ms: int) -> None:
        if close_time_ms <= 0:
            return
        self._aggtrade_published_close_ms[interval] = max(
            close_time_ms,
            self._aggtrade_published_close_ms.get(interval, 0),
        )

    async def request_aggtrade_rest_compare(
        self,
        interval: str,
        close_time_ms: int,
        symbols: set[str],
    ) -> bool:
        comparator = self._aggtrade_comparators.get(interval)
        if comparator is None:
            return False
        return await comparator.request_rest_compare(close_time_ms, symbols)

    def _build_ws_proxy_sock_socks5(
        self,
        proxy_url: str,
        dest_host: str,
        dest_port: int,
    ) -> socket.socket:
        """
        Build a pre-connected socket using SOCKS5 proxy.

        websockets.connect() doesn't implement HTTP/SOCKS proxy itself, but it
        forwards extra kwargs to asyncio.loop.create_connection(), which
        accepts `sock=...`. So we create a TCP socket to the destination via
        the proxy and pass it in.
        """
        parsed = urlparse(proxy_url)
        proxy_host = parsed.hostname
        proxy_port = parsed.port
        if not proxy_host or not proxy_port:
            raise ValueError(f"Invalid ws_proxy_url={proxy_url!r}")

        scheme = (parsed.scheme or "").lower()
        if not scheme.startswith("socks5"):
            raise ValueError(f"Unsupported ws_proxy_url scheme={parsed.scheme!r}, url={proxy_url!r}")

        rdns = scheme == "socks5h"
        username = parsed.username
        password = parsed.password

        sock: socket.socket = socks.socksocket()
        # PySocks uses rdns to decide whether hostname resolution is done remotely.
        sock.set_proxy(
            socks.SOCKS5,
            proxy_host,
            proxy_port,
            username=username,
            password=password,
            rdns=rdns,
        )
        sock.settimeout(20.0)
        sock.connect((dest_host, dest_port))
        sock.settimeout(None)
        sock.setblocking(False)
        return sock

    def _build_ws_proxy_sock_http_connect(
        self,
        proxy_url: str,
        dest_host: str,
        dest_port: int,
    ) -> socket.socket:
        """
        Build a pre-connected socket via an HTTP proxy using CONNECT.

        websockets.connect() will pass unknown kwargs to asyncio's
        loop.create_connection(). If we provide `sock=...`, it will use the
        already-connected socket and perform the TLS handshake (for wss://)
        on top of it.
        """
        parsed = urlparse(proxy_url)
        proxy_host = parsed.hostname
        proxy_port = parsed.port
        if not proxy_host:
            raise ValueError(f"Invalid ws_proxy_url={proxy_url!r}")

        scheme = (parsed.scheme or "").lower()
        if not scheme.startswith("http"):
            raise ValueError(f"Unsupported ws_proxy_url scheme={parsed.scheme!r}, url={proxy_url!r}")

        proxy_port = proxy_port or 80
        username = parsed.username
        password = parsed.password or ""

        headers: dict[str, str] = {}
        if username is not None:
            creds = f"{username}:{password}".encode("utf-8")
            b64 = base64.b64encode(creds).decode("ascii")
            headers["Proxy-Authorization"] = f"Basic {b64}"

        # NOTE: http.client's tunnel support will read until the end of the
        # proxy response headers and leave the tunnel socket open.
        conn = http.client.HTTPConnection(proxy_host, proxy_port, timeout=20.0)
        conn.set_tunnel(dest_host, dest_port, headers=headers or None)
        conn.connect()
        sock = conn.sock
        # Detach so conn.__del__ doesn't close the socket.
        conn.sock = None
        if sock is None:
            raise RuntimeError(f"HTTP proxy tunnel failed for proxy={proxy_url!r}")

        sock.setblocking(False)
        return sock

    async def fetch_symbols(self) -> list[str]:
        if self._cfg.binance.use_test_symbols and self._cfg.binance.test_symbols:
            return sorted(set(self._cfg.binance.test_symbols))

        url = f"{self._cfg.binance.rest_base}/fapi/v1/exchangeInfo"
        async with httpx.AsyncClient(timeout=10.0, proxy=self._cfg.binance.rest_proxy_url) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        symbols: list[str] = []
        for item in data.get("symbols", []):
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
            if item.get("status") != "TRADING":
                continue
            sym = item.get("symbol")
            if sym:
                symbols.append(sym)
        return sorted(set(symbols))

    async def fetch_closed_klines_rest(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        interval: str,
        limit: int,
    ) -> list[ClosedKline]:
        cap = min(limit, self._cfg.backfill.klines_limit_cap)
        url = f"{self._cfg.binance.rest_base}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": cap}
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        raw_rows = resp.json()
        if not isinstance(raw_rows, list):
            return []
        out: list[ClosedKline] = []
        now_ms = int(time.time() * 1000)
        for row in raw_rows:
            if not isinstance(row, list) or len(row) < 8:
                continue
            # REST may include the currently forming bar; only keep closed bars.
            if int(row[6]) > now_ms:
                continue
            out.append(_kline_from_rest_row(symbol, interval, row))
        return out

    async def fetch_exact_closed_kline_rest(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        interval: str,
        close_time_ms: int,
    ) -> ClosedKline | None:
        # Query with endTime to target the expected closed candle window.
        url = f"{self._cfg.binance.rest_base}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": 3,
            "endTime": close_time_ms,
        }
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        raw_rows = resp.json()
        if not isinstance(raw_rows, list):
            return None
        for row in reversed(raw_rows):
            if not isinstance(row, list) or len(row) < 8:
                continue
            if int(row[6]) != close_time_ms:
                continue
            return _kline_from_rest_row(symbol, interval, row)
        return None

    async def _backfill_batch(
        self,
        symbols: list[str],
        interval: str,
        out_queue: asyncio.Queue[ClosedKline],
        limit_override: int | None = None,
    ) -> None:
        if not self._cfg.backfill.enabled:
            return

        limit = self._cfg.backfill.bars_1m if interval == "1m" else self._cfg.backfill.bars_1h
        if interval not in ("1m", "1h"):
            limit = self._cfg.backfill.bars_1m
        if limit_override is not None:
            limit = max(1, limit_override)

        sem = asyncio.Semaphore(max(1, self._cfg.backfill.rest_concurrency))
        timeout = httpx.Timeout(self._cfg.backfill.rest_timeout_seconds)

        async def one(client: httpx.AsyncClient, sym: str) -> list[ClosedKline]:
            async with sem:
                if self._cfg.backfill.rest_request_delay_seconds > 0:
                    await asyncio.sleep(self._cfg.backfill.rest_request_delay_seconds)
                try:
                    rows = await self.fetch_closed_klines_rest(client, sym, interval, limit)
                    BACKFILL_KLINES.labels(interval).inc(len(rows))
                    return rows
                except Exception as exc:
                    BACKFILL_ERRORS.labels(interval).inc()
                    logger.warning(
                        "backfill.fail symbol=%s interval=%s error=%s",
                        sym,
                        interval,
                        exc,
                    )
                    return []

        async with httpx.AsyncClient(timeout=timeout, proxy=self._cfg.binance.rest_proxy_url) as http:
            results = await asyncio.gather(*[one(http, s) for s in symbols])
        merged: list[ClosedKline] = []
        for rows in results:
            merged.extend(rows)
        # Sort by bar time first so downstream batching by close_time can aggregate
        # all symbols for the same candle before considering the next candle.
        merged.sort(key=lambda k: (k.open_time_ms, k.symbol))
        for kline in merged:
            lag = max(0, kline.ingest_time_ms - kline.event_time_ms)
            EXCHANGE_TO_SERVICE_MS.labels(interval, kline.source).observe(lag)
            KLINES_RECEIVED.labels(interval, kline.source).inc()
            touch_ingest(interval)
            await out_queue.put(kline)
        logger.info("backfill.done interval=%s symbols=%s klines=%s", interval, len(symbols), len(merged))

    async def _run_ws_silence_rest_repair(
        self,
        interval: str,
        symbols: list[str],
        out_queue: asyncio.Queue[ClosedKline],
    ) -> None:
        if (
            interval == "1m"
            or not self._cfg.backfill.enabled
            or not self._cfg.websocket.klines_ws_silence_fallback_rest
        ):
            return
        silence_threshold_ms = 30_000
        interval_ms = self._interval_to_ms(interval)
        # Full-universe REST repair is expensive (one request per symbol). While WS
        # stays silent, back off exponentially from the silence threshold up to one
        # kline interval (no new closed candle can appear more often than that).
        max_backoff_ms = max(silence_threshold_ms, min(interval_ms, 15 * 60_000) if interval_ms > 0 else 15 * 60_000)
        backoff_ms = silence_threshold_ms
        next_repair_ms = 0
        while True:
            await asyncio.sleep(10)
            last_ms = self._ws_last_event_ms.get(interval, 0)
            if last_ms <= 0:
                continue
            now_ms = int(time.time() * 1000)
            no_event_ms = now_ms - last_ms
            if no_event_ms < silence_threshold_ms:
                # WS healthy again: reset the backoff.
                backoff_ms = silence_threshold_ms
                next_repair_ms = 0
                continue
            if now_ms < next_repair_ms:
                continue
            logger.warning(
                "ws.silence.rest_repair interval=%s no_event_ms=%s symbols=%s next_backoff_ms=%s",
                interval,
                no_event_ms,
                len(symbols),
                backoff_ms,
            )
            # limit=1 often returns the currently forming candle; use 2 so we can
            # still pick the previous fully closed candle.
            try:
                await self._backfill_batch(symbols, interval, out_queue, limit_override=2)
            except Exception:
                logger.exception("ws.silence.rest_repair_failed interval=%s", interval)
            next_repair_ms = int(time.time() * 1000) + backoff_ms
            backoff_ms = next_silence_repair_backoff_ms(backoff_ms, max_backoff_ms)

    async def iter_closed_klines(
        self,
        symbols: list[str],
        interval: str,
        batch_size: int = 200,
    ) -> AsyncIterator[ClosedKline]:
        # Keep the target batch size, but split further when the combined stream
        # query becomes too long. This avoids silent WS subscriptions with no data.
        symbol_batches = self._build_symbol_batches(
            symbols,
            interval,
            batch_size,
            # SUBSCRIBE mode does not need long URL query splitting.
            max_streams_query_chars=0,
        )
        shard_ids = [f"{interval}#{idx}" for idx in range(len(symbol_batches))]
        queue: asyncio.Queue[ClosedKline] = asyncio.Queue(maxsize=10000)
        tasks = [
            asyncio.create_task(self._consume_batch(batch, interval, shard_ids[idx], queue, len(shard_ids)))
            for idx, batch in enumerate(symbol_batches)
            if batch and idx < len(shard_ids)
        ]
        rest_repair_task = asyncio.create_task(
            self._run_ws_silence_rest_repair(interval, symbols, queue),
        )
        watchdog_task: asyncio.Task[None] | None = None
        if self._cfg.binance.ws_watchdog_enabled:
            self._ws_last_event_ms[interval] = int(time.time() * 1000)
            self._ws_watchdog_alerted[interval] = False
            self._ws_shard_last_event_ms[interval] = {
                shard_id: int(time.time() * 1000) for shard_id in shard_ids
            }
            self._ws_shard_watchdog_alerted[interval] = {shard_id: False for shard_id in shard_ids}
            watchdog_task = asyncio.create_task(
                self._run_ws_watchdog(interval, len(symbols), shard_ids),
            )
        try:
            while True:
                yield await queue.get()
        finally:
            for t in tasks:
                t.cancel()
            rest_repair_task.cancel()
            if watchdog_task is not None:
                watchdog_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watchdog_task
            with contextlib.suppress(asyncio.CancelledError):
                await rest_repair_task
            await asyncio.gather(*tasks, return_exceptions=True)

    async def iter_aggtrade_klines(
        self,
        symbols: list[str],
        interval: str,
        batch_size: int = 200,
        *,
        emit_grace_ms: int | None = None,
        compare_with_rest_kline: bool = True,
        rest_compare_delay_seconds: float = 2.0,
        rest_compare_concurrency: int = 8,
        rest_diagnostic_enabled: bool = True,
        rest_diagnostic_sample_size: int = 50,
        rest_diagnostic_offsets_ms: tuple[int, ...] = (800, 1800, 3000, 5000),
        price_tolerance: Decimal = Decimal("0"),
        volume_tolerance: Decimal = Decimal("0.00000001"),
        quote_tolerance: Decimal = Decimal("0.000001"),
        rest_compare_summary_callback: AggTradeCompareSummaryCallback | None = None,
    ) -> AsyncIterator[ClosedKline]:
        interval_ms = self._interval_to_ms(interval)
        if interval_ms <= 0:
            raise ValueError(f"Unsupported kline interval for aggTrade aggregation: {interval!r}")
        if emit_grace_ms is None:
            emit_grace_ms = self._cfg.websocket.aggtrade_emit_grace_ms

        symbol_batches = self._build_stream_symbol_batches(
            symbols,
            stream_suffix="@aggTrade",
            batch_size=batch_size,
            max_streams_query_chars=0,
            log_interval=f"{interval}:aggtrade",
        )
        shard_ids = [f"aggtrade:{interval}#{idx}" for idx in range(len(symbol_batches))]
        queue: asyncio.Queue[ClosedKline] = asyncio.Queue(maxsize=10000)
        now_ms = int(time.time() * 1000)
        # The current candle may be partial if the service starts mid-interval.
        # Only compare candles whose full interval was observed from aggTrade.
        min_validate_open_time_ms = ((now_ms // interval_ms) + 1) * interval_ms
        final_compare_delay_ms = int(max(0.0, rest_compare_delay_seconds) * 1000)
        rest_compare_queue: asyncio.Queue[_AggTradeRestCompareRequest] | None = (
            asyncio.Queue(maxsize=max(100, len(symbols) * 2)) if compare_with_rest_kline else None
        )
        comparator = (
            _AggTradeKlineComparator(
                interval=interval,
                interval_ms=interval_ms,
                expected_symbols=set(symbols),
                expected_shards=set(shard_ids),
                price_tolerance=price_tolerance,
                volume_tolerance=volume_tolerance,
                quote_tolerance=quote_tolerance,
                rest_compare_queue=rest_compare_queue,
                summary_callback=rest_compare_summary_callback,
                min_validate_open_time_ms=min_validate_open_time_ms,
                diagnostic_enabled=rest_diagnostic_enabled,
                diagnostic_sample_size=rest_diagnostic_sample_size,
                diagnostic_offsets_ms=rest_diagnostic_offsets_ms,
                final_compare_delay_ms=final_compare_delay_ms,
            )
            if compare_with_rest_kline
            else None
        )
        if comparator is not None:
            self._aggtrade_comparators[interval] = comparator
        tasks = [
            asyncio.create_task(
                self._consume_aggtrade_batch(
                    batch,
                    interval,
                    shard_ids[idx],
                    queue,
                    len(shard_ids),
                    interval_ms=interval_ms,
                    emit_grace_ms=emit_grace_ms,
                    comparator=comparator,
                )
            )
            for idx, batch in enumerate(symbol_batches)
            if batch and idx < len(shard_ids)
        ]
        compare_task: asyncio.Task[None] | None = None
        if comparator is not None and rest_compare_queue is not None:
            logger.info(
                "aggtrade.rest_compare.enabled interval=%s symbols=%s delay_seconds=%s concurrency=%s min_validate_open_time_ms=%s",
                interval,
                len(symbols),
                max(0.0, rest_compare_delay_seconds),
                max(1, rest_compare_concurrency),
                min_validate_open_time_ms,
            )
            compare_task = asyncio.create_task(
                self._run_rest_kline_compare(
                    interval,
                    comparator,
                    rest_compare_queue,
                    delay_seconds=max(0.0, rest_compare_delay_seconds),
                    concurrency=max(1, rest_compare_concurrency),
                ),
            )
        watchdog_task: asyncio.Task[None] | None = None
        if self._cfg.binance.ws_watchdog_enabled:
            now_ms = int(time.time() * 1000)
            self._ws_last_event_ms[interval] = now_ms
            self._ws_watchdog_alerted[interval] = False
            self._ws_shard_last_event_ms[interval] = {shard_id: now_ms for shard_id in shard_ids}
            self._ws_shard_watchdog_alerted[interval] = {shard_id: False for shard_id in shard_ids}
            watchdog_task = asyncio.create_task(
                self._run_ws_watchdog(interval, len(symbols), shard_ids),
            )
        try:
            while True:
                yield await queue.get()
        finally:
            for task in tasks:
                task.cancel()
            if compare_task is not None:
                compare_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await compare_task
            if comparator is not None and self._aggtrade_comparators.get(interval) is comparator:
                self._aggtrade_comparators.pop(interval, None)
            if watchdog_task is not None:
                watchdog_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watchdog_task
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_rest_kline_compare(
        self,
        interval: str,
        comparator: _AggTradeKlineComparator,
        close_queue: asyncio.Queue[_AggTradeRestCompareRequest],
        *,
        delay_seconds: float,
        concurrency: int,
    ) -> None:
        sem = asyncio.Semaphore(max(1, concurrency))
        timeout = httpx.Timeout(self._cfg.backfill.rest_timeout_seconds)
        request_delay_seconds = max(0.0, self._cfg.backfill.rest_request_delay_seconds)

        async def fetch_one(
            client: httpx.AsyncClient,
            close_time_ms: int,
            symbol: str,
            rest_results: dict[str, ClosedKline],
            rest_errors: list[str],
            rest_missing: set[str],
        ) -> None:
            async with sem:
                for attempt in range(2):
                    if request_delay_seconds > 0:
                        await asyncio.sleep(request_delay_seconds)
                    try:
                        kline = await self.fetch_exact_closed_kline_rest(client, symbol, interval, close_time_ms)
                    except httpx.HTTPStatusError as exc:
                        status_code = exc.response.status_code
                        if status_code in (418, 429) and attempt == 0:
                            retry_after_raw = exc.response.headers.get("Retry-After")
                            try:
                                retry_after_seconds = float(retry_after_raw or "1")
                            except ValueError:
                                retry_after_seconds = 1.0
                            retry_after_seconds = max(1.0, min(30.0, retry_after_seconds))
                            logger.warning(
                                "aggtrade.rest_compare.rate_limited interval=%s close_time_ms=%s symbol=%s status=%s retry_after_seconds=%s",
                                interval,
                                close_time_ms,
                                symbol,
                                status_code,
                                retry_after_seconds,
                            )
                            await asyncio.sleep(retry_after_seconds)
                            continue
                        rest_errors.append(f"{symbol}({exc})")
                        logger.warning(
                            "aggtrade.rest_compare.fetch_fail interval=%s close_time_ms=%s symbol=%s error=%s",
                            interval,
                            close_time_ms,
                            symbol,
                            exc,
                        )
                        return
                    except Exception as exc:
                        rest_errors.append(f"{symbol}({exc})")
                        logger.warning(
                            "aggtrade.rest_compare.fetch_fail interval=%s close_time_ms=%s symbol=%s error=%s",
                            interval,
                            close_time_ms,
                            symbol,
                            exc,
                        )
                        return
                    if kline is None:
                        rest_missing.add(symbol)
                        return
                    rest_results[symbol] = kline
                    return

        async with httpx.AsyncClient(timeout=timeout, proxy=self._cfg.binance.rest_proxy_url) as client:
            while True:
                request = await close_queue.get()
                queued_started_ms = int(time.time() * 1000)
                try:
                    wait_ms = request.snapshot_due_ms - int(time.time() * 1000)
                    if wait_ms > 0:
                        await asyncio.sleep(wait_ms / 1000)
                    fetch_started_ms = int(time.time() * 1000)
                    local_snapshot = comparator.snapshot_agg(
                        request.close_time_ms,
                        request.symbols,
                        snapshot_ms=fetch_started_ms,
                    )
                    rest_results: dict[str, ClosedKline] = {}
                    rest_errors: list[str] = []
                    rest_missing: set[str] = set()
                    await asyncio.gather(
                        *(
                            fetch_one(
                                client,
                                request.close_time_ms,
                                symbol,
                                rest_results,
                                rest_errors,
                                rest_missing,
                            )
                            for symbol in request.symbols
                        )
                    )
                    finished_ms = int(time.time() * 1000)
                    await comparator.finish_compare_request(
                        request,
                        local_snapshot=local_snapshot,
                        rest_snapshot=rest_results,
                        rest_missing=rest_missing,
                        rest_errors=rest_errors,
                        validation_delay_seconds=request.snapshot_offset_ms / 1000,
                        validation_fetch_elapsed_ms=max(0, finished_ms - fetch_started_ms),
                        validation_total_elapsed_ms=max(0, finished_ms - queued_started_ms),
                        snapshot_started_ms=fetch_started_ms,
                    )
                    logger.info(
                        "aggtrade.rest_compare.done interval=%s close_time_ms=%s snapshot=%s symbols=%s elapsed_ms=%s",
                        interval,
                        request.close_time_ms,
                        request.snapshot_label,
                        len(request.symbols),
                        int(time.time() * 1000) - queued_started_ms,
                    )
                finally:
                    close_queue.task_done()

    def _build_symbol_batches(
        self,
        symbols: list[str],
        interval: str,
        batch_size: int,
        max_streams_query_chars: int = 0,
    ) -> list[list[str]]:
        return self._build_stream_symbol_batches(
            symbols,
            stream_suffix=f"@kline_{interval}",
            batch_size=batch_size,
            max_streams_query_chars=max_streams_query_chars,
            log_interval=interval,
        )

    def _build_stream_symbol_batches(
        self,
        symbols: list[str],
        *,
        stream_suffix: str,
        batch_size: int,
        max_streams_query_chars: int = 0,
        log_interval: str,
    ) -> list[list[str]]:
        batches: list[list[str]] = []
        current: list[str] = []

        for symbol in symbols:
            stream_name = f"{symbol.lower()}{stream_suffix}"
            # Use encoded query length as the true budget (important for non-ascii symbols).
            if current:
                projected_chars = len(
                    urlencode(
                        {
                            "streams": "/".join(
                                [*(f"{s.lower()}{stream_suffix}" for s in current), stream_name]
                            )
                        }
                    )
                )
            else:
                projected_chars = len(urlencode({"streams": stream_name}))
            hit_size_limit = len(current) >= max(1, batch_size)
            hit_query_limit = max_streams_query_chars > 0 and projected_chars > max_streams_query_chars
            if current and (hit_size_limit or hit_query_limit):
                batches.append(current)
                current = [symbol]
            else:
                current.append(symbol)

        if current:
            batches.append(current)

        if batches:
            logger.info(
                "ws.batches.built interval=%s requested_batch_size=%s actual_batches=%s largest_batch=%s max_query_chars=%s",
                log_interval,
                batch_size,
                len(batches),
                max(len(b) for b in batches),
                max_streams_query_chars,
            )
        return batches

    def _combined_stream_base(self) -> str:
        ws_base = self._cfg.binance.ws_base.split("?", 1)[0].rstrip("/")
        if ws_base.endswith("/stream"):
            return ws_base
        if ws_base.endswith("/ws"):
            # Support new market endpoint style:
            #   wss://fstream.binance.com/market/ws
            # Combined streams should use sibling /stream path.
            return f"{ws_base[:-3]}/stream"
        return f"{ws_base}/stream"

    async def _consume_aggtrade_batch(
        self,
        symbols: list[str],
        interval: str,
        shard_id: str,
        out_queue: asyncio.Queue[ClosedKline],
        total_shards: int,
        *,
        interval_ms: int,
        emit_grace_ms: int,
        comparator: _AggTradeKlineComparator | None,
    ) -> None:
        stream_names = [f"{symbol.lower()}@aggTrade" for symbol in symbols]
        stream_base = self._combined_stream_base()
        url = f"{stream_base}?streams={'/'.join(stream_names)}"
        dest_host = urlparse(url).hostname or "fstream.binance.com"
        emit_grace_ms = max(0, emit_grace_ms)
        bars: dict[tuple[str, int], _AggTradeBar] = {}
        emitted_keys: set[tuple[str, int]] = set()
        emitted_order: deque[tuple[str, int]] = deque()
        latest_emitted_open_by_symbol: dict[str, int] = {}
        max_emitted_keys = max(1000, len(symbols) * 12)
        last_emit_check_ms = 0
        partial_skip_logged: set[tuple[str, int]] = set()

        def mark_ws_event_healthy(now_ms: int) -> None:
            self._ws_last_event_ms[interval] = now_ms
            interval_shards = self._ws_shard_last_event_ms.setdefault(interval, {})
            interval_shards[shard_id] = now_ms
            self._ws_watchdog_alerted[interval] = False
            shard_alerted = self._ws_shard_watchdog_alerted.setdefault(interval, {})
            shard_alerted[shard_id] = False

        def drop_open_bars(_reason: str) -> None:
            bars.clear()

        async def emit_due(now_ms: int) -> None:
            due_keys = [
                key
                for key, bar in bars.items()
                if bar.close_time_ms + 1 + emit_grace_ms <= now_ms
            ]
            if not due_keys:
                return
            due_keys.sort(key=lambda key: (key[1], key[0]))
            for key in due_keys:
                bar = bars.pop(key, None)
                if bar is None:
                    continue
                kline = bar.to_closed_kline(
                    ingest_time_ms=now_ms,
                    shard_id=shard_id,
                    shard_symbol_count=len(symbols),
                    total_shards=total_shards,
                )
                emitted_keys.add(key)
                latest_emitted_open_by_symbol[key[0]] = max(
                    latest_emitted_open_by_symbol.get(key[0], 0),
                    key[1],
                )
                emitted_order.append(key)
                while len(emitted_order) > max_emitted_keys:
                    old_key = emitted_order.popleft()
                    emitted_keys.discard(old_key)
                lag = max(0, kline.ingest_time_ms - kline.event_time_ms)
                EXCHANGE_TO_SERVICE_MS.labels(interval, kline.source).observe(lag)
                KLINES_RECEIVED.labels(interval, kline.source).inc()
                touch_ingest(interval)
                if comparator is not None:
                    await comparator.record_agg(kline)
                await out_queue.put(kline)

        async def maybe_emit_due(now_ms: int, *, force: bool = False) -> None:
            nonlocal last_emit_check_ms
            if not force and now_ms - last_emit_check_ms < 10:
                return
            last_emit_check_ms = now_ms
            await emit_due(now_ms)

        while True:
            try:
                proxy_url = self._cfg.binance.ws_proxy_url or _ws_proxy_from_env(dest_host)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=120.0, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(
                        url,
                        proxy=proxy_url,
                        autoping=True,
                        heartbeat=30.0,
                    ) as ws:
                        local_generation = self._ws_rebuild_generation.get(shard_id, 0)
                        connected_ms = int(time.time() * 1000)
                        min_emit_open_time_ms = ((connected_ms // interval_ms) + 1) * interval_ms
                        WS_CONNECTIONS.labels(interval).inc()
                        logger.info(
                            "aggtrade.ws.connected interval=%s shard=%s symbols=%s emit_grace_ms=%s min_emit_open_time_ms=%s compare_with_rest_kline=%s",
                            interval,
                            shard_id,
                            len(symbols),
                            emit_grace_ms,
                            min_emit_open_time_ms,
                            comparator is not None,
                        )
                        if comparator is not None:
                            comparator.mark_shard_connected(shard_id, connected_ms)
                        while True:
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=0.1)
                            except TimeoutError:
                                now_ms = int(time.time() * 1000)
                                await maybe_emit_due(now_ms, force=True)
                                if self._ws_rebuild_generation.get(shard_id, 0) != local_generation:
                                    logger.warning(
                                        "aggtrade.ws.rebuild.triggered interval=%s shard=%s symbols=%s",
                                        interval,
                                        shard_id,
                                        len(symbols),
                                    )
                                    drop_open_bars("watchdog_rebuild")
                                    break
                                continue
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                raw = msg.data.encode("utf-8")
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                raw = msg.data
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise msg.data
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSING,
                            ):
                                logger.warning(
                                    "aggtrade.ws.closed interval=%s shard=%s symbols=%s type=%s",
                                    interval,
                                    shard_id,
                                    len(symbols),
                                    msg.type,
                                )
                                drop_open_bars("ws_closed")
                                break
                            else:
                                continue
                            now_ms = int(time.time() * 1000)
                            if self._ws_rebuild_generation.get(shard_id, 0) != local_generation:
                                logger.warning(
                                    "aggtrade.ws.rebuild.triggered interval=%s shard=%s symbols=%s",
                                    interval,
                                    shard_id,
                                    len(symbols),
                                )
                                drop_open_bars("watchdog_rebuild")
                                break

                            payload = orjson.loads(raw)
                            if "code" in payload or "msg" in payload:
                                logger.error(
                                    "aggtrade.ws.subscribe.error interval=%s shard=%s code=%s msg=%s",
                                    interval,
                                    shard_id,
                                    payload.get("code"),
                                    payload.get("msg"),
                                )
                                continue
                            data = payload.get("data", payload)
                            if data.get("e") != "aggTrade":
                                continue
                            try:
                                symbol = str(data["s"])
                                trade_time_ms = int(data.get("T") or data["E"])
                                event_time_ms = int(data.get("E") or trade_time_ms)
                                price = Decimal(str(data["p"]))
                                quantity = Decimal(str(data["q"]))
                            except (KeyError, TypeError, ValueError) as exc:
                                logger.warning(
                                    "aggtrade.ws.parse_fail interval=%s shard=%s error=%s payload=%s",
                                    interval,
                                    shard_id,
                                    exc,
                                    data,
                                )
                                continue
                            mark_ws_event_healthy(now_ms)
                            open_time_ms = (trade_time_ms // interval_ms) * interval_ms
                            if open_time_ms < min_emit_open_time_ms:
                                skip_key = (shard_id, open_time_ms)
                                if skip_key not in partial_skip_logged:
                                    partial_skip_logged.add(skip_key)
                                    logger.info(
                                        "aggtrade.partial_period_skipped interval=%s shard=%s symbol=%s open_time_ms=%s min_emit_open_time_ms=%s",
                                        interval,
                                        shard_id,
                                        symbol,
                                        open_time_ms,
                                        min_emit_open_time_ms,
                                    )
                                continue
                            close_time_ms = open_time_ms + interval_ms - 1
                            if comparator is not None:
                                comparator.record_trade(
                                    symbol=symbol,
                                    open_time_ms=open_time_ms,
                                    close_time_ms=close_time_ms,
                                    price=price,
                                    quantity=quantity,
                                    trade_time_ms=trade_time_ms,
                                    event_time_ms=event_time_ms,
                                )
                            if close_time_ms <= self._aggtrade_published_close_ms.get(interval, 0):
                                continue
                            key = (symbol, open_time_ms)
                            latest_emitted_open_time_ms = latest_emitted_open_by_symbol.get(symbol)
                            if key in emitted_keys or (
                                latest_emitted_open_time_ms is not None
                                and open_time_ms <= latest_emitted_open_time_ms
                            ):
                                continue
                            bar = bars.get(key)
                            if bar is None:
                                bars[key] = _AggTradeBar.from_trade(
                                    symbol=symbol,
                                    interval=interval,
                                    open_time_ms=open_time_ms,
                                    close_time_ms=close_time_ms,
                                    price=price,
                                    quantity=quantity,
                                    trade_time_ms=trade_time_ms,
                                    event_time_ms=event_time_ms,
                                )
                            else:
                                bar.update(price, quantity, trade_time_ms, event_time_ms)
                            await maybe_emit_due(now_ms)
            except Exception as exc:
                logger.warning("aggtrade.ws.reconnect interval=%s shard=%s error=%s", interval, shard_id, exc)
                await asyncio.sleep(2.0)

    async def _consume_batch(
        self,
        symbols: list[str],
        interval: str,
        shard_id: str,
        out_queue: asyncio.Queue[ClosedKline],
        total_shards: int,
        *,
        backfill_on_connect: bool = True,
        record_metrics: bool = True,
        record_health: bool = True,
    ) -> None:
        stream_names = [f"{s.lower()}@kline_{interval}" for s in symbols]
        fast_skipped_metric = WS_KLINE_FAST_SKIPPED.labels(interval)
        stream_base = self._combined_stream_base()
        url = f"{stream_base}?streams={'/'.join(stream_names)}"
        dest_host = urlparse(url).hostname or "fstream.binance.com"
        while True:
            try:
                proxy_url = self._cfg.binance.ws_proxy_url or _ws_proxy_from_env(dest_host)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=120.0, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(
                        url,
                        proxy=proxy_url,
                        autoping=True,
                        heartbeat=None,
                    ) as ws:
                        local_generation = self._ws_rebuild_generation.get(shard_id, 0)
                        if record_metrics:
                            WS_CONNECTIONS.labels(interval).inc()
                        logger.info("ws.connected interval=%s shard=%s symbols=%s", interval, shard_id, len(symbols))
                        # Keep 1m warm-start for alert calculations; higher intervals use delayed REST repair.
                        if backfill_on_connect and interval == "1m":
                            await self._backfill_batch(symbols, interval, out_queue)
                        while True:
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                            except TimeoutError:
                                # Periodically wake up to honor watchdog-triggered rebuilds.
                                if self._ws_rebuild_generation.get(shard_id, 0) != local_generation:
                                    logger.warning(
                                        "ws.rebuild.triggered interval=%s shard=%s symbols=%s",
                                        interval,
                                        shard_id,
                                        len(symbols),
                                    )
                                    break
                                continue
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                raw = msg.data.encode("utf-8")
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                raw = msg.data
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise msg.data
                            else:
                                continue
                            if self._ws_rebuild_generation.get(shard_id, 0) != local_generation:
                                logger.warning(
                                    "ws.rebuild.triggered interval=%s shard=%s symbols=%s",
                                    interval,
                                    shard_id,
                                    len(symbols),
                                )
                                break
                            # Treat any successfully received WS frame as shard activity.
                            # This prevents watchdog false positives when only non-closed kline
                            # updates (x=false) are flowing between close boundaries.
                            now_ms = int(time.time() * 1000)
                            if record_health:
                                self._ws_last_event_ms[interval] = now_ms
                                interval_shards = self._ws_shard_last_event_ms.setdefault(interval, {})
                                interval_shards[shard_id] = now_ms
                                self._ws_watchdog_alerted[interval] = False
                                shard_alerted = self._ws_shard_watchdog_alerted.setdefault(interval, {})
                                shard_alerted[shard_id] = False
                            if not _raw_has_closed_kline_marker(raw) and not _raw_maybe_control_frame(raw):
                                if record_metrics:
                                    fast_skipped_metric.inc()
                                    debug_key = f"{interval}:{shard_id}:fast_skip"
                                    if debug_key not in self._ws_parse_debug_logged:
                                        self._ws_parse_debug_logged.add(debug_key)
                                        try:
                                            raw_sample = raw[:200].decode("utf-8", errors="ignore")
                                        except Exception:
                                            raw_sample = "<unprintable>"
                                        logger.info(
                                            "ws.kline_fast_skip_debug interval=%s shard=%s raw_sample=%s",
                                            interval,
                                            shard_id,
                                            raw_sample,
                                        )
                                continue
                            payload = orjson.loads(raw)
                            if "code" in payload or "msg" in payload:
                                logger.error(
                                    "ws.subscribe.error interval=%s shard=%s code=%s msg=%s",
                                    interval,
                                    shard_id,
                                    payload.get("code"),
                                    payload.get("msg"),
                                )
                                continue
                            data = payload.get("data", payload)
                            k = data.get("k", {})
                            if not k.get("x"):
                                debug_key = f"{interval}:{shard_id}"
                                if debug_key not in self._ws_parse_debug_logged:
                                    self._ws_parse_debug_logged.add(debug_key)
                                    try:
                                        raw_sample = raw[:200].decode("utf-8", errors="ignore")
                                    except Exception:
                                        raw_sample = "<unprintable>"
                                    logger.info(
                                        "ws.kline_parse_debug interval=%s shard=%s payload_keys=%s data_keys=%s k_keys=%s k_x=%s raw_sample=%s",
                                        interval,
                                        shard_id,
                                        list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
                                        list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                                        list(k.keys()) if isinstance(k, dict) else type(k).__name__,
                                        k.get("x"),
                                        raw_sample,
                                    )
                            if not k.get("x"):
                                continue

                            ingest_ms = int(time.time() * 1000)
                            kline = ClosedKline(
                                symbol=k["s"],
                                interval=k["i"],
                                open_time_ms=int(k["t"]),
                                close_time_ms=int(k["T"]),
                                open_price=Decimal(k["o"]),
                                high_price=Decimal(k["h"]),
                                low_price=Decimal(k["l"]),
                                close_price=Decimal(k["c"]),
                                volume=Decimal(k["v"]),
                                quote_volume=Decimal(k["q"]),
                                event_time_ms=int(data["E"]),
                                ingest_time_ms=ingest_ms,
                                source="ws",
                                ws_shard_id=shard_id,
                                ws_shard_symbol_count=len(symbols),
                                ws_total_shards=total_shards,
                            )
                            lag = max(0, kline.ingest_time_ms - kline.event_time_ms)
                            if record_metrics:
                                EXCHANGE_TO_SERVICE_MS.labels(interval, kline.source).observe(lag)
                                KLINES_RECEIVED.labels(interval, kline.source).inc()
                                touch_ingest(interval)
                            await out_queue.put(kline)
            except Exception as exc:
                logger.warning("ws.reconnect interval=%s shard=%s error=%s", interval, shard_id, exc)
                await asyncio.sleep(2.0)

    async def _run_ws_watchdog(self, interval: str, expected_symbols: int, shard_ids: list[str]) -> None:
        interval_ms = self._interval_to_ms(interval)
        if interval_ms <= 0:
            logger.warning("ws.watchdog.disabled interval=%s reason=unknown_interval", interval)
            return
        no_data_periods = max(1, self._cfg.binance.ws_watchdog_no_data_periods)
        if any(shard_id.startswith("aggtrade:") for shard_id in shard_ids):
            stale_threshold_ms = max(5, self._cfg.websocket.aggtrade_ws_watchdog_stale_seconds) * 1000
        else:
            stale_threshold_ms = interval_ms * no_data_periods
        check_seconds = max(1, self._cfg.binance.ws_watchdog_check_seconds)
        # Wall time of the last rebuild per shard, used to re-arm the watchdog
        # when a rebuild does not restore data flow.
        last_rebuild_ms: dict[str, int] = {}
        while True:
            await asyncio.sleep(check_seconds)
            now_ms = int(time.time() * 1000)
            shard_last = self._ws_shard_last_event_ms.get(interval, {})
            shard_alerted = self._ws_shard_watchdog_alerted.setdefault(interval, {})
            stale_shards = 0
            for shard_id in shard_ids:
                last_ms = shard_last.get(shard_id, 0)
                stale_ms = max(0, now_ms - last_ms) if last_ms > 0 else stale_threshold_ms
                if stale_ms < stale_threshold_ms:
                    continue
                stale_shards += 1
                if not watchdog_should_rebuild(
                    now_ms=now_ms,
                    stale_ms=stale_ms,
                    stale_threshold_ms=stale_threshold_ms,
                    alerted=shard_alerted.get(shard_id, False),
                    last_rebuild_ms=last_rebuild_ms.get(shard_id, 0),
                ):
                    continue
                last_rebuild_ms[shard_id] = now_ms
                new_generation = self._ws_rebuild_generation.get(shard_id, 0) + 1
                self._ws_rebuild_generation[shard_id] = new_generation
                shard_alerted[shard_id] = True
                logger.warning(
                    "ws.watchdog.rebuild_shard interval=%s shard=%s no_data_ms=%s threshold_ms=%s periods=%s expected_symbols=%s total_shards=%s generation=%s",
                    interval,
                    shard_id,
                    stale_ms,
                    stale_threshold_ms,
                    no_data_periods,
                    expected_symbols,
                    len(shard_ids),
                    new_generation,
                )
            self._ws_watchdog_alerted[interval] = stale_shards > 0

    def _interval_to_ms(self, interval: str) -> int:
        try:
            if interval.endswith("m"):
                return int(interval[:-1]) * 60_000
            if interval.endswith("h"):
                return int(interval[:-1]) * 3_600_000
            if interval.endswith("d"):
                return int(interval[:-1]) * 86_400_000
        except ValueError:
            return 0
        return 0

    def get_ws_activity_snapshot(self) -> dict[str, int | None]:
        now_ms = int(time.time() * 1000)
        out: dict[str, int | None] = {}
        for interval, last_ms in self._ws_last_event_ms.items():
            if last_ms <= 0:
                out[interval] = None
            else:
                out[interval] = max(0, now_ms - last_ms)
        return out

    def get_ws_shard_health_snapshot(self) -> dict[str, dict[str, int]]:
        now_ms = int(time.time() * 1000)
        out: dict[str, dict[str, int]] = {}
        for interval, shard_map in self._ws_shard_last_event_ms.items():
            interval_ms = self._interval_to_ms(interval)
            no_data_periods = max(1, self._cfg.binance.ws_watchdog_no_data_periods)
            stale_threshold_ms = interval_ms * no_data_periods if interval_ms > 0 else 0
            total = len(shard_map)
            stale = 0
            worst_ago_ms = 0
            for last_ms in shard_map.values():
                ago_ms = max(0, now_ms - last_ms) if last_ms > 0 else 0
                worst_ago_ms = max(worst_ago_ms, ago_ms)
                if stale_threshold_ms > 0 and ago_ms >= stale_threshold_ms:
                    stale += 1
            out[interval] = {
                "total_shards": total,
                "stale_shards": stale,
                "worst_shard_ago_ms": worst_ago_ms,
            }
        return out
