#!/usr/bin/env python3
"""
Binance USDT-M closed-kline subscriber.

Correctness path:
recv -> queue timestamp -> parse only k.x=true -> publish/track Binance kline fields

Optional REST audit compares the final WS kline with the exact REST candle
identified by the same (symbol, interval, open_time_ms, close_time_ms).
"""

from __future__ import annotations

import asyncio
import inspect
import os
import sys
import time
from array import array
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import httpx
import orjson
import websockets
from websockets.exceptions import ConnectionClosed

ROOT = Path(__file__).resolve().parent
SYMBOLS_FILE = ROOT / "symbols.txt"

DEFAULT_WS_BASE = "wss://fstream.binance.com/stream?streams="
DEFAULT_REST_BASE = "https://fapi.binance.com"
DEFAULT_AUDIT_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT"

_JSON_WHITESPACE = b" \t\r\n"
_CLOSED_KLINE_KEY = b'"x"'
_COMPARE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trades",
    "taker_buy_volume",
    "taker_buy_quote_volume",
)


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    return int(v) if v.isdigit() else default


def _env_optional_float(name: str, default: Optional[float]) -> Optional[float]:
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return default
    if v in ("0", "none", "false", "off", "disable", "disabled"):
        return None
    try:
        return float(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_csv(name: str, default: str) -> list[str]:
    return _csv(os.environ.get(name, default))


def _env_csv_int(name: str, default: str) -> tuple[int, ...]:
    out: list[int] = []
    for item in _env_csv(name, default):
        try:
            out.append(max(0, int(item)))
        except ValueError:
            continue
    return tuple(sorted(set(out)))


def _raw_has_closed_kline_marker(raw: bytes) -> bool:
    """Fast prefilter for a JSON kline frame containing x=true."""
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


def _parse_intervals() -> tuple[str, ...]:
    raw = os.environ.get("KLINE_INTERVALS", "").strip()
    if not raw:
        raw = os.environ.get("KLINE_INTERVAL", "").strip()
    if not raw:
        raw = "1m,1h"
    intervals: list[str] = []
    seen: set[str] = set()
    for item in _csv(raw.lower()):
        if item in seen:
            continue
        if not (item.endswith("m") or item.endswith("h") or item.endswith("d")):
            raise ValueError(f"Unsupported interval={item!r}")
        int(item[:-1])
        intervals.append(item)
        seen.add(item)
    if not intervals:
        raise ValueError("No KLINE_INTERVALS configured")
    return tuple(intervals)


def _stream_prefix() -> str:
    raw = os.environ.get("BINANCE_WS_BASE", "").strip() or DEFAULT_WS_BASE
    if "?streams=" in raw:
        return raw.split("?streams=", 1)[0].rstrip("/") + "?streams="
    base = raw.split("?", 1)[0].rstrip("/")
    if base.endswith("/ws"):
        base = f"{base[:-3]}/stream"
    elif not base.endswith("/stream"):
        base = f"{base}/stream"
    return f"{base}?streams="


def _proxy_from_env() -> str:
    return (
        os.environ.get("BINANCE_WS_PROXY_URL", "").strip()
        or os.environ.get("WS_PROXY_URL", "").strip()
        or ""
    )


KLINE_INTERVALS = _parse_intervals()
STREAM_PREFIX = _stream_prefix()
REST_BASE = (os.environ.get("BINANCE_REST_BASE", "").strip() or DEFAULT_REST_BASE).rstrip("/")

# Smaller shards reduce the close-boundary burst per socket.
SHARD_SIZE = _env_int("SHARD_SIZE", 88)
LOG_EACH_KLINE = _env_bool("LOG_EACH_KLINE", False)
LOG_PROGRESS_EVERY = _env_int("LOG_PROGRESS_EVERY", 0)
QUEUE_MAXSIZE = _env_int("QUEUE_MAXSIZE", 0)
PROCESSOR_WORKERS = _env_int("PROCESSOR_WORKERS", 6)
LOG_QUEUE_MAXSIZE = _env_int("LOG_QUEUE_MAXSIZE", 100_000)
BINANCE_WS_PROXY_URL = _proxy_from_env()
PING_INTERVAL = _env_optional_float("PING_INTERVAL", 20.0)
PING_TIMEOUT = _env_optional_float("PING_TIMEOUT", 60.0)
OPEN_TIMEOUT = _env_optional_float("OPEN_TIMEOUT", 30.0)
REST_AUDIT_ENABLED = _env_bool("REST_AUDIT_ENABLED", True)
REST_AUDIT_ALL_SYMBOLS = _env_bool("REST_AUDIT_ALL_SYMBOLS", False)
REST_AUDIT_SYMBOLS = {item.upper() for item in _env_csv("REST_AUDIT_SYMBOLS", DEFAULT_AUDIT_SYMBOLS)}
REST_AUDIT_INTERVALS = {
    item.lower() for item in _env_csv("REST_AUDIT_INTERVALS", ",".join(KLINE_INTERVALS))
}
REST_AUDIT_OFFSETS_MS = _env_csv_int("REST_AUDIT_OFFSETS_MS", "1500,5000,10000")
REST_AUDIT_CONCURRENCY = _env_int("REST_AUDIT_CONCURRENCY", 4)
REST_AUDIT_TIMEOUT_SECONDS = _env_optional_float("REST_AUDIT_TIMEOUT_SECONDS", 10.0) or 10.0
REST_AUDIT_QUEUE_MAXSIZE = _env_int("REST_AUDIT_QUEUE_MAXSIZE", 50_000)
BINANCE_REST_PROXY_URL = (
    os.environ.get("BINANCE_REST_PROXY_URL", "").strip()
    or os.environ.get("BINANCE_HTTP_PROXY", "").strip()
    or ""
)
_CONNECT_SUPPORTS_PROXY = "proxy" in inspect.signature(websockets.connect).parameters


def load_symbols(path: Path = SYMBOLS_FILE) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for raw in path.read_text().splitlines():
        symbol = raw.strip().lower()
        if not symbol or symbol in seen:
            continue
        symbols.append(symbol)
        seen.add(symbol)
    if not symbols:
        raise RuntimeError(f"No symbols in {path}")
    return symbols


@dataclass(frozen=True, slots=True)
class ShardSpec:
    shard_id: str
    interval: str
    symbols: tuple[str, ...]
    url: str


def build_ws_shards(symbols: list[str], intervals: tuple[str, ...], shard_size: int = SHARD_SIZE) -> list[ShardSpec]:
    shards: list[ShardSpec] = []
    for interval in intervals:
        for idx, start in enumerate(range(0, len(symbols), shard_size), 1):
            chunk = tuple(symbols[start : start + shard_size])
            streams = "/".join(f"{symbol}@kline_{interval}" for symbol in chunk)
            shards.append(
                ShardSpec(
                    shard_id=f"{interval}#{idx}",
                    interval=interval,
                    symbols=chunk,
                    url=STREAM_PREFIX + streams,
                ),
            )
    return shards


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def ns_to_ms(ns: int) -> int:
    return ns // 1_000_000


def now_ms() -> int:
    return int(time.time() * 1000)


def ns_span_ms(start_ns: int, end_ns: int) -> float:
    if start_ns <= 0 or end_ns <= 0:
        return 0.0
    return max(0.0, (end_ns - start_ns) / 1_000_000)


def fmt_stats(values: list[float]) -> str:
    if not values:
        return "n=0"
    ordered = sorted(values)
    current = values[-1]
    avg = sum(ordered) / len(ordered)
    idx = min(len(ordered) - 1, int((len(ordered) - 1) * 0.95))
    p95 = ordered[idx]
    return (
        f"n={len(ordered)} 本次={current:.3f}ms avg={avg:.3f}ms "
        f"p95={p95:.3f}ms min={ordered[0]:.3f}ms max={ordered[-1]:.3f}ms"
    )


def decimal_equal(left: object, right: object) -> bool:
    try:
        return Decimal(str(left)) == Decimal(str(right))
    except (InvalidOperation, ValueError):
        return str(left) == str(right)


@dataclass(frozen=True, slots=True)
class KlineSnapshot:
    symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    quote_volume: str
    trades: int
    taker_buy_volume: str
    taker_buy_quote_volume: str
    event_time_ms: int
    recv_ms: int
    shard_id: str

    @classmethod
    def from_payload(cls, payload: dict, recv_ns: int, shard_id: str) -> "KlineSnapshot | None":
        data = payload.get("data", payload)
        if not isinstance(data, dict) or data.get("e") != "kline":
            return None
        k = data.get("k")
        if not isinstance(k, dict) or not k.get("x"):
            return None
        return cls(
            symbol=str(k["s"]).upper(),
            interval=str(k["i"]),
            open_time_ms=int(k["t"]),
            close_time_ms=int(k["T"]),
            open=str(k["o"]),
            high=str(k["h"]),
            low=str(k["l"]),
            close=str(k["c"]),
            # Binance kline WS final frame fields:
            # v = base asset volume, q = quote asset volume/turnover.
            volume=str(k["v"]),
            quote_volume=str(k["q"]),
            trades=int(k.get("n") or 0),
            taker_buy_volume=str(k.get("V") or "0"),
            taker_buy_quote_volume=str(k.get("Q") or "0"),
            event_time_ms=int(data.get("E") or k["T"]),
            recv_ms=ns_to_ms(recv_ns),
            shard_id=shard_id,
        )

    @property
    def symbol_key(self) -> str:
        return self.symbol.lower()


@dataclass
class BatchStats:
    close_time_ms: int = 0
    first_recv_ns: int = 0
    last_recv_ns: int = 0
    first_proc_ns: int = 0
    last_proc_ns: int = 0
    first_symbol: str = ""
    last_symbol: str = ""
    first_shard_id: str = ""
    last_shard_id: str = ""
    event_min_ms: int = 0
    event_max_ms: int = 0
    event_to_recv_ms: list[float] = field(default_factory=list)
    recv_to_parse_ms: list[float] = field(default_factory=list)
    shard_counts: dict[str, int] = field(default_factory=dict)
    shard_recv_min_ns: dict[str, int] = field(default_factory=dict)
    shard_recv_max_ns: dict[str, int] = field(default_factory=dict)
    shard_event_min_ms: dict[str, int] = field(default_factory=dict)
    shard_event_max_ms: dict[str, int] = field(default_factory=dict)

    def update(self, *, snapshot: KlineSnapshot, recv_ns: int, proc_ns: int) -> None:
        self.close_time_ms = snapshot.close_time_ms
        if self.first_recv_ns <= 0 or recv_ns < self.first_recv_ns:
            self.first_recv_ns = recv_ns
            self.first_proc_ns = proc_ns
            self.first_symbol = snapshot.symbol
            self.first_shard_id = snapshot.shard_id
        if recv_ns >= self.last_recv_ns:
            self.last_recv_ns = recv_ns
            self.last_proc_ns = proc_ns
            self.last_symbol = snapshot.symbol
            self.last_shard_id = snapshot.shard_id

        if snapshot.event_time_ms > 0:
            self.event_min_ms = (
                snapshot.event_time_ms
                if self.event_min_ms <= 0
                else min(self.event_min_ms, snapshot.event_time_ms)
            )
            self.event_max_ms = max(self.event_max_ms, snapshot.event_time_ms)
            self.event_to_recv_ms.append(float(ns_to_ms(recv_ns) - snapshot.event_time_ms))
        self.recv_to_parse_ms.append(ns_span_ms(recv_ns, proc_ns))

        shard_id = snapshot.shard_id
        self.shard_counts[shard_id] = self.shard_counts.get(shard_id, 0) + 1
        shard_min_recv = self.shard_recv_min_ns.get(shard_id, 0)
        self.shard_recv_min_ns[shard_id] = recv_ns if shard_min_recv <= 0 else min(shard_min_recv, recv_ns)
        self.shard_recv_max_ns[shard_id] = max(self.shard_recv_max_ns.get(shard_id, 0), recv_ns)
        if snapshot.event_time_ms > 0:
            shard_min_event = self.shard_event_min_ms.get(shard_id, 0)
            self.shard_event_min_ms[shard_id] = (
                snapshot.event_time_ms if shard_min_event <= 0 else min(shard_min_event, snapshot.event_time_ms)
            )
            self.shard_event_max_ms[shard_id] = max(
                self.shard_event_max_ms.get(shard_id, 0),
                snapshot.event_time_ms,
            )

    def shard_lines(self) -> str:
        parts: list[str] = []
        for shard_id in sorted(self.shard_counts):
            recv_min = self.shard_recv_min_ns.get(shard_id, 0)
            recv_max = self.shard_recv_max_ns.get(shard_id, 0)
            event_min = self.shard_event_min_ms.get(shard_id, 0)
            event_max = self.shard_event_max_ms.get(shard_id, 0)
            parts.append(
                f"{shard_id}：symbols={self.shard_counts[shard_id]} "
                f"E跨度={max(0, event_max - event_min)}ms "
                f"入站跨度={ns_span_ms(recv_min, recv_max):.3f}ms"
            )
        return "；".join(parts) if parts else "n=0"


class BitmapBatchTracker:
    """O(1) per symbol; one consumer task per process, no lock on hot path."""

    __slots__ = ("expected", "symbol_index", "_bits", "_tally", "_reported", "_stats")

    def __init__(self, symbols: list[str]) -> None:
        self.expected = len(symbols)
        self.symbol_index = {s.lower(): i for i, s in enumerate(symbols)}
        self._bits: dict[int, array] = {}
        self._tally: dict[int, int] = {}
        self._reported: set[int] = set()
        self._stats: dict[int, BatchStats] = {}

    def on_closed(
        self,
        *,
        snapshot: KlineSnapshot,
        recv_ns: int,
        proc_ns: int,
    ) -> tuple[bool, int, Optional[BatchStats]]:
        open_time_ms = snapshot.open_time_ms
        if open_time_ms in self._reported:
            return False, self.expected, None

        idx = self.symbol_index.get(snapshot.symbol_key)
        if idx is None:
            return False, 0, None

        bits = self._bits.get(open_time_ms)
        if bits is None:
            bits = array("B", [0]) * self.expected
            self._bits[open_time_ms] = bits
            self._tally[open_time_ms] = 0
            self._stats[open_time_ms] = BatchStats(close_time_ms=snapshot.close_time_ms)

        if bits[idx]:
            return False, self._tally[open_time_ms], None

        bits[idx] = 1
        n = self._tally[open_time_ms] + 1
        self._tally[open_time_ms] = n
        self._stats[open_time_ms].update(snapshot=snapshot, recv_ns=recv_ns, proc_ns=proc_ns)
        if n >= self.expected:
            self._reported.add(open_time_ms)
            self._bits.pop(open_time_ms, None)
            self._tally.pop(open_time_ms, None)
            return True, n, self._stats.pop(open_time_ms, None)
        return False, n, None


class RestAuditor:
    def __init__(self, log_func) -> None:
        self._log = log_func
        self._queue: asyncio.Queue[KlineSnapshot] = asyncio.Queue(maxsize=REST_AUDIT_QUEUE_MAXSIZE)
        self._sem = asyncio.Semaphore(max(1, REST_AUDIT_CONCURRENCY))
        self._client: httpx.AsyncClient | None = None
        self._children: set[asyncio.Task[None]] = set()
        self._dropped = 0

    @property
    def enabled(self) -> bool:
        return REST_AUDIT_ENABLED and bool(REST_AUDIT_OFFSETS_MS) and (REST_AUDIT_ALL_SYMBOLS or bool(REST_AUDIT_SYMBOLS))

    async def start(self) -> list[asyncio.Task[None]]:
        if not self.enabled:
            return []
        proxy = BINANCE_REST_PROXY_URL or None
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(REST_AUDIT_TIMEOUT_SECONDS),
            proxy=proxy,
        )
        self._log(
            "[audit] enabled "
            f"intervals={sorted(REST_AUDIT_INTERVALS)} "
            f"symbols={'ALL' if REST_AUDIT_ALL_SYMBOLS else sorted(REST_AUDIT_SYMBOLS)} "
            f"offsets_ms={REST_AUDIT_OFFSETS_MS} rest_base={REST_BASE} proxy={proxy or '-'}"
        )
        return [asyncio.create_task(self._dispatcher(), name="rest-audit")]

    async def close(self) -> None:
        for task in list(self._children):
            task.cancel()
        if self._children:
            await asyncio.gather(*self._children, return_exceptions=True)
        if self._client is not None:
            await self._client.aclose()

    def observe(self, snapshot: KlineSnapshot) -> None:
        if not self.enabled:
            return
        if REST_AUDIT_INTERVALS and snapshot.interval not in REST_AUDIT_INTERVALS:
            return
        if not REST_AUDIT_ALL_SYMBOLS and snapshot.symbol not in REST_AUDIT_SYMBOLS:
            return
        try:
            self._queue.put_nowait(snapshot)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._dropped <= 5 or self._dropped % 100 == 0:
                self._log(f"[audit] queue full dropped={self._dropped}")

    async def _dispatcher(self) -> None:
        while True:
            snapshot = await self._queue.get()
            for offset_ms in REST_AUDIT_OFFSETS_MS:
                task = asyncio.create_task(self._audit_after(snapshot, offset_ms))
                self._children.add(task)
                task.add_done_callback(self._children.discard)

    async def _audit_after(self, snapshot: KlineSnapshot, offset_ms: int) -> None:
        due_ms = snapshot.close_time_ms + 1 + offset_ms
        wait_ms = due_ms - now_ms()
        if wait_ms > 0:
            await asyncio.sleep(wait_ms / 1000)
        request_start_ms = now_ms()
        try:
            rest = await self._fetch_rest(snapshot)
        except Exception as exc:
            self._log(
                f"[audit error] interval={snapshot.interval} symbol={snapshot.symbol} "
                f"close={fmt_ts(snapshot.close_time_ms)} offset_ms={offset_ms} "
                f"error={type(exc).__name__}:{exc}"
            )
            return
        request_done_ms = now_ms()
        if rest is None:
            self._log(
                f"[audit missing] interval={snapshot.interval} symbol={snapshot.symbol} "
                f"close={fmt_ts(snapshot.close_time_ms)} offset_ms={offset_ms} "
                f"request_after_close_ms={request_start_ms - snapshot.close_time_ms}"
            )
            return
        diffs = compare_snapshots(snapshot, rest)
        prefix = "ok" if not diffs else "diff"
        diff_text = "-" if not diffs else ",".join(diffs)
        self._log(
            f"[audit {prefix}] interval={snapshot.interval} symbol={snapshot.symbol} "
            f"close={fmt_ts(snapshot.close_time_ms)} offset_ms={offset_ms} "
            f"request_after_close_ms={request_start_ms - snapshot.close_time_ms} "
            f"cost_ms={request_done_ms - request_start_ms} diff={diff_text} "
            f"ws_v={snapshot.volume} rest_v={rest.volume} "
            f"ws_q={snapshot.quote_volume} rest_q={rest.quote_volume} "
            f"ws_n={snapshot.trades} rest_n={rest.trades}"
        )

    async def _fetch_rest(self, snapshot: KlineSnapshot) -> KlineSnapshot | None:
        if self._client is None:
            return None
        params = {
            "symbol": snapshot.symbol,
            "interval": snapshot.interval,
            "startTime": snapshot.open_time_ms,
            "endTime": snapshot.close_time_ms,
            "limit": 3,
        }
        async with self._sem:
            resp = await self._client.get(f"{REST_BASE}/fapi/v1/klines", params=params)
            resp.raise_for_status()
            rows = resp.json()
        if not isinstance(rows, list):
            return None
        for row in reversed(rows):
            if not isinstance(row, list) or len(row) < 11:
                continue
            if int(row[0]) != snapshot.open_time_ms or int(row[6]) != snapshot.close_time_ms:
                continue
            return KlineSnapshot(
                symbol=snapshot.symbol,
                interval=snapshot.interval,
                open_time_ms=int(row[0]),
                close_time_ms=int(row[6]),
                open=str(row[1]),
                high=str(row[2]),
                low=str(row[3]),
                close=str(row[4]),
                volume=str(row[5]),
                quote_volume=str(row[7]),
                trades=int(row[8]),
                taker_buy_volume=str(row[9]),
                taker_buy_quote_volume=str(row[10]),
                event_time_ms=snapshot.close_time_ms,
                recv_ms=now_ms(),
                shard_id="rest",
            )
        return None


def compare_snapshots(ws: KlineSnapshot, rest: KlineSnapshot) -> list[str]:
    diffs: list[str] = []
    if ws.open_time_ms != rest.open_time_ms:
        diffs.append("open_time")
    if ws.close_time_ms != rest.close_time_ms:
        diffs.append("close_time")
    for field_name in _COMPARE_FIELDS:
        if not decimal_equal(getattr(ws, field_name), getattr(rest, field_name)):
            diffs.append(field_name)
    return diffs


class KlineSubscriber:
    def __init__(self, shards: list[ShardSpec], symbols: list[str], intervals: tuple[str, ...]) -> None:
        self.shards = shards
        self.symbols = symbols
        self.intervals = intervals
        self.trackers = {interval: BitmapBatchTracker(symbols) for interval in intervals}
        self.raw_queue: asyncio.Queue[tuple[ShardSpec, bytes, int]]
        self.tracker_queue: asyncio.Queue[tuple[KlineSnapshot, int, int]]
        self.log_queue: asyncio.Queue[str]
        self.audit: RestAuditor
        self._stats_closed: dict[str, int] = {interval: 0 for interval in intervals}
        self._stats_skipped = 0
        self._stats_raw_dropped = 0

    def _init_queues(self) -> None:
        self.raw_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE or 0)
        self.tracker_queue = asyncio.Queue()
        self.log_queue = asyncio.Queue(maxsize=LOG_QUEUE_MAXSIZE)
        self.audit = RestAuditor(self._log)

    async def run(self) -> None:
        self._init_queues()
        audit_tasks = await self.audit.start()
        print(
            f"symbols={len(self.symbols)} intervals={','.join(self.intervals)} shards={len(self.shards)} "
            f"shard_size~{SHARD_SIZE} processors={PROCESSOR_WORKERS} log_each={LOG_EACH_KLINE} "
            f"ws_base={STREAM_PREFIX.split('?streams=', 1)[0]} proxy={BINANCE_WS_PROXY_URL or '-'} "
            f"ping_interval={PING_INTERVAL} ping_timeout={PING_TIMEOUT}",
            flush=True,
        )
        tasks = [
            asyncio.create_task(self._log_writer(), name="log-writer"),
            asyncio.create_task(self._tracker_loop(), name="tracker"),
            *[asyncio.create_task(self._processor(i), name=f"proc-{i}") for i in range(PROCESSOR_WORKERS)],
            *[asyncio.create_task(self._run_shard(shard), name=f"shard-{shard.shard_id}") for shard in self.shards],
            asyncio.create_task(self._stats_reporter(), name="stats"),
            *audit_tasks,
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.audit.close()

    async def _log_writer(self) -> None:
        while True:
            line = await self.log_queue.get()
            sys.stdout.write(line)
            if not line.endswith("\n"):
                sys.stdout.write("\n")
            sys.stdout.flush()

    def _log(self, msg: str) -> None:
        try:
            self.log_queue.put_nowait(msg)
        except asyncio.QueueFull:
            pass

    async def _stats_reporter(self) -> None:
        while True:
            await asyncio.sleep(60)
            closed = ",".join(f"{interval}:{self._stats_closed.get(interval, 0)}" for interval in self.intervals)
            self._log(
                f"[stats] closed={closed} fast_skipped={self._stats_skipped} "
                f"raw_dropped={self._stats_raw_dropped} raw_q={self.raw_queue.qsize()} "
                f"tracker_q={self.tracker_queue.qsize()} log_q={self.log_queue.qsize()}"
            )

    async def _run_shard(self, shard: ShardSpec) -> None:
        backoff = 1.0
        while True:
            try:
                self._log(f"[shard {shard.shard_id}] connecting streams={len(shard.symbols)}")
                connect_kwargs = {
                    "compression": None,
                    "ping_interval": PING_INTERVAL,
                    "ping_timeout": PING_TIMEOUT,
                    "max_queue": None,
                    "open_timeout": OPEN_TIMEOUT,
                }
                if BINANCE_WS_PROXY_URL:
                    if _CONNECT_SUPPORTS_PROXY:
                        connect_kwargs["proxy"] = BINANCE_WS_PROXY_URL
                    else:
                        self._log(
                            f"[shard {shard.shard_id}] proxy ignored: websockets version does not support proxy=..."
                        )
                async with websockets.connect(shard.url, **connect_kwargs) as ws:
                    self._log(f"[shard {shard.shard_id}] connected streams={len(shard.symbols)}")
                    backoff = 1.0
                    async for raw in ws:
                        if isinstance(raw, str):
                            raw = raw.encode()
                        recv_ns = time.time_ns()
                        try:
                            self.raw_queue.put_nowait((shard, raw, recv_ns))
                        except asyncio.QueueFull:
                            self._stats_raw_dropped += 1
            except ConnectionClosed as exc:
                self._log(f"[shard {shard.shard_id}] closed: {exc}")
            except Exception as exc:
                self._log(f"[shard {shard.shard_id}] error: {exc}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _processor(self, worker_id: int) -> None:
        while True:
            shard, raw, recv_ns = await self.raw_queue.get()
            try:
                if not _raw_has_closed_kline_marker(raw):
                    self._stats_skipped += 1
                    continue
                msg = orjson.loads(raw)
            except orjson.JSONDecodeError:
                continue

            if not isinstance(msg, dict):
                continue
            snapshot = KlineSnapshot.from_payload(msg, recv_ns=recv_ns, shard_id=shard.shard_id)
            if snapshot is None:
                continue
            if snapshot.interval != shard.interval:
                self._log(
                    f"[parse warn] shard={shard.shard_id} expected_interval={shard.interval} got={snapshot.interval}"
                )
                continue
            if snapshot.interval not in self.trackers:
                continue

            proc_ns = time.time_ns()
            parse_latency_ms = (proc_ns - recv_ns) / 1_000_000
            self._stats_closed[snapshot.interval] = self._stats_closed.get(snapshot.interval, 0) + 1
            self.audit.observe(snapshot)

            if LOG_EACH_KLINE:
                self._log(
                    f"[closed] interval={snapshot.interval} shard={shard.shard_id} worker={worker_id} "
                    f"symbol={snapshot.symbol} open={fmt_ts(snapshot.open_time_ms)} "
                    f"close={fmt_ts(snapshot.close_time_ms)} c={snapshot.close} "
                    f"v={snapshot.volume} q={snapshot.quote_volume} n={snapshot.trades} "
                    f"recv_minus_event_ms={snapshot.recv_ms - snapshot.event_time_ms} "
                    f"parse_ms={parse_latency_ms:.3f}"
                )

            self.tracker_queue.put_nowait((snapshot, recv_ns, proc_ns))

    async def _tracker_loop(self) -> None:
        while True:
            snapshot, recv_ns, proc_ns = await self.tracker_queue.get()
            tracker = self.trackers.get(snapshot.interval)
            if tracker is None:
                continue

            done, progress, batch_stats = tracker.on_closed(
                snapshot=snapshot,
                recv_ns=recv_ns,
                proc_ns=proc_ns,
            )

            if LOG_PROGRESS_EVERY and progress > 0 and progress % LOG_PROGRESS_EVERY == 0:
                self._log(
                    f"[progress] interval={snapshot.interval} open={fmt_ts(snapshot.open_time_ms)} "
                    f"{progress}/{tracker.expected}"
                )

            if done:
                done_ns = time.time_ns()
                done_ms = done_ns // 1_000_000
                stats = batch_stats or BatchStats(close_time_ms=snapshot.close_time_ms)
                first_recv_ms = ns_to_ms(stats.first_recv_ns)
                last_recv_ms = ns_to_ms(stats.last_recv_ns)
                first_proc_ms = ns_to_ms(stats.first_proc_ns)
                close_to_first_recv = first_recv_ms - snapshot.close_time_ms
                close_to_last_recv = last_recv_ms - snapshot.close_time_ms
                close_to_done = done_ms - snapshot.close_time_ms
                recv_spread = ns_span_ms(stats.first_recv_ns, stats.last_recv_ns)
                proc_spread = ns_span_ms(stats.first_proc_ns, stats.last_proc_ns)
                event_spread = max(0, stats.event_max_ms - stats.event_min_ms)
                event_to_last_recv = last_recv_ms - stats.event_min_ms if stats.event_min_ms > 0 else 0
                self._log(
                    f"\n*** 全部 {tracker.expected} 个交易对 {snapshot.interval} K线已收盘 ***\n"
                    f"  开盘时间: {fmt_ts(snapshot.open_time_ms)}\n"
                    f"  K线结束: {fmt_ts(snapshot.close_time_ms)}\n"
                    f"  首条收包: {fmt_ts(first_recv_ms)} symbol={stats.first_symbol} shard={stats.first_shard_id}\n"
                    f"  末条收包: {fmt_ts(last_recv_ms)} symbol={stats.last_symbol} shard={stats.last_shard_id}\n"
                    f"  首条处理完成: {fmt_ts(first_proc_ms)}\n"
                    f"  全量汇总完成: {fmt_ts(done_ms)}\n"
                    f"  收盘至首条收包: {close_to_first_recv:+d} ms\n"
                    f"  收盘至末条收包: {close_to_last_recv:+d} ms\n"
                    f"  收盘至全量汇总完成: {close_to_done:+d} ms\n"
                    f"  全市场 Binance WS事件时间分布跨度(max E - min E): {event_spread} ms\n"
                    f"  全市场 WS入站分布跨度(first recv -> last recv): {recv_spread:.3f} ms\n"
                    f"  全市场 处理完成分布跨度(first parse -> last parse): {proc_spread:.3f} ms\n"
                    f"  首个 Binance事件(E) -> 末条收包: {event_to_last_recv} ms\n"
                    f"  单条 Binance WS事件(E)->本机收包: {fmt_stats(stats.event_to_recv_ms)}\n"
                    f"  本机收包->解析完成: {fmt_stats(stats.recv_to_parse_ms)}\n"
                    f"  按 Binance WS 分片统计: {stats.shard_lines()}\n"
                    f"  末条收包至汇总耗时: {(done_ns - recv_ns) / 1_000_000:.3f} ms\n"
                )


def _install_uvloop() -> None:
    if not _env_bool("USE_UVLOOP", True):
        return
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass


async def _async_main() -> None:
    symbols = load_symbols()
    shards = build_ws_shards(symbols, KLINE_INTERVALS)
    if len(symbols) != 528:
        print(f"warning: expected 528 symbols, got {len(symbols)}", file=sys.stderr)
    await KlineSubscriber(shards, symbols, KLINE_INTERVALS).run()


def main() -> None:
    _install_uvloop()
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\nstopped", flush=True)


if __name__ == "__main__":
    main()
