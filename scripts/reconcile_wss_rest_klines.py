#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
import httpx
import orjson


DEFAULT_MAJOR_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT"
DEFAULT_OFFSETS_MS = "300,3300,6300"
DEFAULT_FULL_OFFSETS_MS = "all"
COMPARE_FIELDS = (
    "open_time_ms",
    "close_time_ms",
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
FIELD_CODES = {
    "open_time_ms": "OT",
    "close_time_ms": "CT",
    "open": "O",
    "high": "H",
    "low": "L",
    "close": "C",
    "volume": "V",
    "quote_volume": "QV",
    "trades": "T",
    "taker_buy_volume": "TBV",
    "taker_buy_quote_volume": "TBQ",
}


@dataclass
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
    source: str
    exchange_event_ms: int | None = None
    receive_ms: int | None = None
    shard_id: str = ""


class RequestPacer:
    def __init__(self, min_interval_ms: int) -> None:
        self._min_interval_ms = max(0, min_interval_ms)
        self._lock = asyncio.Lock()
        self._next_ms = 0

    async def wait(self) -> None:
        if self._min_interval_ms <= 0:
            return
        async with self._lock:
            now = now_ms()
            wait_ms = self._next_ms - now
            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1000)
                now = now_ms()
            self._next_ms = max(now, self._next_ms) + self._min_interval_ms


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_csv(raw: str) -> list[str]:
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def parse_int_csv(raw: str) -> list[int]:
    out: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if item:
            out.append(int(item))
    return sorted(set(out))


def stream_base_from_ws_base(ws_base: str) -> str:
    ws_base = ws_base.split("?", 1)[0].rstrip("/")
    if ws_base.endswith("/stream"):
        return ws_base
    if ws_base.endswith("/ws"):
        return f"{ws_base[:-3]}/stream"
    return f"{ws_base}/stream"


def proxy_from_env(dest_host: str) -> str | None:
    no_proxy = os.getenv("NO_PROXY") or os.getenv("no_proxy") or ""
    no_proxy_items = [item.strip().lower() for item in no_proxy.split(",") if item.strip()]
    if dest_host.lower() in no_proxy_items:
        return None
    return (
        os.getenv("BINANCE_HTTP_PROXY")
        or os.getenv("HTTPS_PROXY")
        or os.getenv("HTTP_PROXY")
        or None
    )


def ms_text(value: int | None) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(value / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def md_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def decimal_equal(left: str, right: str) -> bool:
    try:
        return Decimal(left) == Decimal(right)
    except (InvalidOperation, ValueError):
        return left == right


def ws_snapshot_from_payload(payload: dict[str, Any], receive_ms: int, shard_id: str) -> KlineSnapshot | None:
    data = payload.get("data", payload)
    if not isinstance(data, dict):
        return None
    k = data.get("k")
    if not isinstance(k, dict) or not k.get("x"):
        return None
    return KlineSnapshot(
        symbol=str(k["s"]).upper(),
        interval=str(k["i"]),
        open_time_ms=int(k["t"]),
        close_time_ms=int(k["T"]),
        open=str(k["o"]),
        high=str(k["h"]),
        low=str(k["l"]),
        close=str(k["c"]),
        volume=str(k["v"]),
        quote_volume=str(k["q"]),
        trades=int(k.get("n") or 0),
        taker_buy_volume=str(k.get("V") or "0"),
        taker_buy_quote_volume=str(k.get("Q") or "0"),
        source="wss",
        exchange_event_ms=int(data.get("E") or 0),
        receive_ms=receive_ms,
        shard_id=shard_id,
    )


def rest_snapshot_from_row(symbol: str, interval: str, row: list[Any], receive_ms: int) -> KlineSnapshot:
    return KlineSnapshot(
        symbol=symbol,
        interval=interval,
        open_time_ms=int(row[0]),
        close_time_ms=int(row[6]),
        open=str(row[1]),
        high=str(row[2]),
        low=str(row[3]),
        close=str(row[4]),
        volume=str(row[5]),
        quote_volume=str(row[7]),
        trades=int(row[8]) if len(row) > 8 else 0,
        taker_buy_volume=str(row[9]) if len(row) > 9 else "0",
        taker_buy_quote_volume=str(row[10]) if len(row) > 10 else "0",
        source="rest",
        receive_ms=receive_ms,
    )


def compare_snapshots(ws: KlineSnapshot | None, rest: KlineSnapshot | None) -> dict[str, Any]:
    if ws is None:
        return {"match": False, "reason": "missing_wss", "diffs": {}}
    if rest is None:
        return {"match": False, "reason": "missing_rest", "diffs": {}}
    diffs: dict[str, dict[str, Any]] = {}
    for field in COMPARE_FIELDS:
        ws_value = getattr(ws, field)
        rest_value = getattr(rest, field)
        if isinstance(ws_value, str) or isinstance(rest_value, str):
            same = decimal_equal(str(ws_value), str(rest_value))
        else:
            same = ws_value == rest_value
        if not same:
            diffs[field] = {"wss": ws_value, "rest": rest_value}
    return {"match": not diffs, "reason": "" if not diffs else "field_diff", "diffs": diffs}


def diff_codes(diffs: dict[str, Any]) -> str:
    if not diffs:
        return "-"
    return ",".join(FIELD_CODES.get(field, field) for field in diffs.keys())


def volume_ratio(ws: KlineSnapshot | None, rest: KlineSnapshot | None, field: str) -> str:
    if ws is None or rest is None:
        return ""
    try:
        left = Decimal(str(getattr(ws, field)))
        right = Decimal(str(getattr(rest, field)))
    except (InvalidOperation, ValueError):
        return ""
    if left == 0 or right == 0:
        return ""
    ratio = max(left / right, right / left)
    return f"{ratio:.6f}"


def safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


async def fetch_symbols(rest_base: str, proxy: str | None, timeout_seconds: float) -> list[str]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds), proxy=proxy) as client:
        resp = await client.get(f"{rest_base.rstrip('/')}/fapi/v1/exchangeInfo")
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
        symbol = item.get("symbol")
        if symbol:
            symbols.append(str(symbol).upper())
    return sorted(set(symbols))


async def fetch_server_time_offset(rest_base: str, proxy: str | None, timeout_seconds: float) -> dict[str, Any]:
    url = f"{rest_base.rstrip('/')}/fapi/v1/time"
    t0 = now_ms()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds), proxy=proxy) as client:
            resp = await client.get(url)
            t1 = now_ms()
            resp.raise_for_status()
            server_time = int(resp.json()["serverTime"])
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__, "message": str(exc)}
    midpoint = (t0 + t1) / 2
    return {
        "ok": True,
        "server_time_ms": server_time,
        "local_before_ms": t0,
        "local_after_ms": t1,
        "rtt_ms": t1 - t0,
        "offset_ms": server_time - midpoint,
    }


def build_symbol_batches(symbols: list[str], batch_size: int) -> list[list[str]]:
    batch_size = max(1, batch_size)
    return [symbols[idx : idx + batch_size] for idx in range(0, len(symbols), batch_size)]


def should_sample(interval: str, close_time_ms: int, sample_minutes: set[int]) -> bool:
    if interval != "1m":
        return True
    minute = datetime.fromtimestamp((close_time_ms + 1) / 1000).minute
    return minute in sample_minutes


async def fetch_rest_snapshot(
    client: httpx.AsyncClient,
    pacer: RequestPacer,
    rest_base: str,
    symbol: str,
    interval: str,
    close_time_ms: int,
) -> dict[str, Any]:
    await pacer.wait()
    request_start_ms = now_ms()
    url = f"{rest_base.rstrip('/')}/fapi/v1/klines"
    try:
        resp = await client.get(
            url,
            params={
                "symbol": symbol,
                "interval": interval,
                "limit": 3,
                "endTime": close_time_ms,
            },
        )
        receive_ms = now_ms()
        used_weight = resp.headers.get("x-mbx-used-weight-1m") or resp.headers.get("x-mbx-used-weight")
        status = resp.status_code
        if status in (418, 429):
            return {
                "symbol": symbol,
                "status": status,
                "error": "rate_limited",
                "used_weight": used_weight,
                "request_start_ms": request_start_ms,
                "response_ms": receive_ms,
                "request_cost_ms": receive_ms - request_start_ms,
            }
        resp.raise_for_status()
        rows = resp.json()
        if isinstance(rows, list):
            for row in reversed(rows):
                if isinstance(row, list) and len(row) >= 8 and int(row[6]) == close_time_ms:
                    return {
                        "symbol": symbol,
                        "status": status,
                        "used_weight": used_weight,
                        "request_start_ms": request_start_ms,
                        "response_ms": receive_ms,
                        "request_cost_ms": receive_ms - request_start_ms,
                        "rest": asdict(rest_snapshot_from_row(symbol, interval, row, receive_ms)),
                    }
        return {
            "symbol": symbol,
            "status": status,
            "error": "missing_close_time",
            "used_weight": used_weight,
            "request_start_ms": request_start_ms,
            "response_ms": receive_ms,
            "request_cost_ms": receive_ms - request_start_ms,
        }
    except Exception as exc:
        response_ms = now_ms()
        return {
            "symbol": symbol,
            "status": 0,
            "error": type(exc).__name__,
            "message": str(exc),
            "request_start_ms": request_start_ms,
            "response_ms": response_ms,
            "request_cost_ms": response_ms - request_start_ms,
        }


async def fetch_rest_group(
    client: httpx.AsyncClient,
    pacer: RequestPacer,
    args: argparse.Namespace,
    group_name: str,
    symbols: list[str],
    ws_by_symbol: dict[str, KlineSnapshot],
    interval: str,
    close_time_ms: int,
) -> dict[str, Any]:
    start_ms = now_ms()
    sem = asyncio.Semaphore(max(1, args.rest_concurrency))

    async def one(symbol: str) -> dict[str, Any]:
        async with sem:
            return await fetch_rest_snapshot(client, pacer, args.rest_base, symbol, interval, close_time_ms)

    results = await asyncio.gather(*(one(symbol) for symbol in symbols))
    end_ms = now_ms()
    items: dict[str, Any] = {}
    mismatch_symbols: list[str] = []
    rate_limited_symbols: list[str] = []
    max_volume_ratio = Decimal("1")
    max_quote_ratio = Decimal("1")
    for result in results:
        symbol = str(result["symbol"])
        rest_raw = result.get("rest")
        rest = KlineSnapshot(**rest_raw) if isinstance(rest_raw, dict) else None
        comparison = compare_snapshots(ws_by_symbol.get(symbol), rest)
        if not comparison["match"]:
            mismatch_symbols.append(symbol)
        if result.get("status") in (418, 429):
            rate_limited_symbols.append(symbol)
        symbol_ws = ws_by_symbol.get(symbol)
        ws_receive_ms = int(symbol_ws.receive_ms or 0) if symbol_ws is not None else 0
        if ws_receive_ms > 0:
            result["request_start_after_wss_symbol_ms"] = safe_int(result.get("request_start_ms")) - ws_receive_ms
            result["response_after_wss_symbol_ms"] = safe_int(result.get("response_ms")) - ws_receive_ms
        for field in ("volume", "quote_volume"):
            ratio_text = volume_ratio(ws_by_symbol.get(symbol), rest, field)
            if not ratio_text:
                continue
            ratio = Decimal(ratio_text)
            if field == "volume":
                max_volume_ratio = max(max_volume_ratio, ratio)
            else:
                max_quote_ratio = max(max_quote_ratio, ratio)
        items[symbol] = {
            **result,
            "comparison": comparison,
            "volume_ratio": volume_ratio(ws_by_symbol.get(symbol), rest, "volume"),
            "quote_volume_ratio": volume_ratio(ws_by_symbol.get(symbol), rest, "quote_volume"),
        }
    request_starts = [safe_int(result.get("request_start_ms")) for result in results if safe_int(result.get("request_start_ms")) > 0]
    response_times = [safe_int(result.get("response_ms")) for result in results if safe_int(result.get("response_ms")) > 0]
    first_wss_receive = min((int(snap.receive_ms or 0) for snap in ws_by_symbol.values() if snap.receive_ms), default=0)
    last_wss_receive = max((int(snap.receive_ms or 0) for snap in ws_by_symbol.values() if snap.receive_ms), default=0)
    first_request_ms = min(request_starts) if request_starts else start_ms
    last_response_ms = max(response_times) if response_times else end_ms
    return {
        "group": group_name,
        "symbols": len(symbols),
        "request_start_ms": start_ms,
        "request_end_ms": end_ms,
        "request_cost_ms": end_ms - start_ms,
        "first_request_ms": first_request_ms,
        "last_response_ms": last_response_ms,
        "request_span_ms": last_response_ms - first_request_ms,
        "first_request_after_wss_first_ms": first_request_ms - first_wss_receive if first_wss_receive else None,
        "last_response_after_wss_first_ms": last_response_ms - first_wss_receive if first_wss_receive else None,
        "last_response_after_wss_last_ms": last_response_ms - last_wss_receive if last_wss_receive else None,
        "mismatch_symbols": mismatch_symbols,
        "all_match": not mismatch_symbols,
        "rate_limited_symbols": rate_limited_symbols,
        "max_volume_ratio": str(max_volume_ratio),
        "max_quote_volume_ratio": str(max_quote_ratio),
        "items": items,
    }


def wss_summary(ws_by_symbol: dict[str, KlineSnapshot], expected_symbols: list[str]) -> dict[str, Any]:
    receive_values = [snap.receive_ms for snap in ws_by_symbol.values() if snap.receive_ms]
    event_values = [snap.exchange_event_ms for snap in ws_by_symbol.values() if snap.exchange_event_ms]
    missing = sorted(set(expected_symbols) - set(ws_by_symbol.keys()))
    first_receive = min(receive_values) if receive_values else None
    last_receive = max(receive_values) if receive_values else None
    first_event = min(event_values) if event_values else None
    last_event = max(event_values) if event_values else None
    return {
        "expected_symbols": len(expected_symbols),
        "got_symbols": len(ws_by_symbol),
        "missing_symbols": missing,
        "first_receive_ms": first_receive,
        "last_receive_ms": last_receive,
        "receive_span_ms": (last_receive - first_receive) if first_receive and last_receive else None,
        "first_exchange_event_ms": first_event,
        "last_exchange_event_ms": last_event,
        "exchange_event_span_ms": (last_event - first_event) if first_event and last_event else None,
    }


async def reconcile_batch(
    args: argparse.Namespace,
    interval: str,
    expected_symbols: list[str],
    major_symbols: list[str],
    ws_by_symbol: dict[str, KlineSnapshot],
    close_time_ms: int,
    server_time_offset: dict[str, Any],
) -> tuple[Path, Path]:
    out_dir = Path(args.output_dir) / interval
    out_dir.mkdir(parents=True, exist_ok=True)
    close_stamp = datetime.fromtimestamp(close_time_ms / 1000).strftime("%Y%m%d_%H%M%S")
    base = out_dir / f"wss_rest_reconcile_{interval}_{close_stamp}_{close_time_ms}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")

    offset_set = set(args.full_offsets_ms)
    if args.full_offsets_all:
        offset_set = set(args.offsets_ms)
    expected_symbol_set = set(expected_symbols)
    major_request_symbols = [symbol for symbol in major_symbols if symbol in expected_symbol_set]
    full_request_symbols = list(expected_symbols)
    first_receive_ms = min((snap.receive_ms or 0 for snap in ws_by_symbol.values()), default=now_ms())
    rounds: list[dict[str, Any]] = []
    payload = {
        "config": {
            "interval": interval,
            "offsets_ms": args.offsets_ms,
            "full_offsets_ms": sorted(offset_set),
            "rest_concurrency": args.rest_concurrency,
            "rest_min_interval_ms": args.rest_min_interval_ms,
            "rest_base": args.rest_base,
            "ws_base": args.ws_base,
            "expected_symbols": len(expected_symbols),
            "major_symbols": major_symbols,
        },
        "server_time_offset": server_time_offset,
        "target": {
            "interval": interval,
            "close_time_ms": close_time_ms,
            "close_time": ms_text(close_time_ms),
        },
        "wss": {},
        "rest_rounds": rounds,
    }

    def refresh_wss_payload() -> None:
        payload["wss"] = {
            "summary": wss_summary(ws_by_symbol, expected_symbols),
            "rows": {symbol: asdict(snap) for symbol, snap in sorted(ws_by_symbol.items())},
        }

    refresh_wss_payload()
    write_reconcile_outputs(json_path, md_path, payload)
    pacer = RequestPacer(args.rest_min_interval_ms)
    write_lock = asyncio.Lock()
    timeout = httpx.Timeout(args.rest_timeout_seconds)
    proxy = args.rest_proxy or os.getenv("BINANCE_HTTP_PROXY") or None

    async def run_round(client: httpx.AsyncClient, offset_ms: int) -> None:
        scheduled_ms = first_receive_ms + offset_ms
        delay_ms = scheduled_ms - now_ms()
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000)
        round_start_ms = now_ms()
        major = await fetch_rest_group(
            client,
            pacer,
            args,
            "major",
            major_request_symbols,
            ws_by_symbol,
            interval,
            close_time_ms,
        )
        full = None
        if args.include_full_rest and offset_ms in offset_set:
            full = await fetch_rest_group(
                client,
                pacer,
                args,
                "full",
                full_request_symbols,
                ws_by_symbol,
                interval,
                close_time_ms,
            )
        round_end_ms = now_ms()
        round_item = {
            "offset_ms": offset_ms,
            "scheduled_ms": scheduled_ms,
            "actual_start_ms": round_start_ms,
            "actual_offset_ms": round_start_ms - first_receive_ms,
            "round_end_ms": round_end_ms,
            "round_cost_ms": round_end_ms - round_start_ms,
            "major": major,
            "full": full,
        }
        async with write_lock:
            rounds.append(round_item)
            rounds.sort(key=lambda item: item["offset_ms"])
            refresh_wss_payload()
            write_reconcile_outputs(json_path, md_path, payload)
        print(
            json.dumps(
                {
                    "event": "rest_round",
                    "interval": interval,
                    "close_time_ms": close_time_ms,
                    "offset_ms": offset_ms,
                    "actual_offset_ms": round_item["actual_offset_ms"],
                    "major_cost_ms": major["request_cost_ms"],
                    "major_mismatch": len(major["mismatch_symbols"]),
                    "full_cost_ms": full["request_cost_ms"] if full else None,
                    "full_mismatch": len(full["mismatch_symbols"]) if full else None,
                    "rate_limited_major": len(major["rate_limited_symbols"]),
                    "rate_limited_full": len(full["rate_limited_symbols"]) if full else None,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    async with httpx.AsyncClient(timeout=timeout, proxy=proxy) as client:
        await asyncio.gather(*(run_round(client, offset_ms) for offset_ms in args.offsets_ms))

    write_reconcile_outputs(json_path, md_path, payload)
    return json_path, md_path


def write_reconcile_outputs(json_path: Path, md_path: Path, payload: dict[str, Any]) -> None:
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(payload, json_path.name), encoding="utf-8")


def render_markdown(payload: dict[str, Any], json_name: str) -> str:
    interval = payload["target"]["interval"]
    close_time_ms = payload["target"]["close_time_ms"]
    wss = payload["wss"]["summary"]
    lines = [
        f"# WSS vs REST reconcile {interval}",
        "",
        f"- close_time_ms: {close_time_ms}",
        f"- close_time: {payload['target']['close_time']}",
        f"- json: `{json_name}`",
        f"- expected_symbols: {wss['expected_symbols']}",
        f"- wss_got_symbols: {wss['got_symbols']}",
        f"- wss_receive_span_ms: {wss['receive_span_ms']}",
        f"- wss_exchange_event_span_ms: {wss['exchange_event_span_ms']}",
        f"- server_time_offset_ms: {payload['server_time_offset'].get('offset_ms')}",
        "",
        "## REST Timeline",
        "",
        "| offset_ms | actual_offset_ms | major_cost_ms | major_resp_after_wss_last_ms | major_match | major_mismatch | major_max_v_ratio | major_max_qv_ratio | full_cost_ms | full_resp_after_wss_last_ms | full_match | full_mismatch | full_max_v_ratio | full_max_qv_ratio | rate_limit |",
        "|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|",
    ]
    for item in payload["rest_rounds"]:
        major = item["major"]
        full = item.get("full") or {}
        rate_limit = len(major.get("rate_limited_symbols") or []) + len(full.get("rate_limited_symbols") or [])
        lines.append(
            "| "
            + " | ".join(
                [
                    md_cell(item["offset_ms"]),
                    md_cell(item["actual_offset_ms"]),
                    md_cell(major.get("request_cost_ms")),
                    md_cell(major.get("last_response_after_wss_last_ms")),
                    md_cell(major.get("all_match")),
                    md_cell(len(major.get("mismatch_symbols") or [])),
                    md_cell(major.get("max_volume_ratio")),
                    md_cell(major.get("max_quote_volume_ratio")),
                    md_cell(full.get("request_cost_ms")),
                    md_cell(full.get("last_response_after_wss_last_ms")),
                    md_cell(full.get("all_match")),
                    md_cell(len(full.get("mismatch_symbols") or []) if full else ""),
                    md_cell(full.get("max_volume_ratio")),
                    md_cell(full.get("max_quote_volume_ratio")),
                    md_cell(rate_limit),
                ],
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## REST Group Timing",
            "",
            "| offset_ms | group | symbols | first_request_ms | last_response_ms | request_span_ms | request_cost_ms | first_req_after_wss_first_ms | last_resp_after_wss_first_ms | last_resp_after_wss_last_ms | rate_limited_symbols |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ],
    )
    for item in payload["rest_rounds"]:
        for group_name in ("major", "full"):
            group = item.get(group_name)
            if not group:
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        md_cell(item["offset_ms"]),
                        md_cell(group_name),
                        md_cell(group.get("symbols")),
                        md_cell(group.get("first_request_ms")),
                        md_cell(group.get("last_response_ms")),
                        md_cell(group.get("request_span_ms")),
                        md_cell(group.get("request_cost_ms")),
                        md_cell(group.get("first_request_after_wss_first_ms")),
                        md_cell(group.get("last_response_after_wss_first_ms")),
                        md_cell(group.get("last_response_after_wss_last_ms")),
                        md_cell(len(group.get("rate_limited_symbols") or [])),
                    ],
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "## Major Details",
            "",
            "| offset_ms | symbol | match | diff | rest_request_cost_ms | rest_response_after_wss_symbol_ms | wss_volume | rest_volume | volume_ratio | wss_quote_volume | rest_quote_volume | quote_volume_ratio | status | used_weight | error |",
            "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ],
    )
    wss_rows = payload["wss"]["rows"]
    for item in payload["rest_rounds"]:
        offset_ms = item["offset_ms"]
        major_items = item["major"].get("items") or {}
        for symbol, detail in sorted(major_items.items()):
            rest = detail.get("rest") or {}
            ws = wss_rows.get(symbol) or {}
            comparison = detail.get("comparison") or {}
            lines.append(
                "| "
                + " | ".join(
                    [
                        md_cell(offset_ms),
                        md_cell(symbol),
                        md_cell(comparison.get("match")),
                        md_cell(diff_codes(comparison.get("diffs") or {})),
                        md_cell(detail.get("request_cost_ms")),
                        md_cell(detail.get("response_after_wss_symbol_ms")),
                        md_cell(ws.get("volume")),
                        md_cell(rest.get("volume")),
                        md_cell(detail.get("volume_ratio")),
                        md_cell(ws.get("quote_volume")),
                        md_cell(rest.get("quote_volume")),
                        md_cell(detail.get("quote_volume_ratio")),
                        md_cell(detail.get("status")),
                        md_cell(detail.get("used_weight")),
                        md_cell(detail.get("error")),
                    ],
                )
                + " |"
            )
    lines.append("")
    return "\n".join(lines)


async def consume_interval(
    args: argparse.Namespace,
    interval: str,
    expected_symbols: list[str],
    major_symbols: list[str],
    server_time_offset: dict[str, Any],
) -> None:
    stream_base = stream_base_from_ws_base(args.ws_base)
    symbol_batches = build_symbol_batches(expected_symbols, args.ws_shard_size)
    queue: asyncio.Queue[KlineSnapshot] = asyncio.Queue(maxsize=100_000)
    done = asyncio.Event()
    sample_minutes = set(args.one_m_sample_minutes)
    completed = 0
    running_reconciles: dict[int, asyncio.Task[tuple[Path, Path]]] = {}
    finished_reconciles: set[int] = set()
    batches: dict[int, dict[str, KlineSnapshot]] = {}
    first_seen_ms: dict[int, int] = {}

    async def run_shard(idx: int, symbols: list[str]) -> None:
        streams = "/".join(f"{symbol.lower()}@kline_{interval}" for symbol in symbols)
        url = f"{stream_base}?streams={streams}"
        proxy = args.ws_proxy or proxy_from_env(urlparse(url).hostname or "")
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=60.0, sock_read=None)
        while not done.is_set():
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, proxy=proxy, autoping=True, heartbeat=None) as ws:
                        print(
                            json.dumps(
                                {
                                    "event": "wss_connected",
                                    "interval": interval,
                                    "shard": idx,
                                    "symbols": len(symbols),
                                    "proxy": proxy or "",
                                },
                                ensure_ascii=False,
                            ),
                            flush=True,
                        )
                        while not done.is_set():
                            msg = await ws.receive(timeout=10.0)
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                raw = msg.data.encode("utf-8")
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                raw = msg.data
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            else:
                                continue
                            if b'"x":true' not in raw and b'"x": true' not in raw:
                                continue
                            snap = ws_snapshot_from_payload(orjson.loads(raw), now_ms(), f"{interval}#{idx}")
                            if snap is not None:
                                await queue.put(snap)
            except Exception as exc:
                if not done.is_set():
                    print(
                        json.dumps(
                            {
                                "event": "wss_reconnect",
                                "interval": interval,
                                "shard": idx,
                                "error": str(exc),
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
                    await asyncio.sleep(2)

    async def finish_reconciles() -> None:
        nonlocal completed
        for close_time_ms, task in list(running_reconciles.items()):
            if not task.done():
                continue
            running_reconciles.pop(close_time_ms, None)
            try:
                json_path, md_path = task.result()
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "event": "reconcile_failed",
                            "interval": interval,
                            "close_time_ms": close_time_ms,
                            "error": type(exc).__name__,
                            "message": str(exc),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                done.set()
                return
            finished_reconciles.add(close_time_ms)
            batches.pop(close_time_ms, None)
            first_seen_ms.pop(close_time_ms, None)
            completed += 1
            print(
                json.dumps(
                    {
                        "event": "reconcile_written",
                        "interval": interval,
                        "close_time_ms": close_time_ms,
                        "json_path": str(json_path),
                        "markdown_path": str(md_path),
                        "completed": completed,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            if args.rounds > 0 and completed >= args.rounds:
                done.set()
                return

    async def maybe_start_reconciles() -> None:
        for close_time_ms in sorted(list(batches.keys())):
            if close_time_ms in running_reconciles or close_time_ms in finished_reconciles:
                continue
            if not should_sample(interval, close_time_ms, sample_minutes):
                batches.pop(close_time_ms, None)
                first_seen_ms.pop(close_time_ms, None)
                continue
            ws_rows = batches.get(close_time_ms) or {}
            if not ws_rows:
                continue
            running_reconciles[close_time_ms] = asyncio.create_task(
                reconcile_batch(
                    args,
                    interval,
                    expected_symbols,
                    major_symbols,
                    ws_rows,
                    close_time_ms,
                    server_time_offset,
                ),
            )
            print(
                json.dumps(
                    {
                        "event": "reconcile_started",
                        "interval": interval,
                        "close_time_ms": close_time_ms,
                        "first_wss_receive_ms": first_seen_ms.get(close_time_ms),
                        "wss_symbols_at_start": len(ws_rows),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    tasks = [asyncio.create_task(run_shard(idx, batch)) for idx, batch in enumerate(symbol_batches, start=1)]
    try:
        while not done.is_set():
            await finish_reconciles()
            await maybe_start_reconciles()
            try:
                snap = await asyncio.wait_for(queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            if snap.interval != interval or snap.close_time_ms in finished_reconciles:
                continue
            close_rows = batches.setdefault(snap.close_time_ms, {})
            close_rows[snap.symbol] = snap
            first_seen_ms.setdefault(snap.close_time_ms, snap.receive_ms or now_ms())
    finally:
        done.set()
        for task in tasks:
            task.cancel()
        for task in running_reconciles.values():
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*running_reconciles.values(), return_exceptions=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Log Binance WSS closed-kline vs REST reconciliation snapshots.")
    parser.add_argument("--intervals", default="1m,1h", help="Comma separated intervals to monitor.")
    parser.add_argument("--major-symbols", default=DEFAULT_MAJOR_SYMBOLS)
    parser.add_argument("--symbols-1m", default="majors", help="'majors', 'all', or comma separated symbols.")
    parser.add_argument("--symbols-1h", default="all", help="'majors', 'all', or comma separated symbols.")
    parser.add_argument("--offsets-ms", default=DEFAULT_OFFSETS_MS)
    parser.add_argument("--full-offsets-ms", default=DEFAULT_FULL_OFFSETS_MS, help="'all' or comma separated offsets for full-symbol REST.")
    parser.add_argument("--include-full-rest", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--one-m-sample-minutes", default="5,35", help="Only write 1m files when close boundary minute is in this list.")
    parser.add_argument("--rounds", type=int, default=1, help="Files to write per interval. 0 means run forever.")
    parser.add_argument("--ws-base", default="wss://fstream.binance.com/market/stream")
    parser.add_argument("--rest-base", default="https://fapi.binance.com")
    parser.add_argument("--ws-proxy", default="")
    parser.add_argument("--rest-proxy", default="")
    parser.add_argument("--rest-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--rest-concurrency", type=int, default=8)
    parser.add_argument("--rest-min-interval-ms", type=int, default=50, help="Minimum gap between REST request starts.")
    parser.add_argument("--ws-shard-size", type=int, default=180)
    parser.add_argument("--output-dir", default="logs/reconcile")
    args = parser.parse_args()
    args.intervals = [item.lower() for item in parse_csv(args.intervals)]
    args.major_symbols = parse_csv(args.major_symbols)
    args.offsets_ms = parse_int_csv(args.offsets_ms)
    args.full_offsets_all = args.full_offsets_ms.strip().lower() == "all"
    args.full_offsets_ms = args.offsets_ms if args.full_offsets_all else parse_int_csv(args.full_offsets_ms)
    args.one_m_sample_minutes = parse_int_csv(args.one_m_sample_minutes)
    return args


def resolve_symbols(selection: str, all_symbols: list[str], major_symbols: list[str]) -> list[str]:
    raw = selection.strip()
    if raw.lower() == "all":
        return all_symbols
    if raw.lower() == "majors":
        return [symbol for symbol in major_symbols if symbol in set(all_symbols)]
    return [symbol for symbol in parse_csv(raw) if symbol in set(all_symbols)]


async def main_async() -> None:
    args = parse_args()
    rest_proxy = args.rest_proxy or os.getenv("BINANCE_HTTP_PROXY") or None
    all_symbols = await fetch_symbols(args.rest_base, rest_proxy, args.rest_timeout_seconds)
    if not all_symbols:
        raise SystemExit("No Binance symbols found")
    server_time_offset = await fetch_server_time_offset(args.rest_base, rest_proxy, args.rest_timeout_seconds)
    print(
        json.dumps(
            {
                "event": "startup",
                "intervals": args.intervals,
                "all_symbols": len(all_symbols),
                "major_symbols": args.major_symbols,
                "rest_min_interval_ms": args.rest_min_interval_ms,
                "server_time_offset": server_time_offset,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    tasks = []
    for interval in args.intervals:
        selection = args.symbols_1m if interval == "1m" else args.symbols_1h
        expected_symbols = resolve_symbols(selection, all_symbols, args.major_symbols)
        if not expected_symbols:
            raise SystemExit(f"No symbols selected for interval={interval}")
        tasks.append(
            asyncio.create_task(
                consume_interval(args, interval, expected_symbols, args.major_symbols, server_time_offset),
            ),
        )
    await asyncio.gather(*tasks)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
