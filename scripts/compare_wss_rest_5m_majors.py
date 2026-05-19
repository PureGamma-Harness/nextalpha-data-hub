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


DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT"
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
    service_ingest_ms: int | None = None


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_symbols(raw: str) -> list[str]:
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


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


def ws_snapshot_from_payload(payload: dict[str, Any]) -> KlineSnapshot | None:
    data = payload.get("data", payload)
    if not isinstance(data, dict):
        return None
    k = data.get("k")
    if not isinstance(k, dict) or not k.get("x"):
        return None
    return KlineSnapshot(
        symbol=str(k["s"]),
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
        service_ingest_ms=now_ms(),
    )


def rest_snapshot_from_row(symbol: str, interval: str, row: list[Any]) -> KlineSnapshot:
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
        exchange_event_ms=None,
        service_ingest_ms=now_ms(),
    )


def decimal_equal(left: str, right: str) -> bool:
    try:
        return Decimal(left) == Decimal(right)
    except (InvalidOperation, ValueError):
        return left == right


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


def md_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def diff_codes(diffs: dict[str, Any]) -> str:
    if not diffs:
        return "-"
    return ",".join(FIELD_CODES.get(field, field) for field in diffs.keys())


def first_all_match_round(rest_rounds: list[dict[str, Any]]) -> dict[str, Any] | None:
    for round_item in rest_rounds:
        if round_item.get("all_match"):
            return round_item
    return None


def first_stable_all_match_round(rest_rounds: list[dict[str, Any]]) -> dict[str, Any] | None:
    for idx, round_item in enumerate(rest_rounds):
        if round_item.get("all_match") and all(item.get("all_match") for item in rest_rounds[idx:]):
            return round_item
    return None


def first_symbol_match_round(rest_rounds: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    for round_item in rest_rounds:
        item = (round_item.get("symbols") or {}).get(symbol, {})
        comparison = item.get("comparison") or {}
        if comparison.get("match"):
            return round_item
    return None


def first_stable_symbol_match_round(rest_rounds: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    for idx, round_item in enumerate(rest_rounds):
        item = (round_item.get("symbols") or {}).get(symbol, {})
        comparison = item.get("comparison") or {}
        if not comparison.get("match"):
            continue
        remaining = rest_rounds[idx:]
        if all(((entry.get("symbols") or {}).get(symbol, {}).get("comparison") or {}).get("match") for entry in remaining):
            return round_item
    return None


def round_offset_text(round_item: dict[str, Any] | None, poll_interval_ms: int) -> str:
    if not round_item:
        return "-"
    poll_index = int(round_item.get("poll_index") or 0)
    if poll_index <= 0:
        return "-"
    return f"{poll_index} (+{(poll_index - 1) * poll_interval_ms}ms)"


async def fetch_rest_snapshot(
    client: httpx.AsyncClient,
    rest_base: str,
    symbol: str,
    interval: str,
    close_time_ms: int,
) -> KlineSnapshot | None:
    url = f"{rest_base.rstrip('/')}/fapi/v1/klines"
    resp = await client.get(
        url,
        params={
            "symbol": symbol,
            "interval": interval,
            "limit": 3,
            "endTime": close_time_ms,
        },
    )
    resp.raise_for_status()
    rows = resp.json()
    if not isinstance(rows, list):
        return None
    for row in reversed(rows):
        if isinstance(row, list) and len(row) >= 8 and int(row[6]) == close_time_ms:
            return rest_snapshot_from_row(symbol, interval, row)
    return None


async def collect_wss(
    args: argparse.Namespace,
    symbols: list[str],
    target_ready: asyncio.Event,
    done: asyncio.Event,
    state: dict[str, Any],
) -> None:
    stream_names = [f"{symbol.lower()}@kline_{args.interval}" for symbol in symbols]
    url = f"{stream_base_from_ws_base(args.ws_base)}?streams={'/'.join(stream_names)}"
    proxy = args.ws_proxy or proxy_from_env(urlparse(url).hostname or "")
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=60.0, sock_read=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(url, proxy=proxy, autoping=True, heartbeat=None) as ws:
            state["wss_url"] = url
            state["wss_connected_ms"] = now_ms()
            print(
                json.dumps(
                    {"event": "wss_connected", "url": url, "proxy": proxy or ""},
                    ensure_ascii=False,
                ),
                flush=True,
            )
            while not done.is_set():
                try:
                    msg = await ws.receive(timeout=10.0)
                except TimeoutError:
                    print(
                        json.dumps({"event": "wss_waiting", "waited_seconds": 10}, ensure_ascii=False),
                        flush=True,
                    )
                    continue
                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = msg.data.encode("utf-8")
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    raw = msg.data
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    raise RuntimeError("wss closed")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError(f"wss error: {ws.exception()}")
                else:
                    continue
                payload = orjson.loads(raw)
                snap = ws_snapshot_from_payload(payload)
                if snap is None:
                    continue
                if state.get("target_close_time_ms") is None:
                    state["target_close_time_ms"] = snap.close_time_ms
                    state["target_open_time_ms"] = snap.open_time_ms
                    state["target_first_wss_symbol"] = snap.symbol
                    state["target_first_wss_ingest_ms"] = snap.service_ingest_ms
                    target_ready.set()
                if snap.close_time_ms != state.get("target_close_time_ms"):
                    continue
                state.setdefault("wss", {})[snap.symbol] = snap
                print(
                    json.dumps(
                        {
                            "event": "wss_closed",
                            "symbol": snap.symbol,
                            "close_time_ms": snap.close_time_ms,
                            "exchange_to_service_ms": (
                                snap.service_ingest_ms - snap.exchange_event_ms
                                if snap.service_ingest_ms and snap.exchange_event_ms
                                else None
                            ),
                            "got_symbols": sorted(state.get("wss", {}).keys()),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                if len(state.get("wss", {})) >= len(symbols) and state.get("rest_done"):
                    done.set()


async def poll_rest(
    args: argparse.Namespace,
    symbols: list[str],
    target_ready: asyncio.Event,
    done: asyncio.Event,
    state: dict[str, Any],
) -> None:
    await target_ready.wait()
    close_time_ms = int(state["target_close_time_ms"])
    proxy = args.rest_proxy or os.getenv("BINANCE_HTTP_PROXY") or None
    timeout = httpx.Timeout(args.rest_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout, proxy=proxy) as client:
        poll_start_ms = now_ms()

        async def poll_one(idx: int, scheduled_start_ms: int) -> None:
            delay_ms = scheduled_start_ms - now_ms()
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)
            round_start_ms = now_ms()
            tasks = [
                fetch_rest_snapshot(client, args.rest_base, symbol, args.interval, close_time_ms)
                for symbol in symbols
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            round_items: dict[str, Any] = {}
            mismatch_symbols: list[str] = []
            for symbol, result in zip(symbols, results, strict=True):
                if isinstance(result, Exception):
                    item = {
                        "error": type(result).__name__,
                        "message": str(result),
                        "rest": None,
                        "comparison": {"match": False, "reason": "rest_error", "diffs": {}},
                    }
                    mismatch_symbols.append(symbol)
                else:
                    rest_snap = result
                    ws_snap = state.get("wss", {}).get(symbol)
                    comparison = compare_snapshots(ws_snap, rest_snap)
                    if not comparison["match"]:
                        mismatch_symbols.append(symbol)
                    item = {
                        "rest": asdict(rest_snap) if rest_snap is not None else None,
                        "comparison": comparison,
                    }
                round_items[symbol] = item
            state.setdefault("rest_rounds", []).append(
                {
                    "poll_index": idx,
                    "scheduled_start_ms": scheduled_start_ms,
                    "round_start_ms": round_start_ms,
                    "round_end_ms": now_ms(),
                    "schedule_delay_ms": round_start_ms - scheduled_start_ms,
                    "since_first_wss_ms": round_start_ms - int(state["target_first_wss_ingest_ms"]),
                    "symbols": round_items,
                    "mismatch_symbols": mismatch_symbols,
                    "all_match": not mismatch_symbols,
                },
            )
            print(
                json.dumps(
                    {
                        "event": "rest_poll",
                        "poll_index": idx,
                        "close_time_ms": close_time_ms,
                        "all_match": not mismatch_symbols,
                        "mismatch_symbols": mismatch_symbols,
                        "schedule_delay_ms": round_start_ms - scheduled_start_ms,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

        poll_tasks = [
            asyncio.create_task(poll_one(idx + 1, poll_start_ms + idx * args.poll_interval_ms))
            for idx in range(args.poll_count)
        ]
        await asyncio.gather(*poll_tasks)
        state["rest_rounds"].sort(key=lambda item: int(item["poll_index"]))
    state["rest_done"] = True
    if len(state.get("wss", {})) >= len(symbols):
        done.set()


def write_outputs(args: argparse.Namespace, symbols: list[str], state: dict[str, Any]) -> tuple[Path, Path]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    close_time_ms = int(state.get("target_close_time_ms") or 0)
    stamp = datetime.fromtimestamp(close_time_ms / 1000).strftime("%Y%m%d_%H%M%S") if close_time_ms else datetime.now().strftime("%Y%m%d_%H%M%S")
    base = output_dir / f"wss_rest_compare_{args.interval}_{stamp}_{close_time_ms or now_ms()}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")

    serializable = {
        "config": {
            "symbols": symbols,
            "interval": args.interval,
            "poll_count": args.poll_count,
            "poll_interval_ms": args.poll_interval_ms,
            "rest_base": args.rest_base,
            "ws_base": args.ws_base,
        },
        "target": {
            "open_time_ms": state.get("target_open_time_ms"),
            "close_time_ms": state.get("target_close_time_ms"),
            "first_wss_symbol": state.get("target_first_wss_symbol"),
            "first_wss_ingest_ms": state.get("target_first_wss_ingest_ms"),
        },
        "wss": {symbol: asdict(snap) for symbol, snap in state.get("wss", {}).items()},
        "rest_rounds": state.get("rest_rounds", []),
        "wss_url": state.get("wss_url"),
    }
    json_path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# WSS vs REST compare {args.interval}",
        "",
        f"- symbols: {', '.join(symbols)}",
        f"- close_time_ms: {state.get('target_close_time_ms')}",
        f"- first_wss_symbol: {state.get('target_first_wss_symbol')}",
        f"- poll_count: {args.poll_count}",
        f"- poll_interval_ms: {args.poll_interval_ms}",
        f"- json_detail: `{json_path.name}`",
        "",
        "## WSS arrival",
        "",
        "| symbol | event_ms | ingest_ms | exchange_to_service_ms | open | high | low | close | volume | quote_volume | trades |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for symbol in symbols:
        snap = state.get("wss", {}).get(symbol)
        if snap is None:
            lines.append(f"| {symbol} | missing | missing | missing | | | | | | | |")
            continue
        e2s = (
            snap.service_ingest_ms - snap.exchange_event_ms
            if snap.service_ingest_ms and snap.exchange_event_ms
            else ""
        )
        lines.append(
            f"| {symbol} | {snap.exchange_event_ms} | {snap.service_ingest_ms} | {e2s} | "
            f"{snap.open} | {snap.high} | {snap.low} | {snap.close} | {snap.volume} | {snap.quote_volume} | {snap.trades} |"
        )

    rest_rounds = state.get("rest_rounds", [])
    first_rest_start_ms = min((int(item.get("round_start_ms") or 0) for item in rest_rounds), default=0)

    lines.extend(
        [
            "",
            "## REST request timeline",
            "",
            "字段说明：`scheduled_offset_ms` 是计划请求间隔（0/100/200...）；`actual_offset_ms` 是实际发起时间相对第一轮 REST 的偏移；`since_first_wss_ms` 是相对第一条 WSS 闭合 K 线进入本机的偏移。",
            "",
            "| poll | scheduled_offset_ms | actual_offset_ms | since_first_wss_ms | request_cost_ms | all_match | mismatch_symbols |",
            "|---:|---:|---:|---:|---:|---|---|",
        ],
    )
    for round_item in state.get("rest_rounds", []):
        poll_index = int(round_item.get("poll_index") or 0)
        scheduled_offset_ms = (poll_index - 1) * int(args.poll_interval_ms)
        round_start_ms = int(round_item.get("round_start_ms") or 0)
        round_end_ms = int(round_item.get("round_end_ms") or 0)
        actual_offset_ms = round_start_ms - first_rest_start_ms if first_rest_start_ms and round_start_ms else ""
        request_cost_ms = round_end_ms - round_start_ms if round_start_ms and round_end_ms else ""
        lines.append(
            f"| {poll_index} | +{scheduled_offset_ms} | {actual_offset_ms} | "
            f"{round_item['since_first_wss_ms']} | {request_cost_ms} | {round_item['all_match']} | "
            f"{', '.join(round_item['mismatch_symbols']) or '-'} |"
        )

    lines.extend(
        [
            "",
            "## Match matrix",
            "",
            "单元格含义：`OK` 表示该轮 REST 与 WSS 完全一致；否则展示差异字段代码。字段代码：`C=close`，`V=volume`，`QV=quote_volume`，`T=trades`，`TBV=taker_buy_volume`，`TBQ=taker_buy_quote_volume`。",
            "",
            "| poll | scheduled_offset_ms | actual_offset_ms | " + " | ".join(symbols) + " |",
            "|---:|---:|---:|" + "|".join(["---"] * len(symbols)) + "|",
        ],
    )
    for round_item in state.get("rest_rounds", []):
        poll_index = int(round_item.get("poll_index") or 0)
        scheduled_offset_ms = (poll_index - 1) * int(args.poll_interval_ms)
        round_start_ms = int(round_item.get("round_start_ms") or 0)
        actual_offset_ms = round_start_ms - first_rest_start_ms if first_rest_start_ms and round_start_ms else ""
        round_symbols = round_item.get("symbols", {})
        cells: list[str] = []
        for symbol in symbols:
            item = round_symbols.get(symbol, {})
            comparison = item.get("comparison") or {}
            diffs = comparison.get("diffs") or {}
            cells.append("OK" if comparison.get("match") else diff_codes(diffs))
        lines.append(f"| {poll_index} | +{scheduled_offset_ms} | {actual_offset_ms} | " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "## Per-symbol REST timeline",
            "",
            "每个币种一张表，便于观察 REST 在不同请求偏移下返回的数据如何变化；WSS 基准值见上方 `WSS arrival`。",
        ],
    )
    for symbol in symbols:
        lines.extend(
            [
                "",
                f"### {symbol}",
                "",
                "| poll | scheduled_offset_ms | actual_offset_ms | match | diff | close | volume | quote_volume | trades | taker_buy_volume | taker_buy_quote_volume |",
                "|---:|---:|---:|---|---|---:|---:|---:|---:|---:|---:|",
            ],
        )
        for round_item in state.get("rest_rounds", []):
            poll_index = int(round_item.get("poll_index") or 0)
            scheduled_offset_ms = (poll_index - 1) * int(args.poll_interval_ms)
            round_start_ms = int(round_item.get("round_start_ms") or 0)
            actual_offset_ms = round_start_ms - first_rest_start_ms if first_rest_start_ms and round_start_ms else ""
            item = (round_item.get("symbols", {}) or {}).get(symbol, {})
            rest = item.get("rest") or {}
            comparison = item.get("comparison") or {}
            lines.append(
                "| "
                + " | ".join(
                    [
                        md_cell(poll_index),
                        md_cell(f"+{scheduled_offset_ms}"),
                        md_cell(actual_offset_ms),
                        md_cell(comparison.get("match")),
                        md_cell(diff_codes(comparison.get("diffs") or {})),
                        md_cell(rest.get("close")),
                        md_cell(rest.get("volume")),
                        md_cell(rest.get("quote_volume")),
                        md_cell(rest.get("trades")),
                        md_cell(rest.get("taker_buy_volume")),
                        md_cell(rest.get("taker_buy_quote_volume")),
                    ],
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## Field diffs",
            "",
            "| poll | scheduled_offset_ms | symbol | field | wss | rest |",
            "|---:|---:|---|---|---:|---:|",
        ],
    )
    diff_count = 0
    for round_item in state.get("rest_rounds", []):
        poll_index = int(round_item.get("poll_index") or 0)
        scheduled_offset_ms = (poll_index - 1) * int(args.poll_interval_ms)
        round_symbols = round_item.get("symbols", {})
        for symbol in symbols:
            item = round_symbols.get(symbol, {})
            comparison = item.get("comparison") or {}
            diffs = comparison.get("diffs") or {}
            for field, values in diffs.items():
                diff_count += 1
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            md_cell(poll_index),
                            md_cell(f"+{scheduled_offset_ms}"),
                            md_cell(symbol),
                            md_cell(field),
                            md_cell(values.get("wss")),
                            md_cell(values.get("rest")),
                        ],
                    )
                    + " |"
                )
    if diff_count == 0:
        lines.append("| - | - | - | - | - | - |")
    lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def write_summary(
    args: argparse.Namespace,
    symbols: list[str],
    runs: list[dict[str, Any]],
) -> Path:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = output_dir / f"wss_rest_compare_{args.interval}_summary_{stamp}.md"

    lines = [
        f"# WSS vs REST compare {args.interval} summary",
        "",
        f"- rounds: {len(runs)}",
        f"- symbols: {', '.join(symbols)}",
        f"- poll_count: {args.poll_count}",
        f"- poll_interval_ms: {args.poll_interval_ms}",
        "",
        "## Round summary",
        "",
        "`first_all_match_poll` 表示第一次五个币都完全匹配；`first_stable_all_match_poll` 表示从该轮开始到第 30 轮一直完全匹配。如果后面又出现差异，这一列会是 `-`。",
        "",
        "| round | close_time_ms | first_wss_symbol | all_match_count | mismatch_poll_count | first_all_match_poll | first_stable_all_match_poll | markdown | json |",
        "|---:|---:|---|---:|---:|---|---|---|---|",
    ]
    for idx, run in enumerate(runs, start=1):
        state = run["state"]
        rest_rounds = state.get("rest_rounds", [])
        all_match_count = sum(1 for item in rest_rounds if item.get("all_match"))
        mismatch_poll_count = len(rest_rounds) - all_match_count
        first_all = first_all_match_round(rest_rounds)
        first_stable = first_stable_all_match_round(rest_rounds)
        md_name = Path(run["markdown_path"]).name
        json_name = Path(run["json_path"]).name
        lines.append(
            "| "
            + " | ".join(
                [
                    md_cell(idx),
                    md_cell(state.get("target_close_time_ms")),
                    md_cell(state.get("target_first_wss_symbol")),
                    md_cell(all_match_count),
                    md_cell(mismatch_poll_count),
                    md_cell(round_offset_text(first_all, args.poll_interval_ms)),
                    md_cell(round_offset_text(first_stable, args.poll_interval_ms)),
                    f"[{md_name}]({md_name})",
                    f"[{json_name}]({json_name})",
                ],
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Symbol stable matrix",
            "",
            "每个单元格展示该币种第一次匹配和稳定匹配的 poll/偏移，格式为 `first / stable`。",
            "",
            "| round | close_time_ms | " + " | ".join(symbols) + " |",
            "|---:|---:|" + "|".join(["---"] * len(symbols)) + "|",
        ],
    )
    for idx, run in enumerate(runs, start=1):
        state = run["state"]
        rest_rounds = state.get("rest_rounds", [])
        cells = []
        for symbol in symbols:
            first_match = first_symbol_match_round(rest_rounds, symbol)
            stable_match = first_stable_symbol_match_round(rest_rounds, symbol)
            cells.append(
                f"{round_offset_text(first_match, args.poll_interval_ms)} / "
                f"{round_offset_text(stable_match, args.poll_interval_ms)}"
            )
        lines.append(
            f"| {idx} | {state.get('target_close_time_ms')} | "
            + " | ".join(md_cell(cell) for cell in cells)
            + " |"
        )

    lines.extend(
        [
            "",
            "## Mismatch timeline",
            "",
            "按轮列出每次 REST poll 的差异币种，便于快速看 REST 返回是否出现回跳。",
            "",
            "| round | poll | scheduled_offset_ms | mismatch_symbols |",
            "|---:|---:|---:|---|",
        ],
    )
    for idx, run in enumerate(runs, start=1):
        rest_rounds = run["state"].get("rest_rounds", [])
        for round_item in rest_rounds:
            poll_index = int(round_item.get("poll_index") or 0)
            mismatch_symbols = round_item.get("mismatch_symbols") or []
            lines.append(
                f"| {idx} | {poll_index} | +{(poll_index - 1) * args.poll_interval_ms} | "
                f"{md_cell(', '.join(mismatch_symbols) or '-')} |"
            )

    lines.append("")
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    return summary_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Binance WSS closed klines with repeated REST reads.")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--rounds", type=int, default=1, help="How many closed-kline windows to verify.")
    parser.add_argument("--poll-count", type=int, default=30)
    parser.add_argument("--poll-interval-ms", type=int, default=100)
    parser.add_argument("--rest-base", default="https://fapi.binance.com")
    parser.add_argument("--ws-base", default="wss://fstream.binance.com/market/stream")
    parser.add_argument("--rest-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--rest-proxy", default="", help="Optional proxy for REST, e.g. http://127.0.0.1:7890")
    parser.add_argument("--ws-proxy", default="", help="Optional proxy for WSS, e.g. http://127.0.0.1:7890")
    parser.add_argument("--output-dir", default="logs")
    return parser.parse_args()


async def run_once(args: argparse.Namespace, symbols: list[str], round_index: int, total_rounds: int) -> dict[str, Any]:
    target_ready = asyncio.Event()
    done = asyncio.Event()
    state: dict[str, Any] = {"rest_done": False}

    print(
        json.dumps(
            {
                "event": "start",
                "round": round_index,
                "rounds": total_rounds,
                "symbols": symbols,
                "interval": args.interval,
                "poll_count": args.poll_count,
                "poll_interval_ms": args.poll_interval_ms,
                "output_dir": args.output_dir,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    wss_task = asyncio.create_task(collect_wss(args, symbols, target_ready, done, state))
    rest_task = asyncio.create_task(poll_rest(args, symbols, target_ready, done, state))
    done_task = asyncio.create_task(done.wait())
    try:
        while True:
            finished, _pending = await asyncio.wait(
                {wss_task, rest_task, done_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if done_task in finished:
                break
            for task in finished:
                exc = task.exception()
                if exc is not None:
                    raise exc
    finally:
        for task in (wss_task, rest_task, done_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(wss_task, rest_task, done_task, return_exceptions=True)
    json_path, md_path = write_outputs(args, symbols, state)
    print(
        json.dumps(
            {
                "event": "done",
                "round": round_index,
                "rounds": total_rounds,
                "json_path": str(json_path),
                "markdown_path": str(md_path),
                "wss_symbols": sorted(state.get("wss", {}).keys()),
                "rest_rounds": len(state.get("rest_rounds", [])),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return {"state": state, "json_path": json_path, "markdown_path": md_path}


async def main_async() -> None:
    args = parse_args()
    symbols = parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("No symbols configured")
    if args.rounds < 1:
        raise SystemExit("--rounds must be >= 1")

    runs = []
    for idx in range(1, args.rounds + 1):
        runs.append(await run_once(args, symbols, idx, args.rounds))

    if len(runs) > 1:
        summary_path = write_summary(args, symbols, runs)
        print(
            json.dumps(
                {
                    "event": "summary",
                    "rounds": len(runs),
                    "summary_path": str(summary_path),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
