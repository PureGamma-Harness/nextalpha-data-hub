from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from urllib.parse import urlencode, urlsplit, urlunsplit

import websockets


PROFILES = {
    "majors-1m": {
        "ws_url": "wss://datahub.questvector.ai/ws/klines",
        "topics": "klines.majors.1m",
    },
    "klines-1h": {
        "ws_url": "wss://datahub.questvector.ai/ws/klines",
        "topics": "klines.all.1h,klines.majors.1h",
    },
}


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example downstream strategy consumer for kline websocket batches.",
    )
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES.keys()),
        default="majors-1m",
        help="Connection preset. Use majors-1m for alert-side 1m major kline topic.",
    )
    parser.add_argument(
        "--ws-url",
        default="",
        help="Override websocket URL. Defaults to wss://datahub.questvector.ai/ws/klines.",
    )
    parser.add_argument("--topics", default="", help="Override comma-separated topics.")
    parser.add_argument("--downstream-region", default="ap-northeast-1")
    parser.add_argument("--no-ack", action="store_true", help="Receive only; do not send ACKs.")
    return parser.parse_args()


def url_with_topics(url: str, topics: str) -> str:
    if not topics:
        return url
    parts = urlsplit(url)
    query_key = "topics" if "," in topics else "topic"
    extra_query = urlencode({query_key: topics})
    query = f"{parts.query}&{extra_query}" if parts.query else extra_query
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def ensure_no_proxy_for_loopback(url: str) -> None:
    host = (urlsplit(url).hostname or "").lower()
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return
    for name in ("NO_PROXY", "no_proxy"):
        current = [item.strip() for item in os.environ.get(name, "").split(",") if item.strip()]
        for item in ("127.0.0.1", "localhost", "::1"):
            if item not in current:
                current.append(item)
        os.environ[name] = ",".join(current)


async def integrate_rows(rows: list[dict]) -> dict[str, dict]:
    # Replace this with your strategy's batch merge / cache update logic.
    return {str(row["symbol"]): row for row in rows if row.get("symbol")}


async def run_strategy_compute(rows_by_symbol: dict[str, dict]) -> dict:
    # Replace this with factor calculation / signal generation.
    return {"symbols": sorted(rows_by_symbol.keys()), "signal_count": 0}


async def place_orders(_compute_result: dict) -> None:
    # Replace this with live order placement. Keep it idempotent by close_time/topic.
    return None


def build_row_acks(rows: list[dict], receive_ms: int, downstream_region: str) -> list[dict]:
    acks: list[dict] = []
    for row in rows:
        event_id = row.get("event_id")
        if not event_id:
            continue
        ack = {
            "event_id": event_id,
            "downstream_receive_ms": receive_ms,
            "downstream_region": downstream_region,
        }
        if row.get("topic"):
            ack["topic"] = row["topic"]
        acks.append(ack)
    return acks


def batch_meta(rows: list[dict]) -> dict:
    first = rows[0] if rows else {}
    return {
        "topic": str(first.get("topic") or ""),
        "interval": str(first.get("interval") or ""),
        "close_time_ms": int(first.get("close_time_ms") or 0),
        "batch_push_done_ms": int(first.get("batch_push_done_ms") or 0),
        "batch_done": any(bool(row.get("batch_done")) for row in rows),
    }


async def send_batch_ack(
    ws: websockets.WebSocketClientProtocol,
    ack_type: str,
    meta: dict,
    downstream_region: str,
    fallback_field: str,
) -> None:
    if not meta["batch_done"] or meta["close_time_ms"] <= 0:
        return
    payload = {
        "type": ack_type,
        "topic": meta["topic"],
        "close_time_ms": meta["close_time_ms"],
        "downstream_region": downstream_region,
    }
    if meta["batch_push_done_ms"] > 0:
        payload["batch_push_done_ms"] = meta["batch_push_done_ms"]
    else:
        payload[fallback_field] = now_ms()
    await ws.send(json.dumps(payload, ensure_ascii=False))


async def run() -> None:
    args = parse_args()
    defaults = PROFILES[args.profile]
    ws_url = args.ws_url or defaults["ws_url"]
    topics = args.topics or defaults["topics"]
    full_url = url_with_topics(ws_url, topics)
    ensure_no_proxy_for_loopback(full_url)

    stop = False

    def request_stop(_sig: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    while not stop:
        try:
            async with websockets.connect(full_url, ping_interval=20, ping_timeout=20) as ws:
                print(json.dumps({"event": "connected", "url": full_url, "topics": topics}, ensure_ascii=False))
                while not stop:
                    raw = await ws.recv()
                    receive_ms = now_ms()
                    message = json.loads(raw)
                    rows = message if isinstance(message, list) else [message]
                    rows = [row for row in rows if isinstance(row, dict)]
                    if not rows:
                        continue

                    meta = batch_meta(rows)
                    symbols = sorted(str(row.get("symbol") or "") for row in rows if row.get("symbol"))
                    print(
                        json.dumps(
                            {
                                "event": "kline_batch",
                                **meta,
                                "rows": len(rows),
                                "symbols": symbols,
                                "downstream_receive_ms": receive_ms,
                            },
                            ensure_ascii=False,
                        ),
                    )

                    if not args.no_ack:
                        row_acks = build_row_acks(rows, receive_ms, args.downstream_region)
                        if row_acks:
                            await ws.send(json.dumps({"acks": row_acks}, ensure_ascii=False))
                        await send_batch_ack(ws, "batch_ack", meta, args.downstream_region, "downstream_receive_ms")

                    rows_by_symbol = await integrate_rows(rows)
                    if not args.no_ack:
                        await send_batch_ack(
                            ws,
                            "batch_integrated_ack",
                            meta,
                            args.downstream_region,
                            "downstream_integrated_ms",
                        )

                    compute_result = await run_strategy_compute(rows_by_symbol)
                    if not args.no_ack:
                        await send_batch_ack(
                            ws,
                            "batch_compute_ack",
                            meta,
                            args.downstream_region,
                            "downstream_compute_ms",
                        )

                    await place_orders(compute_result)
                    if not args.no_ack:
                        await send_batch_ack(
                            ws,
                            "batch_order_ack",
                            meta,
                            args.downstream_region,
                            "downstream_order_ms",
                        )
        except Exception as exc:
            if stop:
                break
            print(
                json.dumps(
                    {
                        "event": "reconnect",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "retry_seconds": 2,
                    },
                    ensure_ascii=False,
                ),
            )
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(run())
