from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
import textwrap
from urllib.parse import urlencode, urlsplit, urlunsplit

import websockets

DEFAULT_TOPICS = "klines.all.1h,klines.majors.1h"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mock downstream WebSocket consumer for closed kline feed",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Examples:
              # Full-market 1h and major-symbol 1h batches from the kline service.
              python tests/smoke_downstream_client.py \\
                --ws-url ws://127.0.0.1:8766/ws/klines \\
                --topics klines.all.1h,klines.majors.1h \\
                --klines-interval 1h \\
                --ack-enabled

              # Major-symbol 1m batches reused from the alert service ingestion path.
              python tests/smoke_downstream_client.py \\
                --ws-url ws://127.0.0.1:8765/ws/klines \\
                --topics klines.majors.1m \\
                --klines-interval 1m \\
                --ack-enabled
            """,
        ),
    )
    parser.add_argument(
        "--ws-url",
        default="wss://datahub.questvector.ai/ws/klines",
    )
    parser.add_argument(
        "--topic",
        default="",
        help="Single kline topic to subscribe, e.g. klines.all.1h",
    )
    parser.add_argument(
        "--topics",
        default="",
        help=(
            "Comma-separated kline topics to subscribe; overrides --topic when set. "
            f"Default when neither is set: {DEFAULT_TOPICS}"
        ),
    )
    parser.add_argument("--max-messages", type=int, default=0, help="0 means run forever")
    parser.add_argument("--downstream-region", default="ap-northeast-1")
    parser.add_argument(
        "--klines-interval",
        default="1h",
        help="Hint only: server klines bar size; used in wait-timeout log message",
    )
    parser.add_argument(
        "--ack-enabled",
        action="store_true",
        help="Send ack payloads back to service over the same websocket",
    )
    parser.add_argument(
        "--print-rows",
        action="store_true",
        help="Print every row in each batch. By default only batch summaries are printed.",
    )
    return parser.parse_args()


def _split_topics(requested: str) -> list[str]:
    return [item.strip() for item in requested.split(",") if item.strip()]


def _url_with_topics(url: str, requested: str) -> str:
    if not requested:
        return url
    parts = urlsplit(url)
    existing_query = parts.query
    extra_query = urlencode({"topics" if "," in requested else "topic": requested})
    query = f"{existing_query}&{extra_query}" if existing_query else extra_query
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _ensure_no_proxy_for_loopback(url: str) -> None:
    host = (urlsplit(url).hostname or "").lower()
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return
    loopback_hosts = ["127.0.0.1", "localhost", "::1"]
    for name in ("NO_PROXY", "no_proxy"):
        current = os.environ.get(name, "")
        existing = [item.strip() for item in current.split(",") if item.strip()]
        merged = [*existing]
        for host_item in loopback_hosts:
            if host_item not in merged:
                merged.append(host_item)
        os.environ[name] = ",".join(merged)


async def _run() -> None:
    args = parse_args()
    requested_topics = args.topics.strip() or args.topic.strip() or DEFAULT_TOPICS
    ws_url = _url_with_topics(args.ws_url, requested_topics)
    _ensure_no_proxy_for_loopback(ws_url)
    subscribed_topics = _split_topics(requested_topics)
    stop = False

    def _stop(_sig: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    seen = 0
    print(
        json.dumps(
            {
                "event": "downstream.start",
                "ws_url": ws_url,
                "topics": subscribed_topics,
                "ack_enabled": args.ack_enabled,
                "print_rows": args.print_rows,
            },
            ensure_ascii=False,
        ),
    )
    integrated_rows_by_close: dict[tuple[str, int], dict[str, dict]] = {}
    while not stop:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                print(json.dumps({"event": "downstream.connected", "ws_url": ws_url}, ensure_ascii=False))
                while not stop:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15)
                    except TimeoutError:
                        print(
                            json.dumps(
                                {
                                    "event": "downstream.waiting",
                                    "seconds_without_message": 15,
                                    "klines_interval_hint": args.klines_interval,
                                    "hint": (
                                        f"No batch yet; {args.klines_interval} bars emit after each "
                                        f"close (matches server websocket.klines_interval)"
                                    ),
                                },
                                ensure_ascii=False,
                            ),
                        )
                        continue
                    msg = json.loads(raw)
                    rows = msg if isinstance(msg, list) else [msg]
                    now_ms = int(time.time() * 1000)
                    ack_items: list[dict] = []
                    batch_done_close_time_ms: int | None = None
                    batch_push_done_ms: int = 0
                    batch_topic = ""
                    batch_interval = ""
                    symbols: list[str] = []
                    batch_done = False
                    for row in rows:
                        row_topic = str(row.get("topic") or "")
                        batch_topic = batch_topic or row_topic
                        batch_interval = batch_interval or str(row.get("interval") or "")
                        symbol = str(row.get("symbol") or "")
                        if symbol:
                            symbols.append(symbol)
                        exchange_event_ms = int(row.get("exchange_event_ms", 0) or 0)
                        service_publish_ms = int(row.get("service_publish_ms", 0) or 0)
                        ex_to_downstream = now_ms - exchange_event_ms if exchange_event_ms else None
                        pub_to_downstream = now_ms - service_publish_ms if service_publish_ms else None
                        if args.print_rows:
                            print(
                                json.dumps(
                                    {
                                        **row,
                                        "downstream_receive_ms": now_ms,
                                        "exchange_to_downstream_ms": ex_to_downstream,
                                        "service_publish_to_downstream_ms": pub_to_downstream,
                                    },
                                    ensure_ascii=False,
                                ),
                            )
                        if bool(row.get("batch_done")) and row.get("close_time_ms") is not None:
                            batch_done = True
                            try:
                                batch_done_close_time_ms = int(row.get("close_time_ms"))
                            except (TypeError, ValueError):
                                batch_done_close_time_ms = None
                            else:
                                try:
                                    batch_push_done_ms = int(row.get("batch_push_done_ms") or 0)
                                except (TypeError, ValueError):
                                    batch_push_done_ms = 0
                                batch_topic = row_topic
                        try:
                            close_time_key = int(row.get("close_time_ms") or 0)
                        except (TypeError, ValueError):
                            close_time_key = 0
                        if close_time_key > 0 and symbol:
                            integrated_rows_by_close.setdefault((row_topic, close_time_key), {})[symbol] = row
                        event_id = row.get("event_id")
                        if args.ack_enabled and event_id:
                            ack_item = {
                                "event_id": event_id,
                                "downstream_receive_ms": now_ms,
                                "downstream_region": args.downstream_region,
                            }
                            if row_topic:
                                ack_item["topic"] = row_topic
                            ack_items.append(ack_item)
                        seen += 1
                        if args.max_messages > 0 and seen >= args.max_messages:
                            stop = True
                            break
                    print(
                        json.dumps(
                            {
                                "event": "downstream.receive_batch",
                                "topic": batch_topic,
                                "interval": batch_interval,
                                "close_time_ms": batch_done_close_time_ms,
                                "batch_size": len(rows),
                                "batch_done": batch_done,
                                "symbols_sample": sorted(symbols)[:10],
                                "downstream_receive_ms": now_ms,
                            },
                            ensure_ascii=False,
                        ),
                    )
                    if args.ack_enabled and ack_items:
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "ack_batch",
                                    "acks": ack_items,
                                },
                                ensure_ascii=False,
                            ),
                        )
                    if args.ack_enabled and batch_done_close_time_ms is not None:
                        batch_ack_body: dict = {
                            "type": "batch_ack",
                            "close_time_ms": batch_done_close_time_ms,
                            "downstream_region": args.downstream_region,
                        }
                        if batch_topic:
                            batch_ack_body["topic"] = batch_topic
                        if batch_push_done_ms > 0:
                            batch_ack_body["batch_push_done_ms"] = batch_push_done_ms
                        else:
                            batch_ack_body["downstream_receive_ms"] = now_ms
                        await ws.send(json.dumps(batch_ack_body, ensure_ascii=False))
                        print(
                            json.dumps(
                                {
                                    "event": "downstream.send_batch_ack",
                                    "topic": batch_topic,
                                    "close_time_ms": batch_done_close_time_ms,
                                    "rows": len(ack_items),
                                },
                                ensure_ascii=False,
                            ),
                        )
                        batch_key = (batch_topic, batch_done_close_time_ms)
                        merged_count = len(integrated_rows_by_close.get(batch_key, {}))
                        integrated_body: dict = {
                            "type": "batch_integrated_ack",
                            "close_time_ms": batch_done_close_time_ms,
                            "downstream_region": args.downstream_region,
                            "merged_symbols": merged_count,
                        }
                        if batch_topic:
                            integrated_body["topic"] = batch_topic
                        if batch_push_done_ms > 0:
                            integrated_body["batch_push_done_ms"] = batch_push_done_ms
                        else:
                            integrated_body["downstream_integrated_ms"] = int(time.time() * 1000)
                        await ws.send(json.dumps(integrated_body, ensure_ascii=False))
                        # Mock downstream "compute finished" ack.
                        compute_body: dict = {
                            "type": "batch_compute_ack",
                            "close_time_ms": batch_done_close_time_ms,
                            "downstream_region": args.downstream_region,
                        }
                        if batch_topic:
                            compute_body["topic"] = batch_topic
                        if batch_push_done_ms > 0:
                            compute_body["batch_push_done_ms"] = batch_push_done_ms
                        else:
                            compute_body["downstream_compute_ms"] = int(time.time() * 1000)
                        await ws.send(json.dumps(compute_body, ensure_ascii=False))
                        # Mock downstream "order finished" ack.
                        order_body: dict = {
                            "type": "batch_order_ack",
                            "close_time_ms": batch_done_close_time_ms,
                            "downstream_region": args.downstream_region,
                        }
                        if batch_topic:
                            order_body["topic"] = batch_topic
                        if batch_push_done_ms > 0:
                            order_body["batch_push_done_ms"] = batch_push_done_ms
                        else:
                            order_body["downstream_order_ms"] = int(time.time() * 1000)
                        await ws.send(json.dumps(order_body, ensure_ascii=False))
                        integrated_rows_by_close.pop(batch_key, None)
        except Exception as exc:
            if stop:
                break
            err_msg = str(exc).strip() or repr(exc)
            print(
                json.dumps(
                    {
                        "event": "downstream.reconnect",
                        "error": err_msg,
                        "error_type": type(exc).__name__,
                        "hint": "If error is empty, check EC2/security group allows inbound TCP 8766 from this machine",
                        "retry_in_seconds": 2,
                    },
                    ensure_ascii=False,
                ),
            )
            await asyncio.sleep(2)

    print(json.dumps({"event": "downstream.stop", "seen_messages": seen}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(_run())
