from __future__ import annotations

import argparse
import json
import signal
import time

import websockets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mock downstream WebSocket consumer for closed kline feed",
    )
    parser.add_argument("--ws-url", default="wss://datahub.questvector.ai/ws/kline")
    parser.add_argument("--max-messages", type=int, default=0, help="0 means run forever")
    parser.add_argument("--downstream-region", default="ap-northeast-1")
    parser.add_argument(
        "--ack-enabled",
        action="store_true",
        help="Send ack payloads back to service over the same websocket",
    )
    return parser.parse_args()


async def _run() -> None:
    args = parse_args()
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
                "ws_url": args.ws_url,
            },
            ensure_ascii=False,
        ),
    )
    integrated_rows_by_close: dict[int, dict[str, dict]] = {}
    while not stop:
        try:
            async with websockets.connect(args.ws_url, ping_interval=20, ping_timeout=20) as ws:
                print(json.dumps({"event": "downstream.connected", "ws_url": args.ws_url}, ensure_ascii=False))
                while not stop:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15)
                    except TimeoutError:
                        print(
                            json.dumps(
                                {
                                    "event": "downstream.waiting",
                                    "seconds_without_message": 15,
                                    "hint": "5m kline arrives on each 5m candle close (temporary mode)",
                                },
                                ensure_ascii=False,
                            ),
                        )
                        continue
                    msg = json.loads(raw)
                    rows = msg if isinstance(msg, list) else [msg]
                    now_ms = int(time.time() * 1000)
                    print(
                        json.dumps(
                            {
                                "event": "downstream.receive_batch",
                                "batch_size": len(rows),
                                "downstream_receive_ms": now_ms,
                            },
                            ensure_ascii=False,
                        ),
                    )
                    ack_items: list[dict] = []
                    batch_done_close_time_ms: int | None = None
                    batch_push_done_ms: int = 0
                    for row in rows:
                        exchange_event_ms = int(row.get("exchange_event_ms", 0) or 0)
                        service_publish_ms = int(row.get("service_publish_ms", 0) or 0)
                        ex_to_downstream = now_ms - exchange_event_ms if exchange_event_ms else None
                        pub_to_downstream = now_ms - service_publish_ms if service_publish_ms else None
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
                            try:
                                batch_done_close_time_ms = int(row.get("close_time_ms"))
                            except (TypeError, ValueError):
                                batch_done_close_time_ms = None
                            else:
                                try:
                                    batch_push_done_ms = int(row.get("batch_push_done_ms") or 0)
                                except (TypeError, ValueError):
                                    batch_push_done_ms = 0
                        try:
                            close_time_key = int(row.get("close_time_ms") or 0)
                        except (TypeError, ValueError):
                            close_time_key = 0
                        symbol = str(row.get("symbol") or "")
                        if close_time_key > 0 and symbol:
                            integrated_rows_by_close.setdefault(close_time_key, {})[symbol] = row
                        event_id = row.get("event_id")
                        if args.ack_enabled and event_id:
                            ack_items.append(
                                {
                                    "event_id": event_id,
                                    "downstream_receive_ms": now_ms,
                                    "downstream_region": args.downstream_region,
                                },
                            )
                        seen += 1
                        if args.max_messages > 0 and seen >= args.max_messages:
                            stop = True
                            break
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
                        if batch_push_done_ms > 0:
                            batch_ack_body["batch_push_done_ms"] = batch_push_done_ms
                        else:
                            batch_ack_body["downstream_receive_ms"] = now_ms
                        await ws.send(json.dumps(batch_ack_body, ensure_ascii=False))
                        merged_count = len(integrated_rows_by_close.get(batch_done_close_time_ms, {}))
                        integrated_body: dict = {
                            "type": "batch_integrated_ack",
                            "close_time_ms": batch_done_close_time_ms,
                            "downstream_region": args.downstream_region,
                            "merged_symbols": merged_count,
                        }
                        if batch_push_done_ms > 0:
                            integrated_body["batch_push_done_ms"] = batch_push_done_ms
                        else:
                            integrated_body["downstream_integrated_ms"] = int(time.time() * 1000)
                        await ws.send(json.dumps(integrated_body, ensure_ascii=False))
                        integrated_rows_by_close.pop(batch_done_close_time_ms, None)
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
    import asyncio

    asyncio.run(_run())
