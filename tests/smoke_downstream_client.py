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
    parser.add_argument("--ws-url", default="ws://127.0.0.1:8766/ws/klines")
    parser.add_argument("--max-messages", type=int, default=0, help="0 means run forever")
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
                        seen += 1
                        if args.max_messages > 0 and seen >= args.max_messages:
                            stop = True
                            break
        except Exception as exc:
            if stop:
                break
            print(
                json.dumps(
                    {
                        "event": "downstream.reconnect",
                        "error": str(exc),
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
