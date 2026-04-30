from __future__ import annotations

import argparse
import json
import signal
import time

import websockets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mock downstream WebSocket consumer for alerts feed",
    )
    parser.add_argument("--ws-url", default="wss://datahub.questvector.ai/ws/alerts")
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
                "event": "alerts_client.start",
                "ws_url": args.ws_url,
            },
            ensure_ascii=False,
        ),
    )

    while not stop:
        try:
            async with websockets.connect(args.ws_url, ping_interval=20, ping_timeout=20) as ws:
                print(json.dumps({"event": "alerts_client.connected", "ws_url": args.ws_url}, ensure_ascii=False))
                while not stop:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15)
                    except TimeoutError:
                        print(
                            json.dumps(
                                {
                                    "event": "alerts_client.waiting",
                                    "seconds_without_message": 15,
                                    "hint": "alerts only appear when threshold is triggered",
                                },
                                ensure_ascii=False,
                            ),
                        )
                        continue
                    msg = json.loads(raw)
                    now_ms = int(time.time() * 1000)
                    exchange_event_ms = int(msg.get("exchange_event_ms", 0) or 0)
                    service_ingest_ms = int(msg.get("service_ingest_ms", 0) or 0)
                    e2e_ms = now_ms - exchange_event_ms if exchange_event_ms else None
                    e2s_ms = service_ingest_ms - exchange_event_ms if (exchange_event_ms and service_ingest_ms) else None

                    print(
                        json.dumps(
                            {
                                "event": "alerts_client.receive",
                                "type": msg.get("type"),
                                "symbol": msg.get("symbol"),
                                "interval": msg.get("interval"),
                                "direction": msg.get("direction"),
                                "move_pct": msg.get("move_pct"),
                                "threshold_pct": msg.get("threshold_pct"),
                                "downstream_receive_ms": now_ms,
                                "exchange_to_downstream_ms": e2e_ms,
                                "exchange_to_service_ms": e2s_ms,
                                "service_region": msg.get("service_region"),
                            },
                            ensure_ascii=False,
                        ),
                    )
                    seen += 1
                    if args.max_messages > 0 and seen >= args.max_messages:
                        stop = True
        except Exception as exc:
            if stop:
                break
            print(
                json.dumps(
                    {
                        "event": "alerts_client.reconnect",
                        "error": str(exc),
                        "retry_in_seconds": 2,
                    },
                    ensure_ascii=False,
                ),
            )
            await asyncio.sleep(2)

    print(json.dumps({"event": "alerts_client.stop", "seen_messages": seen}, ensure_ascii=False))


if __name__ == "__main__":
    import asyncio

    asyncio.run(_run())
