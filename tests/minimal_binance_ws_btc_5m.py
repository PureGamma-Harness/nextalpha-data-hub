from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

import aiohttp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal Binance BTCUSDT 5m websocket check")
    parser.add_argument("--max-seconds", type=int, default=90)
    parser.add_argument("--url", default="wss://fstream.binance.com/stream?streams=btcusdt@kline_5m")
    parser.add_argument(
        "--proxy",
        default=None,
        help="Optional proxy URL, e.g. http://host.docker.internal:10808",
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    proxy = args.proxy or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    start = time.time()
    frames = 0
    kline_frames = 0
    closed_frames = 0

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(args.url, proxy=proxy, autoping=True, heartbeat=None) as ws:
            print(
                json.dumps(
                    {"event": "minimal_ws.connected", "url": args.url, "proxy": proxy},
                    ensure_ascii=False,
                ),
            )
            while time.time() - start < args.max_seconds:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=10.0)
                except asyncio.TimeoutError:
                    print(json.dumps({"event": "minimal_ws.waiting", "seconds_without_frame": 10}, ensure_ascii=False))
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = msg.data
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    raw = msg.data.decode("utf-8", errors="ignore")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise RuntimeError(f"ws error: {msg.data!r}")
                else:
                    continue

                frames += 1
                payload = json.loads(raw)
                data = payload.get("data", payload) if isinstance(payload, dict) else None
                k = data.get("k") if isinstance(data, dict) else None

                if isinstance(k, dict):
                    kline_frames += 1
                    is_closed = bool(k.get("x"))
                    if is_closed:
                        closed_frames += 1
                    print(
                        json.dumps(
                            {
                                "event": "minimal_ws.kline",
                                "frame_no": frames,
                                "interval": k.get("i"),
                                "is_closed": is_closed,
                                "open_time_ms": k.get("t"),
                                "close_time_ms": k.get("T"),
                                "close": k.get("c"),
                            },
                            ensure_ascii=False,
                        ),
                    )
                else:
                    print(
                        json.dumps(
                            {
                                "event": "minimal_ws.non_kline",
                                "frame_no": frames,
                                "keys": list(payload.keys()) if isinstance(payload, dict) else str(type(payload)),
                            },
                            ensure_ascii=False,
                        ),
                    )

    print(
        json.dumps(
            {
                "event": "minimal_ws.summary",
                "seconds": args.max_seconds,
                "frames": frames,
                "kline_frames": kline_frames,
                "closed_frames": closed_frames,
            },
            ensure_ascii=False,
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
