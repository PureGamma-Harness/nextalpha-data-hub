from __future__ import annotations

import argparse
import asyncio
import json
from math import ceil
from statistics import mean
import time

import websockets


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if p <= 0:
        return values[0]
    if p >= 100:
        return values[-1]
    rank = ceil((p / 100.0) * len(values)) - 1
    idx = min(max(rank, 0), len(values) - 1)
    return values[idx]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check latency percentiles from kline websocket feed",
    )
    parser.add_argument("--ws-url", default="ws://127.0.0.1:8766/ws/klines")
    parser.add_argument("--sample-size", type=int, default=100, help="Number of websocket messages to sample")
    parser.add_argument("--max-seconds", type=int, default=900, help="Stop waiting after this many seconds")
    parser.add_argument("--min-samples", type=int, default=20, help="Fail if sampled messages below threshold")
    parser.add_argument("--downstream-region", default="ap-northeast-1")
    parser.add_argument("--warn-e2e-p95-ms", type=float, default=1500.0)
    parser.add_argument("--warn-e2s-p95-ms", type=float, default=500.0, help="exchange_to_service p95 warning")
    parser.add_argument("--warn-s2d-p95-ms", type=float, default=1000.0, help="service_to_downstream p95 warning")
    parser.add_argument("--strict", action="store_true", help="Return non-zero when any warning threshold is exceeded")
    return parser.parse_args()


def summarize(values: list[float]) -> dict[str, float]:
    if not values:
        return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    v = sorted(values)
    return {
        "count": float(len(v)),
        "mean": mean(v),
        "p50": percentile(v, 50),
        "p95": percentile(v, 95),
        "p99": percentile(v, 99),
        "max": v[-1],
    }


async def _collect_samples(args: argparse.Namespace) -> tuple[list[float], list[float], list[float], int, int]:
    e2e_values: list[float] = []
    e2s_values: list[float] = []
    s2d_values: list[float] = []
    region_aligned_cnt = 0
    region_known_cnt = 0

    deadline = time.time() + args.max_seconds
    async with websockets.connect(args.ws_url, ping_interval=20, ping_timeout=20) as ws:
        while len(e2e_values) < args.sample_size and time.time() < deadline:
            timeout_s = max(0.1, min(5.0, deadline - time.time()))
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
            except asyncio.TimeoutError:
                continue

            msg = json.loads(raw)
            rows = msg if isinstance(msg, list) else [msg]
            now_ms = int(time.time() * 1000)
            downstream_region = args.downstream_region

            for row in rows:
                exchange_event_ms = int(row.get("exchange_event_ms", 0) or 0)
                service_ingest_ms = int(row.get("service_ingest_ms", 0) or 0)
                service_publish_ms = int(row.get("service_publish_ms", 0) or 0)
                service_region = row.get("service_region")

                if exchange_event_ms:
                    e2e_values.append(float(now_ms - exchange_event_ms))
                if exchange_event_ms and service_ingest_ms:
                    e2s_values.append(float(service_ingest_ms - exchange_event_ms))
                if service_publish_ms:
                    s2d_values.append(float(now_ms - service_publish_ms))

                if service_region:
                    region_known_cnt += 1
                    if service_region == downstream_region:
                        region_aligned_cnt += 1

    return e2e_values, e2s_values, s2d_values, region_aligned_cnt, region_known_cnt


def main() -> int:
    args = parse_args()
    e2e_values, e2s_values, s2d_values, region_aligned_cnt, region_known_cnt = asyncio.run(_collect_samples(args))

    e2e_sum = summarize(e2e_values)
    e2s_sum = summarize(e2s_values)
    s2d_sum = summarize(s2d_values)
    region_aligned_ratio = (region_aligned_cnt / region_known_cnt) if region_known_cnt else 0.0

    warnings: list[str] = []
    if len(e2e_values) < args.min_samples:
        warnings.append(f"sample too small: {len(e2e_values)} < {args.min_samples}")
    if e2e_sum["p95"] > args.warn_e2e_p95_ms:
        warnings.append(f"e2e p95 too high: {e2e_sum['p95']:.2f}ms > {args.warn_e2e_p95_ms}ms")
    if e2s_sum["p95"] > args.warn_e2s_p95_ms:
        warnings.append(f"exchange_to_service p95 too high: {e2s_sum['p95']:.2f}ms > {args.warn_e2s_p95_ms}ms")
    if s2d_sum["p95"] > args.warn_s2d_p95_ms:
        warnings.append(f"service_to_downstream p95 too high: {s2d_sum['p95']:.2f}ms > {args.warn_s2d_p95_ms}ms")

    report = {
        "event": "latency.summary",
        "ws_url": args.ws_url,
        "sample_size_requested": args.sample_size,
        "sample_count": len(e2e_values),
        "exchange_to_service_ms": e2s_sum,
        "service_to_downstream_ms": s2d_sum,
        "end_to_end_ms": e2e_sum,
        "region_alignment_ratio": region_aligned_ratio,
        "warnings": warnings,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if warnings and args.strict:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
