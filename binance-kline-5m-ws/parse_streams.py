#!/usr/bin/env python3
"""Parse kline stream file and regenerate shard URLs + symbols list."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

KLINE_SEG = re.compile(r"^(.+)@kline_[0-9]+[mhdw]$")


def parse_tokens(path: Path) -> list[str]:
    tokens: list[str] = []
    for line in path.read_text().splitlines():
        if not line.startswith("wss://"):
            continue
        streams = line.split("streams=", 1)[1]
        for seg in streams.split("/"):
            m = KLINE_SEG.match(seg.strip())
            if m:
                tokens.append(m.group(1))
    return tokens


def build_shards(tokens: list[str], shard_size: int | None = None) -> list[list[str]]:
    if shard_size is None:
        shard_sizes: tuple[int, ...] = (200, 200, 128)
        shards: list[list[str]] = []
        idx = 0
        for size in shard_sizes:
            shards.append(tokens[idx : idx + size])
            idx += size
        if idx < len(tokens):
            shards.append(tokens[idx:])
        return [s for s in shards if s]
    return [tokens[i : i + shard_size] for i in range(0, len(tokens), shard_size)]


def write_outputs(
    out_dir: Path,
    tokens: list[str],
    interval: str = "3m",
    shard_size: int | None = 88,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    shards = build_shards(tokens, shard_size)
    suffix = f"@kline_{interval}"
    lines: list[str] = []
    for i, shard in enumerate(shards, 1):
        streams = "/".join(f"{s}{suffix}" for s in shard)
        url = f"wss://fstream.binance.com/market/stream?streams={streams}"
        lines.append(f"# shard {i}/{len(shards)} symbols={len(shard)} chars={len(url)}")
        lines.append(url)
        lines.append("")
    out_path = out_dir / f"klines_ws_{interval}_528.txt"
    out_path.write_text("\n".join(lines).rstrip() + "\n")
    (out_dir / "symbols.txt").write_text("\n".join(tokens) + "\n")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert kline WS file to another interval")
    parser.add_argument("source", type=Path, help="Source klines_ws_*.txt")
    parser.add_argument("-o", "--out-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--interval", default="3m", help="Target interval, e.g. 3m, 5m, 1h")
    parser.add_argument(
        "--shard-size",
        type=int,
        default=88,
        help="Symbols per WS connection (default: 88)",
    )
    args = parser.parse_args()
    tokens = parse_tokens(args.source)
    out = write_outputs(args.out_dir, tokens, args.interval, args.shard_size)
    print(f"tokens={len(tokens)} unique={len(set(tokens))} interval={args.interval} -> {out}")


if __name__ == "__main__":
    main()
