from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True)
class ClosedKline:
    symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    event_time_ms: int
    ingest_time_ms: int
    source: str = "ws"
    # Binance combined-stream shard (passthrough batching); empty for REST/synthetic.
    ws_shard_id: str = ""
    ws_shard_symbol_count: int = 0
    ws_total_shards: int = 0
