from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ServiceMode = Literal["alerts", "klines", "all"]
KlinesSource = Literal["kline", "aggtrade"]


class BinanceConfig(BaseModel):
    ws_base: str = "wss://fstream.binance.com/market/stream"
    rest_base: str = "https://fapi.binance.com"
    symbols_refresh_seconds: int = 1800
    # Optional explicit proxy for WS socket connection.
    # e.g. socks5://host:port
    ws_proxy_url: str | None = None
    # Optional explicit proxy for Binance REST requests.
    rest_proxy_url: str | None = None
    ws_watchdog_enabled: bool = True
    ws_watchdog_no_data_periods: int = 2
    ws_watchdog_check_seconds: int = 15
    use_test_symbols: bool = False
    test_symbols: list[str] = Field(default_factory=list)


class BackfillConfig(BaseModel):
    enabled: bool = True
    rest_timeout_seconds: float = 15.0
    rest_concurrency: int = 8
    rest_request_delay_seconds: float = 0.02
    bars_1m: int = 32
    bars_1h: int = 3
    klines_limit_cap: int = 1500


class RedisConfig(BaseModel):
    url: str = "redis://127.0.0.1:6379/0"
    key_prefix: str = "nw:"
    stream_1h_closed: str = "stream:kline:1h:closed"
    stream_downstream_ack: str = "stream:kline:1h:ack"
    stream_latency_events: str = "stream:latency:events"
    stream_maxlen: int = 200000
    published_1h_ttl_seconds: int = 9 * 3600
    one_m_ring_max_bars: int = 480
    one_m_ring_ttl_seconds: int = 36 * 3600
    pending_latency_max: int = 100_000


class AlertsConfig(BaseModel):
    threshold_1m_pct: float = 10.0
    threshold_5m_pct: float = 15.0
    threshold_1h_pct: float = 25.0
    threshold_4h_pct: float = 40.0
    cooldown_seconds: int = 900


class WebhookConfig(BaseModel):
    enabled: bool = False
    url: str = ""
    # Optional proxy for Lark webhook calls.
    proxy_url: str | None = None
    # Optional dedicated webhook for threshold alert notifications.
    # If empty, alerts reuse `url`.
    alerts_url: str = ""


class LatencyConfig(BaseModel):
    e2e_warn_ms: int = 1500


class MetricsConfig(BaseModel):
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 9090


class WebSocketPushConfig(BaseModel):
    host: str = "0.0.0.0"
    alerts_port: int = 8765
    klines_port: int = 8766
    klines_interval: str = "5m"
    # Kline batch publishing uses local @aggTrade aggregation.
    # The legacy "kline" value is accepted for old configs, but App forces it off
    # so the Kline service does not open a second @kline_<interval> WebSocket.
    klines_source: KlinesSource = "aggtrade"
    klines_ws_batch_size: int = 80
    # Only used when klines_source=aggtrade.
    aggtrade_emit_grace_ms: int = 800
    # Deprecated compatibility knob. Individual symbol event lag no longer holds
    # emission or triggers reconnect; shard-level no-data watchdog handles broken WSS.
    aggtrade_ws_max_event_lag_seconds: float = 0.0
    # Deprecated compatibility knob. Late trades for already emitted bars are ignored silently.
    aggtrade_late_trade_log_limit_per_close: int = 0
    # Legacy field name: when true, compare local aggTrade bars with REST klines;
    # this does not open a @kline_<interval> WebSocket. After a stable observation
    # window, set it to false to stop spending REST request weight on validation.
    aggtrade_compare_with_kline: bool = True
    aggtrade_rest_compare_delay_seconds: float = 10.0
    aggtrade_rest_compare_concurrency: int = 8
    aggtrade_rest_compare_lark_timeout_seconds: float = 120.0
    # aggTrade is a live trade stream; it should not be silent for whole kline periods.
    aggtrade_ws_watchdog_stale_seconds: int = 30
    aggtrade_price_tolerance: str = "0"
    aggtrade_volume_tolerance: str = "0.00000001"
    aggtrade_quote_tolerance: str = "0.000001"
    # If True, publish each closed kline row immediately to downstream clients
    # without server-side close_time batch aggregation.
    klines_passthrough_mode: bool = False
    # Max wait (ms) to hold a shard-local buffer before flushing a partial batch.
    klines_passthrough_shard_max_wait_ms: int = 3_000
    klines_replay_count: int = 3
    klines_flush_grace_ms: int = 20_000
    klines_full_batch_only: bool = False
    klines_full_batch_max_wait_ms: int = 120_000
    klines_min_publish_ratio: float = 1.0
    # In full-batch mode, optionally allow publishing after max wait
    # when missing symbols are within this count.
    klines_max_missing_symbols: int = 0
    # 0 means auto by interval: 5m->2min, 1h->10min
    klines_rest_repair_delay_ms: int = 0
    # Emit full missing symbol list for each published batch
    klines_log_full_missing_symbols: bool = True
    # If True, when WS is silent for a while, run low-frequency REST fallback repair.
    # Keep this on for testing, and disable in low-latency production once WS is stable.
    klines_ws_silence_fallback_rest: bool = True


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    app_name: str = "nextalpha-market-watcher"
    service_region: str = "unknown"
    log_level: str = "INFO"
    service_mode: ServiceMode = "all"
    binance: BinanceConfig = BinanceConfig()
    backfill: BackfillConfig = BackfillConfig()
    redis: RedisConfig = RedisConfig()
    alerts: AlertsConfig = AlertsConfig()
    webhook: WebhookConfig = WebhookConfig()
    latency: LatencyConfig = LatencyConfig()
    metrics: MetricsConfig = MetricsConfig()
    websocket: WebSocketPushConfig = WebSocketPushConfig()
