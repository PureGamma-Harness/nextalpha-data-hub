# nextalpha-market-watcher

Binance U perpetual market watcher for:

- Alerting thresholds based on closed candles.
- Pushing closed 1h klines to downstream strategy consumers.
- Latency observability across exchange ingress, service publish, and downstream receipt.

## Quick start

### Docker Compose（推荐）

在仓库根目录使用 **`config.docker.yaml`**。  
**告警**与 **1h 收盘推送** 拆成两个服务，可单独重启：

| 服务 | 职责 | 宿主机指标端口 |
|------|------|----------------|
| `market-watcher-alerts` | 全量币对涨跌幅告警（1m/5m/1h/4h）+ WebSocket 推送 | `http://localhost:9090/metrics` |
| `market-watcher-klines` | 全量币对 1h 已收盘 K 线 WebSocket 推送 | `http://localhost:9091/metrics` |

```bash
docker compose up -d --build
```

只重建/重启其中一个示例：

```bash
docker compose up -d --build market-watcher-alerts
docker compose restart market-watcher-klines
```

- 客户端接入（FastAPI WebSocket）：
  - 告警 WebSocket: `ws://<host>:8765/ws/alerts`
  - K线 WebSocket: `ws://<host>:8766/ws/klines`

线上启动示例：

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build \
  market-watcher-alerts market-watcher-klines
```

改配置：编辑 `config.docker.yaml` 后 `docker compose restart market-watcher-alerts market-watcher-klines`。  
本地试跑少量交易对：将其中 `binance.use_test_symbols` 设为 `true` 并填写 `test_symbols`。

本机一条进程跑全功能（不推荐与 compose 双服务同时跑同一 Redis 全量）：  
`nextalpha-watcher run --config config.example.yaml --mode all`

### 本机 Python

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
nextalpha-watcher run --config ./config.example.yaml
```

## WebSocket payload latency fields

WebSocket 推送消息中包含以下时间字段，方便客户端自行计算延迟：

- `exchange_event_ms`：交易所事件时间
- `service_ingest_ms`：服务收到时间
- `service_publish_ms`：服务发送时间

## Reliability and observability

- **REST backfill**: after each WebSocket connect, the service pulls recent closed `1m` / `1h` klines via `GET /fapi/v1/klines` (rate-limited by concurrency and per-request delay) to reduce gaps after disconnects.
- **1m dedupe for alerts**: strictly increasing `open_time_ms` per symbol avoids double-counting when backfill overlaps live WS.
- **Prometheus**: when `metrics.enabled` is true, `GET /metrics` is served on `metrics.host:metrics.port` (default `0.0.0.0:9090`). Key series: `watcher_klines_received_total`, `watcher_exchange_to_service_ms`, `watcher_backfill_*`, `watcher_klines_published_1h_total`, `watcher_last_ingest_unix_seconds`.
- **1m window in memory**: each symbol keeps up to ~8h 1m closed candles in memory for multi-interval alert calculation.
- **Symbol refresh**: periodically re-fetches `exchangeInfo`; when the symbol set changes, ingestion tasks restart and alert state for removed symbols is pruned.
- **Shutdown**: SIGINT/SIGTERM (where supported) stops ingestion gracefully and closes Redis connections. Pending latency tracking is capped at `redis.pending_latency_max` with FIFO eviction.

## Downstream smoke test

为了模拟策略端接入（直连 WebSocket），项目提供了 `tests/smoke_downstream_client.py`：

```bash
python tests/smoke_downstream_client.py --ws-url ws://127.0.0.1:8766/ws/klines
```

完整说明见 `tests/README.md`。

告警链路客户端模拟：

```bash
python tests/smoke_alerts_client.py --ws-url ws://127.0.0.1:8765/ws/alerts
```

快速输出最近 ACK 的延迟分位数（P50/P95/P99）：

```bash
python tests/smoke_check_latency.py --ws-url ws://127.0.0.1:8766/ws/klines --strict
```
