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

## 当前实现细节（核心逻辑）

### 1) 整体数据链路

- **交易对集合**：启动时通过 Binance `exchangeInfo` 拉取全量 U 本位永续交易对（`PERPETUAL` + `USDT` + `TRADING`），并按周期定时刷新。
- **告警链路**（`market-watcher-alerts`）：
  - 消费 `1m` 已收盘 K 线；
  - 在 `1m/5m/1h/4h` 收盘边界计算涨跌幅并对比阈值；
  - 触发后同时：
    - 推送到 `ws://<host>:8765/ws/alerts`
    - 推送 Lark 告警通知（中文模板，含客户端连接数）。
- **K 线链路**（`market-watcher-klines`）：
  - 消费配置周期（当前联调常用 `5m`，生产可切 `1h`）的已收盘 K 线；
  - 按 `close_time_ms` 做“全市场同一收盘时刻批次聚合”；生产默认 `websocket.klines_passthrough_mode: false`，服务端聚齐（或达放行策略）后**整批一次**下发；若改为 `true` 则为分片凑齐或超时后**多轮**直推。
  - 推送到 `ws://<host>:8766/ws/klines`（单条消息为 `list[dict]` 批量）。

### 2) WebSocket 分片与批次聚合

- **分片规则**：
  - 按 `websocket.klines_ws_batch_size` 将全量 symbol 分成多个 shard（例如 530 个币、batch_size=200 -> 3 个 shard）；
  - 每个 shard 建立一条 Binance 合并流连接，独立消费。
- **批次键**：`close_time_ms`
  - 各 shard 的同一收盘时刻数据会汇聚到同一个 `pending_by_close[close_time_ms]`；
  - 当满足发布条件后一次性广播该批次。
- **去重与幂等**：
  - 每 symbol 只接受严格递增 `close_time_ms`；
  - 已发布 `close_time_ms` 放入去重集合，避免重复发布同一收盘批次。

### 3) 发布策略（Full-batch + 容忍缺失 + REST 修补）

- **全量优先**：`klines_full_batch_only: true` 时，优先等待全量 symbol 到齐再发布。
- **最大等待放行**：达到 `klines_full_batch_max_wait_ms` 后，如果缺失 `<= klines_max_missing_symbols`，可容忍放行。
- **REST 修补**：
  - 对长时间未达标批次，按 `klines_rest_repair_delay_ms` 延迟后触发定向 REST 修补缺失 symbol；
  - 修补后若达到发布条件即发布；
  - 批次中每条数据包含 `source=ws/rest`，便于计算 REST 占比。
- **注意**：当前实现中 `klines_full_batch_max_wait_ms` 直接按配置值生效（下限 1000ms），不再被 `flush_grace` 强行抬高。

### 4) ACK 回执与延迟口径

- 客户端可回传 ACK（`event_id/downstream_receive_ms/downstream_region`）。
- 服务端维护两类“服务到客户端”口径：
  - **不含 ACK**：从 `service_publish_ms` 到 websocket 广播完成；
  - **含 ACK**：从 `service_publish_ms` 到收到下游 ACK 的 `downstream_receive_ms`。
- 核心延迟字段：
  - `exchange_event_ms -> service_ingest_ms`（Binance 到服务）
  - `service_ingest_ms -> service_publish_ms`（服务端整合）
  - `service_publish_ms -> downstream_receive_ms`（服务到下游，ACK口径）

### 5) 告警计算细节

- 告警仅基于**已收盘** 1m K 线。
- 多周期判断在各自收盘边界执行（1m/5m/1h/4h）。
- 为避免误报，窗口增加了**连续性校验**：若最近 N 根 1m 不连续则跳过本次判断。
- 冷却维度：`symbol + interval + direction(up/down)`，由 `alerts.cooldown_seconds` 控制。
- 告警方向对下游 payload 保持 `up/down`（兼容程序解析），Lark 文案展示为中文“上涨/下跌”。

### 6) Lark 推送内容（当前）

- **K线批次与ACK汇总**（单条合并消息）包含：
  - 推送模式（聚合整批一次性下发 / 随到齐即一次性推送多轮统计）
  - 收盘时间（格式化 `YYYY-MM-DD HH:MM:SS`）
  - **收盘至推送完成**：`last_push_done_ms - close_time_ms`（`close_time_ms` 为 Binance 当根最后一毫秒；`last_push_done_ms` 为本批最后一次 WebSocket 写出；passthrough 多轮时取末轮）
  - 推送数量/期望总数/缺失数量
  - WS/REST 条数
  - Binance到服务端、服务端整合耗时、服务端到客户端（不含ACK）、服务端到客户端（ACK）
  - 每项统计均展示：`n`、`本次`、`avg`、`p95`、`min`、`max`
  - K线客户端连接数、ACK上报客户端数、ACK已回执/期望总数
- **告警通知**包含：
  - 币种、周期、方向、上一根K线收盘价、当前对比K线收盘价、涨跌幅、触发阈值、告警客户端连接数。

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
python tests/smoke_downstream_client.py --ws-url ws://127.0.0.1:8766/ws/klines --klines-interval 1h
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
