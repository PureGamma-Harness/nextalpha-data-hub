# tests

用于本地或服务器上做「下游客户端接入」烟雾测试，验证：

- 能否同时消费 `klines.all.1h`、`klines.majors.1h` 两个 K 线 topic；
- 能否通过 alert 服务复用 1m 摄取消费 `klines.majors.1m` topic；
- 端到端延迟字段是否连通。

## 1) 先启动服务

在仓库根目录：

```bash
docker compose up -d --build
```

## 2) 启动模拟下游客户端（直连 klines WebSocket）

```bash
python tests/smoke_downstream_client.py \
  --ws-url ws://127.0.0.1:8766/ws/klines \
  --klines-interval 1h \
  --ack-enabled \
  --downstream-region ap-northeast-1
```

参数说明：

- `--klines-interval`：仅用于超时等待日志里的周期提示（默认 `1h`，与全量 `websocket.klines_interval` 对齐）
- `--topic` / `--topics`：订阅指定 K 线 topic；不传时默认订阅 `klines.all.1h,klines.majors.1h`
- `--print-rows`：打印每条 row；默认只打印批次摘要，避免全量 1h 刷屏
- `--max-messages N`：消费 N 条后自动退出
- `--ack-enabled`：把 `event_id + downstream_receive_ms + downstream_region` 回传给服务端
- `--downstream-region`：下游所属 region（用于服务侧 region 对齐统计）

重点币 1m topic 由 alert 服务产生，不再由 Kline 服务重复订阅 Binance。测试方式：

```bash
python tests/smoke_downstream_client.py \
  --ws-url ws://127.0.0.1:8765/ws/klines \
  --topics klines.majors.1m \
  --klines-interval 1m \
  --ack-enabled \
  --downstream-region ap-northeast-1
```

## 2.1) 启动告警客户端测试（直连 alerts WebSocket）

```bash
python tests/smoke_alerts_client.py \
  --ws-url ws://127.0.0.1:8765
```

会打印告警消息字段（`symbol/interval/direction/move_pct/threshold_pct`）和延迟信息。

## 3) 观察结果

脚本会打印 JSON 行日志，例如：

- `downstream.receive_batch`：表示模拟客户端收到一个 topic 批次
- `downstream.send_batch_ack`：表示已回传该 topic 的 `batch_ack` / `batch_integrated_ack` / `batch_compute_ack` / `batch_order_ack`

## 4) 额外检查（可选）

```bash
docker compose logs -f market-watcher-klines
```

可以看到 watcher 侧 `latency.final ...` 日志。

## 5) 自动统计延迟分位数（验收）

脚本会连接 `klines` WebSocket 采样最近 N 条消息，输出：

- `exchange_to_service_ms` 的 P50/P95/P99
- `service_to_downstream_ms` 的 P50/P95/P99
- `end_to_end_ms` 的 P50/P95/P99
- `region_alignment_ratio`

```bash
python tests/smoke_check_latency.py \
  --ws-url ws://127.0.0.1:8766/ws/klines \
  --sample-size 100 \
  --required-batches 3 \
  --expected-symbols 530 \
  --max-missing-symbols 10 \
  --max-rest-ratio 0.05 \
  --warn-e2e-p95-ms 1500 \
  --strict
```

- `--strict`：当超过阈值时返回非 0，适合上线前 gate
- 不带 `--strict`：只输出告警信息，不阻断流程
