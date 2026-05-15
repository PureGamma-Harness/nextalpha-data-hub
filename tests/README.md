# tests

用于本地或服务器上做「下游客户端接入」烟雾测试，验证：

- 能否消费 `1h` 已收盘 K 线 WebSocket；
- 端到端延迟字段是否连通。

## 1) 先启动服务

在仓库根目录：

```bash
docker compose up -d --build
```

## 2) 启动示例下游客户端（直连 klines WebSocket）

```bash
python tests/smoke_downstream_client.py \
  --ws-url ws://127.0.0.1:8766/ws/klines \
  --klines-interval 5m \
  --ack-enabled \
  --downstream-region ap-northeast-1
```

参数说明：

- `--klines-interval`：仅用于超时等待日志里的周期提示（默认 `1h`；当前 K 线联调常用 `5m`）
- `--max-messages N`：消费 N 条后自动退出
- `--ack-enabled`：回传逐条 `event_id` ACK，并在 `batch_done=true` 后回传真实的 `batch_ack`
- `--downstream-region`：下游所属 region（用于服务侧 region 对齐统计）
- 如果真实客户端有整合、计算、下单阶段，可按 `docs/downstream-ack-protocol.md` 在对应阶段完成时回传 `batch_integrated_ack / batch_compute_ack / batch_order_ack`；没有就不需要回传，服务端通知会显示 `n=0`

## 2.1) 启动告警客户端测试（直连 alerts WebSocket）

```bash
python tests/smoke_alerts_client.py \
  --ws-url ws://127.0.0.1:8765
```

会打印告警消息字段（`symbol/interval/direction/move_pct/threshold_pct`）和延迟信息。

## 3) 观察结果

脚本会打印 JSON 行日志，例如：

- `downstream.receive`：表示示例客户端收到上游推送
- `downstream.ack_sent`：表示示例客户端已回传某个 batch 阶段 ACK

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
