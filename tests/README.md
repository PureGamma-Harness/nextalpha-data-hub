# tests

用于本地或服务器上做「下游客户端接入」烟雾测试，验证：

- 能否消费 `1h` 已收盘 K 线 WebSocket；
- 端到端延迟字段是否连通。

## 1) 先启动服务

在仓库根目录：

```bash
docker compose up -d --build
```

## 2) 启动模拟下游客户端（直连 klines WebSocket）

```bash
python tests/smoke_downstream_client.py \
  --ws-url ws://127.0.0.1:8766
```

参数说明：

- `--max-messages N`：消费 N 条后自动退出

## 2.1) 启动告警客户端测试（直连 alerts WebSocket）

```bash
python tests/smoke_alerts_client.py \
  --ws-url ws://127.0.0.1:8765
```

会打印告警消息字段（`symbol/interval/direction/move_pct/threshold_pct`）和延迟信息。

## 3) 观察结果

脚本会打印 JSON 行日志，例如：

- `downstream.receive`：表示模拟客户端收到上游推送

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
  --ws-url ws://127.0.0.1:8766 \
  --sample-size 100 \
  --min-samples 20 \
  --warn-e2e-p95-ms 1500 \
  --strict
```

- `--strict`：当超过阈值时返回非 0，适合上线前 gate
- 不带 `--strict`：只输出告警信息，不阻断流程
