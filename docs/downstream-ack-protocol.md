# Downstream ACK protocol

When your downstream strategy consumer receives an event from `stream:kline:1h:closed`,
it should write one ACK record to `stream:kline:1h:ack`:

Required fields:

- `event_id`: same as upstream payload (`symbol:1h:open_time_ms`)
- `downstream_receive_ms`: consumer receipt timestamp in milliseconds
- `downstream_region`: consumer deployment region tag (example: `ap-northeast-1`)

Example:

```text
XADD stream:kline:1h:ack * event_id BTCUSDT:1h:1713754800000 downstream_receive_ms 1713758400450 downstream_region ap-northeast-1
```

The watcher computes:

- exchange -> service = `service_ingest_ms - exchange_event_ms`
- service -> downstream = `downstream_receive_ms - service_publish_ms`
- end-to-end = `downstream_receive_ms - exchange_event_ms`

Region consistency:

- compare `service_region` and `downstream_region`
- `region_aligned=true` when equal

## WebSocket batch ACK extensions

For the kline WebSocket feed, downstream clients can send these messages back to
the watcher on the same WebSocket connection after receiving `batch_done=true`
rows for a `close_time_ms`.

- `batch_ack`: client received batch
- `batch_integrated_ack`: client merged shards/integrated rows
- `batch_compute_ack`: client completed strategy compute logic
- `batch_order_ack`: client completed live order placement logic

Send only ACKs for stages that really happened. If a client has no compute or
order stage for a batch, do not synthesize those ACKs; the watcher reports them
as `n=0` in the Lark summary.

Common fields:

- `type`: one of the types above
- `close_time_ms`: close time for the batch
- `downstream_region`: client region tag
- `batch_push_done_ms`: echo from server payload when available (preferred)

When `batch_push_done_ms` is echoed, the watcher uses its server receive time to
compute `WS write done -> ACK received`, avoiding clock skew between machines.
The Lark batch summary then reports both:

- `period close -> client received / compute ACK / order ACK`
- `WS write done -> ACK received / compute ACK / order ACK`

Fallback fields when `batch_push_done_ms` is absent:

- `downstream_receive_ms` for `batch_ack`
- `downstream_integrated_ms` for `batch_integrated_ack`
- `downstream_compute_ms` for `batch_compute_ack`
- `downstream_order_ms` for `batch_order_ack`

Example:

```json
{"type":"batch_ack","close_time_ms":1778867999999,"batch_push_done_ms":1778868000237,"downstream_region":"ap-northeast-1"}
```

Optional stage ACKs, sent only after the real stage finishes:

```json
{"type":"batch_compute_ack","close_time_ms":1778867999999,"batch_push_done_ms":1778868000237,"downstream_region":"ap-northeast-1"}
{"type":"batch_order_ack","close_time_ms":1778867999999,"batch_push_done_ms":1778868000237,"downstream_region":"ap-northeast-1"}
```
