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
