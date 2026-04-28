from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any

import orjson
import redis.asyncio as redis

from .config import AppConfig
from .latency import LatencyEvent, LatencyMonitor
from .metrics import KLINES_PUBLISHED_1H, KLINES_SKIPPED_DUPLICATE_1H
from .models import ClosedKline

logger = logging.getLogger(__name__)


class RedisStore:
    def __init__(self, cfg: AppConfig, latency_monitor: LatencyMonitor) -> None:
        self._cfg = cfg
        self._latency_monitor = latency_monitor
        self._client = redis.from_url(cfg.redis.url, decode_responses=True)
        self._pending_latency: dict[str, LatencyEvent] = {}

    async def close(self) -> None:
        await self._client.aclose()

    def _one_m_key(self, symbol: str) -> str:
        return f"{self._cfg.redis.key_prefix}md:1m:{symbol}"

    def _encode_1m_member(self, kline: ClosedKline) -> str:
        payload = {
            "t": kline.open_time_ms,
            "T": kline.close_time_ms,
            "o": str(kline.open_price),
            "h": str(kline.high_price),
            "l": str(kline.low_price),
            "c": str(kline.close_price),
            "v": str(kline.volume),
            "q": str(kline.quote_volume),
            "E": kline.event_time_ms,
            "I": kline.ingest_time_ms,
            "src": kline.source,
        }
        return orjson.dumps(payload).decode("utf-8")

    def _decode_1m_member(self, symbol: str, raw: str) -> ClosedKline:
        d = orjson.loads(raw)
        return ClosedKline(
            symbol=symbol,
            interval="1m",
            open_time_ms=int(d["t"]),
            close_time_ms=int(d["T"]),
            open_price=Decimal(d["o"]),
            high_price=Decimal(d["h"]),
            low_price=Decimal(d["l"]),
            close_price=Decimal(d["c"]),
            volume=Decimal(d["v"]),
            quote_volume=Decimal(d["q"]),
            event_time_ms=int(d["E"]),
            ingest_time_ms=int(d["I"]),
            source=str(d.get("src", "redis")),
        )

    async def append_1m_closed(self, kline: ClosedKline) -> None:
        key = self._one_m_key(kline.symbol)
        member = self._encode_1m_member(kline)
        await self._client.zadd(key, {member: kline.open_time_ms})
        n = await self._client.zcard(key)
        max_bars = self._cfg.redis.one_m_ring_max_bars
        excess = int(n) - max_bars
        if excess > 0:
            await self._client.zremrangebyrank(key, 0, excess - 1)
        await self._client.expire(key, self._cfg.redis.one_m_ring_ttl_seconds)

    async def load_1m_ring(self, symbol: str) -> list[ClosedKline]:
        key = self._one_m_key(symbol)
        members = await self._client.zrange(key, 0, -1)
        return [self._decode_1m_member(symbol, m) for m in members]

    def _evict_pending_if_needed(self) -> None:
        cap = self._cfg.redis.pending_latency_max
        while len(self._pending_latency) >= cap:
            drop = next(iter(self._pending_latency))
            self._pending_latency.pop(drop, None)
            logger.warning("pending_latency.evict event_id=%s cap=%s", drop, cap)

    async def publish_1h_closed(self, kline: ClosedKline) -> str | None:
        publish_ms = int(time.time() * 1000)
        event_id = f"{kline.symbol}:1h:{kline.open_time_ms}"
        idem_key = f"{self._cfg.redis.key_prefix}published:1h:{event_id}"
        if not await self._client.set(
            idem_key,
            "1",
            nx=True,
            ex=self._cfg.redis.published_1h_ttl_seconds,
        ):
            KLINES_SKIPPED_DUPLICATE_1H.inc()
            logger.debug("publish.skip_duplicate event_id=%s", event_id)
            return None

        payload: dict[str, Any] = {
            "event_id": event_id,
            "symbol": kline.symbol,
            "interval": kline.interval,
            "open_time_ms": str(kline.open_time_ms),
            "close_time_ms": str(kline.close_time_ms),
            "open": str(kline.open_price),
            "high": str(kline.high_price),
            "low": str(kline.low_price),
            "close": str(kline.close_price),
            "volume": str(kline.volume),
            "quote_volume": str(kline.quote_volume),
            "exchange_event_ms": str(kline.event_time_ms),
            "service_ingest_ms": str(kline.ingest_time_ms),
            "service_publish_ms": str(publish_ms),
            "service_region": self._cfg.service_region,
            "source": kline.source,
        }
        try:
            await self._client.xadd(
                self._cfg.redis.stream_1h_closed,
                fields=payload,
                maxlen=self._cfg.redis.stream_maxlen,
                approximate=True,
            )
        except Exception:
            await self._client.delete(idem_key)
            raise
        KLINES_PUBLISHED_1H.inc()

        ev = LatencyEvent(
            event_id=event_id,
            symbol=kline.symbol,
            interval=kline.interval,
            exchange_event_ms=kline.event_time_ms,
            service_ingest_ms=kline.ingest_time_ms,
            service_publish_ms=publish_ms,
            service_region=self._cfg.service_region,
        )
        self._evict_pending_if_needed()
        self._pending_latency[event_id] = ev
        await self._emit_internal_latency_event(ev)
        return event_id

    async def _emit_internal_latency_event(self, ev: LatencyEvent) -> None:
        payload = {
            "event_id": ev.event_id,
            "symbol": ev.symbol,
            "interval": ev.interval,
            "exchange_to_service_ms": str(ev.exchange_to_service_ms()),
            "service_region": ev.service_region,
            "phase": "published",
        }
        await self._client.xadd(
            self._cfg.redis.stream_latency_events,
            fields=payload,
            maxlen=self._cfg.redis.stream_maxlen,
            approximate=True,
        )

    async def run_ack_listener(self, stop: asyncio.Event | None = None) -> None:
        stream = self._cfg.redis.stream_downstream_ack
        last_id = "$"
        logger.info("ack.listener.start stream=%s", stream)
        while stop is None or not stop.is_set():
            rows = await self._client.xread({stream: last_id}, block=1000, count=100)
            if stop and stop.is_set():
                break
            if not rows:
                continue
            for _, messages in rows:
                for msg_id, fields in messages:
                    last_id = msg_id
                    await self._handle_ack(fields)

    async def _handle_ack(self, fields: dict[str, str]) -> None:
        event_id = fields.get("event_id")
        if not event_id:
            return

        ev = self._pending_latency.get(event_id)
        if ev is None:
            logger.info("ack.unknown event_id=%s", event_id)
            return

        try:
            ev.downstream_receive_ms = int(fields["downstream_receive_ms"])
        except (KeyError, ValueError):
            logger.warning("ack.invalid event_id=%s fields=%s", event_id, fields)
            return
        ev.downstream_region = fields.get("downstream_region")

        e2e = ev.end_to_end_ms()
        svc_to_downstream = ev.service_to_downstream_ms()
        region_ok = ev.region_aligned()
        logger.info(
            "latency.final event_id=%s symbol=%s e2e_ms=%s exchange_to_service_ms=%s "
            "service_to_downstream_ms=%s service_region=%s downstream_region=%s region_aligned=%s",
            ev.event_id,
            ev.symbol,
            e2e,
            ev.exchange_to_service_ms(),
            svc_to_downstream,
            ev.service_region,
            ev.downstream_region,
            region_ok,
        )

        await self._client.xadd(
            self._cfg.redis.stream_latency_events,
            fields={
                "event_id": ev.event_id,
                "symbol": ev.symbol,
                "interval": ev.interval,
                "e2e_ms": str(e2e),
                "exchange_to_service_ms": str(ev.exchange_to_service_ms()),
                "service_to_downstream_ms": str(svc_to_downstream),
                "service_region": ev.service_region,
                "downstream_region": ev.downstream_region or "",
                "region_aligned": str(region_ok),
                "phase": "ack",
            },
            maxlen=self._cfg.redis.stream_maxlen,
            approximate=True,
        )
        await self._latency_monitor.maybe_notify(ev)
        self._pending_latency.pop(event_id, None)
