from __future__ import annotations

import logging
from dataclasses import dataclass

from .notifier import Notifier

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LatencyEvent:
    event_id: str
    symbol: str
    interval: str
    exchange_event_ms: int
    service_ingest_ms: int
    service_publish_ms: int
    service_region: str
    downstream_receive_ms: int | None = None
    downstream_region: str | None = None

    def exchange_to_service_ms(self) -> int:
        return self.service_ingest_ms - self.exchange_event_ms

    def service_to_downstream_ms(self) -> int | None:
        if self.downstream_receive_ms is None:
            return None
        return self.downstream_receive_ms - self.service_publish_ms

    def end_to_end_ms(self) -> int | None:
        if self.downstream_receive_ms is None:
            return None
        return self.downstream_receive_ms - self.exchange_event_ms

    def region_aligned(self) -> bool | None:
        if self.downstream_region is None:
            return None
        return self.downstream_region == self.service_region


class LatencyMonitor:
    def __init__(self, notifier: Notifier, e2e_warn_ms: int) -> None:
        self._notifier = notifier
        self._e2e_warn_ms = e2e_warn_ms

    async def maybe_notify(self, event: LatencyEvent) -> None:
        e2e = event.end_to_end_ms()
        if e2e is None:
            return
        if e2e <= self._e2e_warn_ms:
            return

        region_ok = event.region_aligned()
        body = (
            f"event_id={event.event_id} symbol={event.symbol} interval={event.interval} "
            f"e2e_ms={e2e} exchange_to_service_ms={event.exchange_to_service_ms()} "
            f"service_to_downstream_ms={event.service_to_downstream_ms()} "
            f"service_region={event.service_region} downstream_region={event.downstream_region} "
            f"region_aligned={region_ok}"
        )
        logger.warning("latency.alert %s", body)
        await self._notifier.send("Latency warning", body)
