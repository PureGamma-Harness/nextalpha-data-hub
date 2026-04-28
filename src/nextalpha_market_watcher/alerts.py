from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal

from .config import AppConfig
from .models import ClosedKline
from .notifier import Notifier

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AlertSignal:
    symbol: str
    interval: str
    direction: str
    threshold_pct: float
    move_pct: float
    open_time_ms: int
    close_time_ms: int
    event_time_ms: int
    ingest_time_ms: int


class AlertEngine:
    def __init__(self, cfg: AppConfig, notifier: Notifier) -> None:
        self._cfg = cfg
        self._notifier = notifier
        self._history: dict[str, deque[ClosedKline]] = defaultdict(
            lambda: deque(maxlen=cfg.redis.one_m_ring_max_bars),
        )
        self._cooldown: dict[str, int] = {}
        self._last_1m_open_ms: dict[str, int] = {}

    def prune_to_symbols(self, keep: set[str]) -> None:
        for sym in list(self._history.keys()):
            if sym not in keep:
                del self._history[sym]
        for sym in list(self._last_1m_open_ms.keys()):
            if sym not in keep:
                del self._last_1m_open_ms[sym]
        for key in list(self._cooldown.keys()):
            sym = key.split(":", 1)[0]
            if sym not in keep:
                del self._cooldown[key]

    async def on_closed_1m(self, kline: ClosedKline) -> list[AlertSignal]:
        last = self._last_1m_open_ms.get(kline.symbol, -1)
        if kline.open_time_ms <= last:
            return []
        self._last_1m_open_ms[kline.symbol] = kline.open_time_ms

        buf = self._history[kline.symbol]
        buf.append(kline)
        out: list[AlertSignal] = []

        maybe_1m = await self._check_interval(kline, "1m", 1, self._cfg.alerts.threshold_1m_pct)
        if maybe_1m is not None:
            out.append(maybe_1m)
        maybe_5m = await self._check_interval(kline, "5m", 5, self._cfg.alerts.threshold_5m_pct)
        if maybe_5m is not None:
            out.append(maybe_5m)
        maybe_1h = await self._check_interval(kline, "1h", 60, self._cfg.alerts.threshold_1h_pct)
        if maybe_1h is not None:
            out.append(maybe_1h)
        maybe_4h = await self._check_interval(kline, "4h", 240, self._cfg.alerts.threshold_4h_pct)
        if maybe_4h is not None:
            out.append(maybe_4h)
        return out

    async def _check_interval(
        self,
        latest_1m: ClosedKline,
        interval: str,
        bars: int,
        threshold_pct: float,
    ) -> AlertSignal | None:
        if not self._is_interval_close_boundary(latest_1m, bars):
            return None
        symbol = latest_1m.symbol
        buf = self._history[symbol]
        if len(buf) < bars:
            return None
        window = list(buf)[-bars:]
        open_price = window[0].open_price
        close_price = window[-1].close_price
        if open_price == 0:
            return None

        pct = (close_price - open_price) / open_price * Decimal(100)
        if abs(pct) < Decimal(str(threshold_pct)):
            return None

        direction = "up" if pct > 0 else "down"
        now = int(time.time())
        key = f"{symbol}:{interval}:{direction}"
        expire_at = self._cooldown.get(key, 0)
        if now < expire_at:
            return None
        self._cooldown[key] = now + self._cfg.alerts.cooldown_seconds

        msg = (
            f"symbol={symbol} interval={interval} move_pct={pct:.4f}% "
            f"threshold={threshold_pct}% direction={direction}"
        )
        logger.warning("alert.trigger %s", msg)
        await self._notifier.send("Kline alert", msg)
        return AlertSignal(
            symbol=symbol,
            interval=interval,
            direction=direction,
            threshold_pct=threshold_pct,
            move_pct=float(pct),
            open_time_ms=window[0].open_time_ms,
            close_time_ms=window[-1].close_time_ms,
            event_time_ms=latest_1m.event_time_ms,
            ingest_time_ms=latest_1m.ingest_time_ms,
        )

    def _is_interval_close_boundary(self, latest_1m: ClosedKline, bars: int) -> bool:
        # Binance close_time_ms ends with ...999; +1 aligns to the next minute boundary.
        next_ms = latest_1m.close_time_ms + 1
        interval_ms = bars * 60_000
        if interval_ms <= 0:
            return False
        return next_ms % interval_ms == 0
