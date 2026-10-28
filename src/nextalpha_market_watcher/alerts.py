from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal
from collections.abc import Awaitable, Callable

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
    reference_close_price: str
    current_close_price: str


class AlertEngine:
    def __init__(
        self,
        cfg: AppConfig,
        notifier: Notifier,
        alert_clients_count_getter: Callable[[], Awaitable[int]] | None = None,
    ) -> None:
        self._cfg = cfg
        self._notifier = notifier
        self._alert_clients_count_getter = alert_clients_count_getter
        self._history: dict[str, deque[ClosedKline]] = defaultdict(
            lambda: deque(maxlen=cfg.redis.one_m_ring_max_bars),
        )
        self._cooldown: dict[str, int] = {}
        self._last_1m_open_ms: dict[str, int] = {}
        # Strong references to in-flight webhook sends so they are not GC'd.
        self._notify_tasks: set[asyncio.Task[None]] = set()

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
        if not self._is_contiguous_1m_window(window):
            # Skip alert evaluation when 1m bars are not continuous.
            # This avoids false positives/negatives after transient WS gaps.
            return None
        open_price = window[0].open_price
        close_price = window[-1].close_price
        if open_price == 0:
            return None

        pct = (close_price - open_price) / open_price * Decimal(100)
        if abs(pct) < Decimal(str(threshold_pct)):
            return None

        direction = "up" if pct > 0 else "down"
        direction_text = "上涨" if direction == "up" else "下跌"
        alert_clients = 0
        if self._alert_clients_count_getter is not None:
            try:
                alert_clients = max(0, int(await self._alert_clients_count_getter()))
            except Exception:
                alert_clients = 0
        now = int(time.time())
        key = f"{symbol}:{interval}:{direction}"
        expire_at = self._cooldown.get(key, 0)
        if now < expire_at:
            return None
        self._cooldown[key] = now + self._cfg.alerts.cooldown_seconds

        msg = (
            f"币种：{symbol}\n"
            f"周期：{interval}\n"
            f"方向：{direction_text}\n"
            f"上一根K线收盘价：{open_price}\n"
            f"当前对比K线收盘价：{close_price}\n"
            f"涨跌幅：{pct:.4f}%\n"
            f"触发阈值：{interval} {threshold_pct}%\n"
            f"告警客户端连接数：{alert_clients}"
        )
        logger.warning("alert.trigger %s", msg)
        # Fire-and-forget: webhook retries (up to ~20s) must not block alert fan-out.
        self._schedule_notify("告警通知", msg, symbol, interval, direction)
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
            reference_close_price=str(open_price),
            current_close_price=str(close_price),
        )

    def _schedule_notify(self, title: str, msg: str, symbol: str, interval: str, direction: str) -> None:
        task = asyncio.create_task(self._notifier.send(title, msg))
        self._notify_tasks.add(task)

        def _done(t: asyncio.Task[None]) -> None:
            self._notify_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                # Notification failure should not interrupt core alert ingestion.
                logger.error(
                    "alert.notify_failed symbol=%s interval=%s direction=%s",
                    symbol,
                    interval,
                    direction,
                    exc_info=exc,
                )

        task.add_done_callback(_done)

    async def drain_notifications(self, timeout: float | None = None) -> None:
        """Wait for in-flight webhook sends (used on shutdown and in tests)."""
        if not self._notify_tasks:
            return
        await asyncio.wait(set(self._notify_tasks), timeout=timeout)

    def _is_interval_close_boundary(self, latest_1m: ClosedKline, bars: int) -> bool:
        # Binance close_time_ms ends with ...999; +1 aligns to the next minute boundary.
        next_ms = latest_1m.close_time_ms + 1
        interval_ms = bars * 60_000
        if interval_ms <= 0:
            return False
        return next_ms % interval_ms == 0

    def _is_contiguous_1m_window(self, window: list[ClosedKline]) -> bool:
        if len(window) <= 1:
            return True
        prev_open = window[0].open_time_ms
        for row in window[1:]:
            if row.open_time_ms - prev_open != 60_000:
                return False
            prev_open = row.open_time_ms
        return True
