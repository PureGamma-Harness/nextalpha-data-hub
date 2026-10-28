from __future__ import annotations

import asyncio
import time
from decimal import Decimal

from nextalpha_market_watcher.alerts import AlertEngine
from nextalpha_market_watcher.config import AppConfig
from nextalpha_market_watcher.fastapi_ws_hub import FastAPIWsHub
from nextalpha_market_watcher.models import ClosedKline


class FakeWs:
    def __init__(self, delay: float = 0.0, fail: bool = False) -> None:
        self.delay = delay
        self.fail = fail
        self.sent: list[str] = []
        self.closed = False

    async def send_text(self, message: str) -> None:
        if self.fail:
            raise RuntimeError("broken pipe")
        await asyncio.sleep(self.delay)
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


async def test_broadcast_is_concurrent_and_drops_slow_and_broken_clients() -> None:
    hub = FastAPIWsHub(name="test", send_timeout_s=0.2)
    fast_a, fast_b = FakeWs(), FakeWs()
    slow = FakeWs(delay=5.0)
    broken = FakeWs(fail=True)
    hub._clients.update({fast_a, fast_b, slow, broken})

    started = time.monotonic()
    sent = await hub.broadcast_json({"hello": "world"})
    elapsed = time.monotonic() - started

    assert sent == 2
    assert elapsed < 1.0
    assert fast_a.sent == ['{"hello":"world"}']
    assert fast_b.sent == ['{"hello":"world"}']
    assert await hub.client_count() == 2
    assert slow not in hub._clients and broken not in hub._clients
    assert slow.closed and broken.closed


async def test_broadcast_with_no_clients() -> None:
    hub = FastAPIWsHub(name="empty")
    assert await hub.broadcast_json({"x": 1}) == 0


class SlowNotifier:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def send(self, title: str, body: str) -> None:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        raise RuntimeError("webhook down")


def _kline(symbol: str, open_ms: int, open_px: str, close_px: str) -> ClosedKline:
    return ClosedKline(
        symbol=symbol,
        interval="1m",
        open_time_ms=open_ms,
        close_time_ms=open_ms + 59_999,
        open_price=Decimal(open_px),
        high_price=Decimal(close_px),
        low_price=Decimal(open_px),
        close_price=Decimal(close_px),
        volume=Decimal("1"),
        quote_volume=Decimal("1"),
        event_time_ms=open_ms + 60_000,
        ingest_time_ms=open_ms + 60_000,
        source="ws",
    )


async def test_alert_does_not_wait_for_webhook() -> None:
    notifier = SlowNotifier()
    engine = AlertEngine(AppConfig(), notifier)  # type: ignore[arg-type]
    # A 20% 1m move on a minute boundary triggers the 1m alert.
    signals = await asyncio.wait_for(
        engine.on_closed_1m(_kline("BTCUSDT", 1_700_000_040_000, "100", "120")),
        timeout=1.0,
    )
    assert [s.interval for s in signals] == ["1m"]
    await asyncio.wait_for(notifier.started.wait(), timeout=1.0)
    assert notifier.calls == 1
    # Failure in the background send is logged, not raised, and the task is released.
    notifier.release.set()
    await engine.drain_notifications(timeout=1.0)
    assert not engine._notify_tasks
