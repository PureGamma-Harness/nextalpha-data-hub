from __future__ import annotations

import asyncio

import pytest

from nextalpha_market_watcher.binance_client import (
    next_silence_repair_backoff_ms,
    watchdog_should_rebuild,
)
from nextalpha_market_watcher.config import AppConfig
from nextalpha_market_watcher.fastapi_server import runtime_task_status


def test_silence_repair_backoff_doubles_and_caps() -> None:
    steps = [30_000]
    for _ in range(8):
        steps.append(next_silence_repair_backoff_ms(steps[-1], 600_000))
    assert steps[:6] == [30_000, 60_000, 120_000, 240_000, 480_000, 600_000]
    assert steps[-1] == 600_000


def test_watchdog_triggers_once_then_rearms_after_threshold() -> None:
    threshold = 60_000
    # Not stale yet.
    assert not watchdog_should_rebuild(
        now_ms=100_000, stale_ms=59_999, stale_threshold_ms=threshold, alerted=False, last_rebuild_ms=0
    )
    # First stale detection fires.
    assert watchdog_should_rebuild(
        now_ms=100_000, stale_ms=60_000, stale_threshold_ms=threshold, alerted=False, last_rebuild_ms=0
    )
    # Already alerted, rebuild was recent: do not fire again.
    assert not watchdog_should_rebuild(
        now_ms=130_000, stale_ms=90_000, stale_threshold_ms=threshold, alerted=True, last_rebuild_ms=100_000
    )
    # Still stale a full threshold after the rebuild: re-arm.
    assert watchdog_should_rebuild(
        now_ms=160_000, stale_ms=120_000, stale_threshold_ms=threshold, alerted=True, last_rebuild_ms=100_000
    )


def test_bars_1m_default_covers_4h_alert_window() -> None:
    cfg = AppConfig()
    assert cfg.backfill.bars_1m >= 240
    assert cfg.backfill.bars_1m <= cfg.backfill.klines_limit_cap
    assert cfg.redis.one_m_ring_max_bars >= 240


async def test_runtime_task_status() -> None:
    assert runtime_task_status(None)[0] == "starting"

    async def boom() -> None:
        raise RuntimeError("secret detail")

    failed = asyncio.create_task(boom())
    await asyncio.gather(failed, return_exceptions=True)
    status, detail = runtime_task_status(failed)
    assert status == "failed"
    assert detail == "RuntimeError"

    pending = asyncio.create_task(asyncio.sleep(10))
    assert runtime_task_status(pending) == ("ok", "")
    pending.cancel()
    await asyncio.gather(pending, return_exceptions=True)
    assert runtime_task_status(pending)[0] == "stopped"

    async def ok() -> None:
        return None

    finished = asyncio.create_task(ok())
    await finished
    assert runtime_task_status(finished)[0] == "stopped"


@pytest.mark.parametrize(
    ("close_time_ms", "expected"),
    [
        (1_700_000_000_000 - 1, "2023-11-14 22:13:19 UTC"),
        (0, "0"),
    ],
)
def test_format_close_time_is_utc(close_time_ms: int, expected: str) -> None:
    from nextalpha_market_watcher.app import App

    # _format_close_time does not use instance state.
    assert App._format_close_time(object.__new__(App), close_time_ms) == expected
