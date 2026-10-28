from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import JSONResponse, Response

from .app import App
from .config import ServiceMode, load_config
from .fastapi_ws_hub import FastAPIWsHub
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def runtime_task_status(task: asyncio.Task[None] | None) -> tuple[str, str]:
    """Return (status, detail) for the background runtime task."""
    if task is None:
        return "starting", "runtime task not started"
    if not task.done():
        return "ok", ""
    if task.cancelled():
        return "stopped", "runtime task cancelled"
    exc = task.exception()
    if exc is not None:
        # Only expose the exception type; full details go to the logs.
        return "failed", type(exc).__name__
    return "stopped", "runtime task exited"


def create_app(config_path: str, service_mode: ServiceMode | None = None) -> FastAPI:
    cfg = load_config(config_path, service_mode=service_mode)
    # FastAPI exposes /metrics itself; disable standalone prometheus HTTP listener.
    cfg.metrics.enabled = False
    setup_logging(cfg.log_level)

    alerts_hub = FastAPIWsHub(name="alerts")
    klines_hub = FastAPIWsHub(name="klines")
    app_runtime = App(cfg, alerts_ws=alerts_hub, klines_ws=klines_hub)
    # Reuse existing replay behavior when new client connects.
    klines_hub._on_client_connected = app_runtime._on_klines_client_connected
    klines_hub._on_client_message = app_runtime._on_klines_client_message

    stop_event = asyncio.Event()
    runtime_task: asyncio.Task[None] | None = None

    def _log_runtime_exit(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("runtime.task.failed", exc_info=exc)
        elif not stop_event.is_set():
            logger.error("runtime.task.exited_unexpectedly")

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        nonlocal runtime_task
        runtime_task = asyncio.create_task(app_runtime.run(stop_event))
        runtime_task.add_done_callback(_log_runtime_exit)
        try:
            yield
        finally:
            stop_event.set()
            if runtime_task is not None and not runtime_task.done():
                runtime_task.cancel()
            if runtime_task is not None:
                await asyncio.gather(runtime_task, return_exceptions=True)

    app = FastAPI(title="nextalpha-market-watcher", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        status, detail = runtime_task_status(runtime_task)
        body: dict[str, str] = {"status": status, "mode": cfg.service_mode}
        if detail:
            body["detail"] = detail
        return JSONResponse(body, status_code=200 if status == "ok" else 503)

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.websocket("/ws/alerts")
    async def ws_alerts(ws: WebSocket) -> None:
        await alerts_hub.connect(ws)
        try:
            await alerts_hub.hold_until_disconnect(ws)
        finally:
            await alerts_hub.disconnect(ws)

    @app.websocket("/ws/klines")
    async def ws_klines(ws: WebSocket) -> None:
        await klines_hub.connect(ws)
        try:
            await klines_hub.hold_until_disconnect(ws)
        finally:
            await klines_hub.disconnect(ws)

    return app
