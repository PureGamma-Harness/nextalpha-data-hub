from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import yaml
from fastapi import FastAPI, WebSocket
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from .app import App
from .config import AppConfig, ServiceMode
from .fastapi_ws_hub import FastAPIWsHub
from .logging_utils import setup_logging


def _load_config(path: str, service_mode: ServiceMode | None = None) -> AppConfig:
    data = yaml.safe_load(open(path, encoding="utf-8").read()) or {}
    if service_mode is not None:
        data["service_mode"] = service_mode
    return AppConfig(**data)


def create_app(config_path: str, service_mode: ServiceMode | None = None) -> FastAPI:
    cfg = _load_config(config_path, service_mode=service_mode)
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

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        nonlocal runtime_task
        runtime_task = asyncio.create_task(app_runtime.run(stop_event))
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
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "mode": cfg.service_mode}

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
