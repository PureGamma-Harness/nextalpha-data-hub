from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import orjson
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class FastAPIWsHub:
    """
    WebSocket hub backed by FastAPI's WebSocket connections.

    It mirrors the minimal broadcaster interface used by App:
    - serve_until_stop(stop)
    - broadcast_json(payload)
    - client_count()
    """

    def __init__(
        self,
        name: str,
        on_client_connected: Callable[[WebSocket], Awaitable[None]] | None = None,
    ) -> None:
        self._name = name
        self._on_client_connected = on_client_connected
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def serve_until_stop(self, stop: asyncio.Event) -> None:
        # FastAPI/uvicorn owns the listening socket; we only block until stop.
        await stop.wait()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
            clients = len(self._clients)
        logger.info("ws.client.connected name=%s clients=%s", self._name, clients)
        if self._on_client_connected is not None:
            try:
                await self._on_client_connected(ws)
            except Exception:
                logger.exception("ws.client.bootstrap.failed name=%s", self._name)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)
            clients = len(self._clients)
        logger.info("ws.client.disconnected name=%s clients=%s", self._name, clients)

    async def hold_until_disconnect(self, ws: WebSocket) -> None:
        """
        Keep the websocket alive until peer disconnects.
        """
        try:
            while True:
                await ws.receive_text()
        except Exception:
            return

    async def broadcast_json(self, payload: object) -> int:
        message = orjson.dumps(payload).decode("utf-8")
        async with self._lock:
            clients = tuple(self._clients)
        if not clients:
            return 0

        sent = 0
        stale: list[WebSocket] = []
        for ws in clients:
            try:
                await ws.send_text(message)
                sent += 1
            except Exception:
                stale.append(ws)
        if stale:
            async with self._lock:
                for ws in stale:
                    self._clients.discard(ws)
        return sent

    async def client_count(self) -> int:
        async with self._lock:
            return len(self._clients)
