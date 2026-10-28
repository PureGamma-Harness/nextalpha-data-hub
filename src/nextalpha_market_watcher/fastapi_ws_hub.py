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
        on_client_message: Callable[[WebSocket, str], Awaitable[None]] | None = None,
        send_timeout_s: float = 5.0,
    ) -> None:
        self._name = name
        self._send_timeout_s = max(0.1, float(send_timeout_s))
        self._on_client_connected = on_client_connected
        self._on_client_message = on_client_message
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
                message = await ws.receive_text()
                if self._on_client_message is None:
                    continue
                try:
                    await self._on_client_message(ws, message)
                except Exception:
                    logger.exception("ws.client.message.failed name=%s", self._name)
        except Exception:
            return

    async def broadcast_json(self, payload: object) -> int:
        message = orjson.dumps(payload).decode("utf-8")
        async with self._lock:
            clients = tuple(self._clients)
        if not clients:
            return 0

        async def _send_one(ws: WebSocket) -> bool:
            try:
                await asyncio.wait_for(ws.send_text(message), timeout=self._send_timeout_s)
                return True
            except Exception as exc:
                logger.warning(
                    "ws.client.send_failed name=%s error=%s",
                    self._name,
                    type(exc).__name__,
                )
                return False

        # Send concurrently so one slow client cannot delay everyone else.
        results = await asyncio.gather(*[_send_one(ws) for ws in clients])
        stale = [ws for ws, ok in zip(clients, results) if not ok]
        if stale:
            async with self._lock:
                for ws in stale:
                    self._clients.discard(ws)
                remaining = len(self._clients)
            logger.info("ws.client.dropped name=%s dropped=%s clients=%s", self._name, len(stale), remaining)
            for ws in stale:
                # Best-effort close so the peer's hold_until_disconnect loop ends.
                try:
                    await asyncio.wait_for(ws.close(), timeout=1.0)
                except Exception:
                    pass
        return sum(1 for ok in results if ok)

    async def client_count(self) -> int:
        async with self._lock:
            return len(self._clients)
