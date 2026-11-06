from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import orjson
import websockets
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger(__name__)


class WsBroadcaster:
    def __init__(
        self,
        host: str,
        port: int,
        name: str,
        on_client_connected: Callable[[WebSocketServerProtocol], Awaitable[None]] | None = None,
        default_topic: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._name = name
        self._on_client_connected = on_client_connected
        self._default_topic = default_topic
        self._clients: set[WebSocketServerProtocol] = set()
        self._lock = asyncio.Lock()

    async def serve_until_stop(self, stop: asyncio.Event) -> None:
        async def _handler(ws: WebSocketServerProtocol) -> None:
            async with self._lock:
                self._clients.add(ws)
            logger.info("ws.client.connected name=%s clients=%s", self._name, len(self._clients))
            if self._on_client_connected is not None:
                try:
                    await self._on_client_connected(ws)
                except Exception:
                    logger.exception("ws.client.bootstrap.failed name=%s", self._name)
            try:
                await ws.wait_closed()
            finally:
                async with self._lock:
                    self._clients.discard(ws)
                logger.info("ws.client.disconnected name=%s clients=%s", self._name, len(self._clients))

        async with websockets.serve(_handler, self._host, self._port):
            logger.info("ws.listen name=%s host=%s port=%s", self._name, self._host, self._port)
            await stop.wait()

    async def broadcast_json(self, payload: object, topic: str | None = None) -> int:
        msg = orjson.dumps(payload).decode("utf-8")
        async with self._lock:
            clients = tuple(self._clients)
        if not clients:
            return 0

        async def _send_one(ws: WebSocketServerProtocol) -> bool:
            try:
                await ws.send(msg)
                return True
            except Exception:
                return False

        results = await asyncio.gather(*[_send_one(c) for c in clients], return_exceptions=False)
        sent = sum(1 for ok in results if ok)

        if sent != len(clients):
            async with self._lock:
                # Use send outcomes instead of the legacy .closed attribute,
                # which modern websockets server connections do not expose.
                stale = {client for client, ok in zip(clients, results) if not ok}
                for c in stale:
                    self._clients.discard(c)
        return sent

    async def client_count(self, topic: str | None = None) -> int:
        async with self._lock:
            return len(self._clients)

    async def topics_for(self, _ws: WebSocketServerProtocol) -> set[str]:
        return {self._default_topic} if self._default_topic else set()
