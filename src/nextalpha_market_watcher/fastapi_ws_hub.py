from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from urllib.parse import unquote

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
        default_topic: str | None = None,
    ) -> None:
        self._name = name
        self._on_client_connected = on_client_connected
        self._on_client_message = on_client_message
        self._default_topic = default_topic
        self._clients: set[WebSocket] = set()
        self._client_topics: dict[WebSocket, set[str]] = {}
        self._lock = asyncio.Lock()

    async def serve_until_stop(self, stop: asyncio.Event) -> None:
        # FastAPI/uvicorn owns the listening socket; we only block until stop.
        await stop.wait()

    def _normalize_topics(self, topics: object) -> set[str]:
        raw_items: list[str] = []
        if isinstance(topics, str):
            raw_items = [topics]
        elif isinstance(topics, list | tuple | set):
            raw_items = [str(item) for item in topics]
        elif topics is not None:
            raw_items = [str(topics)]

        normalized: set[str] = set()
        for item in raw_items:
            for part in item.split(","):
                topic = unquote(part).strip()
                if topic:
                    normalized.add(topic)
        if not normalized and self._default_topic:
            normalized.add(self._default_topic)
        return normalized

    def _topics_from_query(self, ws: WebSocket) -> set[str]:
        raw: list[str] = []
        topic = ws.query_params.get("topic")
        topics = ws.query_params.get("topics")
        if topic:
            raw.append(topic)
        if topics:
            raw.append(topics)
        return self._normalize_topics(raw)

    async def connect(self, ws: WebSocket, topics: object = None) -> None:
        await ws.accept()
        client_topics = self._normalize_topics(topics) if topics is not None else self._topics_from_query(ws)
        async with self._lock:
            self._clients.add(ws)
            self._client_topics[ws] = client_topics
            clients = len(self._clients)
        logger.info("ws.client.connected name=%s clients=%s topics=%s", self._name, clients, sorted(client_topics))
        if self._on_client_connected is not None:
            try:
                await self._on_client_connected(ws)
            except Exception:
                logger.exception("ws.client.bootstrap.failed name=%s", self._name)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)
            self._client_topics.pop(ws, None)
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

    async def topics_for(self, ws: WebSocket) -> set[str]:
        async with self._lock:
            topics = set(self._client_topics.get(ws) or set())
        if not topics and self._default_topic:
            topics.add(self._default_topic)
        return topics

    async def set_topics(self, ws: WebSocket, topics: object) -> set[str]:
        client_topics = self._normalize_topics(topics)
        async with self._lock:
            if ws in self._clients:
                self._client_topics[ws] = client_topics
        logger.info("ws.client.topics_set name=%s topics=%s", self._name, sorted(client_topics))
        return client_topics

    async def add_topics(self, ws: WebSocket, topics: object) -> set[str]:
        add = self._normalize_topics(topics)
        async with self._lock:
            current = set(self._client_topics.get(ws) or set())
            current.update(add)
            if ws in self._clients:
                self._client_topics[ws] = current
        logger.info("ws.client.topics_add name=%s topics=%s", self._name, sorted(add))
        return add

    async def remove_topics(self, ws: WebSocket, topics: object) -> set[str]:
        remove = self._normalize_topics(topics)
        async with self._lock:
            current = set(self._client_topics.get(ws) or set())
            current.difference_update(remove)
            if not current and self._default_topic:
                current.add(self._default_topic)
            if ws in self._clients:
                self._client_topics[ws] = current
        logger.info("ws.client.topics_remove name=%s topics=%s", self._name, sorted(remove))
        return remove

    async def broadcast_json(self, payload: object, topic: str | None = None) -> int:
        message = orjson.dumps(payload).decode("utf-8")
        async with self._lock:
            if topic is None:
                clients = tuple(self._clients)
            else:
                clients = tuple(
                    ws
                    for ws in self._clients
                    if topic in self._client_topics.get(ws, set()) or "*" in self._client_topics.get(ws, set())
                )
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

    async def client_count(self, topic: str | None = None) -> int:
        async with self._lock:
            if topic is None:
                return len(self._clients)
            return sum(
                1
                for ws in self._clients
                if topic in self._client_topics.get(ws, set()) or "*" in self._client_topics.get(ws, set())
            )
