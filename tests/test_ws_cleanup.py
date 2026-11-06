"""Offline regression tests for failed WebSocket client cleanup."""

import unittest

from nextalpha_market_watcher.fastapi_ws_hub import FastAPIWsHub
from nextalpha_market_watcher.ws_push import WsBroadcaster


class FakeSocket:
    # Modern websockets connections do not expose a legacy `closed` attribute.
    def __init__(self, fail=False):
        self.fail = fail
        self.messages = []
        self.query_params = {}

    async def accept(self):
        pass

    async def send(self, message):
        if self.fail:
            raise ConnectionError("peer disconnected")
        self.messages.append(message)

    async def send_text(self, message):
        await self.send(message)


class WsBroadcasterCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_send_removes_client_without_legacy_closed_attribute(self):
        hub = WsBroadcaster("127.0.0.1", 0, "test")
        stale, healthy = FakeSocket(fail=True), FakeSocket()
        hub._clients.update((stale, healthy))

        self.assertEqual(await hub.broadcast_json({"message": "one"}), 1)
        self.assertEqual(await hub.client_count(), 1)
        self.assertNotIn(stale, hub._clients)
        self.assertIn(healthy, hub._clients)
        self.assertEqual(await hub.broadcast_json({"message": "two"}), 1)
        self.assertEqual(len(healthy.messages), 2)

    async def test_all_failed_clients_are_removed(self):
        hub = WsBroadcaster("127.0.0.1", 0, "test")
        hub._clients.update((FakeSocket(fail=True), FakeSocket(fail=True)))

        self.assertEqual(await hub.broadcast_json({}), 0)
        self.assertEqual(await hub.client_count(), 0)
        self.assertEqual(await hub.broadcast_json({}), 0)

    async def test_successful_clients_are_preserved(self):
        hub = WsBroadcaster("127.0.0.1", 0, "test")
        hub._clients.update((FakeSocket(), FakeSocket()))

        self.assertEqual(await hub.broadcast_json({}), 2)
        self.assertEqual(await hub.client_count(), 2)


class FastAPIWsHubCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_send_removes_topic_entry_and_preserves_other_clients(self):
        hub = FastAPIWsHub("test")
        stale, healthy, other = FakeSocket(fail=True), FakeSocket(), FakeSocket()
        await hub.connect(stale, topics="prices")
        await hub.connect(healthy, topics="prices")
        await hub.connect(other, topics="other")

        self.assertEqual(await hub.broadcast_json({}, topic="prices"), 1)
        self.assertNotIn(stale, hub._clients)
        self.assertNotIn(stale, hub._client_topics)
        self.assertEqual(await hub.client_count(), 2)
        self.assertEqual(await hub.topics_for(healthy), {"prices"})
        self.assertEqual(await hub.topics_for(other), {"other"})
        self.assertEqual(other.messages, [])
        # The receive loop can still call disconnect after broadcast cleanup.
        await hub.disconnect(stale)
        self.assertEqual(await hub.client_count(), 2)

    async def test_untargeted_broadcast_cleans_all_failed_topic_entries(self):
        hub = FastAPIWsHub("test")
        await hub.connect(FakeSocket(fail=True), topics="one")
        await hub.connect(FakeSocket(fail=True), topics="two")

        self.assertEqual(await hub.broadcast_json({}), 0)
        self.assertEqual(await hub.client_count(), 0)
        self.assertEqual(hub._client_topics, {})


if __name__ == "__main__":
    unittest.main()
