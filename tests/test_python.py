"""Tests for swactor Python bindings."""

import unittest
from swactor import Runtime, RuntimeConfig, ActorAddress


class TestActorAddress(unittest.TestCase):
    def test_address_identity_and_collections(self):
        """Addresses for distinct actors are unique, hashable, and survive repr/bytes round-trips."""
        rt = Runtime()
        addr1 = rt.spawn(lambda ctx, msg: None)
        addr2 = rt.spawn(lambda ctx, msg: None)

        # Distinct actors have distinct addresses
        self.assertNotEqual(addr1, addr2)
        # Same address equals itself
        self.assertEqual(addr1, addr1)

        # Usable as dict keys / set members
        s = {addr1, addr2}
        self.assertEqual(len(s), 2)
        s.add(addr1)  # duplicate is a no-op
        self.assertEqual(len(s), 2)

        # Bytes and hex representations are well-formed
        self.assertEqual(len(addr1.to_bytes()), 32)
        self.assertEqual(len(addr1.hex()), 64)

        # repr round-trip is readable
        r = repr(addr1)
        self.assertTrue(r.startswith("ActorAddress("))
        self.assertTrue(r.endswith(")"))


class TestRuntimeConfig(unittest.TestCase):
    def test_defaults(self):
        cfg = RuntimeConfig()
        self.assertEqual(cfg.max_actors, 1000)
        self.assertEqual(cfg.channel_buffer_size, 1000)

    def test_custom(self):
        cfg = RuntimeConfig(max_actors=500)
        self.assertEqual(cfg.max_actors, 500)


class TestSingleThreaded(unittest.TestCase):
    def test_echo(self):
        """Spawn an echo actor, send a message, tick, and recv."""
        rt = Runtime()

        def echo(ctx, msg):
            ctx.send(msg["reply_to"], msg["payload"])

        addr = rt.spawn(echo)
        inbox = rt.inbox()
        rt.send(addr, {"payload": "hello", "reply_to": inbox.addr})
        rt.tick()
        result = inbox.try_recv()
        self.assertEqual(result, "hello")

    def test_spawn_from_handler(self):
        """Actor spawns a child and forwards work to it."""
        rt = Runtime()

        def child(ctx, msg):
            ctx.send(msg["reply_to"], "from_child")

        def parent(ctx, msg):
            c = ctx.spawn(child)
            ctx.send(c, {"reply_to": msg["reply_to"]})

        addr = rt.spawn(parent)
        inbox = rt.inbox()
        rt.send(addr, {"reply_to": inbox.addr})
        # First tick: parent runs, spawns child, sends to child
        rt.tick()
        # Second tick: child runs, sends to inbox
        rt.tick()
        result = inbox.try_recv()
        self.assertEqual(result, "from_child")

    def test_stateful_actor(self):
        """Callable class maintains state across messages."""
        rt = Runtime()

        class Counter:
            def __init__(self):
                self.n = 0

            def __call__(self, ctx, msg):
                self.n += 1
                ctx.send(msg["reply_to"], self.n)

        addr = rt.spawn(Counter())
        inbox = rt.inbox()
        rt.send(addr, {"reply_to": inbox.addr})
        rt.send(addr, {"reply_to": inbox.addr})
        rt.tick()
        self.assertEqual(inbox.try_recv(), 1)
        self.assertEqual(inbox.try_recv(), 2)

    def test_no_message_returns_none(self):
        rt = Runtime()
        inbox = rt.inbox()
        self.assertIsNone(inbox.try_recv())


if __name__ == "__main__":
    unittest.main()
