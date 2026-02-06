"""Tests for swactor Python bindings."""

import unittest
from swactor import Runtime, RuntimeConfig, ActorAddress


class TestActorAddress(unittest.TestCase):
    def test_repr(self):
        rt = Runtime()
        addr = rt.spawn(lambda ctx, msg: None)
        r = repr(addr)
        self.assertTrue(r.startswith("ActorAddress("))
        self.assertTrue(r.endswith(")"))
        # hex string should be 64 chars (32 bytes)
        hex_part = r[len("ActorAddress("):-1]
        self.assertEqual(len(hex_part), 64)

    def test_hex(self):
        rt = Runtime()
        addr = rt.spawn(lambda ctx, msg: None)
        self.assertEqual(len(addr.hex()), 64)

    def test_to_bytes(self):
        rt = Runtime()
        addr = rt.spawn(lambda ctx, msg: None)
        self.assertEqual(len(addr.to_bytes()), 32)

    def test_equality(self):
        rt = Runtime()
        addr = rt.spawn(lambda ctx, msg: None)
        # Same address object should be equal to itself
        self.assertEqual(addr, addr)

    def test_hashable(self):
        rt = Runtime()
        addr1 = rt.spawn(lambda ctx, msg: None)
        addr2 = rt.spawn(lambda ctx, msg: None)
        s = {addr1, addr2}
        self.assertEqual(len(s), 2)
        s.add(addr1)
        self.assertEqual(len(s), 2)


class TestRuntimeConfig(unittest.TestCase):
    def test_defaults(self):
        cfg = RuntimeConfig()
        self.assertEqual(cfg.num_threads, 1)
        self.assertEqual(cfg.max_actors, 1000)
        self.assertEqual(cfg.actor_max_messages, 1000)
        self.assertEqual(cfg.mailbox_waterlevel, 10)
        self.assertEqual(cfg.spin_threshold, 64)
        self.assertEqual(cfg.yield_threshold, 256)
        self.assertEqual(cfg.sleep_increment_us, 50)
        self.assertEqual(cfg.sleep_max_us, 1000)

    def test_custom(self):
        cfg = RuntimeConfig(num_threads=4, max_actors=500)
        self.assertEqual(cfg.num_threads, 4)
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


class TestMultiThreaded(unittest.TestCase):
    def test_run_shutdown_join(self):
        """Multi-threaded runtime can spawn, send, and receive."""
        import time

        rt = Runtime(RuntimeConfig(num_threads=2))

        def echo(ctx, msg):
            ctx.send(msg["reply_to"], msg["payload"])

        addr = rt.spawn(echo)
        inbox = rt.inbox()
        handle = rt.run()
        handle.send(addr, {"payload": "mt_hello", "reply_to": inbox.addr})

        # Poll for result
        result = None
        for _ in range(100):
            result = inbox.try_recv()
            if result is not None:
                break
            time.sleep(0.01)
        self.assertEqual(result, "mt_hello")

        handle.shutdown()
        handle.join()

    def test_run_consumes_runtime(self):
        """After run(), tick() should raise."""
        rt = Runtime(RuntimeConfig(num_threads=2))
        handle = rt.run()
        with self.assertRaises(RuntimeError):
            rt.tick()
        handle.shutdown()
        handle.join()


if __name__ == "__main__":
    unittest.main()
