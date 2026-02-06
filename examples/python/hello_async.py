"""Async hello world: runtime runs in background threads, driven from asyncio."""

import asyncio
from swactor import Runtime, RuntimeConfig


async def recv(inbox, timeout=1.0):
    """Poll an inbox until a message arrives."""
    while timeout > 0:
        msg = inbox.try_recv()
        if msg is not None:
            return msg
        await asyncio.sleep(0.01)
        timeout -= 0.01
    return None


async def main():
    rt = Runtime(RuntimeConfig(num_threads=2))

    def echo(ctx, msg):
        ctx.send(msg["reply_to"], f"hello, {msg['name']}!")

    addr = rt.spawn(echo)
    inbox = rt.inbox()
    handle = rt.run()

    for name in ["alice", "bob", "charlie"]:
        handle.send(addr, {"name": name, "reply_to": inbox.addr})
        reply = await recv(inbox)
        print(reply)

     # show us our actors!
    print(handle.stats())
    handle.shutdown()
    handle.join()


asyncio.run(main())
