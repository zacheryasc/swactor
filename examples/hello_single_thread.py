"""Hello world: spawn an echo actor, send a message, get it back."""

from swactor import Runtime

def echo(ctx, msg):
    ctx.send(msg["reply_to"], f"hello, {msg['name']}!")

rt = Runtime()
addr = rt.spawn(echo)
inbox = rt.inbox()
rt.send(addr, {"name": "world", "reply_to": inbox.addr})
rt.tick()
print(inbox.try_recv())
