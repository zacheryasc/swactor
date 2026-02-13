# Stage 5 — Interactive Browser Demo

Visual verification page for the in-browser swactor runtime. Single self-contained
HTML file that loads the `--target web` wasm build and exposes every API surface
through a live dashboard.

## Running

```bash
# Build for browser (one-time, or after Rust changes)
cd crates/wasm && wasm-pack build --target web --out-dir pkg-web

# Serve (any static server works — needs correct .wasm MIME type)
cd crates/wasm && python3 -m http.server 8080
```

Open `http://localhost:8080/demo.html`.

## What It Covers

| Feature | How to verify |
|---|---|
| Runtime tick loop | Start/Pause button, Step for single tick, adjustable 1–60 tps |
| Actor spawning | Spawn Counter, Relay, GroupMember, Sentinel from dropdown |
| Message delivery | Send u32 to any actor, inbox polling shows received values |
| Cross-actor relay | Spawn Relay → target Counter, send to relay, counter accumulates |
| Actor stopping | Stop button on each card, actor disappears from viz |
| Watching / death notifications | Spawn Sentinel watching an actor, stop the watched actor |
| Name registry | Register/Lookup/Unregister names, live list in sidebar |
| Groups | GroupMember auto-joins on spawn, Broadcast sends to all members |
| Stats | Live actor count, total messages, total panics, uptime, tick count |

## Architecture

```
demo.html
  ├── imports pkg-web/wasm.js (ES module, --target web)
  ├── creates WasmRuntime (single-threaded, StdExtension)
  ├── requestAnimationFrame tick loop
  ├── canvas visualization (actor circle graph + edges)
  └── event log (spawn, send, recv, death, naming, groups)
```

All state lives in the page. No build step, no bundler, no framework — just
the wasm module and vanilla JS.

## Suggested Walkthrough

1. **Counter basics** — Spawn a Counter, Step once, click "Send 1", Step again.
   Inbox log shows the running total.
2. **Relay chain** — Spawn Counter #1, then Relay targeting #1. Send to the relay,
   observe the counter accumulating.
3. **Death watching** — Spawn a Counter, then a Sentinel watching it. Stop the
   counter. The sentinel reports the death and self-terminates.
4. **Groups** — Spawn 3 GroupMembers in "workers". Hit "Broadcast 42". All three
   receive the message.
5. **Naming** — Register "@main" for an actor. Lookup confirms it resolves. Unregister
   and verify it's gone.
6. **Burst load** — Spawn several counters, click "Send ×10" on each, start the
   runtime at 60 tps. Watch messages processed climb.
