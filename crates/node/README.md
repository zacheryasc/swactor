# node

The `node` crate produces the `swactor` binary — the batteries-included entry point for running a swactor node. It composes the actor runtime, SWIM-based cluster membership, content-addressed datastore, data streams, an HTTP dashboard, and an optional embedded relay server into a single process.

Run `swactor --help` for full CLI usage.

## What a Default Node Does

### Identity

Every node has a persistent Ed25519 keypair stored at `~/.swactor/identity/node.key.json`. This is the node's identity across restarts — deleting it makes the node appear as a new peer to the cluster. The keypair's public key doubles as the node ID (used in SWIM, peer auth, and invite codes). A deterministic human-readable name (e.g. `swift-falcon`) is derived from the key so you can tell nodes apart in logs and the dashboard.

### Networking

The default transport is **iroh** (QUIC over UDP). Nodes find each other via invite codes — base58-encoded public keys exchanged out-of-band. `swactor join <code>` adds a peer to the allow-list and sets it as the seed node for the next startup.

Once connected, **SWIM protocol** handles cluster membership: protocol probes every 500ms, 600ms probe timeout (tuned for relay round-trips), 2 indirect probes, 4s suspicion window. All intervals are in ticks where **1 tick = 100ms** (the main loop period).

**Peer auth** operates in two modes: open (no `peers_file`) or allow-list (`peers.json`). In allow-list mode, SWIM messages from unknown nodes are dropped at the transport layer. New peers can be added via `swactor join` or the dashboard UI, both of which hot-update the allow-list.

> **Note (deferred, not urgent): gossip opens a fresh QUIC stream per message.**
> The iroh driver reuses the per-peer *connection* but opens and finishes a new
> uni-stream for every gossip/SWIM message (`iroh_driver.rs` `send_wire` →
> `open_uni`/`finish` per send; reader does `accept_uni` + `read_to_end` per
> message). The stream-per-message shape exists only because the wire frame has
> no payload-length field and relies on stream-EOF to delimit. No per-message RTT
> is paid (uni-streams are unilateral), but each send costs a tokio task spawn, a
> fresh read-side allocation, and a slot against `max_concurrent_uni_streams`.
>
> Two cheaper shapes, when it's worth doing:
> - **QUIC datagrams** for the small probe traffic (Ping/Ack/PingReq). One
>   datagram = one self-delimiting message, no stream state at all, and
>   best-effort delivery *matches* SWIM's own loss-tolerance instead of fighting
>   it with redundant QUIC retransmits. Capped at ~path-MTU, so it doesn't cover
>   large gossip (e.g. a `JoinResponse` with a big member list).
> - **One persistent length-prefixed stream per connection** for the larger /
>   must-arrive messages — the same persistent-stream discipline used for blob
>   edges.
>
> Low priority; tracked here so it isn't lost.

### Relay

Nodes with a public IP auto-promote to embedded relay servers (port 3340). Candidacy is evaluated at startup: the node checks its outbound IP is non-RFC1918 and the relay port is bindable. Relay URLs are announced via SWIM gossip so other nodes discover them automatically. Nodes behind NAT use relays for indirect connectivity — this is why the probe timeout is 600ms instead of the typical 300ms.

### Storage

The **datastore** is a content-addressed, chunked store. Default config persists to `~/.swactor/datastore/`. Auth is enabled by default (ACL files in `~/.swactor/auth/`). The datastore runs as a group of actors inside the runtime and is driven by the main tick loop — GC runs every 1000 ticks (~100s) and dissemination every 50 ticks (~5s). A `StreamManager` actor bridges iroh QUIC streams into the datastore for bulk data transfer between nodes.

Disable with `--no-datastore`. Use `--storage-path` to change location, or omit it from config for in-memory only.

### Observability

An HTTP dashboard serves on **port 9090**. It exposes runtime stats, cluster membership state, tracing output, and a peer management UI (add/remove peers). The dashboard receives a snapshot of the distribution layer every tick.

### State & Lifecycle

All persistent state lives under `~/.swactor/`. Deleting this directory fully resets the node (new identity, empty cluster, empty datastore). The node shuts down cleanly on SIGINT (Ctrl+C). `swactor install` copies the binary to `~/.swactor/bin/swactor` and registers it as a system service (systemd user unit, OpenRC/sysvinit init script, or `@reboot` crontab depending on the host).

## Exposed Ports

| Port | Service | Configurable via |
|------|---------|-----------------|
| 9090 | HTTP dashboard | `--dashboard-port` |
| 3340 | Embedded relay (if eligible) | `--relay-port` |

## Data Directory (`~/.swactor/`)

```
~/.swactor/
├── node.toml          # Node configuration
├── peers.json         # Peer allow-list
├── identity/
│   └── node.key.json  # Persistent Ed25519 keypair
├── datastore/         # Content-addressed chunk storage
├── auth/              # ACL and owner key files
└── bin/
    └── swactor        # Installed binary (after `swactor install`)
```

## Architecture

```
CLI Args + TOML Config
        │
        ▼
  Config Resolution (CLI > config > defaults)
        │
        ▼
  Identity (Ed25519 Keypair) ──► Node Name
        │
        ▼
  ┌─────────────────────────────────────────────┐
  │              Actor Runtime                   │
  │  (2 threads, StdExtension, stats hook)       │
  │                                              │
  │  ┌──────────────┐  ┌──────────────────────┐  │
  │  │ StreamManager │  │   Datastore Group    │  │
  │  │   (actor)     │◄─┤ (store, gateway,     │  │
  │  │               │  │  bridge actors)      │  │
  │  └──────────────┘  └──────────────────────┘  │
  └──────────────┬──────────────────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │   Distribution Driver (iroh)     │
  │  SWIM probes, gossip, relay      │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │         HTTP Dashboard           │
  │  :9090 — stats, tracing, peers   │
  └──────────────────────────────────┘
                 │
                 ▼
         Main Tick Loop (100ms)
    recv → tick → streams → joins →
    heartbeats → snapshot → datastore
```
