# CI Webhook Relay via Iroh — Development History

> Covers the implementation of `ci-relay` and the iroh webhook receiver in
> `local-runner`, enabling Forgejo webhooks to reach a NAT'd CI runner via
> iroh's QUIC transport with automatic NAT traversal.
>
> ~3 files created · ~2 files modified · ~350 insertions
>
> *Branch: `spot-instance`*

---

## Table of Contents

1. [Problem & Motivation](#1-problem--motivation)
2. [Architecture](#2-architecture)
3. [What Was Built](#3-what-was-built)
4. [ci-relay Binary](#4-ci-relay-binary)
5. [local-runner Iroh Receiver](#5-local-runner-iroh-receiver)
6. [Wire Protocol](#6-wire-protocol)
7. [Connection Flow](#7-connection-flow)
8. [Design Decisions & Tradeoffs](#8-design-decisions--tradeoffs)
9. [Manual Testing Guide](#9-manual-testing-guide)
10. [Known Gaps & Future Improvements](#10-known-gaps--future-improvements)

---

## 1. Problem & Motivation

The CI runner (`local-runner`) was designed for same-LAN usage: Forgejo sends
webhooks over HTTP to the runner's listen port. In the real deployment:

- **Forgejo** runs on a VPS (`zachery.lol` / `139.59.195.69`)
- **CI runner** runs on a Thinkpad at home (`192.168.1.102`), behind NAT

The VPS cannot reach the Thinkpad directly — no inbound port is open, no
static IP, no UPnP. Traditional solutions (SSH reverse tunnel, VPN, port
forwarding on router) all require ongoing configuration and are fragile.

iroh is already integrated in swactor's distribution layer (`iroh_driver.rs`)
for SWIM protocol traffic. It provides QUIC connections with automatic NAT
traversal via relay servers — exactly what's needed to bridge the webhook gap.

### Why Not Just SSH Tunnel?

An SSH tunnel (`ssh -R 8787:localhost:8787 zachery.lol`) would work, but:

- Tunnels drop on network changes (laptop suspend, WiFi roaming)
- Requires autossh or systemd to keep alive
- Another moving part to debug when CI stops working
- Doesn't reuse any existing infrastructure

iroh handles reconnection, relay fallback, and NAT traversal automatically.
The implementation reuses the same tagged-message-over-QUIC-stream pattern
already proven in `iroh_driver.rs`.

---

## 2. Architecture

```
┌─────────────────────────────┐        ┌──────────────────────────────────┐
│ VPS (zachery.lol)           │        │ Thinkpad (192.168.1.102)         │
│                             │  iroh  │                                  │
│  Forgejo ──webhook──► Relay ├───────►│ local-runner                     │
│                       :8787 │  QUIC  │  (coordinator, runner, reporter) │
│                             │        │                                  │
└─────────────────────────────┘        └──────────────────────────────────┘
```

**VPS side** — `ci-relay` binary:
- HTTP listener receives webhook POSTs from Forgejo (localhost only)
- iroh endpoint accepts the runner's inbound connection
- Forwards parsed `WebhookEvent` payloads over iroh uni streams

**Thinkpad side** — `local-runner` with `--relay-node-id`:
- Connects to the VPS relay's iroh endpoint on startup
- Receives `WebhookEvent` over iroh uni streams
- Feeds events into `LocalCoordinator` via existing `Webhook` message
- Status updates go directly Thinkpad → Forgejo API over HTTPS (no relay needed)

The relay is intentionally minimal — it's a bridge, not a CI component. All CI
logic stays in `local-runner`.

---

## 3. What Was Built

| Component | Location | Nature |
|-----------|----------|--------|
| ci-relay binary | `crates/ci-relay/Cargo.toml`, `src/main.rs` | **New** — VPS webhook relay |
| Iroh receiver | `crates/local-runner/src/main.rs` | **Modified** — iroh webhook source |
| Dependencies | `crates/local-runner/Cargo.toml` | **Modified** — added iroh, tokio, serde_json |
| Workspace | `Cargo.toml` | **Modified** — added ci-relay to members |

---

## 4. ci-relay Binary

### `crates/ci-relay/src/main.rs`

The relay runs two subsystems on a single process:

1. **iroh acceptor** (tokio task): accepts inbound connections from the runner,
   caches the most recent one in `Arc<TokioMutex<Option<Connection>>>`
2. **HTTP listener** (main thread, blocking `tiny_http`): receives Forgejo
   webhook POSTs, verifies HMAC, parses event, forwards over iroh

### Webhook Handling

Reuses the same verification and parsing logic as `webhook_server.rs`:

- HMAC-SHA256 verification via `X-Forgejo-Signature` header (skippable with empty secret)
- Event type from `X-Forgejo-Event` header: `push` → `Push`, `create` → `Tag`, `pull_request` → `Merge`
- JSON parsing via `parse_webhook_json()` (re-exported from `swactor-ci`)

The relay uses `parse_webhook_json` directly rather than duplicating parsing
logic. This keeps webhook interpretation consistent between HTTP and iroh paths.

### Forwarding

On webhook receipt, the relay:
1. Serializes the `WebhookEvent` to JSON
2. Opens a unidirectional QUIC stream on the cached connection
3. Writes the tagged message (`ci::WebhookEvent` tag + JSON payload)
4. Finishes the stream

If no runner is connected, the relay returns HTTP 502 to Forgejo. Forgejo will
retry the webhook per its configured retry policy.

### CLI

```
ci-relay [OPTIONS]

Options:
    --port <PORT>      HTTP port for Forgejo webhooks [default: 8787]
    --secret <SECRET>  HMAC-SHA256 secret [default: "" (no verification)]
```

On startup, the relay prints its iroh Node ID — this is the value the runner
needs for `--relay-node-id`.

---

## 5. local-runner Iroh Receiver

### New CLI Flag

```
--relay-node-id <HEX>    Iroh Node ID of the VPS ci-relay
```

When `--relay-node-id` is provided:
- The HTTP webhook listener is **not started** (no port conflict, no exposure)
- An `iroh-receiver` thread starts instead

When omitted, behavior is unchanged — the HTTP listener starts on `--port`
as before.

### `start_iroh_receiver()`

Spawns a dedicated thread (`iroh-receiver`) with its own single-threaded tokio
runtime:

1. Creates an iroh `Endpoint` with ALPN `b"swactor/ci/1"`
2. Connects to the relay's `PublicKey` (parsed from the hex flag)
3. Enters a receive loop:
   - `conn.accept_uni()` with 1-second timeout
   - On stream: reads tagged message, deserializes `WebhookEvent`
   - Sends `LocalCoordinatorMsg::Webhook(event)` to the coordinator via the swactor runtime
   - On timeout: checks the `stop` flag (for graceful shutdown via Ctrl-C)
   - On connection error: breaks and exits

The thread respects the same `AtomicBool` stop flag as the main loop, so
Ctrl-C cleanly shuts down both the swactor runtime and the iroh connection.

---

## 6. Wire Protocol

### ALPN

```rust
const CI_ALPN: &[u8] = b"swactor/ci/1";
```

Distinct from SWIM traffic (`b"swactor/swim/1"`). This allows both protocols
to coexist on the same iroh endpoint in the future if needed.

### Frame Format

Same tagged-message format as `iroh_driver.rs`:

```
[4 bytes: tag_len (big-endian u32)]
[tag_len bytes: tag string]
[remaining bytes: payload]
```

For webhook events:
- Tag: `"ci::WebhookEvent"` (17 bytes)
- Payload: JSON-serialized `WebhookEvent`

### Transport

Each webhook is one unidirectional QUIC stream. The relay opens the stream,
writes the tagged message, and finishes. The runner reads the message and the
stream closes. No persistent framing or multiplexing needed — QUIC streams
are lightweight.

---

## 7. Connection Flow

```
1. VPS starts ci-relay
   → iroh Endpoint binds
   → prints Node ID (ed25519 public key, hex)
   → HTTP listener starts on --port
   → waits for runner connection

2. Thinkpad starts local-runner --relay-node-id <hex>
   → iroh Endpoint binds
   → connects to relay's PublicKey
   → iroh handles NAT traversal (direct or via relay server)
   → relay logs "Runner connected: <runner-node-id>"

3. Forgejo sends webhook POST to localhost:8787 on VPS
   → relay verifies HMAC, parses event
   → relay opens uni stream on cached connection
   → writes tagged WebhookEvent
   → runner receives, deserializes, dispatches to coordinator

4. Coordinator triggers pipeline
   → StatusReporter posts status to Forgejo API directly
   (Thinkpad → zachery.lol over HTTPS, no relay involvement)
```

The iroh connection is initiated by the runner (outbound from NAT), so no port
forwarding is needed. iroh's relay servers handle the initial rendezvous, then
attempt direct QUIC hole-punching for subsequent traffic.

---

## 8. Design Decisions & Tradeoffs

### 8.1 Separate Binary vs. Library Module

**Choice**: `ci-relay` is a standalone binary, not a module in `swactor-ci`.

**Why**: The relay runs on the VPS, which doesn't need swactor's runtime,
actors, or any CI execution logic. A small binary with minimal dependencies
deploys easily. It only depends on `swactor-ci` for `parse_webhook_json` and
the `WebhookEvent`/`EventType` types.

**Tradeoff**: Two binaries to build and deploy instead of one. Acceptable
given they run on different machines.

### 8.2 Runner Connects to Relay (Not Vice Versa)

**Choice**: The runner initiates the iroh connection to the relay.

**Why**: The runner is behind NAT. iroh can traverse NAT for established
connections, but the initial rendezvous requires at least one side to be
reachable. The VPS relay has a public IP and gets a stable relay URL from iroh's
infrastructure. The runner connects outbound, which always works regardless of
NAT type.

### 8.3 Single Cached Connection (Not Connection Pool)

**Choice**: The relay caches exactly one runner connection in
`Arc<TokioMutex<Option<Connection>>>`.

**Why**: There's one runner. If a new connection arrives (e.g., runner
restarts), it replaces the old one. No pool management needed.

**Tradeoff**: If multiple runners were needed, this would need a map. For
single-runner use, the simplicity is worth it.

### 8.4 Own Tokio Runtime Per Thread

**Choice**: The iroh-receiver thread creates its own single-threaded tokio
runtime rather than sharing the swactor runtime or the main thread's runtime.

**Why**: swactor's runtime is not tokio — it's a custom actor scheduler. The
iroh receiver needs async for QUIC operations. A dedicated single-threaded
runtime keeps the iroh I/O isolated from actor scheduling. Same pattern as
`IrohDriver` in the distribution layer (which owns a multi-thread runtime).

### 8.5 HTTP 502 When No Runner Connected

**Choice**: If Forgejo sends a webhook but no runner is connected, the relay
returns HTTP 502 (Bad Gateway).

**Why**: 502 tells Forgejo the upstream is unavailable. Forgejo will retry
the webhook according to its retry policy. This is better than 200 (silently
dropping) or 500 (suggesting a relay bug). When the runner reconnects, the
next webhook will succeed.

---

## 9. Manual Testing Guide

### Prerequisites

Build both binaries:

```bash
cargo build -p ci-relay -p local-runner
```

### 9.1 Local Smoke Test (Single Machine)

This tests the full relay path without needing two machines or Forgejo.

**Terminal 1 — Start the relay:**

```bash
./target/debug/ci-relay --port 9787
```

Output:
```
ci-relay started
  Iroh Node ID: <NODE_ID_HEX>
  Webhook HTTP: http://0.0.0.0:9787

Waiting for runner to connect...
Listening for webhooks...
```

Copy the Node ID.

**Terminal 2 — Start the runner:**

You need a `.ci.yml` file. Create a minimal one:

```yaml
# /tmp/test-ci.yml
pipelines:
  test:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      hello:
        run: echo "hello from CI"
```

Then start:

```bash
./target/debug/local-runner \
  --relay-node-id <NODE_ID_HEX> \
  --yaml /tmp/test-ci.yml \
  --work-dir /tmp/ci-work-test
```

You should see:
```
  Iroh local ID: <RUNNER_ID>
  Connecting to relay <NODE_ID>...
  Connected to relay!
Local CI runner started
  Webhook: via iroh relay
```

And in Terminal 1:
```
Runner connected: <RUNNER_ID>
```

**Terminal 3 — Send a fake webhook:**

```bash
curl -X POST http://localhost:9787 \
  -H "Content-Type: application/json" \
  -H "X-Forgejo-Event: push" \
  -d '{
    "ref": "refs/heads/main",
    "after": "abc123def456789012345678901234567890abcd",
    "repository": {
      "name": "test-repo",
      "owner": { "login": "testuser" }
    }
  }'
```

Expected output:

- **curl** returns: `ok`
- **Terminal 1** (relay):
  ```
  webhook: abc123de main on testuser/test-repo
    → forwarded to runner
  ```
- **Terminal 2** (runner):
  ```
  iroh: received webhook abc123de on main
  ```

The runner will also try to post status to Forgejo and log URL errors (since
we didn't pass `--forgejo-url`) — that's expected and confirms the event
reached the coordinator.

### 9.2 HMAC Verification Test

Start the relay with a secret:

```bash
./target/debug/ci-relay --port 9787 --secret mysecret
```

**Without signature — should be rejected (401):**

```bash
curl -v -X POST http://localhost:9787 \
  -H "X-Forgejo-Event: push" \
  -d '{"ref":"refs/heads/main","after":"abc123","repository":{"name":"r","owner":{"login":"u"}}}'
```

**With correct signature:**

```bash
# Compute HMAC-SHA256
BODY='{"ref":"refs/heads/main","after":"abc123","repository":{"name":"r","owner":{"login":"u"}}}'
SIG=$(echo -n "$BODY" | openssl dgst -sha256 -hmac "mysecret" | awk '{print $2}')

curl -X POST http://localhost:9787 \
  -H "X-Forgejo-Event: push" \
  -H "X-Forgejo-Signature: $SIG" \
  -d "$BODY"
```

Should return `ok` and forward to the runner.

### 9.3 Runner Reconnection Test

1. Start relay and runner as in 9.1
2. Kill the runner (Ctrl-C in Terminal 2)
3. Restart the runner with the same `--relay-node-id`
4. The relay should log `Runner connected: <ID>` again
5. Send another webhook — it should flow through

### 9.4 No Runner Connected Test

1. Start the relay only (no runner)
2. Send a webhook via curl
3. Should get HTTP 502 and relay logs: `forward failed: no runner connected`

### 9.5 Full End-to-End with Forgejo

For a real deployment:

**On VPS:**

```bash
./ci-relay --port 8787 --secret <your-webhook-secret>
```

**On Thinkpad:**

```bash
./local-runner \
  --relay-node-id <NODE_ID_FROM_VPS> \
  --forgejo-url https://zachery.lol \
  --forgejo-token <your-forgejo-api-token> \
  --yaml .ci.yml \
  --work-dir ~/ci-work \
  --repo-url https://zachery.lol/<owner>/<repo>.git
```

**In Forgejo (repo settings → Webhooks):**

- Target URL: `http://localhost:8787`
- Secret: `<your-webhook-secret>`
- Events: Push, Create (tags), Pull Request

Push a commit and watch:
1. Relay logs the webhook and forwards it
2. Runner logs the received event and starts a pipeline
3. Forgejo shows commit status checks (pending → success/failure)

### 9.6 Inspecting Iroh Connectivity

Both binaries print their iroh Node ID on startup. To verify they're using
relay servers (expected when both are behind NAT or on different networks),
look for connection timing:

- **Fast connection (~1-3s)**: direct QUIC hole-punch succeeded
- **Slower connection (~5-10s)**: using iroh relay server fallback

If connection hangs indefinitely, check that both machines have internet
access and can reach iroh's relay servers (`https://relay.iroh.network`).

---

## 10. Known Gaps & Future Improvements

| Gap | Effort | Impact | Notes |
|-----|--------|--------|-------|
| Reconnection on runner side | Small | High | If the iroh connection drops mid-operation, the runner currently exits the receive loop. Should retry with backoff. |
| Multiple runner support | Medium | Medium | Relay caches one connection. For running CI on multiple machines, need a connection map keyed by runner identity. |
| Health check / heartbeat | Small | Medium | Neither side detects a silently dead connection until the next webhook. A periodic ping would surface stale connections faster. |
| Relay authentication | Small | Medium | Any iroh endpoint can connect to the relay. Should verify the runner's public key against an allowlist. |
| Binary size | Small | Low | ci-relay pulls in `swactor-ci` (which includes all CI types). A slimmer dependency with just `WebhookEvent` + `parse_webhook_json` would reduce the VPS binary. |
| Logging | Small | Low | Both binaries use `eprintln!`. Structured logging (tracing) would help in production. |

---

## Files Created/Modified

| Action | File | Purpose |
|--------|------|---------|
| Created | `crates/ci-relay/Cargo.toml` | Relay binary manifest |
| Created | `crates/ci-relay/src/main.rs` | Webhook relay: HTTP → iroh |
| Modified | `crates/local-runner/Cargo.toml` | Added iroh, tokio, serde_json deps |
| Modified | `crates/local-runner/src/main.rs` | Added `--relay-node-id` flag and iroh receiver |
| Modified | `Cargo.toml` (workspace root) | Added ci-relay to workspace members |
