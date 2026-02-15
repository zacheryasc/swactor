# Bugfixes: TCP Wire Hints + iroh Driver Join Protocol

> Two bugs found during first LAN cluster test without Docker.
> Same session, same root pattern: send path built, receive path left incomplete, no integration test.

---

## Table of Contents

### Part 1 — TCP Wire Frame Address Hints Never Decoded
*3 files changed · ~40 insertions, ~30 deletions*

1. [Symptom](#1-symptom)
2. [Root Cause](#2-root-cause)
3. [How It Happened](#3-how-it-happened)
4. [The Fix](#4-the-fix)
5. [Test Added](#5-test-added)
6. [Why Existing Tests Missed It](#6-why-existing-tests-missed-it)
7. [Preventing This Class of Bug](#7-preventing-this-class-of-bug)

### Part 2 — iroh Driver Join Protocol Never Worked
*1 file changed · ~40 insertions, ~60 deletions*

8. [Symptom (iroh)](#8-symptom-iroh)
9. [Root Cause (iroh)](#9-root-cause-iroh)
10. [How It Happened (iroh)](#10-how-it-happened-iroh)
11. [The Fix (iroh)](#11-the-fix-iroh)
12. [What IROH_TRANSPORT.md Said vs What the Code Did](#12-what-iroh_transportmd-said-vs-what-the-code-did)
13. [Current State: What Works, What Worries Me](#13-current-state-what-works-what-worries-me)
14. [Preventing This Class of Bug (Revised)](#14-preventing-this-class-of-bug-revised)

---

## 1. Symptom

Two `swactor-node` processes on separate machines (devuan-hpz at 192.168.1.106, thinkpad at 192.168.1.102) started, connected over TCP, and the joiner printed `Joining cluster via seed 192.168.1.102:7000` with no error. Yet both nodes reported empty SWIM member lists indefinitely. The AGENTS diagnostic protocol confirmed it:

```
curl localhost:9090/api/investigate?cmd=overview
→ "actors": 10, "workers": 2, ...   (runtime healthy)

curl localhost:9090/events | grep members
→ "members":[]                       (SWIM membership empty)
```

TCP connectivity was verified (`nc -zv 192.168.1.102 7000` → open). The port was listening. The join message was sent. But membership never formed.

---

## 2. Root Cause

Three related bugs in the TCP transport layer, all stemming from an incomplete implementation of address hints in the wire protocol.

### Bug A: Encoding/decoding mismatch

`encode_wire_envelope_with_hints()` in `driver.rs` wrote frames in an extended format:

```
[4B frame_len][32B dest][4B tag_len][tag][4B hints_len][hints_json][payload_json]
```

But `read_wire_envelope()` in `transport.rs` decoded the original format:

```
[4B frame_len][32B dest][4B tag_len][tag][remaining → payload]
```

Everything after the type tag — including the 4-byte `hints_len` field and the hints JSON — was slurped into `payload`. When `serde_json::from_slice` tried to deserialize the message, it hit the `hints_len` prefix bytes (not valid JSON) and silently failed.

### Bug B: Hints never extracted from the wire

Even if decoding had been correct, `TcpAcceptor::try_recv()` returned `Vec<(WireEnvelope, SocketAddr)>` with no mechanism to pass hints back to the caller.

### Bug C: `learn_hints()` never called

`NodeDriver::recv()` discarded the peer address (`_peer_addr`) and never called the existing `learn_hints()` method, leaving the `PeerAddressBook` permanently empty. Without the address book, the seed couldn't resolve the joiner's `NodeId` to a `SocketAddr` to send the `JoinResponse` back.

### The cascade

1. Node A sends `JoinRequest` with hints `[{A.node_id, A.listen_addr}]` to B
2. B's decoder corrupts the payload → `JoinRequest` deserializes anyway (simple struct, hints prepended but serde is lenient with trailing data for some formats — but actually fails here because the hints_len bytes precede the JSON)
3. Even if B somehow processes the `JoinRequest` and generates a `SendJoinResponse` action, B calls `resolve_addr(A.node_id)` which fails because A's address was never learned from hints
4. `send_action` prints `driver: send error: no address known for node ...` to stderr
5. A never receives the `JoinResponse`, membership stays empty on both sides

---

## 3. How It Happened

The address hints system was designed during the distribution realization phase (see `DOCKER_REALIZATION.md`) but was never completed. The evidence is in the code itself:

**`driver.rs:331-342` contained this comment block:**

```rust
// Extract hints from the envelope's payload prefix (if present)
// For simplicity in the wire format, hints are embedded at the end of the
// type_tag as a JSON suffix. But actually, we'll use the existing frame format
// and embed hints in a slightly different way.
//
// Actually, for backwards compatibility with the existing wire format,
// we'll detect and parse hints from the peer_addr on the TCP socket.
// The actual hint extraction happens via the message payloads for now.
//
// For this first pass: we parse the message and extract the sender's NodeId
// from the message itself, then associate it with the peer address.
```

This reads as a stream of consciousness — three contradictory approaches considered, none implemented. The comment "for this first pass" suggests intent to revisit, but the revisit never happened.

**Likely sequence of events:**

1. The encoding side (`encode_wire_envelope_with_hints`, `send_wire_with_hints`) was implemented first — it's the simpler direction (just add bytes to the buffer)
2. The decoding side was deferred. The comment block shows uncertainty about how to handle it
3. The Docker integration tests — which should have caught this — used a 5-node cluster where all nodes joined the same seed. The seed learned joiner addresses not from hints but from the TCP peer address on the accepted connection. In a Docker bridge network with static IPs, the peer address **happens to be the same as the listen address** (no NAT, no ephemeral ports for the listener side). So the Docker tests passed by accident
4. The `learn_hints()` method was written (correct implementation) but the call site in `recv()` was never added
5. The existing `transport_and_codec.rs` TCP tests used `TcpTransport::send_to()` which calls `encode_wire_envelope()` (no hints), not `encode_wire_envelope_with_hints()`. So the roundtrip tests passed because they never exercised the extended frame format

**In short**: the send path was built, the receive path was left as a TODO, and the test infrastructure didn't exercise the gap.

---

## 4. The Fix

### `crates/distribution/src/transport.rs`

**Unified wire format.** Changed `encode_wire_envelope()` to always write a `[4B hints_len=0]` field, making both the hint-aware and hint-free encoders produce the same frame structure:

```
[4B frame_len][32B dest][4B tag_len][tag][4B hints_len][hints][payload]
```

Updated `read_wire_envelope()` and `read_envelope_blocking()` to parse `hints_len`, extract hints bytes, then read the remaining as payload. Changed return type to `(WireEnvelope, Vec<u8>)`.

Updated `try_recv()` return type to `Vec<(WireEnvelope, SocketAddr, Vec<u8>)>` to propagate hints.

### `crates/distribution/src/driver.rs`

**Wired hints into the receive pipeline.** Updated `recv()` to:

1. Destructure the 3-tuple from `try_recv`
2. Deserialize hints bytes as `Vec<AddressHint>`
3. Call `self.learn_hints()` **before** dispatching the message

The ordering matters: hints must be learned before dispatch because `dispatch_incoming` may generate response actions (e.g., `SendJoinResponse`) that need to resolve the sender's address from the address book.

Removed the stale comment block in `dispatch_incoming`.

### `crates/distribution/tests/transport_and_codec.rs`

Updated existing TCP test destructuring for the new 3-tuple. Added `two_drivers_complete_join_handshake` scenario test (see below).

---

## 5. Test Added

```rust
#[test]
fn two_drivers_complete_join_handshake()
```

**Scenario:** Two `NodeDriver` instances on localhost. Driver A joins Driver B. After two `recv()` rounds (B processes join request + sends response, A processes response), assert that A's member list is non-empty.

**Why this test catches the bug:** If hints are broken, B cannot resolve A's address to send the `JoinResponse`. A never receives it, and its member list stays empty. The test asserts on the observable outcome (join completes) without coupling to hint extraction internals.

This is a contract-level test that would survive a complete refactor of the hint mechanism — as long as two drivers can join over TCP, it passes.

---

## 6. Why Existing Tests Missed It

### Simulation: bypasses wire encoding entirely

`crates/simulation/src/distribution/sim.rs:619` — `deliver_actions_tagged_with_net()` matches on `NodeAction` variants and calls handler methods directly:

```rust
NodeAction::SendJoinResponse { to, members, .. } => {
    if let Some(ref mut node) = nodes[idx] {
        let resp = node.handle_join_response(members.clone());
        // ...
    }
}
```

No `WireEnvelope`, no TCP, no `encode_wire_envelope_with_hints`, no `read_wire_envelope`. The simulation tests exercise the SWIM protocol state machine in isolation from the transport. This is a valid architecture for testing protocol correctness — but it creates a blind spot at the transport boundary.

### TCP transport tests: used the wrong encoder

The existing `wire_envelope_roundtrips_through_tcp` test used `TcpTransport::send_to()`, which calls `encode_wire_envelope()` (the hint-free encoder). The hint-aware encoder `encode_wire_envelope_with_hints()` lived in `driver.rs` and was never tested in isolation or via a roundtrip.

### Docker integration tests: worked by coincidence

In the Docker bridge network, each container has a static IP. When node-2 connects to the seed, the seed sees the peer address as `10.0.1.11:EPHEMERAL` — but the original `driver.rs` learned addresses from the `from_addr` field inside the `Ping` message, not from wire hints. The join path bypassed hints entirely because `handle_join_request(from)` doesn't need an address — it returns a `SendJoinResponse { to: from_node_id }`, and the address was already in the book from earlier Ping exchanges.

Wait — actually, that's not right either. Looking more carefully: in Docker, the seed received the `JoinRequest` and generated `SendJoinResponse { to: joiner_node_id }`. It then needed to `resolve_addr(joiner_node_id)`. Since `from_addr` was only in Ping messages (not JoinRequest), how did Docker tests pass?

The answer is in the original `driver.rs` before the hints refactor: the `JoinRequest` message originally carried a `from_addr: SocketAddr` field (see `DOCKER_REALIZATION.md` §4), and the driver learned the joiner's address from it directly. The hints mechanism was added later as a more general replacement, but the `from_addr` field was removed from `JoinRequest` at the same time. The hints were supposed to carry that information instead — but the receive side was never completed.

This means the bug was **introduced** during the hints refactor itself. The old `from_addr`-based path worked; the new hints-based path was half-built.

---

## 7. Preventing This Class of Bug

### The pattern: asymmetric encode/decode implementations

This is a classic serialization bug. The encoder and decoder were implemented at different times, possibly by different prompts/sessions, and the decoder was left incomplete. The encoder compiles and runs fine in isolation — you can write bytes to TCP all day. The decoder compiles and runs fine too — it just reads the wrong bytes. No type system catches this because both sides deal in `Vec<u8>`.

### Recommendations

**1. Roundtrip tests for every wire format change.**

Any time the wire format gains a new field or section, add a test that encodes a frame and decodes it back, asserting field equality. The existing `wire_envelope_roundtrips_through_tcp` test did this for the basic format but was never updated for the extended format with hints. Rule: **if you add an encoder, add the matching decoder test in the same commit.**

**2. Integration tests that assert on protocol outcomes, not just connectivity.**

The Docker tests asserted that nodes converge (alive_count >= N). This is good but insufficient — the tests passed because the old `from_addr` mechanism was still partially functional. A stronger assertion would have been: "the seed's address book contains the joiner's address after a join" — but that's white-box. The best middle ground is scenario tests like `two_drivers_complete_join_handshake` that test the full join flow over real TCP without Docker overhead.

**3. One canonical frame format.**

The root cause was two encoder functions (`encode_wire_envelope` and `encode_wire_envelope_with_hints`) producing different frame layouts consumed by one decoder. The fix unified them: `encode_wire_envelope` now writes `hints_len=0`, so there's exactly one frame format. **Never have two encoders for one decoder.**

**4. Don't defer the receive side.**

The comment block in `dispatch_incoming` was a red flag: three approaches considered, none implemented, marked "first pass." If the send side is too complex to decode immediately, that's a sign the design needs simplification before the send side ships. Ship encode and decode together or not at all.

**5. Simulation/transport boundary coverage.**

The simulation's direct-call architecture is correct for testing protocol logic at speed. But it means every transport-layer feature (wire format extensions, connection management, address resolution) needs its own test layer. Consider a "simulation over loopback TCP" mode that exercises the wire format without requiring Docker.

---

## Files Modified

| File | Change |
|------|--------|
| `crates/distribution/src/transport.rs` | Unified wire format with hints_len field; updated encoder, both decoders, and `try_recv` |
| `crates/distribution/src/driver.rs` | `recv()` extracts and learns hints before dispatch; removed stale comment |
| `crates/distribution/tests/transport_and_codec.rs` | Updated destructuring in 3 existing tests; added `two_drivers_complete_join_handshake` |

## Verification (TCP)

- `cargo test -p distribution` — 149 tests pass (including new scenario test)
- `cargo test` — full workspace green (35 core tests + 149 distribution tests)
- Live 2-node LAN cluster: both nodes report each other as `alive` with resolved addresses via the AGENTS protocol

---

# Bugfix: iroh Driver Join Protocol Never Worked

> 1 file changed · ~40 insertions, ~60 deletions
>
> Discovered immediately after fixing TCP hints, when testing iroh transport for the first time between two real machines

---

## Table of Contents (Part 2)

8. [Symptom (iroh)](#8-symptom-iroh)
9. [Root Cause (iroh)](#9-root-cause-iroh)
10. [How It Happened (iroh)](#10-how-it-happened-iroh)
11. [The Fix (iroh)](#11-the-fix-iroh)
12. [What IROH_TRANSPORT.md Said vs What the Code Did](#12-what-iroh_transportmd-said-vs-what-the-code-did)
13. [Current State: What Works, What Worries Me](#13-current-state-what-works-what-worries-me)
14. [Preventing This Class of Bug (Revised)](#14-preventing-this-class-of-bug-revised)

---

## 8. Symptom (iroh)

After fixing the TCP wire hints bug and confirming a 2-node TCP cluster, we switched to `--transport iroh` to test the QUIC/P2P path. Local node (devuan-hpz) started as seed. Thinkpad joined with `--seed-node-id <local's public key>`.

```
Node fcc58a98 started (iroh)
Joining cluster via seed f4b6e9fe
iroh driver: join error to f4b6e9fe...: connection lost
```

The iroh connection was established (iroh's DNS address lookup via pkarr/n0 resolved the seed), but the join handshake failed with "connection lost." Membership stayed empty on both sides.

---

## 9. Root Cause (iroh)

Three bugs in `iroh_driver.rs`, all in the connection/stream management layer. Like the TCP hints bug, each one alone would prevent the join handshake from completing.

### Bug A: `send_join_request` blocked on a bidi response that could never arrive

The joiner opened a **bidirectional** QUIC stream to send the `JoinRequest` and then waited for the `JoinResponse` on the recv half of the same stream:

```rust
let (mut send, mut recv) = conn.open_bi().await?;
write_message(&mut send, tag.as_bytes(), &payload).await?;
send.finish()?;
// Blocks here forever:
let (resp_tag, resp_payload) = read_message(&mut recv).await?;
```

The seed's `read_streams()` accepted the bidi stream but **discarded the send half**:

```rust
Ok(Ok((_send, mut recv))) => {
    match read_message(&mut recv).await { ... }
```

The JoinRequest was read and dispatched. `dispatch_incoming` generated `NodeAction::SendJoinResponse`. `send_actions` called `send_message`, which opened a **new uni stream** on a separate connection. The response went out — but not on the bidi stream the joiner was waiting on. The joiner blocked indefinitely until the QUIC idle timeout fired → "connection lost."

### Bug B: Accepted connections were never cached

`receive_pending()` accepted incoming connections via `endpoint.accept()`, read their streams, then let the `Connection` drop at the end of the `match` arm. The connection was never inserted into `self.connections`:

```rust
Ok(Some(incoming)) => {
    if let Ok(conn) = incoming.await {
        let remote_id = conn.remote_id();
        self.read_streams(&conn, remote_id, &mut messages).await;
        // conn drops here — never cached
    }
}
```

This meant the seed had no way to send messages back to the joiner through the connection the joiner established.

### Bug C: `dispatch_incoming` tried to `connect()` back to the joiner

Because the accepted connection was lost, the seed's JoinRequest handler tried to establish a **new outbound** connection to the joiner:

```rust
"swactor_dist::JoinRequest" => {
    // ...
    if !self.connections.contains_key(&from) {
        let endpoint = self.endpoint.clone();
        if let Ok(conn) = self.rt.block_on(async {
            endpoint.connect(key, ALPN).await
        }) {
            self.connections.insert(from, conn);
        }
    }
```

This required the **joiner** to have already published its address to n0's DNS/pkarr infrastructure — a process that takes seconds. If the joiner hadn't published yet, `endpoint.connect()` failed silently. Even if it succeeded, this created a second connection instead of reusing the one the joiner already established — doubling connection state and introducing asymmetric routing.

### The cascade

1. Joiner connects to seed via iroh (address resolved through DNS/pkarr), opens bidi stream, sends JoinRequest, blocks on bidi recv
2. Seed accepts connection, reads JoinRequest from bidi stream, discards `_send` half
3. Seed processes JoinRequest → generates `SendJoinResponse { to: joiner_id }`
4. Seed's `send_message()` calls `get_or_connect(joiner_id)` — no cached connection
5. Seed tries `endpoint.connect(joiner_key, ALPN)` — fails if joiner hasn't published to DNS yet, or creates a redundant second connection
6. Even if step 5 succeeds, response goes out on a uni stream of a different connection — the joiner never sees it
7. Joiner's bidi recv times out → "connection lost"
8. Membership stays empty on both sides

---

## 10. How It Happened (iroh)

`IROH_TRANSPORT.md` §7 describes the intended join protocol:

> *"Joiner calls `join(&[PublicKey])` — for each seed, opens a bidi stream, sends `JoinRequest`, reads `JoinResponse`"*
>
> *"Seed receives `JoinRequest` on a bidi stream, generates response via `node.handle_join_request()`, writes `JoinResponse` back on the same stream"*

The design called for the seed to write the `JoinResponse` back on the **same bidi stream**. The code never implemented this. Here's what was actually built:

1. **Joiner side**: correctly opens bidi, sends request, waits for response on bidi recv half. This matches the design.
2. **Seed side**: reads bidi streams via `read_streams()`, but discards the send half (`_send`). Messages are collected into a `Vec<(tag, payload, from_key)>` — no mechanism to carry the send stream back to the dispatcher. The response goes through `dispatch_incoming` → `send_actions` → `send_message` → opens a new uni stream. This does **not** match the design.

The disconnect: `read_streams` was written to collect messages generically (from both uni and bidi streams). The generic collection model (`Vec<(String, Vec<u8>, PublicKey)>`) has no slot for a "response channel." The bidi send half would need to be threaded through to the JoinRequest handler specifically — a special case the generic model doesn't accommodate.

The likely sequence:

1. `send_message` and `get_or_connect` were implemented first — they handle all outgoing messages generically through uni streams
2. `receive_pending` and `read_streams` were implemented as the generic receive path
3. `send_join_request` was written to use bidi, matching the design doc
4. The seed-side bidi response path was **never implemented** — the generic receive/dispatch/send pipeline was assumed to handle it, but it routes responses through `send_message` which opens new uni streams
5. The `dispatch_incoming` JoinRequest handler added a `connect()` back to the joiner as a workaround for not having the accepted connection cached — but this workaround depends on DNS publication timing
6. No integration test ever exercised the two-driver join path over real iroh connections (the three existing iroh tests check identity and snapshots only)

**In short**: the same pattern as the TCP hints bug. The send path was built. The design doc described a receive path. The receive path was never connected to the send path. No test covered the gap.

---

## 11. The Fix (iroh)

### `crates/distribution/src/iroh_driver.rs`

**Changed `send_join_request` to fire-and-forget.** Replaced bidi stream with uni stream. The joiner sends the `JoinRequest` and returns immediately. The `JoinResponse` arrives later through the normal `recv()` loop — the seed sends it back over the connection the joiner established (which is now properly cached).

```rust
// Before: blocked on bidi response that never came
let (mut send, mut recv) = conn.open_bi().await?;
write_message(&mut send, tag.as_bytes(), &payload).await?;
send.finish()?;
let (resp_tag, resp_payload) = read_message(&mut recv).await?;

// After: fire-and-forget on uni stream
let mut send = conn.open_uni().await?;
write_message(&mut send, tag.as_bytes(), &payload).await?;
send.finish()?;
```

**Changed `receive_pending` to return accepted connections.** Return type changed from `Vec<(String, Vec<u8>, PublicKey)>` to `(Vec<...>, Vec<(NodeId, Connection)>)`. `recv()` inserts new connections into `self.connections` via `entry().or_insert()` before dispatching messages.

This ordering matters: connections must be cached **before** dispatch, because `dispatch_incoming` may generate response actions that need to route back through the newly cached connection.

**Removed the `connect()` back-connect in `dispatch_incoming`.** The JoinRequest handler no longer tries to establish a new outbound connection to the joiner. The accepted incoming connection is already cached from `receive_pending`. `send_message` → `get_or_connect` finds it in the cache.

**Removed bidi stream handling from `read_streams`.** Since all messages now use uni streams, the bidi accept loop was removed. This eliminates dead code and makes the stream model consistent: uni streams only, everywhere.

---

## 12. What IROH_TRANSPORT.md Said vs What the Code Did

| IROH_TRANSPORT.md §7 claim | Actual behavior before fix |
|---|---|
| "Opens a bidi stream, sends JoinRequest, reads JoinResponse" | Correct on joiner side. But seed never wrote to the bidi send half. |
| "Seed receives JoinRequest on a bidi stream, generates response via handle_join_request(), writes JoinResponse back on the same stream" | Seed read from bidi, dispatched to generic handler, sent response on a **new uni stream** via `send_message`. Never wrote back on the bidi stream. |
| "Both uni and bidi streams are polled" (§7, Receiving Messages) | Bidi streams were polled, but the send half was discarded. Only the recv half was read — functionally identical to uni. |
| "On send, the driver checks the cache" (§7, Connection Caching) | Accepted connections were never put in the cache. Only outbound connections (from `get_or_connect`) were cached. |

The design doc was written to describe intended behavior. The code was written to pass identity/snapshot tests. The gap between intent and implementation was never tested because no integration test exercised the multi-node join path.

After the fix, the design is simpler than what the doc described: **all messages use uni streams, including JoinRequest**. The bidi request-response pattern is gone entirely. The JoinResponse arrives asynchronously through the normal `recv()` loop, same as Ping/Ack/PingReq. The join protocol now works identically to how it works over TCP — fire JoinRequest, seed processes and sends JoinResponse via its own send path, joiner picks it up on the next recv cycle.

`IROH_TRANSPORT.md` §7 and §10.3 should be updated to reflect this. The doc currently describes a bidi join protocol that no longer exists.

---

## 13. Current State: What Works, What Worries Me

### What works

- Two nodes on separate machines join and maintain SWIM membership over iroh QUIC
- Peer discovery via n0's DNS/pkarr infrastructure (joiner resolves seed's public key → relay URL → direct address)
- SWIM probes flow bidirectionally (Ping/Ack over uni streams on cached connections)
- Connection caching: seed caches the joiner's accepted connection, joiner caches its outbound connection
- Hot reconnect: `send_message` evicts stale connections and retries once

### What worries me

**1. No integration test for iroh join handshake.**

The three existing iroh tests (`iroh_driver_creates_with_unique_identity`, `iroh_driver_snapshot_contains_node_id`, `iroh_driver_identity_matches_iroh_endpoint`) test identity alignment and snapshot structure. None of them test two `IrohDriver`s joining and exchanging SWIM probes. The TCP driver has `two_drivers_complete_join_handshake` — the iroh driver has no equivalent.

Writing one is non-trivial because `IrohDriver` owns a tokio runtime internally and needs iroh's address lookup infrastructure to resolve peers. A loopback test would either need an in-memory address lookup or `Endpoint::builder().address_lookup(MemoryLookup)` wiring. This should be the next thing built.

**2. `entry().or_insert()` silently drops fresh connections.**

When `recv()` caches new connections:

```rust
self.connections.entry(node_id).or_insert(conn);
```

If a connection for that `NodeId` already exists (e.g., a stale outbound connection), the fresh inbound connection is silently dropped. The driver continues using the old (possibly broken) connection. This should use `insert()` to unconditionally replace, or at minimum check `close_reason()` on the existing connection before deciding which to keep.

**3. 1ms timeout polling is a scheduling lottery.**

`receive_pending` and `read_streams` use `tokio::time::timeout(Duration::from_millis(1), ...)`. If a message arrives 2ms after the poll, it waits until the next main loop iteration (100ms later). For SWIM probes with a 3-second timeout, this is fine. For join latency, it means the JoinResponse takes at least one main loop cycle (100ms) to arrive instead of arriving immediately.

The alternative — longer poll timeouts — would make `recv()` block longer, delaying `tick()` and heartbeats. The right fix is making the main loop async (select on endpoint events + tick timer), but that's a larger refactor.

**4. Relay dependency on n0's infrastructure.**

`Endpoint::builder()` applies the `N0` preset which publishes addresses to and resolves from n0.computer's pkarr relay and DNS servers. If those servers go down, nodes can't discover each other by public key alone. For LAN-only clusters, this is unnecessary overhead and a reliability risk. The `address-lookup-mdns` feature (mDNS local discovery) would eliminate the WAN dependency for LAN clusters but requires the `address-lookup-mdns` cargo feature on iroh, which isn't currently enabled.

**5. The `_from` parameter in `dispatch_incoming` is unused.**

After removing the `connect()` call, the `from: NodeId` parameter is no longer used. It's renamed to `_from` to suppress the warning, but its existence is a code smell — it suggests the dispatcher might need sender identity for something, but currently doesn't. The sender identity is already embedded in the message payloads (`Ping.from`, `JoinRequest.from`, etc.), so the parameter is truly redundant.

**6. Connection lifecycle is unclear on longer timescales.**

The `connections` HashMap grows monotonically — connections are added but only removed when a send fails. If a node joins, leaves, and a new node with a different identity takes its place, the old connection lingers. There's no periodic cleanup, no max connection count, no TTL. For a 2-node test this is irrelevant. For a 50-node cluster running for hours, the HashMap could accumulate stale entries.

---

## 14. Preventing This Class of Bug (Revised)

Both the TCP hints bug and the iroh driver bug share the same root pattern. Updating the recommendations from §7 with what we learned.

### The pattern: design docs that describe untested behavior

Both bugs were in code that had accompanying design documentation (DOCKER_REALIZATION.md for TCP hints, IROH_TRANSPORT.md §7 for iroh join). The docs described correct behavior. The code didn't implement it. The tests didn't check.

A design doc is not a test. A design doc that describes send-then-receive behavior is especially dangerous because both sides compile independently — the compiler can't tell you that the send side is writing bytes nobody reads, or that the receive side is discarding a stream handle the send side is waiting on.

### Revised recommendations

**1. Every driver gets a join handshake integration test. (Upgraded from "roundtrip tests" to "scenario tests.")**

Not "test that encoding roundtrips" — test that **two drivers can join and form a cluster**. The TCP driver now has `two_drivers_complete_join_handshake`. The iroh driver needs the equivalent. The test asserts on the observable outcome (member list is non-empty after join), not on internal state. If the join protocol changes, the test still passes as long as joining works.

**2. Don't mix stream patterns in the same protocol.**

The original iroh driver used uni streams for Ping/Ack/PingReq/JoinResponse and bidi streams for JoinRequest→JoinResponse. The `read_streams` function had to handle both, and the bidi path was broken. The fix: uni streams for everything. One stream pattern, one receive path, one send path. If you need request-response semantics, implement them at the application level (correlation IDs) rather than at the stream level.

**3. If the design doc says "the seed writes back on the same stream," test exactly that.**

The IROH_TRANSPORT.md §7 design was reasonable. The bug wasn't in the design — it was in the implementation diverging from the design without anyone noticing. If a design doc describes a specific data flow, write a test that asserts on that flow before moving on. The test would have immediately shown that the seed wasn't writing to the bidi send half.

In this case, we chose a different design (uni-only, fire-and-forget join) rather than fixing the bidi implementation. That's fine — the simpler design is better. But the doc should be updated to match, and the test should enforce whichever design is chosen.

**4. Cache every connection you accept.**

If `endpoint.accept()` gives you a connection, put it in your connection map. If you don't, you have a one-way channel — you can read from the peer but not write back. This is a general rule for connection-oriented transports: accepted connections are valuable because the remote already established them. Creating a new outbound connection is expensive (address lookup, TLS handshake, relay negotiation) and may fail if the remote hasn't published its address yet.

**5. Test the transport, not just the protocol.**

The simulation tests exercise SWIM correctness at protocol speed. The TCP `two_drivers_complete_join_handshake` exercises the TCP transport. The iroh identity tests exercise endpoint construction. Nobody tested **iroh SWIM over iroh transport**. Each layer was tested in isolation; the integration between them was assumed to work. It didn't.

The testing pyramid for the distribution layer should be:
- **Protocol tests** (simulation): SWIM state machine correctness, fast, deterministic
- **Transport tests** (per-driver): two drivers join over real transport, observable outcome
- **Integration tests** (multi-machine or Docker): full nodes with dashboard, actors, and real network conditions

We have the first tier. We have half of the second (TCP only). We have none of the third for iroh. The iroh transport test is the most urgent gap.

---

## Files Modified (iroh fix)

| File | Change |
|------|--------|
| `crates/distribution/src/iroh_driver.rs` | `send_join_request`: bidi→uni fire-and-forget; `receive_pending`: returns new connections; `recv()`: caches accepted connections before dispatch; `dispatch_incoming`: removed redundant `connect()` back to joiner; `read_streams`: removed dead bidi handling |

## Verification (iroh)

- `cargo build -p distribution --features iroh` — clean (1 pre-existing warning)
- `cargo test -p distribution` — 149 tests pass (TCP path unaffected)
- Live 2-node LAN cluster over iroh:
  - Local (f4b6e9fe) sees thinkpad (fcc58a98) as `alive`
  - Thinkpad (fcc58a98) sees local (f4b6e9fe) as `alive`
  - SWIM probes flowing bidirectionally (16+ probe rounds observed)
  - Peer discovery via n0 DNS/pkarr infrastructure — no manual address configuration
