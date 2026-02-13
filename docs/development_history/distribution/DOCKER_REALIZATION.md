# Distribution Realization — Development History

> Covers all work to bridge the pure-logic distributed runtime to real TCP networking,
> package it as a Docker-deployable node binary, verify it against the simulation
> tests via a 5-node Docker cluster, and validate cross-machine behavior via a
> LAN cluster split across two physical machines.
>
> ~22 files changed · ~1,600 insertions
>
> *Branch: `distribution-realization`*

---

## Table of Contents

1. [Overview & Motivation](#1-overview--motivation)
2. [What Was Built](#2-what-was-built)
3. [Development Phases](#3-development-phases)
4. [Wire Protocol Gap — Piggyback Extension](#4-wire-protocol-gap--piggyback-extension)
5. [NodeDriver — TCP ↔ NodeAction Bridge](#5-nodedriver--tcp--nodeaction-bridge)
6. [REST API Endpoint](#6-rest-api-endpoint)
7. [Node Binary](#7-node-binary)
8. [Docker Infrastructure](#8-docker-infrastructure)
9. [Integration Test Harness](#9-integration-test-harness)
10. [Cross-Machine LAN Cluster](#10-cross-machine-lan-cluster)
11. [Design Decisions & Tradeoffs](#11-design-decisions--tradeoffs)
12. [Bugs Encountered](#12-bugs-encountered)
13. [Known Gaps & Future Improvements](#13-known-gaps--future-improvements)
14. [Test Coverage Summary](#14-test-coverage-summary)

---

## 1. Overview & Motivation

The distribution layer (`crates/distribution/`) was built as a set of **pure state machines** — `DistributedNode::tick()` produces `Vec<NodeAction>` that the caller translates to network I/O. All existing tests used in-process method calls: simulation nodes forwarded actions directly via `node.handle_ping(...)` without real networking.

This left a critical gap: **no code existed to actually run the protocol over TCP**. The `TcpTransport` and `TcpAcceptor` were implemented and tested in isolation, and the `NodeAction` enum described exactly what messages to send where, but the bridge between them was missing. From DISTRIBUTION.md §13.5:

> *"The actual wiring of `node.tick() → transport.send()` for each `NodeAction` is missing."*

This work closes that gap by:

1. **Extending wire protocol messages** with piggyback fields required for SWIM dissemination
2. **Creating NodeDriver** — the bridge that maps `NodeAction` → TCP sends and TCP receives → handler calls
3. **Adding a REST endpoint** for programmatic cluster health queries
4. **Packaging a node binary** (`swactor-node`) with CLI, dashboard, and actor registration
5. **Building Docker infrastructure** for a 5-node cluster with static IPs
6. **Writing integration tests** that mirror the simulation scenarios and verify real TCP behavior matches simulation expectations

The result: `docker compose up` spins up 5 nodes that form a SWIM cluster, register actors in the Kademlia directory, and can be observed via the runtime dashboard — matching the outcomes of the simulation tests.

---

## 2. What Was Built

| Component | Location | Lines | Files |
|-----------|----------|-------|-------|
| Wire protocol extension | `crates/distribution/src/messages.rs` | ~15 | 1 modified |
| NodeDriver | `crates/distribution/src/driver.rs` | ~264 | 1 new |
| REST API endpoint | `crates/runtime-dashboard/src/server.rs` | ~30 | 1 modified |
| Node binary | `crates/node/` | ~200 | 2 new |
| Dockerfile | `Dockerfile` | 9 | 1 new |
| Docker Compose (single-machine) | `tests/docker/docker-compose.yml` | 71 | 1 new |
| Docker Compose (LAN) | `tests/docker/docker-compose.lan-*.yml` | ~80 | 2 new |
| LAN orchestration script | `tests/docker/run-lan-cluster.sh` | ~100 | 1 new |
| Test harness | `tests/docker/` | ~400 | 4 new |
| LAN integration tests | `tests/docker/tests/lan_cluster.rs` | ~200 | 1 new |
| Test updates | `crates/distribution/tests/transport_and_codec.rs` | ~5 | 1 modified |
| Workspace config | `Cargo.toml` (root) | ~2 | 1 modified |

---

## 3. Development Phases

### Phase 1 — Extend wire protocol messages with piggyback

SWIM propagates membership changes by "piggybacking" encoded gossip data on every Ping, Ack, and PingReq message. The internal `NodeAction::SendPing` carried a `piggyback: Vec<u8>` field, but the wire-level `Ping` struct in `messages.rs` did not. Without the piggyback field in the wire message, SWIM dissemination could not function over TCP — nodes would send pings and acks but never propagate membership updates.

Additionally, `Ping` needed a `from_addr: SocketAddr` field because `handle_ping()` requires the sender's **listen address** (not the TCP ephemeral port of the incoming connection).

### Phase 2 — Create NodeDriver (TCP ↔ NodeAction bridge)

The core bridge component. Owns a `DistributedNode`, `TcpTransport`, and `TcpAcceptor`. Translates between the pure state machine world and real TCP I/O.

### Phase 3 — Add `/api/distribution` REST endpoint

The dashboard's SSE stream provides real-time snapshot updates, but integration tests need a synchronous polling endpoint. Added a simple GET handler that returns `DistributionNodeSnapshot` as JSON.

### Phase 4 — Create node binary crate

A CLI binary (`swactor-node`) that wires together the NodeDriver, actor runtime, and dashboard into a deployable process.

### Phase 5 — Docker infrastructure

Multi-stage Dockerfile and 5-service docker-compose.yml with a bridge network and static IPs.

### Phase 6 — Integration tests

Rust test crate with utilities for cluster lifecycle management and 4 `#[ignore]` test scenarios that mirror the simulation tests.

### Phase 7 — Cross-machine LAN cluster

Split the single-machine cluster into two compose files — one for each physical machine — using `network_mode: host` for real LAN communication. Added `LanClusterHandle` to orchestrate builds and container lifecycle across machines via SSH. 4 new LAN test scenarios mirror the single-machine tests but exercise real network boundaries.

### Phase 8 — Stale connection fix and test hardening

Discovered and fixed a stale TCP connection pool bug in `transport.rs` where killed-and-restarted nodes couldn't rejoin because the seed's pool still held a dead connection. Added build-once optimization via `std::sync::Once` and tightened all convergence timeouts from 60–90s to 30s.

---

## 4. Wire Protocol Gap — Piggyback Extension

### The Problem

SWIM dissemination works by attaching membership gossip to protocol messages. The `DisseminationQueue` encodes updates into a `Vec<u8>` via `pack_piggyback()`, and `NodeAction::SendPing` carries this as `piggyback: Vec<u8>`. But the wire-level `Ping` struct only had `{ from, sequence }` — no piggyback field. This meant:

- In-process simulation: works — `handle_ping()` receives the piggyback directly from the action
- Over TCP: broken — the piggyback bytes are never serialized into the wire message

### The Fix

**`messages.rs`** — Added fields to three structs:

```rust
pub struct Ping {
    pub from: NodeId,
    pub from_addr: SocketAddr,   // NEW: sender's listen address
    pub sequence: u64,
    #[serde(default)]
    pub piggyback: Vec<u8>,      // NEW: SWIM gossip payload
}

pub struct Ack {
    pub from: NodeId,
    pub sequence: u64,
    #[serde(default)]
    pub piggyback: Vec<u8>,      // NEW
}

pub struct PingReq {
    pub from: NodeId,
    pub target: NodeId,
    pub target_addr: SocketAddr,
    pub sequence: u64,
    #[serde(default)]
    pub piggyback: Vec<u8>,      // NEW
}
```

**`#[serde(default)]`** ensures backward compatibility — if a message arrives without piggyback (e.g., from an older node), it deserializes as an empty `Vec<u8>` rather than failing.

**`from_addr` on Ping**: The `handle_ping()` method signature requires `from_addr: SocketAddr` to learn the sender's cluster-visible listen address. Without this, the receiving node would only see the TCP ephemeral port, which is useless for SWIM (you need to know where to send Ack/PingReq *back* to the sender's listen address).

**`transport_and_codec.rs`** — Updated Ping constructors in two tests to include the new fields.

---

## 5. NodeDriver — TCP ↔ NodeAction Bridge

### `crates/distribution/src/driver.rs` (264 lines)

```
NodeDriver
  ├── node: DistributedNode      — pure state machine
  ├── transport: TcpTransport    — connection pool for outgoing TCP
  ├── acceptor: TcpAcceptor      — non-blocking listener for incoming TCP
  └── streams: Vec<TcpStream>    — accepted connections (reused across recv calls)
```

### Outgoing: NodeAction → TCP

`tick()` calls `node.tick()` → iterates the returned `Vec<NodeAction>` → maps each to a wire message and sends via TCP:

| NodeAction | Wire Message | Destination |
|------------|-------------|-------------|
| `SendPing { to_addr, sequence, piggyback, .. }` | `Ping { from, from_addr, sequence, piggyback }` | `to_addr` |
| `SendAck { to_addr, sequence, piggyback, .. }` | `Ack { from, sequence, piggyback }` | `to_addr` |
| `SendPingReq { relay_addr, target, target_addr, sequence, piggyback, .. }` | `PingReq { from, target, target_addr, sequence, piggyback }` | `relay_addr` |
| `SendJoinRequest { to_addr }` | `JoinRequest { from, addr }` | `to_addr` |
| `SendJoinResponse { to_addr, members, .. }` | `JoinResponse { members }` | `to_addr` |
| `MembershipChanged { .. }` | *(no network I/O)* | — |

Messages are encoded via `serde_json::to_vec()` (not the `Codec<M>` trait — see [§10.2](#102-direct-serde-vs-codec-trait)) and wrapped in a `WireEnvelope` for TCP framing.

### Incoming: TCP → Handler

`recv()` calls `acceptor.try_recv()` → for each `(WireEnvelope, SocketAddr)`, dispatches by `type_tag`:

| type_tag | Handler | Returns |
|----------|---------|---------|
| `"swactor_dist::Ping"` | `node.handle_ping(from, from_addr, seq, &piggyback)` | `Vec<NodeAction>` (Ack) |
| `"swactor_dist::Ack"` | `node.handle_ack(from, seq, &piggyback)` | `Vec<NodeAction>` |
| `"swactor_dist::PingReq"` | `node.handle_ping_req(from, target, target_addr, seq, &piggyback)` | `Vec<NodeAction>` |
| `"swactor_dist::JoinRequest"` | `node.handle_join_request(from, addr)` | `Vec<NodeAction>` |
| `"swactor_dist::JoinResponse"` | `node.handle_join_response(members)` | `Vec<NodeAction>` |

Response actions (e.g., the Ack generated by handle_ping) are immediately sent via the same `send_actions()` path.

### SWIM_DEST Dummy Address

The `WireEnvelope` format requires a `dest: ActorAddress` field (transport was designed for actor-level routing). SWIM messages route by `SocketAddr`, not `ActorAddress`, so a dummy `const SWIM_DEST: ActorAddress = ActorAddress([0u8; 32])` is used. The field is ignored on the receive side — dispatch is by `type_tag`.

### Public API

```rust
impl NodeDriver {
    fn new(config: DistributedNodeConfig) -> Result<Self, Error>;
    fn join(&mut self, seeds: &[SocketAddr]);
    fn tick(&mut self);             // advance SWIM + send outgoing
    fn recv(&mut self);             // process incoming TCP
    fn snapshot(&self) -> DistributionNodeSnapshot;
    fn node(&self) -> &DistributedNode;
    fn node_mut(&mut self) -> &mut DistributedNode;
    fn node_id(&self) -> NodeId;
    fn listen_addr(&self) -> SocketAddr;
}
```

---

## 6. REST API Endpoint

### `/api/distribution` in `crates/runtime-dashboard/src/server.rs`

Feature-gated with `#[cfg(feature = "distribution")]`. Returns `DistributionNodeSnapshot` as JSON on GET.

```rust
#[cfg(feature = "distribution")]
fn handle_distribution_api(
    request: tiny_http::Request,
    distribution: Arc<Mutex<Option<Arc<dyn DistributionStatsProvider>>>>,
) {
    // Lock → snapshot → serialize → respond 200 with JSON
    // Returns {} if no provider attached
}
```

The route is registered alongside existing routes (`/`, `/actors`, `/distribution`, `/events`):

```
"/api/distribution" => handle_distribution_api(request, distribution)
```

This endpoint is what the Docker integration tests poll to verify cluster state.

---

## 7. Node Binary

### `crates/node/` — `swactor-node`

**Cargo.toml dependencies**: `distribution`, `runtime-dashboard`, `swactor`, `clap`, `ctrlc`

**CLI arguments**:

```
swactor-node --listen <IP:PORT> [--seed <IP:PORT>] [--dashboard-port <PORT>] [--actors <N>]
```

| Arg | Default | Purpose |
|-----|---------|---------|
| `--listen` | (required) | SWIM protocol listen address |
| `--seed` | (none) | Seed node to join; omit for the seed itself |
| `--dashboard-port` | 9090 | HTTP dashboard port |
| `--actors` | 0 | Number of dummy `HeartbeatActor`s to register |

### Startup Sequence

1. Parse CLI args
2. Set SIGTERM/SIGINT handler (`ctrlc`)
3. Start dashboard HTTP server
4. Create actor runtime (2 threads, 1024 max actors)
5. Create `NodeDriver` with SWIM config (probe_interval=5, probe_timeout=3, indirect_probes=2, suspicion_timeout=20)
6. If `--seed` provided: `driver.join(&[seed])`
7. Spawn `--actors` dummy HeartbeatActors, register each in the node's directory
8. Wire `SnapshotProvider` to dashboard (decoupled via `Arc<Mutex<Option<Snapshot>>>`)
9. Main loop (100ms sleep):
   - `driver.recv()` — process incoming TCP
   - `driver.tick()` — SWIM protocol + send outgoing TCP
   - Send Heartbeat to each actor (keeps them alive)
   - Update cached snapshot for dashboard

### SnapshotProvider Decoupling

Same pattern used in `dashboard_demo.rs`: the dashboard SSE thread reads a cached `Option<DistributionNodeSnapshot>` behind `Arc<Mutex<>>`, while the main loop writes a fresh snapshot each tick. The SSE thread never contends for the NodeDriver — snapshots can be up to 100ms stale, which is fine for monitoring.

---

## 8. Docker Infrastructure

### Dockerfile (9 lines)

Multi-stage build:

```dockerfile
FROM rust:1.93-slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p node

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/swactor-node /usr/local/bin/
ENTRYPOINT ["swactor-node"]
```

Builder stage compiles the workspace in release mode. Runtime stage is a minimal Debian image with only the binary.

### docker-compose.yml — 5-Node Cluster

```
Network: 10.0.1.0/24 (bridge)

┌─────────────────────────────────────────────────────────────────┐
│  seed (10.0.1.10)     --listen 10.0.1.10:7000                  │
│  Dashboard: host:9091 → container:9090                          │
│  No --seed (this IS the seed)                                   │
├─────────────────────────────────────────────────────────────────┤
│  node-2 (10.0.1.11)  --listen 10.0.1.11:7000 --seed 10.0.1.10 │
│  Dashboard: host:9092 → container:9090                          │
├─────────────────────────────────────────────────────────────────┤
│  node-3 (10.0.1.12)  --listen 10.0.1.12:7000 --seed 10.0.1.10 │
│  Dashboard: host:9093 → container:9090                          │
├─────────────────────────────────────────────────────────────────┤
│  node-4 (10.0.1.13)  --listen 10.0.1.13:7000 --seed 10.0.1.10 │
│  Dashboard: host:9094 → container:9090                          │
├─────────────────────────────────────────────────────────────────┤
│  node-5 (10.0.1.14)  --listen 10.0.1.14:7000 --seed 10.0.1.10 │
│  Dashboard: host:9095 → container:9090                          │
└─────────────────────────────────────────────────────────────────┘
```

Each node registers 2 actors (`--actors 2`), for 10 total across the cluster.

**Static IPs**: Avoids DNS resolution complexity. Each node knows its own IP and the seed's IP at startup. SWIM dissemination handles the rest — after joining, nodes learn about each other through piggybacked gossip.

**Port mapping**: Each container's dashboard (port 9090) is mapped to a unique host port (9091–9095) so the test harness can query each node independently.

---

## 9. Integration Test Harness

### `tests/docker/` — Workspace Member

**Structure**:
```
tests/docker/
├── Cargo.toml              — depends on distribution, reqwest, serde_json
├── docker-compose.yml      — 5-node cluster definition
├── src/
│   └── lib.rs              — test utilities
└── tests/
    └── cluster.rs          — 4 integration test scenarios
```

### Test Utilities (`src/lib.rs`)

| Function/Type | Purpose |
|---------------|---------|
| `ClusterHandle` | RAII wrapper — `start()` runs `docker compose up`, `Drop` runs `docker compose down` |
| `poll_distribution(port)` | GET `/api/distribution` → `Option<DistributionNodeSnapshot>` |
| `wait_for_convergence(ports, expected_alive, timeout)` | Poll until all nodes see `>= expected_alive` members |
| `wait_for_death_detection(ports, max_alive, timeout)` | Poll until all nodes see `<= max_alive` members |
| `kill_node(service)` | `docker compose stop <service>` |
| `restart_node(service)` | `docker compose start <service>` |

**Compose file resolution**: Uses `env!("CARGO_MANIFEST_DIR")` to build an absolute path to `docker-compose.yml` at compile time. This avoids path-doubling issues when `cargo test` runs from a different working directory.

### 4 Test Scenarios (`tests/cluster.rs`)

All marked `#[ignore]` — require Docker. Run with: `cargo test -p docker-tests -- --ignored`

#### Test 1: `cluster_of_five_converges`
*Mirrors: `distribution_sim.rs::cluster_of_five_converges`*

```
Given: 5 nodes started via docker compose
When:  wait up to 30s for convergence
Then:  all 5 nodes report alive_count >= 4
       and routing_table_size >= 3
```

#### Test 2: `node_death_is_detected`
*Mirrors: `distribution_sim.rs::node_death_is_detected`*

```
Given: converged 5-node cluster
When:  docker compose stop node-3
Then:  within 30s, surviving 4 nodes report alive_count <= 4
       and at least one survivor sees dead_count >= 1
```

#### Test 3: `killed_node_rejoins`
*Mirrors: `distribution_sim.rs::killed_node_rejoins`*

```
Given: converged cluster, node-3 killed and detected dead
When:  docker compose start node-3
Then:  within 30s, node-3 reports alive_count >= 1
```

#### Test 4: `actors_resolvable_across_cluster`
*Mirrors: `distribution_sim.rs::actors_resolvable_across_cluster`*

```
Given: converged 5-node cluster, each with 2 registered actors
When:  query each node's snapshot
Then:  each node has directory_entry_count >= 2
       total directory entries across cluster >= 10
       total cache entries >= 5
```

### Simulation ↔ Docker Parity

The simulation tests run in-process with direct method calls. The Docker tests exercise the same protocol logic but over real TCP connections, Docker networking, and process boundaries. Both assert the same behavioral properties:

| Property | Simulation Test | Docker Test |
|----------|----------------|-------------|
| 5-node cluster converges | `cluster_of_five_converges` | `cluster_of_five_converges` |
| Dead node detected | `node_death_is_detected` | `node_death_is_detected` |
| Killed node rejoins | `killed_node_rejoins` | `killed_node_rejoins` |
| Actors in directory | `actors_resolvable_across_cluster` | `actors_resolvable_across_cluster` |

---

## 10. Cross-Machine LAN Cluster

### Motivation

The single-machine Docker cluster validates SWIM over TCP within a bridge network on one host. This leaves a gap: real deployments span multiple machines with distinct network stacks. The LAN cluster tests exercise this by splitting 5 nodes across two physical machines communicating over a real Ethernet LAN.

### Infrastructure

**Machines**:
- **devuan-hpz** (192.168.1.106): runs seed + node-2 (2 nodes)
- **thinkpad** (192.168.1.102): runs node-3, node-4, node-5 (3 nodes)

**Split compose files**: Unlike the single-machine cluster (bridge network with static IPs), the LAN cluster uses `network_mode: host` so containers bind directly to the host's LAN interface.

```
docker-compose.lan-hpz.yml         docker-compose.lan-thinkpad.yml
┌──────────────────────────┐       ┌───────────────────────────────┐
│ seed   192.168.1.106:7000│       │ node-3  192.168.1.102:7000    │
│ node-2 192.168.1.106:7001│       │ node-4  192.168.1.102:7001    │
│ Dashboards: 9091, 9092   │       │ node-5  192.168.1.102:7002    │
└──────────────────────────┘       │ Dashboards: 9093, 9094, 9095  │
        ↕ LAN (2ms)               └───────────────────────────────┘
```

Each thinkpad node seeds to `192.168.1.106:7000` (the hpz seed). With `network_mode: host`, each node needs a unique port on its host — hence 7000/7001 on hpz and 7000/7001/7002 on thinkpad.

### Orchestration

**Repo sync**: thinkpad has no rsync, so `LanClusterHandle` uses `tar czf | scp | ssh tar xzf` to push the workspace (excluding `target/` and `.git/`).

**Build-once optimization**: A `static BUILD_LAN_ONCE: Once` ensures that repo sync + `docker compose build` on both machines happens exactly once per test run. Subsequent `LanClusterHandle::start()` calls skip the build and just run `docker compose up -d`. This reduced the full 4-test suite from ~840s to ~630s.

**Remote control**: `kill_remote_node()` and `restart_remote_node()` execute `docker compose stop/start` on the thinkpad via SSH.

### Shell Script (`run-lan-cluster.sh`)

A standalone orchestration script for quick LAN validation outside of `cargo test`. Syncs repo, builds on both machines, starts both sides, polls all 5 dashboards for convergence, reports pass/fail, and tears down via a trap handler on exit.

### LAN Test Scenarios (`tests/docker/tests/lan_cluster.rs`)

All marked `#[test] #[ignore]`, run with: `cargo test -p docker-tests -- --ignored lan_ --test-threads=1`

#### Test 1: `lan_cluster_converges`
```
Given: 5 nodes split across hpz and thinkpad
When:  wait up to 30s for convergence
Then:  all 5 nodes report alive_count >= 4 and routing_table_size >= 3
```

#### Test 2: `lan_remote_node_death_detected`
```
Given: converged LAN cluster
When:  kill node-3 on thinkpad
Then:  within 30s, 4 survivors see alive_count <= 4
       and at least one survivor sees dead_count >= 1
```

#### Test 3: `lan_killed_remote_node_rejoins`
```
Given: converged cluster, node-3 killed and detected dead
When:  restart node-3 on thinkpad
Then:  within 30s, node-3 reports alive_count >= 1
```

#### Test 4: `lan_actors_resolvable_cross_machine`
```
Given: converged 5-node LAN cluster, each with 2 registered actors
When:  query each node's snapshot
Then:  each node has directory_entry_count >= 2
       total directory entries >= 10, total cache >= 5
```

---

## 11. Design Decisions & Tradeoffs

### 11.1 NodeDriver as Separate Module (not in node binary)

**Choice**: `driver.rs` lives in `crates/distribution/`, not in `crates/node/`.

**Why**: The driver is reusable — any binary that wants to run a DistributedNode over TCP can use it. The node binary (`crates/node/`) is one consumer; future consumers might embed distribution in a larger application. Keeping the driver in the distribution crate means it stays testable alongside the protocol logic.

### 11.2 Direct serde_json vs. Codec Trait

**Choice**: NodeDriver uses `serde_json::to_vec()`/`serde_json::from_slice()` directly, not the `Codec<M>` trait or `CodecRegistry`.

**Why**: The `Codec<M>` trait is parametric — `JsonCodec` implements `Codec<Ping>`, `Codec<Ack>`, etc. as separate trait impls. You can't write generic code like `codec.encode(any_message)` because each message type is a different impl. The CodecRegistry solves this on the send side via type erasure (`TypeId → encoder`), but it requires `Box<dyn Any>` downcasting which adds complexity for no benefit here — the driver already knows the concrete message type at each call site.

Using `serde_json` directly is simpler and equivalent — the JsonCodec just calls `serde_json` internally. When the codec is eventually swapped to bincode/msgpack, the driver can switch to the new serializer just as easily.

### 11.3 Static IPs over DNS

**Choice**: Docker Compose services use static IPs (`10.0.1.10`–`10.0.1.14`) rather than Docker DNS names.

**Why**: The SWIM protocol routes by `SocketAddr`, not hostname. Using DNS would require DNS resolution at startup plus a hostname→addr mapping. Static IPs are simpler and deterministic. The subnet `10.0.1.0/24` is a private range unlikely to conflict with host networking.

**Tradeoff**: Less flexible — adding a 6th node requires editing the compose file with a new static IP. Acceptable for a fixed test cluster.

### 11.4 `#[ignore]` Tests over Separate Test Target

**Choice**: Docker tests use `#[test] #[ignore]` rather than a separate binary or integration test feature flag.

**Why**: Standard Rust convention. `cargo test` skips them by default; `cargo test -- --ignored` runs them. No extra CI configuration needed. The test crate is already in its own workspace member (`tests/docker/`), providing isolation.

### 11.5 Host Networking for LAN Cluster

**Choice**: LAN compose files use `network_mode: host` instead of Docker bridge networking.

**Why**: Bridge networking with port forwarding would work for single-machine tests but not for cross-machine communication — a container on machine A needs to reach a container on machine B at its real LAN IP. With host networking, containers bind directly to the host's interface and are reachable at the host's LAN address. This requires unique ports per container on each host (7000, 7001, ... instead of all using 7000).

### 11.6 Build-Once via `std::sync::Once`

**Choice**: Docker images are built once per test run using `std::sync::Once`, then reused across all 4 tests.

**Why**: Each `docker compose up --build` triggers a full Rust release build inside Docker (~40s on hpz, ~50s on thinkpad). With 4 serial tests, that's 8 redundant builds. Separating `docker compose build` (guarded by `Once`) from `docker compose up -d` (per-test) cuts total runtime from ~840s to ~630s. The first test pays the build cost; tests 2–4 just start pre-built containers.

### 11.7 100ms Tick Loop over Async Runtime

**Choice**: The node binary uses a synchronous 100ms `thread::sleep` loop, not tokio/async-std.

**Why**: The entire distribution layer is synchronous (`DistributedNode` is `!Send`). Introducing an async runtime adds complexity with no benefit — the tick loop is CPU-light (one tick processes a handful of messages) and the 100ms sleep provides natural backpressure. The TCP transport uses non-blocking I/O for the acceptor and blocking I/O with connection pooling for outgoing sends.

### 11.8 `#[serde(default)]` for Backward Compatibility

**Choice**: New `piggyback` fields use `#[serde(default)]` so missing fields deserialize as empty `Vec<u8>`.

**Why**: If a node running old code (without piggyback) sends a Ping to a node running new code, the message should still deserialize successfully. The new node sees an empty piggyback — no gossip propagated, but no crash either. This matters during rolling upgrades.

---

## 12. Bugs Encountered

### 12.1 Compose File Path Doubling

**Symptom**: `cargo test -p docker-tests -- --ignored` failed with:
```
open tests/docker/tests/docker/docker-compose.yml: no such file or directory
```

**Cause**: The compose file path was defined as a relative constant:
```rust
const COMPOSE_FILE: &str = "tests/docker/docker-compose.yml";
```
But `cargo test` runs with the crate root as working directory. Since the crate root is already `tests/docker/`, the resolved path became `tests/docker/tests/docker/docker-compose.yml` — doubled.

**Fix**: Replaced the relative constant with `env!("CARGO_MANIFEST_DIR")`:
```rust
const COMPOSE_DIR: &str = env!("CARGO_MANIFEST_DIR");

fn compose_file() -> String {
    let mut p = PathBuf::from(COMPOSE_DIR);
    p.push("docker-compose.yml");
    p.to_string_lossy().into_owned()
}
```

This compiles the crate's absolute filesystem path into the binary, so the compose file is always found regardless of working directory.

### 12.2 Codec Trait Parametric Mismatch

**Symptom**: First version of `driver.rs` attempted:
```rust
self.codec.encode(&msg)  // where codec: JsonCodec
```

Compilation failed because `JsonCodec` implements `Codec<Ping>`, `Codec<Ack>`, etc. as separate trait impls. A single `codec` variable can't be used generically across all message types without trait object gymnastics.

**Fix**: Bypassed the Codec trait entirely. Used `serde_json::to_vec()` and `serde_json::from_slice()` directly. The driver knows the concrete type at each match arm, so generic dispatch isn't needed.

### 12.3 Stale TCP Connection Pool on Node Rejoin

**Symptom**: The `lan_killed_remote_node_rejoins` test failed — the restarted node's dashboard responded (it was running) but reported `alive_count=0`. The node never received a `JoinResponse` from the seed.

**Cause**: `TcpTransport` maintains a connection pool keyed by `SocketAddr`. When node-3 was killed (container stopped), the seed's pool still held a TCP connection to `192.168.1.102:7000`. When node-3 restarted and sent a `JoinRequest`, the seed generated a `JoinResponse` and called `send_to(192.168.1.102:7000, ...)`. The pool returned the stale connection — `try_clone()` succeeded (the FD was still valid), but `write_all()` silently failed or the data went into a dead socket. The `JoinResponse` was never delivered.

**Fix**: Added retry-on-write-failure logic to `TcpTransport::send_to()`:

```rust
pub fn send_to(&self, addr: SocketAddr, envelope: WireEnvelope) -> Result<(), Error> {
    let buf = encode_wire_envelope(&envelope);
    let mut stream = self.get_or_connect(addr)?;
    match stream.write_all(&buf) {
        Ok(()) => Ok(()),
        Err(_) => {
            // Evict stale connection and retry once
            self.pool.lock().unwrap().remove(&addr);
            let mut stream = self.get_or_connect(addr)?;
            stream
                .write_all(&buf)
                .map_err(|e| Error::from(format!("TCP send to {addr}: {e}")))
        }
    }
}
```

On write failure, the stale entry is evicted and a fresh connection is established. This handles the common case of a peer that died and came back at the same address. The retry is limited to one attempt — if the second write also fails, the error propagates.

**Impact**: This bug only manifests in kill/restart scenarios where a node returns at the same `SocketAddr`. It would not appear in simulation tests (no real TCP) or in the single-machine bridge cluster (Docker assigns new IPs on restart). It required real LAN testing with `network_mode: host` to surface.

---

## 13. Known Gaps & Future Improvements

| Gap | Effort | Impact | Notes |
|-----|--------|--------|-------|
| Kademlia messages not wired in driver | Medium | High | NodeDriver only handles SWIM messages. FindNode/FindValue/Store RPCs are not sent or received. Full Kademlia lookup requires this. |
| No graceful shutdown protocol | Small | Medium | Node binary calls `driver.node().leave()` but doesn't drain in-flight messages or wait for death dissemination |
| Heartbeat actors are fire-and-forget | Small | Low | HeartbeatActor never responds; actor liveness isn't verified |
| No health check in Docker | Small | Medium | Compose could use `HEALTHCHECK` to avoid `--wait` fallback path |
| No TLS | Medium | Medium | All TCP traffic is plaintext. Fine for a test cluster on a private network; not suitable for production |
| No resource limits | Small | Low | Docker containers have no memory/CPU limits; could OOM on constrained hosts |
| No partition testing | Medium | High | Docker supports `iptables`-based network partitions but no test exercises split-brain scenarios yet |

---

## 14. Test Coverage Summary

### Existing Tests — Unchanged

All 182 existing tests continue to pass:
- 134 distribution crate tests (133 original + 1 from updated constructors)
- 47 simulation tests
- 1 swactor core test

### Docker Integration Tests — 4 Single-Machine Scenarios

| Test | Mirrors Simulation | Asserts |
|------|-------------------|---------|
| `cluster_of_five_converges` | `distribution_sim::cluster_of_five_converges` | alive_count >= 4, routing_table_size >= 3 |
| `node_death_is_detected` | `distribution_sim::node_death_is_detected` | alive_count <= 4, dead_count >= 1 |
| `killed_node_rejoins` | `distribution_sim::killed_node_rejoins` | alive_count >= 1 after restart |
| `actors_resolvable_across_cluster` | `distribution_sim::actors_resolvable_across_cluster` | directory_entry_count >= 2, total >= 10, cache >= 5 |

Run with: `cargo test -p docker-tests -- --ignored cluster --test-threads=1`

### LAN Integration Tests — 4 Cross-Machine Scenarios

| Test | Asserts |
|------|---------|
| `lan_cluster_converges` | 5 nodes across 2 machines: alive_count >= 4, routing_table_size >= 3 |
| `lan_remote_node_death_detected` | Kill node on thinkpad: survivors see alive_count <= 4, dead_count >= 1 |
| `lan_killed_remote_node_rejoins` | Restart killed node: rejoins with alive_count >= 1 |
| `lan_actors_resolvable_cross_machine` | directory_entry_count >= 2 per node, total >= 10, cache >= 5 |

Run with: `cargo test -p docker-tests -- --ignored lan_ --test-threads=1`

All convergence timeouts are 30 seconds. Convergence happens in seconds over the LAN; 30s is a generous safety margin that still catches real failures quickly.

### Verification

- `cargo check --workspace` — clean
- `cargo test` — all 182 tests pass
- `cargo build --release -p node` — node binary builds
- Local 2-node TCP smoke test — nodes discover each other, dashboard returns valid JSON
- Single-machine Docker tests: 4/4 pass (on thinkpad)
- LAN Docker tests: 4/4 pass (hpz + thinkpad, ~630s total)

---

## Files Created/Modified

| Action | File | Purpose |
|--------|------|---------|
| Modified | `crates/distribution/src/messages.rs` | Added piggyback + from_addr fields |
| Created | `crates/distribution/src/driver.rs` | NodeDriver (TCP ↔ NodeAction bridge) |
| Modified | `crates/distribution/src/lib.rs` | Added `pub mod driver` |
| Modified | `crates/distribution/src/transport.rs` | Stale connection retry in `send_to()` |
| Modified | `crates/distribution/tests/transport_and_codec.rs` | Updated Ping constructors |
| Modified | `crates/runtime-dashboard/src/server.rs` | Added `/api/distribution` route |
| Created | `crates/node/Cargo.toml` | Node binary crate config |
| Created | `crates/node/src/main.rs` | swactor-node CLI binary |
| Modified | `Cargo.toml` (root) | Added `crates/node`, `tests/docker` to workspace |
| Created | `Dockerfile` | Multi-stage Docker build |
| Created | `tests/docker/Cargo.toml` | Docker tests crate config |
| Created | `tests/docker/docker-compose.yml` | 5-node single-machine cluster |
| Created | `tests/docker/docker-compose.lan-hpz.yml` | LAN cluster — hpz side (2 nodes) |
| Created | `tests/docker/docker-compose.lan-thinkpad.yml` | LAN cluster — thinkpad side (3 nodes) |
| Created | `tests/docker/run-lan-cluster.sh` | LAN cluster orchestration script |
| Created | `tests/docker/src/lib.rs` | Test utilities (ClusterHandle, LanClusterHandle, build-once) |
| Created | `tests/docker/tests/cluster.rs` | 4 single-machine integration tests |
| Created | `tests/docker/tests/lan_cluster.rs` | 4 cross-machine LAN integration tests |
