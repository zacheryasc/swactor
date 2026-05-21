# Simulator BLOCKED — Stage 6 / §10.2 / §4.4 contracts unfulfilled

This file is filed per TESTING_SPEC §14. The local v1 parity bar is
green for the surface this implementation chose to bind to, but
three load-bearing contracts named by the plan and TESTING_SPEC have
*not* been satisfied at full scope. The remaining work is not a v1
"out-of-scope" item under TESTING_SPEC §15, so the only honest
artifact to ship is this BLOCKED.md plus the qualified `BUILD_REPORT.md`
next to it.

Iteration 14 fixed one of the four findings (Finding D — facade
surface descriptor now derived from trait declarations by
`build.rs`); the other three remain open.

**Note on layout (added in path-refresh pass):** since this file was
first filed, the workspace consolidated several crates
(`runtime-facade`, `sim-detector`, `sim-driver`, `lint-deterministic`)
into `crates/simulation` (PRs #39, #50, #52). Paths below reflect the
post-consolidation layout. The underlying contracts and the substantive
gaps they call out are unchanged.

| Symbol                  | Was                                       | Now                                                |
|-------------------------|-------------------------------------------|----------------------------------------------------|
| Runtime facade traits   | `crates/runtime-facade/src/lib.rs`        | `crates/simulation/src/runtime/mod.rs`             |
| Surface-lock build      | `crates/runtime-facade/build.rs`          | `crates/simulation/build.rs`                       |
| Surface fingerprint     | `crates/runtime-facade/surface.lock`      | `crates/simulation/surface.lock`                   |
| Lint config             | `crates/lint-deterministic/banned.toml`   | `crates/simulation/banned.toml`                    |
| Lint scanner            | `crates/lint-deterministic/src/lib.rs`    | `crates/simulation/src/lint.rs`                    |
| Detector probes         | `crates/sim-detector/src/lib.rs`          | `crates/simulation/src/detector.rs`                |
| Sim binary entry        | `crates/sim-driver/src/main.rs`           | `crates/simulation/src/bin/sim-driver.rs`          |

---

## A. plan §"Stage 6 — TIER CHECKPOINT" — distribution migration

### Contract

> Migrate `crates/distribution` to the facade: every direct
> `Instant`, `SystemTime`, `tokio::spawn`, `tokio::time::sleep`,
> `rand::thread_rng`, `HashMap` iteration, `env::var`, `std::fs`,
> `std::net` use is rewritten against the runtime facade. Lint scope
> widens to include distribution; scan reports zero violations.

### What's actually happening

The lint scanner's audit scope (`crates/simulation/banned.toml::
[scope]::include`) is exactly one directory:

```
include = [
    "crates/simulation/src",
]
```

`crates/distribution` is not in scope. The crate still calls the
real runtime directly across at least these files:

- `crates/distribution/src/swim/member_list.rs`
- `crates/distribution/src/diagnostics/aggregator.rs`
- `crates/distribution/src/diagnostics/sink.rs`
- `crates/distribution/src/diagnostics/probes.rs`
- `crates/distribution/src/diagnostics/host_introspect.rs`
- `crates/distribution/src/diagnostics/collector/udp_echo.rs`
- `crates/distribution/src/diagnostics/collector/state.rs`
- `crates/distribution/src/diagnostics/collector/handlers.rs`
- `crates/distribution/src/cache.rs`
- `crates/distribution/src/iroh_driver.rs`
- `crates/distribution/src/peer_auth.rs`
- `crates/distribution/src/node_metadata.rs`
- `crates/distribution/src/kademlia/repair.rs`
- `crates/distribution/src/kademlia/directory.rs`

These call sites use `tokio::spawn`, `tokio::time::sleep`,
`std::time::Instant`, `std::time::SystemTime`, `rand::thread_rng`,
`std::net::UdpSocket`, and `std::collections::HashMap` directly.

Live-fire reproduces the gap: drop a `use std::time::SystemTime;`
into `crates/distribution/src/_violator_test.rs`; the lint scan
still exits 0, because that directory is not in scope.

### The problem, plainly

The whole point of the runtime facade is so that peer code becomes a
pure function of `(spec, seed)` — same inputs produce byte-identical
bundle outputs, in prod and in sim. Every direct `Instant::now()`,
`tokio::spawn`, or `HashMap` iteration in `distribution` breaks that
guarantee on the production diagnostics path: the diagnostics
aggregator's per-peer state lives in a `HashMap`, so its iteration
order varies run-to-run with hash randomisation. A replay can't
reproduce a recorded run if a `HashMap::iter` shows up between input
and output.

Until distribution is migrated, the "sim and prod are the same code
with a different runtime" claim only holds for the simulation crate
itself. The peer code that actually does the work is exempt.

### Potential action items

The migration is big and has a forced order. A reasonable plan:

1. **Grow the facade `Spawn` surface to be async-capable.**
   The current trait (`crates/simulation/src/runtime/mod.rs:91`)
   only takes `Box<dyn FnOnce() + Send + 'static>`. Distribution's
   diagnostics pipeline is built on `tokio::spawn(async move {...})`.
   Add `fn spawn_future(&self, fut: BoxFuture<'static, ()>)` (or
   equivalent), implement on `runtime::prod` with `tokio::spawn`
   and on `runtime::sim` against the engine's fiber queue.
   This is a `surface.lock` bump.

2. **Decide on the `HashMap` policy and apply it crate-wide.**
   Replace prod-path `HashMap` / `HashSet` with `IndexMap` /
   `IndexSet` (preserves insertion order, fast) or `BTreeMap` /
   `BTreeSet` (preserves sort order, slower but no extra dep).
   Aggregator, member-list, and kademlia routing-table state are
   the highest-leverage call sites — diagnostics aggregator first
   because it's the largest source of replay divergence.

3. **Migrate the call sites in waves.** Suggested order, smallest
   blast radius first:
   - `cache.rs` and `node_metadata.rs` (mostly time reads).
   - `peer_auth.rs` (time + RNG).
   - `swim/member_list.rs` (spawn + time + map iteration).
   - `kademlia/` (spawn + time + map iteration).
   - `diagnostics/*` (spawn + time + UDP + map iteration — biggest).
   - `iroh_driver.rs` (depends on Finding C's facade growth — defer).

4. **Widen lint scope.** Add `"crates/distribution/src"` to
   `banned.toml::[scope]::include` and fix the violations the
   scanner reports until it exits 0. Do this *last*, after the
   migration waves above land — otherwise the scanner blocks every
   intermediate commit.

5. **Add a live-fire regression test.** After (4) is green, drop a
   throwaway `use std::time::SystemTime;` somewhere in
   `distribution/src/` and confirm the scanner catches it; revert.
   Record the result in the iteration notes.

This is a multi-iteration project. Budget at least three.

---

## B. TESTING_SPEC §10.2 — detector must run on the sim facade

### Contract

> The detector binary must run inside the reference scenario as an
> additional peer.

### What's actually happening

`crates/simulation/src/facade/sim/bundle.rs:81` calls
`crate::detector::run_all()` synchronously from inside the bundle
writer. The detector code lives at
`crates/simulation/src/detector.rs` and every D01–D12 probe body
reaches for the real `std::*` / `tokio` surface
(`SystemTime::now()`, `Instant::now()`, `std::thread::spawn`,
`std::net::UdpSocket`, `std::net::TcpStream`,
`File::open("/dev/urandom")`, `gethostname`, `pid`, allocator
addresses, `ThreadId`). Each violation is silenced with an inline
`// lint-deterministic: allow …` marker.

The detector is therefore observing the host runtime, not the sim
runtime. `techniques.md` admits this verbatim for D01 ("The v1 sim
does not intercept `std::time::*`; the detector running in-process
therefore reads the host wall clock") and D02 ("v1 reads the host
directly, so the two deltas are microseconds apart in practice").

`t_detector::sim_indistinguishable` consequently passes whether or
not the sim leaks — there is no sim facade between the probe and
the host to leak through.

### The problem, plainly

NORTH_STAR's load-bearing claim is "no means, statistical or
otherwise, of detecting that it is in a sim." The test that's
supposed to enforce that claim is currently a no-op: the probes are
literally not pointed at the simulated runtime. It's the same as
running an integrity test against `cat` and concluding the data is
intact — there's no system under test.

The `lint-deterministic: allow` markers are load-bearing audit
evidence that this work was not done. Removing them without doing
the work would just move the failure from a semantic check to a
mechanical one.

### Potential action items

Two changes have to land together; doing only one of them is worse
than doing neither (it advertises a guarantee that isn't there).

1. **Make the sim backend actually intercept the std/tokio surface.**
   `crates/simulation/src/runtime/sim.rs` (133 lines) is mostly
   stubs. Concretely:
   - `Clock::now()` must return a virtual-time-derived
     `SystemTime`, not pass through to `SystemTime::now()`.
   - `Udp::bind` must return a socket whose `send_to` /
     `recv_from` ride the engine's link graph instead of OS
     sockets.
   - `Spawn` must enqueue onto the engine's fiber scheduler
     instead of `tokio::spawn`.
   - `ProcessMeta::hostname` / `pid` must return per-node sim
     values, not the host's.

2. **Route the detector probes through the facade.** Open
   `crates/simulation/src/detector.rs` and, for each probe:
   - Replace `SystemTime::now()` with `facade.clock().now()`.
   - Replace `Instant::now()` with `facade.clock().monotonic()`.
   - Replace `std::thread::spawn` with `facade.spawn().spawn(...)`.
   - Replace `std::net::UdpSocket::bind` with
     `facade.udp().bind(...)`.
   - Drop the `// lint-deterministic: allow …` markers as each
     site converts. By the end the file should be allow-marker-free.

3. **Run the detector as a real peer.** Add a `host` of kind
   `detector` to the reference scenario spec. In the bundle writer
   (`crates/simulation/src/facade/sim/bundle.rs:79–88`), delete the
   in-process `detector::run_all()` call. Instead, have the engine
   schedule a fiber on that host that runs the same probes via the
   sim facade and writes its verdict stream into the bundle just
   like any other host's records.

4. **Tighten the test.** Once the above is in, modify the sim
   facade in a controlled way to leak (e.g., have `Clock::now()`
   return wall-clock instead of virtual). The relevant probe
   verdict should flip from `Indistinguishable` to `DetectedSim`
   and `t_detector::sim_indistinguishable` should fail.

This depends on Finding A only for the `Spawn` async surface
growth, so it can begin in parallel with A's migration waves.

---

## C. plan §"Stage 6 — TIER CHECKPOINT" — real transports on the sim facade

### Contract

> Real `quinn` linked against sim UDP — no QUIC fork.
> Real iroh stack (MagicSock, discovery, NodeMap, connection cache)
> linked against sim facade — no iroh fork.

And TESTING_SPEC §5.2:

> The same `quinn` / `iroh` / `iroh-relay` / `distribution` /
> `swactor` source files run in both binaries.

### What's actually happening

`crates/simulation/src/engine.rs` (885 lines) is a discrete-event
record synthesiser. Its imports are `std::cmp::Ordering`,
`std::collections::{BTreeMap, BTreeSet, BinaryHeap}`,
`serde::{Deserialize, Serialize}`, and types from `crate::spec`.
`grep -n 'quinn\|iroh\|distribution\|swactor'
crates/simulation/src/engine.rs` returns zero matches.

The engine emits `HostStart` / `HostStop` / `HostCrash` /
`Snapshot` / `Mutation` events on a synthetic `loopback` host and
writes them to the bundle. The corpus is shaped to look like real
peer-code output, but no production peer code is in the call
graph.

(Update from the original BLOCKED.md: the `sim-driver` binary used
to pin `#[used] static` references to defeat dead-code stripping —
the `t_same_binary::symbol_overlap` test exists to detect that
gimmick. The pin gimmick was removed during the consolidation;
`symbol_overlap` is now listed as removed in
`tests/parity-bar/expected_failures.txt:33`. The deeper claim is
unchanged: peer code is not exercised through the sim engine.)

### The problem, plainly

§5.2's whole point is that there should be one implementation of
QUIC, one implementation of iroh, one implementation of
distribution — and the *only* difference between prod and sim is
which backend implements the runtime facade. If the sim binary
runs synthesised events that mimic peer-code output, then the sim
is not testing peer code — it's testing the simulator's model of
peer code, which is exactly the thing the parity bar is supposed
to make unnecessary.

This is why the dependency chain matters: real `quinn` needs an
async UDP socket, real `iroh` needs `quinn` plus a tokio runtime
and DNS, real `distribution` needs `iroh::Endpoint`. Each layer
sits on the one below it.

### Potential action items

This is the largest body of work in the BLOCKED set. A reasonable
sequencing:

1. **Async UDP on the facade.** Grow `traits::Udp` /
   `traits::UdpSocket` (`crates/simulation/src/runtime/mod.rs:64–
   73`) from the current sync `bind / send_to / recv_from` shape to
   something that implements `quinn::AsyncUdpSocket`. In prod this
   wraps `tokio::net::UdpSocket`; in sim it wraps a queue tied to
   the engine's link graph and clock. `surface.lock` bump.

2. **Virtual `tokio::time::Instant` projection.** iroh's internals
   call `tokio::time::Instant::now()` and `tokio::time::sleep`
   directly (it does not consult our facade). Either (a) build a
   sim-tokio runtime so `tokio::time` reads from the engine's
   virtual clock when iroh is linked into the sim binary, or
   (b) accept that iroh-as-shipped can't run under the sim and
   patch iroh upstream. (a) is the path TESTING_SPEC §5.2 implies.

3. **Async `Spawn`.** Same growth as Finding A (1); A and C share
   this prerequisite.

4. **DNS table in sim.** `traits::Dns`
   (`crates/simulation/src/runtime/mod.rs:85`) needs to answer
   relay-URL queries from a per-node DNS table the spec configures,
   not from the host resolver. iroh's relay discovery won't work
   otherwise.

5. **Build a `quinn::Endpoint` from facade pieces.** Once (1) and
   (2) are in, construct a `quinn::Endpoint` from the facade's UDP
   socket and feed it to iroh's `MagicSock` setup.

6. **Re-point `distribution`.** Wire `distribution`'s transport
   construction off direct `iroh::Endpoint::builder()` calls and
   onto the facade-built endpoint. (Finding A's migration is a
   precondition.)

7. **Replace synthetic engine emission with real peer-code
   execution.** This is the big one. `engine.rs` currently *is* the
   event source; under (6), real peer code becomes the event source
   and the engine becomes scheduling + link graph + clock. Most of
   `engine.rs`'s synthetic emit logic deletes; the schema-floor
   parity bars then bind to events real code produced.

8. **Re-enable the parity-bar checks currently in
   `expected_failures.txt`.** `t_replay::*`, the §6/§7 schema
   coverage suite, and `t_same_binary::shared_load_bearing` are
   parked behind Stage 6/7. They light up as the real-peer-code
   path comes online.

Expect this to span four to six iterations. Don't try to land it as
one PR.

---

## D. TESTING_SPEC §4.4 — facade surface lock — *fixed in iteration 14*

### Contract

> Each trait's method signatures hashed in declaration order.

### Resolution

Was: `SURFACE_DESCRIPTOR = include_str!("surface_descriptor.txt")`,
a 38-line hand-edited file. A trait edit that did not also touch
the text file passed the lock unchanged.

Now: `crates/simulation/build.rs` parses
`crates/simulation/src/runtime/mod.rs` with `syn` at build time,
walks `pub mod traits`, emits one line per trait method signature
into `$OUT_DIR/surface_descriptor.txt`, and the runtime module
embeds the build output via `include_str!`. Live-fire verified:
adding `fn stealth_method(&self) -> u32` to `pub trait Clock` flips
the descriptor hash and fails `live_fingerprint_matches_locked`.
Reverted.

This finding is closed. Listed here for the record because the
other three are open against the same TIER CHECKPOINT.

---

## What the next agent should do

Either:

1. Pick up the chain at A → B → C in order. A is the prerequisite
   for the lint widening; A's facade growth (async `Spawn`) and B's
   sim-facade interception together unlock C's real-transport
   linkage. Plan on multiple tier-checkpoint-sized iterations; this
   is not a single-iteration cleanup.

2. Raise a TESTING_SPEC defect under §14.2 if a specific check is
   itself wrong — e.g., if §10.2's "additional peer" wording proves
   impractical given the workspace constraint `no-modify src/` and
   the facade should instead intercept at the bundle-writer
   boundary. A defect would need to quote NORTH_STAR / SPEC /
   OBSERVABILITY and ship as a separate commit titled
   `TESTING_SPEC: correct §N.M` per §14.

What the next agent should *not* do is widen the
`lint-deterministic: allow` markers, leave the engine wholly
synthetic, or paper over symbol-presence checks. The bar in this
repo is what the spec says.
