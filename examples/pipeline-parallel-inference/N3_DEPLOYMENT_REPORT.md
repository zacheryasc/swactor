# N=3 vast.ai deployment — investigation report

Session date: 2026-05-20. Branch: `ds-inference`.

## TL;DR

After three live runs against vast.ai, the original "deployment hangs at SWIM
convergence" failure decomposes into **three independent bugs stacked**:

| Layer | What it is | Status |
|---|---|---|
| A | iroh 0.96's `RelayMode::Default` routes through n0's experimental canary cluster, which buffers SWIM gossip for 100+ seconds | **fixed** in-session by running our own iroh-relay on the VPS |
| B | Our SWIM impl flaps via gossip even when probes succeed; self-incarnation runs away (228 self-refutes in 7 min); names never propagate; one peer ends `Dead` despite being healthy | **open bug**, fix path discussed below |
| C | The `pp_tinygrad_worker.py` (or pp-gpu-node monitoring it) crashes ~50-200s into the stage's life with the real worker; never captured the actual exit reason | **separate open bug**, blocked on diagnostic visibility |

Stub mode (`PP_WORKER_STUB=1`) bypasses Layer C. With own-relay + stub, stages
stay alive the full 7 min — proving stage death is the worker, not the cluster.
The cluster *still* fails to resolve `pp-entry` because of Layer B.

## Where we started

- Branch `ds-inference` had a runbook (`VASTAI_STATUS.md`) listing 8 prior failed
  attempts at N≥3 on vast.ai.
- Diagnostics scaffolding was already in place: `swactor-diag-collector`
  (HTTP+UDP receiver), per-node introspection, post-processor.
- Tests were green and the collector binaries built cleanly. The runbook's open
  questions all lived at the iroh / SWIM layer.

## What we ran

### Step 0 — collector on the VPS

`swactor-diag-collector` static-musl build → `scp docean:~/` →
`nohup … --bind 0.0.0.0:9080 --root /var/lib/swactor-diag --udp 0.0.0.0:9081`.
Opened UFW 9080/tcp, 9081/udp. Verified end-to-end: HTTP 404 on `/`, UDP echo
returns 15B.

### Run #1 — canary relay (baseline)

```
SWACTOR_DIAG_RUN_ID=vastai-N3-1
SWACTOR_DIAG_COLLECTOR_URL=http://146.190.110.128:9080
SWACTOR_DIAG_UDP_ECHO=146.190.110.128:9081
# no custom relay → RelayMode::Default
```

Result: `failed to resolve pp-entry` after the 300s resolve deadline; total run
425s.

Critical signal from the bundle's timeline:

```
t=556598  orch sends SWIM Ack to stage-0  (9870 B over relay)
t=741287  orch's last successful Ping/Ack with stage-0
t=743981  stage-0 finally receives 4 backlogged pings  (187 seconds late)
```

The canary relay (`euc1-1.relay.n0.iroh-canary.iroh.link.`) was buffering
SWIM messages for **187 seconds**. SWIM probe_timeout is 15s — the cluster
fell apart inside the first probe round.

Mechanism check: in iroh-0.96, `RelayMode::Default` invokes
`prod::default_relay_map()`. That function literally returns the canary URLs
(`crates/distribution/.../iroh-0.96.1/src/defaults.rs:30`). There is no
"production" iroh relay cluster in this version — `prod` and `staging` are
two named-but-equally-experimental n0 deployments. Setting
`IROH_FORCE_STAGING_RELAYS=1` would only swap us to a different experimental
cluster, not a production one.

### Mid-session fix — bring our own relay

Built a standalone iroh-relay around `iroh_relay::server::Server::spawn`:

- New binary `crates/distribution/src/bin/swactor-iroh-relay.rs`.
- Extended the existing `relay` Cargo feature to pull `tokio/macros` +
  `tokio/signal` (needed by the bin's tokio runtime).
- Added `[[bin]]` entry with `required-features = ["relay"]`.

Built static-musl, deployed to docean: `nohup … --bind 0.0.0.0:7843
--public-host 146.190.110.128`. UFW 7843/tcp opened. Verified
`http://146.190.110.128:7843/` returns the `<h1>Iroh Relay</h1>` landing page.

Plumbed an env-driven relay override through the stack:

- `examples/pipeline-parallel-inference/src/relay_config.rs` —
  `relay_mode_from_env()` returns `RelayMode::Custom(url)` when
  `SWACTOR_IROH_RELAY_URL` is set, else `RelayMode::Default`.
- `pp_smoke_run.rs`, `pp_gpu_node.rs` — both binaries call
  `relay_mode_from_env()` instead of hard-coding `RelayMode::Default`.
- `vastai::DiagEnv` — added `iroh_relay_url: Option<String>` field, populated
  by `DiagEnv::from_process_env()`.
- `vastai::create_instance` — injects `SWACTOR_IROH_RELAY_URL` into every
  rented container's env payload.

### Run #2 — own relay, real worker

Same env as run #1 plus `SWACTOR_IROH_RELAY_URL=http://146.190.110.128:7843/`.
run_id `vastai-N3-2`, duration 412s. Same end state: `failed to resolve
pp-entry`.

But the bundle's metrics tell a different story:

|             | Run #1 (canary) | Run #2 (own relay) | Run #3 (own relay + stub) |
|---|---|---|---|
| ConnectionCacheHit  | 70  | 62  | **2783** |
| ConnectionCacheMiss | 24  | 16  | 5 |
| DialStarted         | 67  | 43  | 8 |
| MessageSent         | 77  | 69  | **2790** |
| SwimTransition      | 33  | 28  | 1027 |
| Orchestrator events | 442 | 417 | 2936 |
| stage-0 events      | 324 | 91  | **4141** |
| stage-0 lifetime    | run | **75s** | **full run** |

Run #2 still showed the connect-timeout pattern. The orch's timeline to
stage-0 has clean traffic for ~47s, then `ConnectionCacheInvalidated
reason=connection-closed`, then three 10s redial timeouts, then permanent loss.

### The stage-death finding

Per-node event timespans on run #2:

```
orchestrator   411s (full run)
stage-0         75s
stage-1        191s
stage-2         51s
```

Background diagnostic threads (`clock_sample`, `udp_echo`) keep emitting
regardless of iroh state. When they also stop, the process is gone. So stages
**were dying mid-run**, not just losing connectivity. The orchestrator's
"connect timeout" was failing because there was nothing on the other end.

We're 8+ attempts in and never caught this before, because:

- `register_name` doesn't emit a diag event — there's no way to tell from the
  bundle whether stage-0 ever registered `pp-entry`.
- `StageActorStatus::ProcessExited` doesn't emit one either — so when the
  Python worker dies and pp-gpu-node exits with status 1, the only record is in
  the vast.ai container stdout, which we destroy along with the instance.
- The local name table isn't included in snapshots (`body.swim` has membership
  + recent messages but not the registry).

### Run #3 — stub mode

To separate worker-crash from cluster bugs, plumbed `PP_WORKER_STUB` (plus
`PYTHON`, `MODEL`, `CUDA`, `MAX_TOKENS`) through `create_instance` so the
orchestrator's env passes through to every rented container.

Result with `PP_WORKER_STUB=1`:

- All four nodes alive the full 430s.
- 2783 ConnectionCacheHits, **8 total DialStarted across the whole run** (vs
  67 in run #1).
- Cluster *still* never resolves `pp-entry`.
- Orch's `self_incarnation` ends at **228** — meaning the orch refuted Suspect
  claims about itself 228 times in 7 minutes.
- One peer (`c0a261b2`) ends `state=Dead` in the orchestrator's view at run end
  despite all instances being demonstrably alive (verified by SSH).

Mid-run SSH into stage-0 confirmed both `pp-gpu-node` (PID 345) and the python
worker (PID 402) were running and stage-0's stdout had thousands of `iroh
driver: received N message(s)` lines interleaved with
`SWIM: alive d11cc185` / `SWIM: suspect d11cc185` — i.e., the stage was
constantly flipping the orchestrator's status.

## Discussion: how to fix SWIM

The summary.md from run #2 caught the smoking gun:

```
First peer to go Dead:
  stage-2 marked stage-1 Dead at t=…
  reason: "suspicion-timeout"
  observer side (stage-2): conn_type=Relay
  peer side (stage-1): conn_type=unknown
  observer probes_ok_at_transition=yes
  peer probes_ok_at_transition=yes
```

Probes succeeded on both sides. Yet stage-2 marked stage-1 Dead. The
transition reason for most other state changes was `gossip` — meaning a third
party told us a peer was Suspect.

Three sub-issues to address, roughly independent in difficulty:

### B1 — the gossip flap loop (the real bug)

Standard SWIM rule: "highest incarnation wins". When peer A claims
`Z=Suspect(incarn=10)` and peer B claims `Z=Alive(incarn=11)`, every receiver
should accept Alive(11) and discard Suspect(10). Z's own refute should bump
incarnation past any stale Suspect within one gossip round.

Our self-incarnation reaching 228 in 420 seconds means roughly one refute every
1.8 seconds. That's far above the probe interval. Either:

- The refute bump isn't being broadcast fast enough to outpace the next gossip
  round, or
- The receiver-side incarnation comparison isn't strictly "newer wins"
  (off-by-one, or accepts equal-and-Suspect over Alive), or
- Suspect/Dead gossip is being generated by peers who *themselves* haven't yet
  seen the latest incarnation, and our impl doesn't suppress that.

Next step: pick one Suspect→Alive→Suspect cycle in the run #3 timeline,
read `crates/distribution/src/swim/{node,probe}.rs` against it, identify
which branch of the gossip-receive code is mis-firing.

### B2 — SWIM message bloat

In run #1, individual SWIM Ack messages were **9.8 KB**, Pings up to 7.5 KB.
That's because membership gossip piggybacks on every probe. With our N=4
cluster and substantial name-table state, the payloads grow into the multi-KB
range.

Big payloads ⇒ head-of-line blocking on relay ⇒ probe latency spikes ⇒ probe
acks miss the timeout window ⇒ Suspect.

Fix: split gossip into its own periodic burst (or piggyback only a bounded
slice). Smaller secondary issue but it amplifies B1.

### B3 — timeouts vs WAN reality

`probe_timeout=15`, `suspicion_timeout=60` are LAN-tuned. Across regions with
relay routing, p99 RTT can spike to 2-3s under load. The probe budget is fine
in normal weather but tight under bursts.

**But** the summary explicitly says `probes_ok_at_transition: yes` — pings ARE
getting acked. The deaths are gossip-driven, not probe-driven. So this is the
*least* important of the three; fix B1 first.

## Discussion: catching this in test, not production

This loop cost ~$2 of vast.ai GPU rental and ~90 minutes of engineering time.
Almost none of the test value required real GPUs or a real vast.ai roundtrip —
it was a pure SWIM problem. Test-side priorities, cheap to expensive:

### A. In-process SWIM simulator with injectable network params

Run N SWIM cores in a single test process. Mock transport queues messages with
configurable latency, jitter, and loss. Property assertions like:

- *"With 200ms ± 50ms latency + 5% packet loss, a 3-node cluster reaches
  all-Alive within 30 seconds and stays Alive for 5 minutes."*
- *"After a 10-second partition + heal, name registrations re-replicate to
  all peers within 30 seconds."*
- *"Self-incarnation never exceeds N + (failures observed) in a steady-state
  cluster."*

`<1 second per iteration`. The repo already has
`crates/distribution/src/swim/` as a unit — likely just needs a sim harness
plus property tests. **Would have caught our exact bug.** Highest leverage
single thing we can build.

### B. Docker-compose harness with `tc netem`

Three containers on the laptop, real iroh + real relay over loopback,
`tc qdisc add dev eth0 root netem delay 100ms 30ms loss 1%` per container.
End-to-end including the relay protocol. ~30 seconds per iteration; good for
CI nightly. Complements A — A catches logical bugs, B catches integration
issues.

### C. Stage-side diagnostic emission gaps

Three small additions (<100 lines total) that would have cut today's debug
loop in half:

1. Emit a `Custom("register_name")` event whenever `register_name` is called,
   carrying `(name, addr, peer_node_id)`.
2. Include the local name table in each snapshot (currently `body.swim` has
   membership + recent messages but no `name → addr` mapping).
3. Emit a `Custom("worker_exited")` event with status / signal **before**
   `std::process::exit(1)` in `wait_for_worker_ready` and friends.

Run #1's investigation would have ended in 2 minutes instead of 90.

### D. `pp-shell <run_id> <stage_idx>` helper

A one-liner CLI that uses the run_id to query collector metadata, finds the
matching vast.ai instance from contract IDs, and SSHes in with pp-gpu-node's
stderr piped to the local terminal. We did this manually with `curl + python +
ssh`; bundling it saves 5 minutes every time anyone wants to look at a live
stage.

## Potential next steps

Ordered by leverage / cost. Picking 1–3 is probably enough to unblock
real N≥3 deployment.

1. **Build option A (in-process SWIM simulator + property tests).** Catches B1
   immediately and is reusable for every future regression. Needs the SWIM
   core to be transport-agnostic — verify by reading
   `crates/distribution/src/swim/`; refactor if needed.

2. **Ship option C (three diagnostic-emission additions).** Cheap and
   compounds. Every future live debug benefits. Worth doing *before*
   investigating Layer C so we can capture what kills the worker.

3. **Fix B1 (the gossip flap).** With the simulator in place, develop
   test-first: write the property test that captures the observed pathology,
   then change SWIM until it passes. Reading `swim/node.rs` and
   `swim/probe.rs` is the entry point.

4. **Investigate Layer C (tinygrad worker crashes).** Requires step 2 OR a
   live SSH-in during a fresh real-worker run. The crash is most likely in
   model loading — `pp_tinygrad_worker.py` probably wants a `MODEL` env it's
   not getting, or tinygrad's CUDA backend is failing on the rented GPU.

5. **Option B (docker-compose harness) + option D (pp-shell helper).** Nice to
   have once we're back to spending time on live-cluster work.

## Artifacts produced this session

Uncommitted changes on `ds-inference`:

- `crates/distribution/src/bin/swactor-iroh-relay.rs` — new standalone relay
  binary.
- `crates/distribution/Cargo.toml` — extended `relay` feature with tokio
  macros/signal; added `[[bin]] swactor-iroh-relay`.
- `examples/pipeline-parallel-inference/src/relay_config.rs` — new module,
  `relay_mode_from_env()`.
- `examples/pipeline-parallel-inference/src/lib.rs` — exposed `relay_config`.
- `examples/pipeline-parallel-inference/src/bin/pp_smoke_run.rs` — uses
  `relay_mode_from_env()` instead of hard-coded `RelayMode::Default`.
- `examples/pipeline-parallel-inference/src/bin/pp_gpu_node.rs` — same, with
  precedence over the prior `seed_relay_env` heuristic.
- `examples/pipeline-parallel-inference/src/vastai.rs` —
  `DiagEnv.iroh_relay_url` field, `is_enabled()` updated, env passthrough for
  `PP_WORKER_STUB` / `PYTHON` / `MODEL` / `CUDA` / `MAX_TOKENS` in
  `create_instance`, and relay-URL injection.
- `examples/pipeline-parallel-inference/tests/t_vastai.rs` — updated
  `DiagEnv` struct literal for the new field.

Bundles on docean (`/var/lib/swactor-diag/bundles/`):

- `vastai-N3-1.tar.gz` — canary baseline, real worker.
- `vastai-N3-2.tar.gz` — own relay, real worker (stages die at 51–191s).
- `vastai-N3-stub.tar.gz` — own relay, stub worker (stages live full run; SWIM
  still fails to settle).

Local extracted bundles:

- `/tmp/bundle.out` (run #1), `/tmp/bundle_v2.out` (run #2),
  `/tmp/bundle_stub.out` (run #3).

## Infrastructure state at end of session

- **docean (146.190.110.128)** running:
  - `swactor-diag-collector` on :9080/tcp + :9081/udp.
  - `swactor-iroh-relay` on :7843/tcp (advertised
    `http://146.190.110.128:7843/`).
  - Both processes started under `nohup`, logs at
    `/var/log/swactor-diag-collector.log` and
    `/var/log/swactor-iroh-relay.log`.
- **vast.ai**: no instances running; all destroyed at end of each run.
- **Local docker image**: `zacheryasc/swactor-pp-gpu:latest` (sha256:9d2cd3…)
  contains the most recent pp binaries with env passthrough + custom relay
  support. Pushed to Docker Hub.
