# Vast.ai N≥3 Status Report

This document captures what is currently working, what is failing, and
what we don't yet understand about running pipeline-parallel inference
on vast.ai at `N ≥ 3`. It is a hand-off for a follow-up design session
on hardening the iroh / SWIM layer; do not treat any of the
recommendations below as decided.

## Scope

Goal of the session was to get `pp-smoke-run --vastai --num-stages 3`
end-to-end on RTX 4090 with `llama3.2:1b`, then `--num-stages 5`. We did
not reach a passing run; the session ended after 8 vastai attempts.

## What is working

Local stack (all green):

- `cargo test --release --test t_binary binary_e2e_n_stub_workers_returns_response -- --ignored` at `N ∈ {2, 3, 5}`.
- In-process `tests/t_integration.rs::n_stage_stub_pipeline_returns_response[N]` at `N ∈ {2, 3, 4, 5}`.
- `scripts/docker-e2e.sh 3` and `scripts/docker-e2e.sh 5` (stub mode, debian:trixie-slim base).

Vastai pieces:

- Image `zacheryasc/swactor-pp-gpu:latest` pulled on every host we tried (2.59 GB, down from 7.55 GB).
- Three instances reach `actual_status=running` within ~60 s when we pick median-priced offers (see "Diagnosed and fixed" below).
- Initial SWIM convergence happens: every rented stage dials the orchestrator's seed, the orchestrator accepts the connection, and the orchestrator observes `alive` for all `N` stages.
- The orchestrator registers `pp-orchestrator` in the cluster registry.
- `pp-smoke-run` destroys every rented contract on every exit path we exercised, including hard-kill via SIGTERM (one earlier attempt leaked 3 instances when killed during the polling loop; the cause was the kill-vs-destroy race, not a code path that omits destroy).

## Diagnosed and fixed during the session

The following changes were applied to the source tree (not yet committed):

1. **Relay-url gossip via SWIM metadata** (`src/bin/pp_gpu_node.rs`, `src/bin/pp_smoke_run.rs`)
   - Each node now calls `driver.node_mut().set_relay_url(...)` after iroh learns its home relay, so the metadata disseminator carries relay URLs to every other node.
   - `pp_gpu_node::build_route` now enriches the per-route `EndpointAddr` with the peer's relay URL (from SWIM metadata, falling back to our own home relay). Same in `pp-smoke-run`'s stage-0 route.
   - Rationale: at `N=2` the orchestrator-stage probes happen to populate iroh's NodeMap as a side effect of the SWIM handshake; at `N ≥ 3` pairs like Stage-0↔Stage-2 (the autoregressive feedback edge) may never probe each other in time, so the actor transport must carry relay info explicitly.

2. **Race-tolerant lease chain** (`src/vastai.rs`)
   - `lease_chain` is now find-and-create per stage, with up to 5 attempts per stage. On `create_instance HTTP 400 no_such_ask` (offer snatched between selection and creation) we exclude that offer and try the next-best one.
   - Rationale: tightening filters shrinks the candidate pool; with N=3 sequential creates the third offer was repeatedly stolen by other renters.

3. **Median-priced offer selection** (`src/vastai.rs`)
   - `find_offer` now sorts the filtered candidate set by `dph_total` ascending and returns the median element instead of the cheapest.
   - Rationale: the cheapest RTX 4090 offers consistently returned `failed to inject CDI devices: unresolvable CDI devices D.<sha>/gpu=N: unknown` at container start. The reliability score (`reliability2 ≥ 0.99`, later `≥ 0.995`) doesn't reflect this because vast.ai may not count container-init failures as "completed jobs." Median pricing skips the broken cheap tier without paying for the most expensive host in the set.

4. **Filter tightening** (`src/vastai.rs`)
   - `reliability2 ≥ 0.995` (was `≥ 0.99`).
   - `cuda_max_good ≥ 12.6` (was `≥ 12.0`, now matches our CUDA 12.6 base).
   - Attempted `driver_version ≥ "555.0"` but vast.ai's API rejects that field shape (HTTP 400 `query values can't be None`); reverted.

5. **Slimmer image** (`Dockerfile`)
   - Base switched from `nvidia/cuda:12.6.3-devel-ubuntu24.04` to `nvidia/cuda:12.6.3-runtime-ubuntu24.04`. Image went from 7.55 GB to 2.59 GB; tinygrad's NVRTC backend works on the runtime image.
   - Rationale: at 7.55 GB the image pull alone consumed >5 min on cheap hosts, blowing past the lease budget before the container could start.

6. **Stub image base** (`Dockerfile.stub`)
   - `debian:bookworm-slim` → `debian:trixie-slim`. Host built binaries with glibc 2.42; bookworm has 2.36 (binaries fail to load). Trixie has 2.41 (works against our binaries that need ≤ 2.39).

7. **Iroh dial retries** (`crates/distribution/src/iroh_driver.rs::get_or_connect`, `examples/pipeline-parallel-inference/src/iroh_transport.rs::IrohActorTransport::send`)
   - Per-attempt timeout bumped from 2 s to 10 s.
   - 3 attempts per dial with 200 ms / 600 ms backoff.
   - Rationale: 2 s is too tight for any WAN handshake that goes through the relay mesh; a single slow handshake was marking peers suspect→dead in one SWIM cycle.

8. **Per-contract poll cap** (`src/bin/pp_smoke_run.rs`)
   - `wait_for_running` now caps at 30 polls (5 min) per contract, down from 60 (10 min). A healthy host reaches `running` in 30–90 s; longer means a recoverable error and rollback is cheaper than waiting.

## What is still failing

After the 8th attempt (with all of the above in place):

1. Three RTX 4090 instances lease cleanly.
2. All three stages dial the orchestrator; orchestrator accepts inbound SWIM connections from all three.
3. Orchestrator's `await_convergence(3, ...)` succeeds.
4. Orchestrator registers `pp-orchestrator`.
5. Orchestrator begins resolving `pp-entry` and ticking SWIM probes outward to the stages.

Then **one specific peer becomes unreachable for outbound SWIM probes from the orchestrator**:

```
iroh driver: connect attempt 1/3 to 2b91108e failed: connect timeout
iroh driver: connect attempt 2/3 to 2b91108e failed: connect timeout
SWIM: suspect 2b91108e
SWIM: dead   2b91108e
```

The peer hash differs per run; the pattern is consistent. Other peers
flicker `suspect → alive` and stabilise. The pp-entry resolve never
completes within our 5-minute budget — most likely because the
unreachable peer happens to be stage 0 (the registrar of `pp-entry`),
though we have not confirmed which stage's hash is the dead one in any
given run.

## What we don't yet understand

These are the open questions that should drive the design session, not
the implementation:

- **Asymmetric routing**: the unreachable peer dialed *us* successfully
  earlier in the run, but our outbound dial *to* it consistently
  times out. We don't know whether this is iroh-canary mesh sub-paths,
  vast.ai host firewalls (some hosts may block UDP egress to relay
  endpoints), or a NodeMap-cache staleness in iroh 0.96.

- **`iroh-canary` vs. iroh production relays**: our orchestrator's home
  relay landed on `*.relay.n0.iroh-canary.iroh.link` in every run. iroh
  0.96's `RelayMode::Default` documentation says "n0 production relays"
  but the URL says canary. We don't know if 0.96 ships canary as
  default deliberately, if there's an override, or whether moving to
  production relays would change the failure mode at all.

- **Which stage is the dead one?** The hash we see in `SWIM: dead
  <prefix>` isn't tied back to a `STAGE=i` in our logs. We don't know
  if it's always stage 0, always last, or random. That distinction
  determines whether more retries / longer budgets could ever succeed:
  - dead = stage 0 → cannot resolve pp-entry, hopeless.
  - dead = middle → forward path may still work; orch only needs stage-0 and stage-N-1 reachability.
  - dead = last → cannot receive InferenceResponse, hopeless.

- **Whether SWIM cluster-size matters**. At `N=2` (orch + 2 stages = 3
  members) we have 3 pairs of edges (3-choose-2). At `N=3` we have 6.
  More edges → more chance one is broken. The session showed the
  failure but didn't establish whether `N=2` on the same host pool
  would also fail today, or whether `N=2` is robust by happenstance.

- **CDI error class**. The `D.<sha>/gpu=N` device class is non-standard
  (real NVIDIA CDI is `nvidia.com/gpu=N`). We treated this as host-side
  per-instance dynamic CDI generation by vast.ai. We did not confirm
  this with vast.ai support or by inspecting `/etc/cdi/` on a rented
  host. The median-priced filter is a workaround for the symptom, not
  a fix.

- **Connection lifecycle**. The iroh driver caches one
  `Connection` per `NodeId`. When a SWIM dial fails, we don't currently
  invalidate the cached connection or force iroh to re-probe a fresh
  address — the next dial just retries against the same cached state.
  Whether this is the right behaviour for canary-relay flapping is
  unclear.

## What we did **not** try (and why)

- **Wiring the dashboard into pp-gpu-node and using
  `POST /api/peers/add` as a brute-force SWIM-join trigger.** Plausible
  workaround but adds an HTTP server + ports per stage and a
  vast.ai-side port mapping; design impact dwarfs the iroh fix it would
  paper over. Should be considered in the design session only if the
  iroh layer cannot be fixed.

- **Different GPU type.** RTX 3090 / A40 / A6000 pools might have
  different host-runtime defaults, but we'd be guessing. Worth doing
  *after* we understand the canary-relay asymmetric-routing root
  cause, not before.

- **iroh production relays** (`RelayMode::Custom(<prod_url>)`). We
  considered this but didn't try it because we don't have a documented
  production relay URL handy and didn't want to guess.

- **Per-peer geo filtering** so the orchestrator and stages share a
  regional relay mesh. Lower-risk than the others, untried because of
  the 5-minute-per-attempt budget.

## Recommended data collection before the next session

To make the next session productive, capture on a fresh run:

1. The output of `driver.snapshot()` from each rented stage **at the
   point where the orchestrator marks one of them dead** (need to add a
   periodic snapshot dump in `pp-gpu-node`).
2. The mapping `<peer hex prefix> ↔ STAGE` for each run, written to
   stdout by `pp-gpu-node` on boot (currently only the full hex is
   printed; the orch logs prefixes).
3. Whether the unreachable peer's `home_relay_url` differs from the
   orchestrator's. This is the single most useful datum for proving
   the relay-mesh hypothesis.
4. A run of `single-gpu-inference --vastai` on the same day. If that
   passes, the failure is specific to multi-peer iroh; if it fails the
   same way, the failure is broader.
5. One run with `--num-stages 2` against the new code paths. If it
   passes consistently and `--num-stages 3` fails consistently, we
   have a clean signal that the mesh degrades with peer count.

None of the above costs more than a few minutes of vast.ai time and
all of them are cheap to add to the binaries.
