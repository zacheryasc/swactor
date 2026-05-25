# vast.ai deployment test

Drives `pp-smoke-run --vastai` against N real GPU instances, with a
collector + iroh-relay on a separate VPS so the run's bundle survives
the instances' destruction. See `N3_DEPLOYMENT_REPORT.md` for the three
classes of bug this loop has historically caught.

## Pre-flight on the VPS

The collector and relay are long-lived on a separate VPS so they
outlive any single rental. The reference deployment is docean
(146.190.110.128). Verify both processes are up before any run:

```sh
ssh docean 'pgrep -fa swactor-diag-collector; pgrep -fa swactor-iroh-relay'
# expect one PID for each
```

If either is missing, rebuild static-musl and redeploy:

```sh
cargo build --release --target x86_64-unknown-linux-musl \
    -p distribution --features "collector relay" \
    --bin swactor-diag-collector --bin swactor-iroh-relay
scp target/x86_64-unknown-linux-musl/release/swactor-diag-{collector,iroh-relay} docean:~/
ssh docean '
  nohup ./swactor-diag-collector --bind 0.0.0.0:9080 --root /var/lib/swactor-diag \
        --udp 0.0.0.0:9081 > /var/log/swactor-diag-collector.log 2>&1 &
  nohup ./swactor-iroh-relay --bind 0.0.0.0:7843 \
        --public-host 146.190.110.128 > /var/log/swactor-iroh-relay.log 2>&1 &'
```

Firewall: `9080/tcp` (collector HTTP), `9081/udp` (echo probe),
`7843/tcp` (iroh-relay) all open. Sanity-check from your laptop:

```sh
curl -sS -o /dev/null -w '%{http_code}\n' http://146.190.110.128:9080/   # → 404 (port is bound)
curl -sS http://146.190.110.128:7843/ | grep -o 'Iroh Relay'             # → Iroh Relay
```

## Building the orchestrator + the GPU image

The orchestrator runs locally. The GPU image runs on the rentals.
Both must come from the same workspace commit so the iroh and SWIM
versions line up.

```sh
# Orchestrator-side binary (used as pp-smoke-run --vastai)
cargo build --release --bin pp-smoke-run

# GPU image — Dockerfile bundles pp-gpu-node + worker
cargo build --release --bin pp-gpu-node
docker build -t zacheryasc/swactor-pp-gpu:latest -f Dockerfile .
docker push zacheryasc/swactor-pp-gpu:latest
```

## Running the deployment test

The orchestrator passes the diagnostics + relay URLs into every rented
container's env via `vastai::create_instance`. Set the same vars the
local stages would see, then invoke `--vastai`:

```sh
RUN_ID="vastai-N3-$(date +%s)"

# Required: collector + relay so the cluster comes up at all and the
# bundle gets persisted (see N3 report Layer A).
export SWACTOR_DIAG_COLLECTOR_URL="http://146.190.110.128:9080"
export SWACTOR_DIAG_UDP_ECHO="146.190.110.128:9081"
export SWACTOR_IROH_RELAY_URL="http://146.190.110.128:7843/"
export SWACTOR_DIAG_RUN_ID="$RUN_ID"

# Optional: switch workers without rebuilding the image.
# Drop PP_WORKER_STUB=1 to exercise the real tinygrad path.
export PP_WORKER_STUB=1
# export MODEL=llama3.2:1b
# export CUDA=1
# export PYTHON=python3

target/release/pp-smoke-run --vastai \
    --api-key "$VAST_API_KEY" \
    --num-stages 3 \
    --gpu RTX_4090 \
    --image zacheryasc/swactor-pp-gpu:latest \
    --prompt "Diag check" \
    --max-tokens 4 \
    2>&1 | tee "$RUN_ID.log"
```

Three N≥2 invariants the run is checking:

1. Cluster converges within `pp-smoke-run`'s convergence deadline
   (every peer sees every other as `Alive`).
2. `pp-entry` resolves on the orchestrator (Layer B / name-gossip
   path).
3. The pipeline returns a non-empty `InferenceResponse`.

Failure of (1) or (2) without (3) → a SWIM or relay bug.
Failure of (3) only → a worker bug.

On any exit the orchestrator destroys every rented instance, so a
hung or crashed run does not leak GPUs. Verify after:

```sh
curl -s -H "Authorization: Bearer $VAST_API_KEY" \
    https://cloud.vast.ai/api/v0/instances/ | jq '.instances | length'
# → 0 (or only your own unrelated instances)
```

## Fetching the bundle from the VPS

The collector finalises the run-id tarball when it receives the
orchestrator's finalize record. It lives both in the collector's bind-
mounted dir and at the HTTP retrieval endpoint:

```sh
curl -fsSO "http://146.190.110.128:9080/diag/bundle/$RUN_ID"
# or, from the VPS itself:
ssh docean "ls -la /var/lib/swactor-diag/bundles/$RUN_ID.tar.gz"
```

## Post-processing + what to look for

```sh
target/release/swactor-diag-postproc "$RUN_ID.tar.gz" -o "$RUN_ID.out"
cat "$RUN_ID.out/summary.md"
```

### Healthy run

`summary.md` shows N+1 nodes (orchestrator + N stages), each with
`finalize_recorded: true` for the orchestrator and several snapshots
per stage. Custom event totals include `worker_starting` and
`worker_ready` for every stage and zero `SwimTransition → Dead`. The
"First peer to go Dead" section is empty.

### SWIM regression (Layer B)

`summary.md` lists peers transitioning to `Dead` despite probes
succeeding (`probes_ok_at_transition: yes` in the per-peer block).
Cross-check `self_incarnation` on the orchestrator snapshot —
anything above ~10 over a 7-minute run is the §10.3 flap (see
SWIM_TUNING_REPORT). Drill into the relevant timeline-NN-to-MM.tsv
for the message sequence around the transition.

### Relay regression (Layer A)

Per-peer reachability blocks show `conn_type=Relay` and probe RTTs
spiking into hundreds of ms or seconds. Confirm with
`Custom(iroh_api_missing)` and the iroh introspection block in the
last snapshot — relay-buffered messages show as huge `last_used_ms`
gaps. The mitigation is the own-relay setup above; running with
`SWACTOR_IROH_RELAY_URL` unset deliberately reproduces the canary
buffering for evidence-collection runs.

### Worker death (Layer C)

`summary.md` shows `Custom(worker_exited)` events. Pull the structured
fields:

```sh
jq '.[] | select(.kind == "worker_exited") | .fields' \
    "$RUN_ID.out/../$(basename $RUN_ID .tar.gz)/stage-0/events/"events-*.json
```

You get `exit_code`, `signal`, `uptime_ms`, the ring-buffered
`stderr_tail` (~256 last lines), and a `python_traceback` when the
worker raised an uncaught exception. For model-load specifically,
`worker_model_load_failed` carries `{model, type, value, traceback}`
in one record.

## Cleanup after a session

The orchestrator destroys rentals on exit, but if it crashed
mid-orchestration check by hand:

```sh
curl -s -H "Authorization: Bearer $VAST_API_KEY" \
    https://cloud.vast.ai/api/v0/instances/ | jq '.instances[].id'
# destroy any survivors:
curl -X DELETE -H "Authorization: Bearer $VAST_API_KEY" \
    "https://cloud.vast.ai/api/v0/instances/<id>/"
```

Bundles older than a few weeks can be pruned from
`docean:/var/lib/swactor-diag/bundles/` to keep the VPS disk usage
low.
