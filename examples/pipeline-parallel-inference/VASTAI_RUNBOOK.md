# Vastai N≥3 Deployment Test — Runbook

Pick up tomorrow. The diagnostics wiring is in place; this run produces the
bundle that will answer the open questions in `VASTAI_STATUS.md`.

## 1. On the VPS (diagnostics collector)

Build the collector binaries, then run the collector. Open inbound `9080/tcp`
and `9081/udp` on the VPS firewall.

```bash
cargo build --release -p distribution --features collector \
    --bin swactor-diag-collector --bin swactor-diag-postproc

./target/release/swactor-diag-collector \
    --bind 0.0.0.0:9080 \
    --root /var/lib/swactor-diag \
    --udp 0.0.0.0:9081
```

Leave it running. Records will land under `/var/lib/swactor-diag/<run_id>/...`
and the finalized tarball under `/var/lib/swactor-diag/bundles/<run_id>.tar.gz`.

## 2. On your orchestrator host

Build the pp binaries and the GPU image, push the image:

```bash
cargo build --release \
    --manifest-path examples/pipeline-parallel-inference/Cargo.toml \
    --bin pp-smoke-run --bin pp-gpu-node

docker build -f examples/pipeline-parallel-inference/Dockerfile \
    -t zacheryasc/swactor-pp-gpu:latest .
docker push zacheryasc/swactor-pp-gpu:latest
```

## 3. Launch the run

```bash
SWACTOR_DIAG_COLLECTOR_URL=http://<vps>:9080 \
SWACTOR_DIAG_RUN_ID=vastai-N3-1 \
SWACTOR_DIAG_UDP_ECHO=<vps>:9081 \
./examples/pipeline-parallel-inference/target/release/pp-smoke-run \
    --vastai \
    --api-key "$VAST_API_KEY" \
    --num-stages 3 \
    --gpu "RTX 4090" \
    --image zacheryasc/swactor-pp-gpu:latest \
    --prompt "Say hello" \
    --max-tokens 8
```

Watch stderr for:

```
pp-smoke-run: forwarding diagnostics to rented stages (collector=http://<vps>:9080)
pp-diag: installed collector=... run_id=vastai-N3-1 role=orchestrator ...
```

Both lines must appear. The first confirms the orchestrator is injecting
`SWACTOR_DIAG_*` env into the vast.ai container create payload; the second
confirms the orchestrator itself is shipping events.

## 4. After the run (success OR failure)

```bash
curl -o bundle.tar.gz http://<vps>:9080/diag/bundle/vastai-N3-1
./target/release/swactor-diag-postproc bundle.tar.gz
cat bundle.tar.gz.out/summary.md
```

`summary.md` names:

- The first peer to go Dead and when
- Which stage that peer was (from the per-node Identity block)
- Each side's `conn_type` at that moment
- Whether raw UDP probes to the collector were succeeding
- Each node's `home_relay_url` (canary vs. production comparison)

That set answers every open question in `VASTAI_STATUS.md` that lives at the
iroh/SWIM layer.

## Troubleshooting checklist (in order)

1. **`pp-smoke-run: forwarding diagnostics ...` line missing** — `SWACTOR_DIAG_COLLECTOR_URL`
   isn't set in the orchestrator's env. Re-run with the env vars on the same
   line as `pp-smoke-run`.
2. **`pp-diag: HttpSink::new failed ...`** — bad URL form. Must include scheme
   (`http://`) and host. No trailing slash needed.
3. **`GET /diag/bundle/...` returns 404** — no finalize record arrived at the
   collector. Either the orchestrator crashed before finalize, or the
   collector's `--root` is on a path that wasn't writable. Check the
   collector's stderr.
4. **Bundle exists but per-stage events missing** — the rented containers
   couldn't reach the VPS. From a vast.ai instance via SSH:
   `curl -fsS http://<vps>:9080/`  should return 404 (route exists, just no
   handler). If it hangs, the VPS firewall is blocking 9080 inbound from
   vast.ai egress.
5. **Inference run hangs/fails the same as before** — that's the bug we're
   diagnosing. Proceed to step 4 above to fetch the bundle; the failure mode
   will be in `summary.md`.

## Cleanup

The orchestrator destroys rented contracts on every exit path it controls.
If something hard-crashes the orchestrator, manually destroy with
`curl -X DELETE -H "Authorization: Bearer $VAST_API_KEY" https://cloud.vast.ai/api/v0/instances/<id>/`
or via the vast.ai web UI.

To reset the collector between runs (optional; new `run_id` already isolates):

```bash
rm -rf /var/lib/swactor-diag/*
```

## Key files for context

- `examples/pipeline-parallel-inference/VASTAI_STATUS.md` — open questions
  from the prior 8 attempts
- `examples/pipeline-parallel-inference/src/vastai.rs` — `DiagEnv` +
  `lease_chain` + `create_instance`
- `examples/pipeline-parallel-inference/src/bin/pp_smoke_run.rs` —
  `run_vastai` orchestrator flow with diagnostics finalize
- `examples/pipeline-parallel-inference/src/diag.rs` — env-driven aggregator
  install (shared by orchestrator + every stage)
- `crates/distribution/src/diagnostics/` — collector, aggregator, sinks,
  introspectors, post-processor

## What we changed in this prep session

(Context for the next agent, in case anything looks off.)

- Added `DiagEnv` struct + threading through `create_instance` /
  `create_pipeline_instances` / `lease_chain`. Defaults to `None` — opt-in
  via process env.
- `run_vastai` now installs diagnostics on the orchestrator (matching
  `run_seed`) and wraps the body in a labelled `(code, exit_reason)` block so
  the bundle's finalize record names which path bailed.
- Two new tests in `tests/t_vastai.rs` lock the diag-env propagation
  contract: one asserts the keys land in every PUT body with correct
  per-stage values; the other asserts opt-in (no leakage when `None`).
- All 9 diagnostics integration tests still pass.

Three pre-existing test failures (`find_offer_excludes_all_prior_offer_ids`,
`lease_chain_finds_n_distinct_offers`,
`lease_chain_creates_n_instances_with_distinct_stage_env`) are mock-setup
bugs from the median-priced offer selection — not in `cargo xtask test
essential` and unrelated to this work. Ignore them for this run.
