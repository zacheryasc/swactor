# PP N=12 Deploy — Session Report

## Problems encountered (trivial → blocking)
- **GPU filter too narrow** — exact `RTX 3060` match: stage 5 hit "no offers" mid-chain. Fixed by VRAM-based selection (`PP_GPU_MIN_RAM_MB`).
- **Offer churn** — `no_such_ask`: offers vanish between search and create.
- **API rate-limit (HTTP 429)** — `provision_stage` retried with zero backoff + no inter-stage pacing, burning the candidate pool. Fixed: 429 backoff + `PP_LEASE_PACE_MS`.
- **Worker crash `Code(2)` on all 12 stages** — two root causes, both invisible at first:
  - `Tensor(str)` rejected by bundled tinygrad → needed `Tensor(Path(...))`.
  - Image shipped only 4 CUDA headers; NVRTC needs the full set (`vector_types.h`). Dockerfile copied a hand-picked subset.
- **Worker stderr swallowed** — StageActor buffers it into a `worker_exit_detail` diag event that needs a collector; none configured → error vanished.
- **SSH auth** — `publickey` denied initially (propagation lag), then worked on 5/12 but **persistently failed on 7/12** (pp-gpu-node holds PID 1 via `exec`; vast key-injection never ran). `vastai attach`/reboot didn't fix it.
- **`vastai execute` unusable** — "Invalid command given" (restricted command set).
- **`--redeploy` blocked** — uses the same SSH/scp, so unusable on the 7 unreachable nodes.
- **Slow/stalled image pulls** — cheap Korea GTX-10-series hosts; one fully stalled (0 bytes), triggering Phase-2 **autoreplace churn**.
- **Autoreplace not disableable** in the running binary.
- **429 on relaunch** — teardown's 12 destroys consumed the budget; absorbed by the new backoff.

## Where we spent the most time
1. **~20 min blind on the silent resolve loop** — connect-timeout SWIM noise looked like the problem but was a red herring; workers had actually crashed instantly.
2. **Getting on a node to see the real error** — SSH flakiness, restricted `execute`, local docker repro, then manual on-node run.
3. **Run #2 image-pull waiting** — many heartbeat ticks on slow/stalled pulls + the re-download after teardown.

## Observability that was clunky / insufficient
- Worker stderr + Python traceback never reach the container log (no collector) — had to reproduce locally and SSH a node to see `Code(2)`'s cause. *(Fixed: pp-gpu-node now prints abnormal-exit stderr.)*
- Resolve loop emits **nothing per-stage** — orchestrator log is just SWIM gossip for up to 20 min; no per-stage worker-ready/download visibility.
- The rich SSE diag stream (`pp_download_progress`, `worker_exit_detail`) was dead — `SWACTOR_DIAG_COLLECTOR_URL` unset.
- vast exposes **no docker-pull %** — `status_msg` only says "Pulling from"; `disk_usage` = -1.
- Connect-timeout logs were prominent but cosmetic — actively misleading.
- Node-id→stage mapping had to be derived by hand from `stage_secrets`.

## Where interaction with the live deployment was limited
- SSH worked on only 5/12 nodes; no reliable shell on the rest.
- `vastai execute` restricted; couldn't run arbitrary diagnostics via API.
- `--redeploy` (the intended fix-forward path) depends on the same broken SSH → fix-forward on live nodes was effectively impossible; had to rebuild the image + re-lease.
- Couldn't pause/disable autoreplace or see/intervene in image-pull progress.
- During the "loading" (pull) phase there's no container, so no SSH at all on the node that mattered most.

## Other notes
- **Sharded fetch works** (~1.8 GB/stage, not 18 GB) — but a stale code comment claims the full GGUF is pulled, which misled diagnosis.
- Core bugs are fixed + validated on a real GPU (stage 0 → `ready`) and in the pushed image; the remaining blocker is purely **host quality** (slow-pull hosts), not code.
- Re-leasing fresh always re-pulls image + re-downloads model; the in-place cache advantage is lost on every teardown.
- Highest-leverage follow-ups: (1) configure a diagnostics collector, (2) emit per-stage resolve/download progress to the orchestrator log, (3) host-throughput preflight or stalled-pull fast-replace, (4) fix the onstart so vast SSH-key injection survives (don't `exec` over it).

## Fixes shipped this session
- `vastai.rs`: 429 backoff in `provision_stage` (find + create paths) and inter-stage pacing (`PP_LEASE_PACE_MS`, default 600ms).
- `pp_tinygrad_worker.py`: `Tensor(gguf_path)` → `Tensor(Path(gguf_path))`.
- `Dockerfile`: copy the full CUDA include set (with a `test -f vector_types.h` build guard) instead of 4 hand-picked headers.
- `stage_actor.rs`: mirror an abnormal worker exit's stderr tail + Python traceback to pp-gpu-node's own stderr (→ container log, collector-independent).
- Image rebuilt + pushed (`zacheryasc/swactor-pp-gpu:latest`, digest `ca373d02…`); both bug fixes validated on a real GPU node (stage 0 reached `ready`).
