# CI Pipeline Deployment — Development History

> Covers the first real deployment of the CI pipeline: Forgejo (VPS) → ci-relay
> (iroh) → local-runner (Thinkpad). Verified end-to-end with a smoke-test
> pipeline that reports status back to Forgejo.
>
> *Branch: `spot-instance`*

---

## What Was Done

### Deployed Components

| Component | Machine | How |
|-----------|---------|-----|
| `.ci.yml` | Repo root | Smoke pipeline: `echo "CI is alive"` on push to `*` |
| `ci-relay` | VPS | Release binary, systemd service |
| `local-runner` | Runner host | Release binary, started via nohup |
| Forgejo webhook | VPS (Docker) | Hook #1, fires on push to relay's HTTP listener |

### Deployment Steps

1. **Created `.ci.yml`** — minimal smoke pipeline (`echo "CI is alive"`)
2. **Generated webhook secret** — `openssl rand -hex 32` → `~/.ssh/forgejo.ci-webhook-secret`
3. **Built release binaries** — `cargo build --release -p ci-relay -p local-runner`
4. **Distributed binaries** — `scp` to VPS (`docean:`) and Thinkpad (`thinkpad:`)
5. **Deployed ci-relay as systemd service** on VPS:
   - Service file: `/etc/systemd/system/ci-relay.service`
   - Iroh Node ID: `<IROH_NODE_ID>`
6. **Started local-runner on Thinkpad** — connects to relay via iroh, confirmed "Connected to relay!"
7. **Configured Forgejo**:
   - Added `[webhook] ALLOWED_HOST_LIST = loopback,<DOCKER_BRIDGE_IP>` to `app.ini` (Forgejo blocks private IPs by default)
   - Restarted Forgejo container
   - Created webhook via API targeting `http://<DOCKER_BRIDGE_IP>:8787`
   - **Fixed UFW firewall** — Docker bridge traffic to port 8787 was blocked by default DROP policy; added a UFW rule allowing the Docker subnet
8. **Verified end-to-end** — pushed commit, Forgejo shows green check:
   - `ci/hello`: success — "Job 'hello' completed"
   - `ci/smoke`: success — "Pipeline 'smoke' success"

### Issue Encountered: UFW Blocking Docker Bridge

The plan assumed Docker bridge traffic (`172.17.0.1`) would reach the host's port 8787 unimpeded. UFW's default INPUT policy is DROP, which blocks this. The fix was a single firewall rule allowing the Docker subnet.

### Credentials & Secrets

| File | Purpose | Location |
|------|---------|----------|
| Forgejo API token | CI status reporting | Spot instance + Thinkpad |
| HMAC webhook secret | Webhook signature verification | Spot instance + Thinkpad |

Secrets are stored outside the repo. The webhook secret is embedded in the systemd service `ExecStart` line on the VPS. To rotate it: update the service file, restart ci-relay, update Forgejo webhook config.

### Connection Details

- **ci-relay** listens on HTTP (webhooks) + iroh (runner connection)
- **local-runner** connects outbound to relay's iroh Node ID (NAT-friendly)
- **Status reports** go directly from runner → Forgejo API over HTTPS (no relay)

---

## Next Step: Real CI Jobs

The smoke-test pipeline proves the plumbing works. The next step is replacing `echo "CI is alive"` with actual CI jobs in `.ci.yml`.

Candidates for the first real pipeline:

1. **`cargo check`** — fast compilation check, catches most errors
2. **`cargo test`** — full test suite (simulation tests can be slow)
3. **`cargo clippy`** — lint pass
4. **Benchmark runs** — the whole reason for running CI on the Thinkpad (consistent hardware)

Things to consider:

- **Rust toolchain on Thinkpad**: `local-runner` shells out to run jobs, so the Thinkpad needs `rustup`/`cargo` installed and on PATH
- **Build cache**: consecutive runs in separate `pipeline-N` dirs won't share a target directory. Consider a shared `CARGO_TARGET_DIR` or `sccache` for faster builds
- **Job timeouts**: no timeout mechanism exists yet; a hung `cargo build` would block the single-threaded job queue forever
- **Multiple jobs**: `.ci.yml` supports multiple jobs per pipeline, but they run sequentially. Could add `cargo check` as a fast gate before `cargo test`
- **Branch filtering**: currently triggers on `*` — may want to restrict benchmarks to `master` only

### Suggested `.ci.yml` Evolution

```yaml
pipelines:
  check:
    triggers:
      - event: push
        branches: ["*"]
    jobs:
      check:
        run: cargo check --workspace
      test:
        run: cargo test --workspace
      clippy:
        run: cargo clippy --workspace -- -D warnings

  bench:
    triggers:
      - event: push
        branches: ["master"]
    jobs:
      bench:
        run: cargo bench --workspace
```

---

## Operational Notes

### Restarting ci-relay (VPS)

```bash
ssh <VPS_HOST>
systemctl restart ci-relay
journalctl -u ci-relay -f
```

### Restarting local-runner (runner host)

```bash
ssh <RUNNER_HOST>
pkill local-runner
nohup ~/local-runner \
  --relay-node-id <IROH_NODE_ID> \
  --forgejo-url https://zachery.lol/code \
  --forgejo-token "$(cat <TOKEN_FILE>)" \
  --yaml ~/.ci.yml \
  --work-dir ~/ci-work \
  --repo-url https://zachery.lol/code/zacheryasc/swactor.git \
  > ~/local-runner.log 2>&1 &
```

### Checking webhook deliveries

```bash
# Forgejo webhook UI: Settings → Webhooks → Hook #1 → Recent Deliveries
# Or test delivery via API:
curl -X POST "https://zachery.lol/code/api/v1/repos/zacheryasc/swactor/hooks/1/tests" \
  -H "Authorization: token <YOUR_TOKEN>"
```
