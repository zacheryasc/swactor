# dashboard

Visual dashboard for the swactor runtime. Provides a live HTTP dashboard, a
terminal UI (TUI), trace recording/replay, and an HTTP API for programmatic
runtime investigation.

## Features

| Feature | Default | Description |
|---------|---------|-------------|
| `distribution` | yes | `/distribution` page with SWIM membership, Kademlia routing, and location cache |
| `tui` | no | Terminal UI with overview, worker detail, and distribution views |

## HTTP Dashboard

Start the dashboard demo and open it in a browser:

```bash
cargo run -p dashboard --example dashboard_demo
```

Pages:
- `http://localhost:9090` — live overview (workers, actors, message rates)
- `http://localhost:9090/actors` — actor table
- `http://localhost:9090/distribution` — SWIM membership, Kademlia routing, cache entries

The demo creates a 4-worker runtime with ping-pong and counter actors, plus a
9-node distribution cluster (1 main node + 8 peers) with simulated SWIM
membership and actor registrations in the directory/cache.

## TUI

A standalone binary that connects to any running dashboard over SSE:

```bash
cargo run -p dashboard --features tui --bin swactor-tui
# or point at a specific endpoint
cargo run -p dashboard --features tui --bin swactor-tui -- http://localhost:9090
```

Views (cycle with Tab):
- **Overview** — htop-style worker bars, summary line, sortable actor table
- **Worker Detail** — focused view of a single worker's actors and phase breakdown
- **Distribution** — cluster summary, scrollable members table, cache entries, routing bucket histogram

Key bindings: `q` quit, `Tab` cycle views, `s` sort column, `r` reverse sort,
arrow keys/`j`/`k` scroll, `Enter` drill into worker, `Esc` back to overview.

## Agent HTTP API (Investigate)

All diagnostic commands are available as HTTP endpoints when the dashboard
server is running. See [AGENTS.md](AGENTS.md) for full protocol documentation.

```bash
curl 'http://localhost:9090/api/investigate?cmd=overview'
curl 'http://localhost:9090/api/investigate?cmd=hot&n=5'
curl 'http://localhost:9090/api/investigate?cmd=workers'
curl 'http://localhost:9090/api/investigate?cmd=worker&id=2'
curl 'http://localhost:9090/api/investigate?cmd=actors&sort=mailbox&limit=10'
curl 'http://localhost:9090/api/investigate?cmd=diff&seconds=2'
```

The same commands are also available via a stdin/stdout REPL for direct
programmatic use (see `investigate::run_investigate`).

## Demos

All examples are run from the workspace root.

**HTTP dashboard** — live workload with distribution cluster, Ctrl+C to stop:

```bash
cargo run -p dashboard --example dashboard_demo
# http://localhost:9090              — runtime overview
# http://localhost:9090/distribution — cluster view
```

**Benchmarks** — four automated scenarios (~20 s total):

```bash
cargo run -p dashboard --example bench_dashboard
# open http://localhost:9090
```

**Record & replay** — records ~10 s of activity, then serves a replay:

```bash
cargo run -p dashboard --example record_and_replay_demo
# live dashboard at http://localhost:9090 during recording
# replay dashboard at http://localhost:9091 after recording finishes
# Ctrl+C to stop
```
