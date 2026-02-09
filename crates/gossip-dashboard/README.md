# gossip-dashboard

Interactive web dashboard for visualizing gossip protocol simulations.

The workflow has two steps:

1. **Generate traces** -- run simulations against TOML config files, producing `.trace.json` files.
2. **Replay traces** -- start the dashboard server, point it at a directory of traces, and explore them in the browser.

## Quick start

```bash
# 1. Generate traces from the bundled configs (outputs to traces/)
cargo run -p gossip-dashboard --example generate_traces

# 2. Launch the dashboard
cargo run -p gossip-dashboard --example replay -- traces
# => open http://localhost:8080
```

## Commands

### `generate_traces`

Runs gossip simulations and writes `.trace.json` files.

```
generate_traces                                        # all bundled configs -> traces/
generate_traces <out-dir>                              # all bundled configs -> <out-dir>/
generate_traces <out-dir> <config.toml> [more.toml …]  # specific configs   -> <out-dir>/
```

Bundled configs live in `examples/configs/`. When no config paths are given, every `.toml` in that directory is run.

Output filenames are derived from the simulation name (lowercased, spaces to underscores). Example output:

```
traces/
  ring_10_nodes.trace.json
  star_7_nodes.trace.json
  chain_8_nodes.trace.json
  full_mesh_6_nodes.trace.json
  partition_&_heal_8_nodes.trace.json
```

### `replay`

Starts an HTTP server that serves the dashboard UI and the trace data.

```
replay <trace-dir> [port]
```

| Argument    | Required | Default | Description                              |
|-------------|----------|---------|------------------------------------------|
| `trace-dir` | yes      | --      | Directory containing `.trace.json` files |
| `port`      | no       | 8080    | Port to bind on                          |

The server exposes three endpoints:

| Route                    | Description                        |
|--------------------------|------------------------------------|
| `GET /`                  | Dashboard HTML                     |
| `GET /traces`            | JSON list of available trace files |
| `GET /trace.json?file=…` | Fetch a specific trace             |

## Configuration (TOML)

Each simulation is defined by a TOML file. Example (`ring_10.toml`):

```toml
name = "Ring (10 nodes)"
topology = "ring"
num_nodes = 10
num_rounds = 15
ticks_per_round = 5
num_threads = 1

[initial_data]
color = "blue"
version = "1"
status = "active"
```

### Fields

| Field              | Type              | Required | Description                                                       |
|--------------------|-------------------|----------|-------------------------------------------------------------------|
| `name`             | string            | yes      | Display name for the simulation                                   |
| `topology`         | string            | yes      | Network topology (see below)                                      |
| `num_nodes`        | integer           | yes      | Number of gossip nodes                                            |
| `num_rounds`       | integer           | yes      | Number of gossip rounds to run                                    |
| `ticks_per_round`  | integer           | yes      | Simulation ticks per round                                        |
| `num_threads`      | integer           | yes      | Worker threads (`1` = deterministic single-threaded)              |
| `heal_after_round` | integer           | no       | Round after which partitioned halves are bridged                  |
| `initial_data`     | table of strings  | no       | Key-value pairs seeded on node 0 before gossip begins             |

### Topologies

| Value         | Shape                                                                    |
|---------------|--------------------------------------------------------------------------|
| `ring`        | Each node connects to the next, forming a circle                         |
| `star`        | Node 0 is a hub with bidirectional links to every other node             |
| `full_mesh`   | Every node connects bidirectionally to every other node                  |
| `chain`       | Unidirectional chain: node 0 -> 1 -> 2 -> ... -> N-1                    |
| `partitioned` | Two isolated full-mesh halves; use `heal_after_round` to bridge them    |

## Bundled configs

| File                      | Topology    | Nodes | Rounds | Notes                      |
|---------------------------|-------------|-------|--------|----------------------------|
| `ring_10.toml`            | ring        | 10    | 15     |                            |
| `star_7.toml`             | star        | 7     | 10     |                            |
| `full_mesh_6.toml`        | full_mesh   | 6     | 8      |                            |
| `chain_8.toml`            | chain       | 8     | 20     |                            |
| `partitioned_8_heal.toml` | partitioned | 8     | 20     | Heals after round 10       |

## Dashboard UI

Once a trace is loaded in the browser:

- **Graph canvas** -- nodes arranged in a circle then refined with force-directed layout. Nodes and edges flash as events are replayed.
- **Stats panel** -- total nodes, edges, messages, current round.
- **Worker logs** -- per-thread activity feed.
- **Event table** -- full event log with columns: Seq, Round, Thread, Node, Event, Details.
- **Playback controls** -- First / Prev / Play / Pause / Next / Last, timeline slider, speed adjustment (10 ms -- 2000 ms per event).
