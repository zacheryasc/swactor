# swactor-gossip

Epidemic gossip protocol built on the [swactor](../../) actor runtime.

Nodes exchange state via randomized push-gossip and converge to a consistent view using last-writer-wins versioned values.

## Crate layout

- **`protocol`** — `GossipActor`, `GossipMessage`, `GossipState` (the core protocol implementation)
- **`sim`** — simulation harness with configurable topologies (Ring, Star, FullMesh, Chain, Partitioned) and optional partition healing
- **`trace`** — per-tick event log and `SimulationTrace` for post-run analysis
- **`properties`** — metrics extraction (delivery ratio, convergence round, redundancy, load balance, etc.) and property-based assertions over traces
- **`report`** / **`property_report`** — self-contained HTML report generators for single-run and multi-scenario results

## Tools

### Dashboard (`gossip-dashboard` crate)

A live web dashboard that streams simulation progress to a browser in real time. See [`crates/gossip-dashboard/`](../gossip-dashboard/).

```sh
cargo run -p gossip-dashboard --example dashboard
# or with a TOML config:
cargo run -p gossip-dashboard --example dashboard -- crates/gossip-dashboard/examples/sim.toml
```

### HTML report

The `gossip_sim` example runs a simulation and writes a standalone HTML report:

```sh
cargo run -p swactor-gossip --example gossip_sim
```

### Property report

Runs multiple scenarios and generates an HTML report checking gossip protocol properties (convergence, consistency, load balance):

```sh
cargo run -p swactor-gossip --example gossip_property_report
```
