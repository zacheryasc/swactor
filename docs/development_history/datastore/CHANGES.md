# Unified Swactor Node with Datastore Dashboard Management

This document summarizes the changes on the `datastore-dashboard` branch.

## Problem

The swactor ecosystem had two separate binaries with no overlap:

- **`swactor-node`** (in `runtime-dashboard`) — distribution + dashboard, no datastore
- **`swactor-store-node`** (in `swactor-datastore`) — datastore + optional dashboard, no distribution

The dashboard's `/datastore` page was read-only (stats via SSE). The standalone datastore had its own management UI on a separate port. Neither binary gave you the full picture.

## Solution

A single batteries-included `swactor-node` crate that combines distribution, dashboard, and datastore. The dashboard now supports full datastore CRUD and lifecycle management. Old binaries remain as lightweight alternatives.

**Quick start:**

```
cargo xtask dev-node
```

Opens an iroh node with in-memory datastore on dashboard port 9090.

## What Changed

### New files

| File | Purpose |
|------|---------|
| `crates/swactor-node/Cargo.toml` | Unified node crate — depends on `runtime-dashboard`, `swactor-datastore`, and `distribution` |
| `crates/swactor-node/src/main.rs` | Combined binary with CLI: `--transport` (iroh default), `--storage-path`, `--no-datastore`, `--dashboard-port`, etc. Main loop merges distribution ticking with datastore GC/dissemination |
| `crates/datastore/src/bridge.rs` | `DatastoreBridge` — implements the dashboard's provider trait by sending actor messages and polling responses. `DatastoreNodeFactory` — spawns a fresh set of datastore actors on demand (used by the start/stop UI) |

### Modified files

**`Cargo.toml` (workspace root)**
- Added `"crates/swactor-node"` to workspace members.

**`crates/datastore/src/lib.rs`**
- Added `pub mod bridge` behind `#[cfg(feature = "node")]`.

**`crates/runtime-dashboard/src/datastore_collector.rs`**
- Expanded `DatastoreStatsProvider` trait with CRUD methods: `list_objects`, `get_object`, `get_data`, `put_data`, `delete_object`, `node_status`, `is_running`, `shutdown_datastore`. All have default impls returning `Err("not supported")` so existing `DatastoreMetrics` impl compiles unchanged.
- Added `ListScope` enum (`Local` / `Swarm`).
- Added `DatastoreFactory` trait for starting datastores from the dashboard.

**`crates/runtime-dashboard/src/lib.rs`**
- Added `datastore_factory` field to `DashboardHandle`.
- Added `set_datastore_factory()` and `datastore_provider()` methods.
- Threads factory through to `spawn_http_server()`.

**`crates/runtime-dashboard/src/server.rs`**
- Switched route matching from path-only to `(method, path)` tuples.
- Added 8 new API routes under `/api/datastore/`:
  - `GET  /api/datastore/list` — list objects (local or swarm scope)
  - `GET  /api/datastore/get` — object metadata + manifest
  - `GET  /api/datastore/data` — download raw bytes
  - `GET  /api/datastore/status` — node identity
  - `POST /api/datastore/put` — upload data
  - `POST /api/datastore/delete` — delete object
  - `POST /api/datastore/start` — start datastore via factory
  - `POST /api/datastore/shutdown` — stop datastore
- SSE `datastore` event now wraps the snapshot in an envelope: `{"is_running": bool, "snapshot": ...}`.

**`crates/runtime-dashboard/src/datastore_html.rs`**
- Full rewrite merging the monitoring dashboard (SSE-driven stats, event timeline, transfers) with the management UI from `ui_html.rs`:
  - Upload panel (file input + optional name)
  - Objects table with Origin column (local/remote badges) and action buttons (download, delete)
  - Detail modal (hash, name, size, node, tags, chunk list)
  - Toast notifications
  - Lifecycle buttons: Start Datastore / Stop (shown based on `is_running` from SSE)

**`xtask/src/main.rs`**
- Added `dev-node` subcommand: builds and runs the unified node with happy defaults (iroh transport, port 9090, 3 actors, in-memory datastore).
- Options: `--port`, `--actors`, `--storage`, `--no-datastore`, `--tcp`, `--listen`, `--release`.

## Design Decisions

- **Iroh is the default transport.** TCP is available via `--tcp` flag or `--transport tcp`.
- **Datastore is on by default** (in-memory). Disable with `--no-datastore`.
- **Dashboard-only API** — no separate datastore HTTP port. The dashboard serves all CRUD routes.
- **Factory pattern** — even when started with `--no-datastore`, the dashboard can start/stop a datastore at runtime via `DatastoreNodeFactory`.
- **No circular dependencies** — `swactor-node` sits atop the dependency graph: `swactor-node` -> `runtime-dashboard` + `swactor-datastore[node]`. The bridge trait lives in `runtime-dashboard` with default method impls.
- **Old binaries kept** — `runtime-dashboard`'s `swactor-node` and `swactor-datastore`'s `swactor-store-node` still work as lightweight alternatives.

## Verification

```
cargo build -p swactor-node -p runtime-dashboard -p swactor-datastore   # clean, 0 warnings
cargo test -p swactor -p distribution -p runtime-dashboard -p swactor-datastore -p swactor-node   # 323 tests pass
```
