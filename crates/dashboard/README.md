# dashboard

Datastream-only HTTP dashboard for visualizing swactor-derived telemetry in a browser. The dashboard consumes folded datastream records and serves live pages over HTTP/SSE.

## Features

| Feature | Default | Description |
|---------|---------|-------------|
| `distribution` | yes | `/distribution` page with SWIM membership, gossip directory routes, peer auth, and location cache data derived from datastream frames |
| Fleet view | yes | `/vastai` page showing nodes folded by `datastream_source::FleetView` |

## HTTP Dashboard

The dashboard is embedded by an application that owns a datastream sink. The
sink folds delivered frames through `datastream_source::FleetView`, then pushes
the resulting stats, activity lines, and cache-backed plugin JSON into the
dashboard handle.

Pages:
- `http://localhost:9090/` — live overview from the selected datastream node
- `http://localhost:9090/actors` — actor table reconstructed from actor telemetry records
- `http://localhost:9090/plugin/distribution` — SWIM membership, gossip directory routes, peer auth, and cache entries
- `http://localhost:9090/plugin/vastai` — fleet/node view fed by the shared fleet cache

The dashboard model is folded by `datastream_source::FleetView`. Producers
publish telemetry records to datastream channels; the dashboard sink folds those
records into cached JSON, pushes activity messages, and updates the HTTP/SSE
views.

## Public API

Embed the dashboard by constructing `DashboardConfig` and calling
`start_dashboard(config)`. The returned handle owns the HTTP server state and
supports externally pushed stats, activity messages, history access, plugin
registration, landing page overrides, extra routers, and shutdown.

Plugins are the extension boundary. New dashboard surfaces should register a
`DashboardPlugin` or use a cache-backed plugin such as
`fleet_cache_plugin(cache)` / `distribution_cache_plugin(cache)`, then feed it
from datastream-derived JSON caches.

## Pipeline app

`apps/pipeline-parallel-inference` enables the dashboard with `PP_DASHBOARD=1`.
Its orchestrator hosts the HTTP server, spawns the `datastream-sink` actor, and
feeds every dashboard view from `FleetView` updates.
