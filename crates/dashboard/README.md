# dashboard

Read-only HTML/SSE dashboard over incoming telemetry frames.

The crate owns the Axum server, bounded raw frame window, view registry, and application plugin-page registry. Component crates can keep telemetry views beside their code and register them through `DashboardHandle::register_view`. An embedding application can pass `DashboardPlugin` values to `DashboardHandle::with_plugins`; each plugin contributes an application-owned router and optional `PluginPage` metadata/HTML. The built-in control-plane view is hosted here because worker/actor/message processing is universal to swactor programs.

The control-plane page fuses machine stats, actor telemetry, and a 50-line
stdout/stderr tail per node stream: node cards (CPU/GPU/net + actor rollup) →
per-node actor roster and output cue → per-actor dossier. Pre-join provisioning
output is keyed by run/node identity and merges into the joined runtime card.
The Rust type name is the actor's display name; the address is the unique key.
Stale streams (silent beyond the liveness window) render in a separate collapsed
pool, superseded `life` generations are evicted immediately, and the stale pool
is hard-capped at 50.

## Routes

- `GET /` — fleet control plane (home; same page as `/view/fleet`)
- `GET /events` — raw incoming frames as SSE
- `GET /api/frames` — recent raw frame window
- `GET /api/views` — registered view metadata
- `GET /view/telemetry/live` — generic live explorer over retained and incoming telemetry frames
- `GET /api/view/telemetry/live` — 2,000 frames per stream/channel, newest lifetime per node, all live streams plus 50 stale streams
- `GET /view/fleet` — fused control-plane page (machine + actors per node)
- `GET /api/view/fleet` — live/stale pools with per-node machine and roster snapshot
- `GET /api/view/fleet/detail?stream=<node#life>&actor=<addr>` — bounded per-actor dossier detail (diet, history, sampled receipts)

Every page carries the unified top navbar, built from the view and plugin-page registries at serve time. Pages include a `<!--swactor:nav-->` placeholder and the server substitutes the links, so registered telemetry views and application plugin pages appear automatically.

Dashboard-owned state is derived only from observed frames, and the dashboard crate sends no control signals back to producers. A plugin router remains owned by the embedding application; composing that router does not grant dashboard views mutation capabilities.
