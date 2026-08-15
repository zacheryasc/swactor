# dashboard

Read-only HTML/SSE dashboard over incoming telemetry frames.

The crate owns the Axum server, bounded raw frame window, and view registry. Component crates can keep their own view implementations beside their code and register them through `DashboardHandle::register_view`. The built-in control-plane view is hosted here because worker/actor/message processing is universal to swactor programs.

The control-plane page fuses machine stats and actor stats per node stream: node cards (CPU/GPU/net + actor rollup) → per-node actor roster → per-actor dossier (identity, message diet, sampled message history). The Rust type name is the actor's display name; the address is the unique key. Stale streams (silent beyond the liveness window) render in a separate collapsed pool, superseded `life` generations are evicted immediately, and the stale pool is hard-capped.

## Routes

- `GET /` — fleet control plane (home; same page as `/view/fleet`)
- `GET /events` — raw incoming frames as SSE
- `GET /api/frames` — recent raw frame window
- `GET /api/views` — registered view metadata
- `GET /view/telemetry/live` — generic live explorer over retained and incoming telemetry frames
- `GET /api/view/telemetry/live` — bounded per-stream/channel explorer snapshot
- `GET /view/fleet` — fused control-plane page (machine + actors per node)
- `GET /api/view/fleet` — live/stale pools with per-node machine and roster snapshot
- `GET /api/view/fleet/detail?stream=<node#life>&actor=<addr>` — bounded per-actor dossier detail (diet, history, sampled receipts)

Every page carries the unified top navbar, built from the view registry at serve time — pages include a `<!--swactor:nav-->` placeholder and the server substitutes the links, so app-registered views appear automatically.

All state is derived from observed frames. The dashboard sends no control signals back to producers.
