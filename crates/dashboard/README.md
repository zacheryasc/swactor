# dashboard

Read-only HTML/SSE dashboard over incoming datastream frames.

The crate owns the Axum server, bounded raw frame window, and view registry. Component crates can keep their own view implementations beside their code and register them through `DashboardHandle::register_view`. The built-in swactor worker page is hosted here because worker/actor/message processing is universal to swactor programs.

## Routes

- `GET /` — dashboard index
- `GET /events` — raw incoming frames as SSE
- `GET /api/frames` — recent raw frame window
- `GET /api/views` — registered view metadata
- `GET /view/datastream/live` — generic live explorer over retained and incoming datastream frames
- `GET /api/view/datastream/live` — bounded per-stream/channel explorer snapshot
- `GET /view/fleet` — compact fleet overview and focused machine telemetry
- `GET /api/view/fleet` — fleet and machine telemetry JSON snapshot
- `GET /view/swactor/workers` — built-in worker page
- `GET /api/view/swactor/workers` — worker page JSON snapshot

All state is derived from observed frames. The dashboard sends no control signals back to producers.
