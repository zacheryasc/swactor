# Dashboard crate contract

Keep this crate read-only with respect to observed programs.

- It may ingest telemetry frames.
- It may retain bounded raw-frame and view state for HTML/API rendering.
- It may host universal swactor runtime views.
- It must not send control signals to observed runtimes.
- It must not require changes outside `crates/dashboard` for dashboard-only work.

Main built-in view: the fused control plane at `/` and `/view/fleet` (node cards with machine + actor rollup, per-node output tail and roster, per-actor dossier via `/api/view/fleet/detail`), backed by `host.*`, `proc.<label>.*`, `runtime.stats`, and sequenced `runtime.actors` census/vital/activity frames when present. It is a pure frame consumer, tolerant of publisher shape. Message history is folded view-side from `messages_processed` totals and deltas. The unified navbar is injected server-side from the view and application plugin-page registries; pages opt in with a `<!--swactor:nav-->` placeholder. Plugin routers remain application-owned composition inputs and must not add dashboard-owned control state.
