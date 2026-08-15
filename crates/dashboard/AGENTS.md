# Dashboard crate contract

Keep this crate read-only with respect to observed programs.

- It may ingest telemetry frames.
- It may retain bounded raw-frame and view state for HTML/API rendering.
- It may host universal swactor runtime views.
- It must not send control signals to observed runtimes.
- It must not require changes outside `crates/dashboard` for dashboard-only work.

Main built-in views: `/view/swactor/workers` (worker-centric) and `/view/swactor/actor-overview` + `/view/swactor/actor-dossier` (actor-centric), all backed by `runtime.stats`, `runtime.workers`, and `runtime.actors` frames when present. Actor views are pure frame consumers and tolerant of publisher shape.
