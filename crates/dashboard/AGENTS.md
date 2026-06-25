# Dashboard crate contract

Keep this crate read-only with respect to observed programs.

- It may ingest datastream frames.
- It may retain bounded raw-frame and view state for HTML/API rendering.
- It may host universal swactor runtime views.
- It must not send control signals to observed runtimes.
- It must not require changes outside `crates/dashboard` for dashboard-only work.

Main built-in view: `/view/swactor/workers`, backed by `runtime.stats`, `runtime.workers`, and `runtime.actors` frames when present.
