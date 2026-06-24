# Datastream Dashboard — Agent Interface

## Data Flow

The dashboard is an HTTP/SSE consumer of datastream-derived models. Agents should inspect the browser endpoints and JSON plugin endpoints.

`datastream_source::FleetView` is the canonical fold from delivered frames to dashboard models. It produces:

- fleet JSON for the node/fleet page
- distribution JSON for the distribution page
- `RuntimeStats` for the selected node overview and actors table
- activity messages for the event stream

## HTTP Pages

- `GET /` — selected node overview
- `GET /actors` — selected node actor rows
- `GET /topology` — topology derived from the latest selected-node stats
- `GET /plugin/distribution` — distribution graph and peer/cache state
- `GET /plugin/vastai` — fleet view
- `GET /events` — server-sent events for stats, activity, history, and plugin updates

## JSON Endpoints

- `GET /api/stats` — latest selected-node `RuntimeStats`, or `{}` before the first selected-node frame
- `GET /api/topology` — topology derived from latest stats, or `{}`
- `GET /api/history` — in-memory worker history
- `GET /api/logs` — retained activity events, optionally filtered by query params
- `GET /api/plugin/vastai` — current fleet JSON cache
- `GET /api/plugin/distribution` — current distribution JSON cache

The same plugin names are used on the SSE stream for incremental browser
updates. Distribution page buttons post to `/api/plugin/distribution/rejoin`
and `/api/plugin/distribution/clear_status`; the datastream-backed plugin
acknowledges them as read-only no-ops.

## Extension Rule

Extend the dashboard through plugins backed by datastream-folded caches. A producer may add telemetry records, a fold may update shared JSON, and a plugin may serve that JSON/page over HTTP and SSE.
