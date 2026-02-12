# Runtime Dashboard — Agent Diagnostic Interface

## Investigate Protocol

There is a line-oriented diagnostic protocol for programmatic runtime
investigation. It reads text commands from stdin and writes JSON responses
to stdout (one object per line). Human-readable output goes to stderr.

### Launching

```bash
cargo run -p runtime-dashboard --example investigate_demo
```

Or programmatically against any running runtime:

```rust
use runtime_dashboard::investigate::run_investigate;
run_investigate(runtime_arc, collector_arc)?;   // blocks on stdin
```

### Response Envelope

Every response is a single JSON object on one line:

```json
{"ok": true,  "command": "overview", "data": { ... }}
{"ok": false, "command": "bogus",    "error": "unknown command `bogus` — try `help`"}
```

### Commands

#### overview
Overall runtime summary.
```
→ overview
← {"ok":true,"command":"overview","data":{
     "workers": 4,
     "actors": 120,
     "total_messages_processed": 584210,
     "total_mailbox_depth": 37,
     "total_panics": 0,
     "total_type_mismatches": 0,
     "sends": {"local": 312000, "cross_worker": 271000, "inbox": 1210}
   }}
```

#### workers
All workers with per-worker counters.
```
→ workers
← {"ok":true,"command":"workers","data":[
     {"id":0,"actors":30,"mailbox_depth":12,"messages_processed":146000,
      "local_sends":78000,"cross_sends":67000,"inbox_sends":300,
      "type_mismatches":0,"panics":0},
     ...
   ]}
```

#### worker &lt;id&gt;
Single worker detail including tick-phase timing and its actors.
```
→ worker 2
← {"ok":true,"command":"worker","data":{
     "id": 2, "actors": 30, "mailbox_depth": 8,
     "messages_processed": 148000,
     "local_sends": 80000, "cross_sends": 67500, "inbox_sends": 500,
     "type_mismatches": 0, "panics": 0,
     "tick_phases": {
       "ticks": 412,
       "active_pct": 78.4,
       "avg_tick_us": 23.7,
       "phases_us": [120, 980, 7200, 90, 1100, 280],
       "phases_pct": [1.2, 10.0, 73.6, 0.9, 11.2, 2.9]
     },
     "actor_details": [
       {"address": "a1b2c3d4…", "mailbox_depth": 3, "last_msg_type": "MyMsg"},
       ...
     ]
   }}
```

The six tick phases (indices 0–5):
0. spawn_drain — draining spawn channel
1. transfer_drain — draining cross-worker transfer channel
2. tick_all — processing actor mailboxes (main work)
3. spawn_drain_2 — draining spawns created during tick
4. pending_local — delivering messages buffered within the worker
5. stats_publish — updating shared stat counters

#### actors [--sort mailbox|worker|address] [--limit N] [--worker W]
Actor listing with sorting, limit, and worker filter.
```
→ actors --sort mailbox --limit 5
→ actors --worker 0 --sort address
→ actors --limit 20
```

#### actor &lt;hex_prefix&gt;
Find actors whose address starts with the given hex prefix.
```
→ actor a1b2
← {"ok":true,"command":"actor","data":{
     "prefix": "a1b2",
     "matches": 1,
     "actors": [{"address": "a1b2c3d4…", "address_full": "a1b2c3d4...(64 hex chars)", "worker_id": 2, "mailbox_depth": 3, "last_msg_type": "MyMsg"}]
   }}
```

#### hot [N]
Top N actors by mailbox depth (default 10). Use this to find backpressure.
```
→ hot 5
```

#### phases [worker_id]
Tick-phase timing breakdown. Without an argument returns all workers.
```
→ phases
→ phases 2
```

#### diff &lt;seconds&gt;
Takes two snapshots separated by N seconds (max 30) and reports deltas.
This is the primary throughput measurement tool.
```
→ diff 2
← {"ok":true,"command":"diff","data":{
     "elapsed_s": 2.001,
     "actors_before": 120, "actors_after": 132,
     "delta_messages": 8432,
     "msg_per_sec": 4213.9,
     "delta_local_sends": 4500,
     "delta_cross_sends": 3900,
     "mailbox_before": 37, "mailbox_after": 42,
     "per_worker": [
       {"worker_id": 0, "delta_messages": 2100, "msg_per_sec": 1049.5,
        "actors_before": 30, "actors_after": 33,
        "mailbox_before": 12, "mailbox_after": 14},
       ...
     ]
   }}
```

#### help
Returns all commands with usage strings.

#### quit
Exits the session.

### HTTP API

All investigate commands are available via HTTP when the dashboard server is
running. The endpoint is `/api/investigate` with query parameters:

```
GET http://localhost:9090/api/investigate?cmd=overview
GET http://localhost:9090/api/investigate?cmd=workers
GET http://localhost:9090/api/investigate?cmd=worker&id=2
GET http://localhost:9090/api/investigate?cmd=actors&sort=mailbox&limit=5&worker=0
GET http://localhost:9090/api/investigate?cmd=actor&prefix=a1b2
GET http://localhost:9090/api/investigate?cmd=hot&n=5
GET http://localhost:9090/api/investigate?cmd=phases&worker=2
GET http://localhost:9090/api/investigate?cmd=diff&seconds=2
GET http://localhost:9090/api/investigate?cmd=help
```

The response format is identical to the stdin protocol — a single JSON object
with `ok`, `command`, and `data` (or `error`) fields.

Note: `diff` blocks the HTTP request for the specified number of seconds
(max 30) while collecting the two snapshots.

### Investigation Playbook

When diagnosing a runtime, a useful sequence:

1. `overview` — get the lay of the land
2. `diff 2` — measure live throughput and detect growth
3. `hot 10` — find actors with deepest mailboxes (backpressure)
4. `workers` — compare per-worker load distribution
5. `worker <id>` — drill into the busiest worker, check phase breakdown
6. `phases` — check if workers are spending time in unexpected phases
7. `actors --worker <id> --sort mailbox` — find hot actors on that worker
8. `actor <prefix>` — get full address and type for a specific actor

### Key Metrics to Watch

| Symptom | Check | Meaning |
|---------|-------|---------|
| High mailbox_depth | `hot 10` | Actor can't keep up — backpressure |
| Uneven msg_per_sec across workers | `diff 2` per_worker | Load imbalance |
| High cross_sends vs local_sends | `overview` sends | Actors that talk are on different workers |
| active_pct near 100% | `phases <id>` | Worker is saturated |
| High phase 1+4 % vs phase 2 | `phases <id>` | Delivery overhead dominates processing |
| Growing actors_after vs actors_before | `diff 5` | Unbounded actor spawning |
| type_mismatches > 0 | `overview` | Messages routed to wrong actor type |
| panics > 0 | `overview` | Actor handlers are panicking |
