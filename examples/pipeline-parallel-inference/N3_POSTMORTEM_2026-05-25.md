# N=3 vast.ai deployment post-mortem — 2026-05-25

Companion to `N3_DEPLOYMENT_REPORT.md` and `DEPLOYMENT_TEST.md`. Covers
one invocation of `pp-smoke-run --vastai --num-stages 3` on 2026-05-25
(`vastai-N3-1779720002`). The cluster came up, lost one peer's relay
session ~5 s into SWIM convergence, never recovered, and was killed by
the operator at ~10 min. The orchestrator never produced an
`InferenceResponse`. The diagnostic bundle was recovered by hand (no
finalize record was written) and post-processed.

## Cleanup note

The run was terminated with `TaskStop` (SIGKILL). The orchestrator's
destroy-on-exit handler did not run. Three rentals (`37777187`,
`37777190`, `37777192`) were destroyed manually by
`DELETE /api/v0/instances/<id>/`. Post-cleanup instance count = 0.

## Sequence

3 instances leased (`37777187` → stage 0 / `95d01a36…`, `37777190`
→ stage 2 / `a040c0d2…`, `37777192` → stage 1 / `0cc5ed32…`).
Orchestrator node id `66b61b4a…`. All four nodes used
`SWACTOR_IROH_RELAY_URL=http://146.190.110.128:7843/` (docean), as
recorded in every node's `body.iroh.home_relay_url` field.

Live log progression:

```
t=0      orchestrator boots, custom-relay banner emitted
t=~135s  contract 37777187 (stage 0) reaches running, others follow
t=158s   3 contracts leased, "waiting for SWIM convergence (3 alive)"
t=~190s  members ["0cc5ed32=alive", "95d01a36=alive", "a040c0d2=suspect"]
         iroh driver: connect attempt N/3 to a040c0d2 failed: connect timeout
         (repeated)
t=~340s  stage-0 marks stage-2 (a040c0d2) Dead, reason "suspicion-timeout"
t=~420s  stage-1 (0cc5ed32) also goes suspect from orchestrator's view
t=~600s  members ["0cc5ed32=dead", "95d01a36=alive", "a040c0d2=dead"]
t=~600s  operator killed the orchestrator (SIGKILL via TaskStop)
```

## Bundle recovery

`GET /diag/bundle/vastai-N3-1779720002` returned HTTP 404. The
collector finalises tarballs only on receipt of a finalize record from
the orchestrator; SIGKILL skipped that step. Per-node staging files
under `docean:/var/lib/swactor-diag/vastai-N3-1779720002/` survived and
were retrievable by tar + scp.

Recovery steps applied to produce a postproc-compatible bundle:

1. Tar `/var/lib/swactor-diag/<run_id>/` from docean and copy down.
2. Synthesize `MANIFEST.json` from the four `boot-000001.json` records
   (run_id, role, stage_index, node_id_hex; file counts via `ls -1`).
3. Reshape staging layout (flat `boot-NNN.json`, `events-NNN.json`,
   `snapshot-NNN.json` under `<node_id_hex>/`) into bundle layout
   (`<label>/{boot.json, events/events-NNN.json, snapshots/snapshot-NNN.json}`)
   per `crates/distribution/src/diagnostics/collector/bundle.rs:73-128`.
4. Re-tar and run `target/release/swactor-diag-postproc`.

`finalize_received: false` in the synthesized manifest. Post-proc
completed; `summary.md` and 12 timeline TSVs generated.

## Bundle findings

### Volume

| node                       | role         | snapshots | event batches |
|----------------------------|--------------|-----------|---------------|
| `66b61b4a` orchestrator    | orchestrator | 130       | 223           |
| `95d01a36` stage-0         | stage        | 100       | 188           |
| `0cc5ed32` stage-1         | stage        | 58        | 87            |
| `a040c0d2` stage-2         | stage        | 23        | 38            |

Stage-2 stopped reporting earliest. Its event volume is ~17% of
the orchestrator's.

### UDP echo probes (collector tier-2 reachability)

| node              | result                                 |
|-------------------|----------------------------------------|
| orchestrator      | ok, rtt=295 ms, 59/59 ok               |
| stage-0           | ok, rtt=182 ms, 38/38 ok               |
| stage-1           | ok, rtt=343 ms, 23/25 ok               |
| stage-2           | timeout, 11/12 ok                      |

### SWIM transitions

`SwimTransition` total: 46.

First `→ Dead`: stage-0 marked stage-2 (`a040c0d2…`) Dead at
`t = 1779720343390` ms, reason `"suspicion-timeout"`. At the
transition moment:

- observer (stage-0): `conn_type=None`, `probes_ok=yes`
- peer (stage-2):    `conn_type=unknown`, `probes_ok=no`

### iroh state — orchestrator's view of stage-2

From `body.iroh.connection_cache[peer=a040c0d2…]` in the latest
orchestrator snapshot:

```
created_at_ms:                    1779720247124
last_successful_send_at_ms:       1779720247125    (Δ = +1 ms)
last_failure_at_ms:               1779720251922    (Δ = +4797 ms after open)
last_failure_reason:              "connection-closed"
observed_conn_type_at_last_use:   "None"
```

`body.iroh.peers[a040c0d2…].relay_urls[0].usage: "inactive"`.

The orchestrator's relay-mediated connection to stage-2 succeeded for
~5 s, was closed with reason `connection-closed`, and was never
re-established. The cache entry remained at `generation: 1`.

### iroh state — stage-2's view of itself

From `body.iroh` in stage-2's latest snapshot:

```
home_relay_url:                http://146.190.110.128:7843/   (our relay)
peers:                         [orchestrator only]            (never saw siblings)
relay_home_change counter:     1                              (one-time setting, no churn)
holepunch_attempts counter:    537
mapping_attempts counter:      6
mapping_failures counter:      5
paths_relay counter:           1
num_conns_opened counter:      1
num_conns_closed counter:      0
send_relay bytes:              43458
recv_data_relay bytes:         14882
```

Stage-2 never appeared in any peer's `peers[]` with `usage: "active"`
after the initial 5-second window.

`RelayChanged` event total across all nodes: 0.

### Custom (worker) events

| node          | `worker_starting` | `worker_ready` | `worker_heartbeat` |
|---------------|-------------------|----------------|--------------------|
| stage-0       | 1                 | 1              | reported           |
| stage-1       | 1                 | 1              | reported           |
| stage-2       | 0                 | 0              | 0                  |
| orchestrator  | 0                 | 0              | 0                  |

`worker_starting` total: 2. `worker_ready` total: 2. Stage-2 emitted
neither.

### One-time iroh API gaps

Every node emitted one `Custom { kind: "iroh_api_missing" }` event at
boot. Fields reported missing from `iroh::endpoint::RemoteInfo`:

```
RemoteInfo.conn_type
RemoteInfo.latency_ms
RemoteInfo.last_used_ms
RemoteInfo.last_received_ms
TransportAddrInfo.source
```

`iroh_version` reported in the event is `"0.96"` (hard-coded string at
`crates/distribution/src/diagnostics/iroh_introspect.rs:237`). The
crate dependency in `examples/pipeline-parallel-inference/Cargo.lock` is
`iroh 0.98.2`.

## Data-collection gaps surfaced by this run

The following information would have helped narrow the cause of the
stage-2 session loss. Listed alongside the existing source path that
either does not emit it or emits a degraded version.

### 1. Relay-side data not collected at all

`swactor-iroh-relay` on docean runs without external observability
pulls. The bundle has zero data from the relay process:

- no per-connection session log (open/close, close reason, bytes)
- no `/metrics` snapshot
- no log tail

The orchestrator-side `connection_cache` reported
`last_failure_reason: "connection-closed"` for stage-2. The actor that
closed the session (relay vs. either endpoint) and the underlying QUIC
close code are not recoverable from the bundle.

### 2. No per-event RelayConnected/RelayDisconnected emission

`body.iroh.metrics.socket.relay_home_change` is a monotonically
increasing counter recorded per snapshot. The bundle reports its final
value (`1` for every node) but no event-stream item for the moment a
relay path is established, lost, or re-established. Stage-2's local
view (`num_conns_closed: 0`) is consistent with iroh not noticing its
own session was dead.

### 3. Boot-record host metadata is null

`crates/distribution/src/diagnostics/identity.rs:73` defines the
fields; every node's `boot.json` carries:

```
container_id:           null
datacenter_id:          null
host_country:           null
host_ip_public:         null
home_relay_url_at_boot: null
hostname:               <docker container short id>
git_sha:                null
iroh_version:           null
```

The orchestrator already has `host_ip_public`, `datacenter_id`, and
`host_country` for each rental at the point `lease_chain` returns
(`RunningInstance` in `examples/pipeline-parallel-inference/src/vastai.rs`).
None of those fields are forwarded into the container or recorded by
`pp_gpu_node` into the boot snapshot. The vast.ai host machine and
datacenter that produced the stage-2 rental are not recoverable from
the bundle.

### 4. No stage-side reachability probe against the relay

`probes` records one outcome per snapshot — UDP echo to
`SWACTOR_DIAG_UDP_ECHO` (`:9081`). There is no analogous probe to the
relay (`:7843`). Whether stage-2 retained transport-level reachability
to docean after its iroh session closed is not directly observable.
The UDP-echo result (stage-2 timeout at 11/12) covers a different port
on the same host.

### 5. No per-peer DialStarted/DialOutcome rollup

Event totals: `DialStarted: 83`, `DialOutcome: 80`. The bundle has the
raw events but `summary.md` does not surface per-peer dial counts. The
3-event drift is not attributed to a specific peer in the post-proc
output.

### 6. Process-level kernel network counters not captured

`body.process` is populated per snapshot. It does not include
`/proc/net/snmp`, `/proc/net/udp`, or per-interface RX/TX drop counts.
For stage-2 (537 holepunch attempts, 5 mapping failures), kernel-level
UDP error/drop counts that would distinguish "iroh sent and the OS
rejected" from "iroh sent and the path silently dropped" are not
present in the bundle.

### 7. No explicit gossip-arrival event on each stage

Stage-2's iroh `peers[]` contains only the orchestrator. Whether
stage-2 learned of `0cc5ed32` and `95d01a36` via `NameRegistry` gossip
but failed to dial them, or never received the gossip at all, is not
directly observable. `MessageReceived: 72` is recorded but is not
broken down by message type or source.

### 8. Bundle assembly requires finalize

`crates/distribution/src/diagnostics/collector/bundle.rs:43-67`
constructs `MANIFEST.json` and the tarball only on receipt of a
finalize record. This run's bundle was reconstructable only because
the collector retained staging files on disk. If the collector were
configured to delete staging files at a TTL shorter than the
operator's diagnostic latency, this bundle would not have been
recoverable.

### 9. `iroh_api_missing` event reports a stale version string

`crates/distribution/src/diagnostics/iroh_introspect.rs:237` emits
`iroh_version: "0.96"` as a literal. `Cargo.lock` shows `iroh 0.98.2`.
The `api_gaps` list at line 547 is computed against the 0.96
`RemoteInfo` shape; whether the same fields are still missing under
0.98 is not verified by the emitted event.

## Artifacts

In repo root after recovery:

```
vastai-N3-1779720002.tar.gz                   reshaped bundle (557 KB)
vastai-N3-1779720002.out/summary.md           postproc summary
vastai-N3-1779720002.out/reachability.tsv
vastai-N3-1779720002.out/timeline-*.tsv       12 per-link timelines
```

Staging copy on docean retained at
`/var/lib/swactor-diag/vastai-N3-1779720002/`.

## Infrastructure state at end of session

- docean (146.190.110.128): collector and relay processes running.
- vast.ai instances under `$VAST_API_KEY`: 0.
