# Diagnostics summary — `fixture-run-1`

## Run
- nodes:                3
- finalize_received:   true
- run_start_ms:        0
- run_end_ms:          0
- duration_ms:         0

## Nodes
- **orchestrator** (role=orchestrator, node_id=10101010…)
  snapshots=2, events=15, finalize_recorded=true
- **stage-0** (role=stage, node_id=20202020…)
  snapshots=1, events=3, finalize_recorded=false
- **stage-1** (role=stage, node_id=30303030…)
  snapshots=1, events=2, finalize_recorded=false

## Hosts
- orchestrator: rental=? ip=? dc=? country=? container=? hostname=? relay=? iroh=? git=?
- stage-0: rental=? ip=? dc=? country=? container=? hostname=? relay=? iroh=? git=?
- stage-1: rental=? ip=? dc=? country=? container=? hostname=? relay=? iroh=? git=?

## First peer to go Dead
- **orchestrator** marked **stage-1** (30303030…) Dead at t=5100 ms
  reason: "suspicion-timeout"
  observer side (orchestrator): conn_type=None
  peer side (stage-1): conn_type=Direct
  observer probes_ok_at_transition=yes
  peer probes_ok_at_transition=unknown

## Relay sessions
- No relay observability data in this bundle (gap 1). To enable: run `swactor-iroh-relay` with `SWACTOR_DIAG_COLLECTOR_URL` set so the relay reports into the same bundle as the nodes.

## Probe outcomes
- orchestrator: udp_echo/collector-udp-echo → ok (rtt=7ms, 3/3 ok)

## Probe RTT distribution
- No SWIM probe lifecycle events captured (gap 2.6 D/S layer not active for this run).

## Kernel network drops
- No non-zero UDP/interface drop deltas observed.

## Gossip receipts (by node, by kind)
- No GossipReceived events captured (no node ran a gossip-emitting source).

## Inference responses
- No InferenceResponseSent events captured (gap 2.4 D/S layer not active for this run).

## Bandwidth (by node, by kind and peer)
- by kind:
  - orchestrator recv pong × 2 (32 bytes)
  - orchestrator sent ping × 2 (64 bytes)
  - stage-0 recv ping × 1 (32 bytes)
  - stage-0 sent pong × 1 (16 bytes)
  - stage-1 recv ping × 1 (32 bytes)
- by peer:
  - orchestrator <- stage-0 × 1 (16 bytes)
  - orchestrator <- stage-1 × 1 (16 bytes)
  - orchestrator -> stage-0 × 1 (32 bytes)
  - orchestrator -> stage-1 × 1 (32 bytes)
  - stage-0 <- orchestrator × 1 (32 bytes)
  - stage-0 -> orchestrator × 1 (16 bytes)
  - stage-1 <- orchestrator × 1 (32 bytes)

## Per-peer dials
- totals: started=3, succeeded=2, failed=1, in-flight=0

| peer | started | succeeded | failed | in-flight | last_outcome | last_outcome_at_ms |
|------|---------|-----------|--------|-----------|--------------|--------------------|
| stage-0 | 1 | 1 | 0 | 0 | Success | 1012 |
| stage-1 | 2 | 1 | 1 | 0 | Timeout | 4000 |

## Event totals (by type)
- ConnectionCacheInvalidated: 1
- DialOutcome: 3
- DialStarted: 3
- MessageReceived: 4
- MessageSent: 3
- SwimTransition: 6
