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

## First peer to go Dead
- **orchestrator** marked **stage-1** (30303030…) Dead at t=5100 ms
  reason: "suspicion-timeout"
  observer side (orchestrator): conn_type=None
  peer side (stage-1): conn_type=Direct
  observer probes_ok_at_transition=yes
  peer probes_ok_at_transition=unknown

## Probe outcomes
- orchestrator: udp_echo/collector-udp-echo → ok (rtt=7ms, 3/3 ok)

## Event totals (by type)
- ConnectionCacheInvalidated: 1
- DialOutcome: 3
- DialStarted: 3
- MessageReceived: 4
- MessageSent: 3
- SwimTransition: 6
