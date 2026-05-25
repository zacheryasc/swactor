# Hole-punching never succeeded — N3 bundle re-analysis

Date: 2026-05-22. Bundles: `vastai-N3-1`, `vastai-N3-2`, `vastai-N3-stub` from
`docean:/var/lib/swactor-diag/bundles/`.

## Finding

**No node ever held a stable direct path.** Across the 430 s N3-stub run,
every directed edge was `Relay` in every snapshot. The single non-Relay
sighting in all three bundles was stage-0→orchestrator in N3-1 going
`Relay → Mixed → Relay` over a ~24 s window, then collapsing; the reverse
direction never saw Mixed, so it was likely a transient asymmetric reading,
not a real two-way direct path. **Stage↔stage was 100% Relay in every run.**

## Suspected cause: symmetric (endpoint-dependent) NAT on vast.ai

Last-snapshot iroh socket counters, N3-stub:

| node         | holepunch_attempts | paths_direct | send_ipv4 | **recv_data_ipv4** |
|--------------|-------------------:|-------------:|----------:|-------------------:|
| orchestrator |                  0 |            0 |   199 072 |              **0** |
| stage-0      |              3 220 |            0 |   255 282 |              **0** |
| stage-1      |              1 072 |            0 |   152 515 |              **0** |
| stage-2      |              3 068 |            0 |   241 752 |              **0** |

Cluster-wide: ~7 360 hole-punch attempts, ~850 K direct-path datagrams sent,
**zero received**. `actor_tick_direct_addr_heartbeat = 0` everywhere (the
direct-path keepalive never ticked, because no direct path ever validated).
`portmap.upnp_available = 0`, `pcp_available = 0`, `mapping_failures ≈
mapping_attempts` on every stage — no NAT control protocol reachable inside
vast.ai containers, so iroh can't request a stable external port.

Ruled out as causes:

- **Address discovery worked.** `net_report.reports_full ≥ 1` on every node;
  thousands of hole-punch attempts means stages had remote candidates from
  the relay's signalling path.
- **UDP isn't blocked.** Every node's `udp_echo` probes to the VPS at
  146.190.110.128:9081 had 100% reply success. Outbound delivers; return on a
  pre-opened mapping delivers.
- **Relay path is fine.** All 8 `DialOutcome` events in N3-stub are `Success`
  in 1.2–1.9 s. iroh just never upgrades from Relay to Direct.

The signature — outbound delivers, return-on-existing-mapping delivers,
return-on-newly-punched-port never delivers — is the classic
endpoint-dependent-mapping fingerprint. vast.ai's container egress NAT
appears to pick a different external source port per destination, so the
candidate stage-A learned about stage-B (the port B used reflecting off the
VPS) is not the port B uses sending to A.

## Implication

Between vast.ai stages, Direct is structurally unreliable on this provider.
The relay is the path, not a fallback. Provision relay bandwidth/headroom
accordingly and size SWIM timeouts around relay RTT.

## Data gaps — what would convert inference to proof

1. **`Tier2Peer.direct_addresses` in snapshots.** Today the peer object
   carries only `conn_type` and `relay_urls`. Adding iroh's
   `direct_addresses` list (from `Endpoint::remote_info()`) would let us see
   which candidates each side learned for each peer, instead of inferring
   "they had some" from `holepunch_attempts > 0`.

2. **UDP-echo source-port reflection.** Have the collector's UDP echo include
   the observed `srcAddr:srcPort` in its reply (currently opaque). Probe from
   each node to two collector destinations and compare external ports — same
   port = endpoint-independent, different = symmetric. One number per node
   would answer the NAT-type question definitively rather than by signature.

3. **Per-edge bandwidth as a packaged metric.** Today reconstructed post-hoc
   by summing `MessageSent.size`/`MessageReceived.size` per `(local, peer)`.
   Either ship `Tier2Peer { bytes_sent_total, bytes_received_total }` (small
   per-peer accumulator on the aggregator; hooks exist in `iroh_driver.rs`),
   or document the post-hoc derivation so future analyses don't re-discover
   it.
