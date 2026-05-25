# N=3 data-coverage gaps

Companion to `N3_POSTMORTEM_2026-05-25.md`. Where the postmortem
documents what we *do* know about the failure, this doc is about the
things we *don't* — and why we should care. Input for the
data-collection upgrade.

The framing is investigator-first: each gap is named for the question
we couldn't answer, not the file that doesn't emit the field.

## The investigation we couldn't finish

Walking back from the symptom — orchestrator's relay-mediated path to
stage-2 died at ~5 s, never recovered, stage-2 went silent — the chain
of questions we'd want to answer is roughly:

1. Did stage-2's underlying relay *tunnel* to docean stay up, or did
   it drop too?
2. If the tunnel stayed up, why didn't iroh re-establish the
   peer-to-peer path?
3. If the tunnel dropped, who closed it (relay vs. stage-2's iroh vs.
   the OS), and why?
4. Was stage-2's host network actually broken at that moment, or was
   this a software-level failure on a working network?
5. Independent of all of the above: why did stage-2 never start its
   Python worker, when stage-0 and stage-1 both did within seconds?

We could not answer **any** of these from the bundle. Each one is
blocked by a specific missing data source.

## The gaps, ranked by how much they hurt this investigation

### 1. The relay is a black box

The biggest single hole. `swactor-iroh-relay` on docean produced
nothing that ended up in the bundle: no session log, no metrics
scrape, no log tail, no record of which node connected, when, how
long, and what closed each session.

The orchestrator's local cache says
`last_failure_reason: "connection-closed"`. That string is iroh's
report of what *iroh* observed at the application layer. It doesn't
tell us whether the relay terminated the session, whether the QUIC
stack on either end did, or whether a NAT mapping expired and the
relay noticed first.

> **What this blocks:** distinguishing a relay-side eviction from an
> endpoint-side close from a path-level timeout. Three very different
> root causes, indistinguishable in the bundle.

### 2. Relay session and peer connection are conflated

`body.iroh.metrics.socket.relay_home_change` is a counter that
increments when a node changes its home relay. `num_conns_opened` and
`num_conns_closed` are counters for iroh peer connections. None of
these tell us, per moment, whether a given node's **tunnel to its
relay** is up.

This matters because of the asymmetry we hit: from stage-2's view
nothing closed (counters quiescent, `relay_home_change: 1` for the
whole run), but the orchestrator-side cache shows the connection
through the relay dying after 5 s. We have no way, from stage-2's
data alone, to say whether its relay tunnel was actually still alive
when the peer connection died.

> **What this blocks:** answering "did stage-2's tunnel survive?" —
> the question that decides whether we're looking at a network
> problem or an iroh state-machine problem.

### 3. No event when a relay path is established, lost, or replaced

We have snapshot counters but no event stream for relay-path
transitions. `RelayChanged` event count across all four nodes for the
whole run: zero. If iroh internally noticed and recovered a relay
session inside one snapshot interval, we'd never see it. If iroh
*didn't* notice a dead session, we equally can't see that.

This is the "no log line for the interesting moment" problem. The
counter says the final state; we want the transitions.

> **What this blocks:** correlating the moment of failure with what
> iroh thought was happening. Right now the only event-stream
> evidence is the orchestrator's connect-timeout retries, which is a
> downstream symptom.

### 4. The Python worker subprocess is invisible until it emits

Stage-2 emitted zero `worker_starting` and zero `worker_ready`
events. Stage-0 and stage-1 emitted both within seconds of boot.
Whatever happened to stage-2's worker — never spawned, spawned and
crashed before its first event, spawned but blocked — left no trace
in our bundle. Stage-2's node process was clearly alive (23
snapshots, 38 event batches), so it isn't a node-process crash.

We don't capture:
- the moment the stage actor decides to spawn the worker
- the subprocess pid, exit code, or stderr tail
- whether the stage actor was *gating* worker spawn on something
  (cluster membership? a peer dial?) that never happened

This is a separate failure from the relay flap, possibly with a
common upstream cause, possibly not. We can't tell.

> **What this blocks:** deciding whether to focus the fix on
> transport, on the stage actor's startup ordering, or on worker
> launch itself.

### 5. We don't know what host stage-2 was on

`boot.json` carries `container_id`, `datacenter_id`, `host_country`,
`host_ip_public`, `hostname`, `home_relay_url_at_boot`, `git_sha`,
`iroh_version` — all null except `hostname`, which is a Docker short
id. The orchestrator already has the public IP, datacenter id, and
country for each rental at the point `lease_chain` returns. None of
that is forwarded into the container or persisted into the boot
snapshot.

So when we say "stage-2's vast.ai rental had a hostile NAT," we
literally cannot point at the machine. We can't re-rent the same host
to reproduce, we can't compare it against the hosts that *did* work,
we can't even tell you which country it was in.

> **What this blocks:** any kind of fleet-level statistics across
> runs ("which datacenters fail more often"), and the ability to
> reproduce the bad rental.

### 6. Iroh introspection is computed against the wrong API version

The `iroh_api_missing` event reports `iroh_version: "0.96"` as a
literal string. The lockfile is `iroh 0.98.2`. The list of
"missing" fields is whatever was missing in 0.96 — we have no idea
what 0.98 actually exposes, because we never checked.

So when stage-2's snapshot reports
`observed_conn_type_at_last_use: "None"`, we don't know whether
that's "iroh told us None" or "we couldn't read the field because
we're holding a 0.96 shape against a 0.98 struct."

> **What this blocks:** trusting any of the per-peer iroh state in
> the bundle. This is corrosive — it undermines the whole iroh
> tier of evidence.

### 7. Bundle assembly is finalize-or-nothing

The collector only writes `MANIFEST.json` and the tarball when the
orchestrator sends a finalize record. SIGKILL skipped that, so
`GET /diag/bundle/<run_id>` returned 404. The bundle we analyzed
was hand-reconstructed from staging files we got to before the
collector's TTL cleaned them up.

A real operator hitting a real production incident is going to kill
things ungracefully. The "we got lucky" failure mode here is bad
enough that we should treat the staging directory as the source of
truth and have finalize be an optimization, not a precondition.

> **What this blocks:** any incident bundle from a hard-killed run.

### 8. Reachability probes only cover one port

We probe UDP echo to `:9081` on docean. Stage-2 timed out 1 of 12.
We don't probe `:7843` (the relay's actual port). So when the relay
session dies, we can't say "but the host could still reach the relay
port at that moment" — only "but the host could still reach a
different port on the same machine."

> **What this blocks:** ruling out transport-level reachability as
> the cause of relay session death.

### 9. No event-level breakdown of dials by peer

We have `DialStarted: 83` and `DialOutcome: 80` as raw event counts.
The 3-event drift is not attributed to a specific peer in
`summary.md`. With three peers it's easy enough to grep manually,
but the summary should be doing this for us, especially at higher N
where per-peer asymmetry is the whole story.

> **What this blocks:** at-a-glance answer to "which peer was hard
> to reach," which is the first question for any cluster failure.

### 10. No gossip-arrival evidence on the silent node

Stage-2's `peers[]` contained only the orchestrator. We don't know
whether stage-2 received `NameRegistry` gossip about its siblings
and failed to dial, or never received the gossip at all. The bundle
has `MessageReceived: 72` for stage-2 but the breakdown isn't
recorded.

> **What this blocks:** distinguishing a control-plane failure
> (gossip didn't arrive) from a data-plane failure (dials based on
> gossip didn't connect).

### 11. No kernel-level network counters

`/proc/net/snmp`, `/proc/net/udp`, per-interface drop counts — none
captured. For stage-2, with 537 holepunch attempts and 5 reported
mapping failures, we can't tell "iroh sent and the OS dropped it"
from "iroh sent and the OS accepted it and the path silently lost
it." These are at the edge of what's worth collecting — modest cost
per snapshot, but the cases where they matter are real.

> **What this blocks:** distinguishing iroh-layer pathology from
> host-network pathology when the two look identical from above.

## What this looks like in priority order

If we only get to fix a few of these for the next deployment:

**Must-have to investigate another N=3 failure:**
- gap 1 (relay-side data)
- gap 4 (worker subprocess visibility)
- gap 5 (host metadata forwarding)
- gap 7 (bundle assembly without finalize)
- gap 6 (iroh API version sanity check)

**Strong-have:**
- gap 3 (relay-path transition events)
- gap 2 (relay-tunnel-state field, separable from peer state)
- gap 10 (gossip-receipt event)

**Nice-to-have:**
- gap 8 (relay-port probe)
- gap 9 (per-peer dial rollup in summary)
- gap 11 (kernel counters)

The "must-haves" are the ones where, looking back at this bundle,
the absence actually prevented a conclusion. The rest would have
made the investigation faster but weren't strictly load-bearing.

## What this implies for the sim

A separate concern that overlaps: most of these gaps are real-network
gaps that the sim doesn't model at all. The sim doesn't have a
relay, doesn't model NAT-mapping behavior, doesn't model
relay-session-up-but-peer-connection-down asymmetry, and doesn't
distinguish kernel-level packet loss from iroh-level path failure.

If we want the sim to reproduce a failure like this one, the data
model the sim exposes has to be at least as rich as the data the
postmortem needed to read — otherwise "we reproduced it in sim"
won't actually mean we understand it. Whatever fields we add to the
bundle should land in the sim's per-tick state too.
