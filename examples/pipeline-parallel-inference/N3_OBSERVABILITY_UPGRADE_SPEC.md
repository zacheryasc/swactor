# N=3 observability upgrade — behavioral spec

Sister doc to `N3_DATA_GAPS.md`. The gaps doc says *what's missing
and why we care*. This doc says *what the system must do once the
gaps are closed.*

Each section is a behavior contract: requirements the running
system has to satisfy after the work is done. Implementation
strategy — which crate, which file, which trait — is left to the
person picking up the work, except where a pattern is load-bearing
to the contract itself (the subprocess introspector is the one
explicit pattern requirement, called out below at the user's
direction).

Throughout: every "the bundle contains X" claim is testable. A
post-deployment run that doesn't satisfy these is a failed upgrade.

## Cross-cutting requirements

1. **Additive evolution.** A node running new code emits bundles
   that a post-processor built against old code can still parse —
   missing fields are absent, not malformed. Symmetrically, a
   post-processor built against new code reads an old bundle by
   showing the new fields as "absent" rather than erroring.

2. **Separation of lifecycle from state.** Anything that has a
   "moment it happened" is an event on the event stream. Anything
   that has a "current value" is a snapshot field. The same fact
   should not be reported both ways unless one is a counter and
   the other is a transition.

3. **Schema-version honesty.** Any version string the bundle
   carries about a dependency must reflect the dependency actually
   linked at build time. The bundle never contains a version
   string that disagrees with the lockfile.

4. **Generic over the use case.** Tier-3 capture surfaces (process,
   subprocess, host, etc.) are wired the same way as the existing
   `ProcessIntrospector`: a trait on the aggregator with a default
   production implementation and the ability to install a test
   fake without going through production paths. A new caller of
   `swactor` should be able to opt into the new surfaces with no
   knowledge of how data flows out.

5. **Boundary stays where it is today.** Generic observability
   primitives live in the distribution crate's diagnostics module.
   Role-specific decisions (which PIDs to register, which probes
   to install, which labels to use) live in the calling crate
   (`examples/pipeline-parallel-inference/...` for this codebase).

---

## 1. Relay observability (gap 1)

After this work, the bundle answers, for every relay-mediated
peer connection that died during a run:

- Who initiated the close: the relay, the remote node, or an idle
  timeout.
- What the close reason was, in a short string the relay assigned.
- How long the session had been open and how many bytes had
  crossed in each direction.
- The relay's own count of active sessions, opens, closes, and
  bytes transferred at end-of-run, broken down by close reason.

The bundle reader can answer "was this a relay-side eviction"
without consulting any external system, by reading the relay's
report and correlating it against the node-side
`connection_cache[peer].last_failure_reason` already in the
bundle.

The post-processor's summary surfaces this correlation per peer
in a "relay sessions" section. When the relay was not observed
(legacy run, relay observability not configured), the section
renders one line explaining that and pointing at this gap.

Acceptance: replay the 2026-05-25 incident with a new bundle.
The summary tells you who closed stage-2's session and why,
without further digging.

---

## 2. Relay-session vs. peer-connection separation (gap 2)

After this work, every snapshot a node emits carries an explicit
answer to "is my tunnel to my relay healthy right now," separate
from "do my peer connections through that tunnel work."

The field carries:
- The relay URL the node is currently using.
- A status (connected / connecting / disconnected / unknown).
- Wall-clock millis of the last status change and the moment the
  current status was entered.
- The last moment the node successfully sent over the tunnel and
  the last moment it received over it.
- Lifetime byte counters in each direction.

When the underlying transport library does not expose enough state
to populate the field truthfully, the snapshot must say so
explicitly: the status is `unknown`, a discriminator field
identifies the value as derived rather than reported, and the
existing `iroh_api_missing` event pattern records the gap by name.
A bundle reader must never have to guess whether `unknown` means
"the tunnel is unknown" vs. "we couldn't ask."

Acceptance: in the 2026-05-25 bundle's stage-2 snapshots, this
field reports either a real status ("disconnected" or "connected")
or `unknown` with `status_source: derived`. The investigator can
distinguish "tunnel alive but peer connection dead" from "tunnel
itself died" without speculation.

---

## 3. Per-transition relay events (gap 3)

After this work, every relay-related state flip produces an event
on the event stream, in addition to whatever counter increments.

Two kinds of flips are observable:
- **Relay session state changed**: the tunnel status field from
  section 2 moved between values. Event carries the relay URL,
  from-status, to-status, and a short reason string when one is
  available.
- **Relay home changed**: the node switched which relay it
  considers home. Event carries the from-URL and the to-URL.

Counters (e.g. `relay_home_change`) are retained for sanity-check
totals, but the per-transition event is the authoritative source.
A bundle reader can reconstruct the relay-state timeline of a
node by replaying the event stream, with no need to derive
transitions from counter deltas across snapshots.

Acceptance: in any run where a node experiences a relay flap, the
event stream contains at least one `RelaySessionStateChanged`
record. A grep for that event kind across the bundle tells you
which nodes flapped and when, with no other inputs.

---

## 4. Subprocess introspector (gap 4) — generic, through swactor

This is the largest section. The user's explicit requirement:
**the Python worker introspection must flow through swactor in a
generic way, like the existing process crate does** — meaning it
is not specific to "the Python worker" or "this example crate,"
but a reusable surface that any future user of `swactor_process`
can opt into.

### Behavior contract

After this work, every subprocess that a node owns via
`swactor_process` is reflected in the bundle on two channels,
identically to how the parent process is reflected today:

- **As snapshot state**: each periodic snapshot carries a
  per-subprocess entry with the subprocess's caller-supplied
  label, PID, parent PID, status (running / exited / unknown),
  spawn time, exit time and code/signal when applicable, RSS,
  virtual size, open FD count, CPU time, and a truncated
  command line.
- **As lifecycle events**: a `SubprocessSpawned` event fires when
  the subprocess starts, and a `SubprocessExited` event fires when
  it ends. Both carry the caller's label, the PID, the command,
  and (for exit) the exit code or terminating signal and uptime
  in millis.

Subprocess capture is a tier-3 surface alongside the existing
process-stats one. It is installed via an introspector trait on
the aggregator, with the same install pattern as today's
`ProcessIntrospector`, `HostIntrospector`, etc. A test can wire a
fake introspector without going through any production code path.

The capture surface is **stage-agnostic** and **worker-agnostic**:
it knows about a PID, a label, and a parent. The fact that "the
Python worker" is one such subprocess is a decision made at the
calling site, not in the introspector.

### Wiring contract — the swactor side

The `swactor_process` driver, when it spawns a child, must
publish the child's PID through its existing notification
channel. The data flow looks like:

1. The owning actor calls into `swactor_process` to spawn.
2. `swactor_process` reports the spawn outcome back through its
   existing notification mechanism, with the PID included.
3. The owning actor forwards "this PID, this label" into the
   subprocess introspector it owns.
4. The owning actor forwards "this PID has exited with this
   status" into the introspector on exit.

The actor's role in step 3-4 is intentionally minimal — a handful
of lines wrapping notifications it already receives. The
introspector does the actual `/proc` reading, lifecycle-event
emission, and snapshot population. A future swactor user gets
subprocess observability by installing the introspector at boot
and forwarding two notification kinds; nothing else.

### Lifecycle event coverage

The pre-existing ad-hoc `Custom { kind: "worker_starting" }` and
`Custom { kind: "worker_exited" }` strings in the example crate
are replaced by the typed `SubprocessSpawned` and
`SubprocessExited` events. The role-specific signal "the
subprocess has produced its first protocol output and is
functioning" (currently `worker_ready`) stays a `Custom` event
because functioning-as-a-pipeline-worker is not a generic
subprocess concept.

### What this gives us for the next investigation

For a stage that didn't start its worker, the bundle now tells us
unambiguously which of three things happened:

- The actor never reached its `on_start` and the subprocess was
  never asked to spawn. No `SubprocessSpawned`. The bug is in
  actor scheduling.
- The subprocess spawned and exited immediately. Both events
  present, with exit code and the existing stderr tail available.
  The bug is in the subprocess itself.
- The subprocess spawned and stayed alive but never produced
  protocol output. `SubprocessSpawned` present, no
  `SubprocessExited`, no `worker_ready` Custom event, and the
  per-snapshot RSS/CPU on the subprocess show whether it's stuck
  or thrashing. The bug is in the subprocess's startup logic
  before its first protocol line.

These three were indistinguishable in the 2026-05-25 bundle.
They are immediately distinguishable after this work.

Acceptance: in any future deployment, a stage that fails to
produce inference output can be classified into one of those
three buckets by reading the bundle alone.

---

## 5. Host metadata forwarding (gap 5)

After this work, every node's boot record carries the physical
host context the node is running on:

- Public IP of the rental.
- Datacenter id and country reported by the cloud provider.
- The provider's identifier for the rental (e.g. vast.ai instance
  id) — enough to re-rent or correlate against provider-side
  logs.
- The hostname as the container sees it.
- The relay URL the node was configured with at boot.
- The git SHA the binary was built from.
- The version string of the underlying transport library, taken
  from what is actually linked (see gap 6).

When a node runs outside the orchestrator's lease flow (e.g. a
locally-launched node for development), the cloud-provider fields
are absent rather than blank or wrong. The bundle reader can tell
"this node was not on vast.ai" from "this node was on vast.ai but
metadata wasn't forwarded" — the former leaves fields absent, the
latter is no longer a possible state.

The post-processor's summary lists each node's host context one
line per node, so "which rental was stage-2" is answerable
without grep.

Acceptance: replay the 2026-05-25 incident's recovery process.
Identifying stage-2's host requires reading one line of the
summary, not cross-referencing provider records.

---

## 6. Iroh API version sanity (gap 6)

After this work:

- The `iroh_api_missing` event reports the version of the
  transport library actually linked into the binary. The version
  string is sourced from the build, not a literal.
- Every tier-2 transport snapshot carries the same version string
  as a field, so a bundle reader does not need to scan the event
  stream to know what version the node ran.
- The list of "API gaps" — fields the bundle reader should treat
  as "we couldn't ask" rather than "we asked and got zero" —
  reflects what the linked version actually omits. Upgrading to a
  version that exposes a previously-missing field causes the gap
  to disappear from the bundle automatically; no code change is
  needed to recompute the list.

Acceptance: bumping the iroh dependency to a version that exposes
`conn_type` produces a bundle whose `api_gaps` no longer mentions
`conn_type`, without any other change.

---

## 7. Bundle assembly without finalize (gap 7)

After this work:

- A bundle is retrievable for any run that has at least one boot
  record in staging, regardless of whether the orchestrator sent
  a finalize record. `GET /diag/bundle/<run_id>` succeeds in both
  cases.
- The retrieved bundle's manifest explicitly states whether
  finalize was received. Bundle readers must not have to guess.
- When finalize was received, the bundle is the canonical one and
  serving it is cheap. When it wasn't, the bundle is synthesized
  at request time from staging files; the latency is fine because
  unfinalized bundles are by definition retrieved during incident
  response.
- Staging files for runs that never finalized are retained at
  least until the operator has had a reasonable window to
  retrieve them (default: 30 days), bounded by a hard
  disk-space cap that trims oldest-first when exceeded.

The hand-rolled recovery process used for the 2026-05-25 incident
(tar staging from the collector, scp it down, reshape, retar) is
no longer needed for any future incident, regardless of how the
orchestrator died.

Acceptance: kill an orchestrator with SIGKILL mid-run. A subsequent
`GET /diag/bundle/<run_id>` returns a usable bundle with
`finalize_received: false` in its manifest.

---

## 8. Relay-port reachability probe (gap 8)

After this work, every node periodically attempts a transport-level
reachability check against the relay's actual port, and reports
the outcome in the same snapshot probe array as the existing UDP
echo. The probe's existence does not require operator
configuration: when the node has been told a relay URL, the relay
probe is automatically registered.

The probe's outcome distinguishes:
- Reached and responded ("ok").
- Reached, no response within deadline ("timeout").
- Host reachable, port closed ("refused").
- Could not resolve target ("unresolved").
- Other error ("error").

A bundle reader can answer "could stage-2 reach the relay port at
moment T" by reading stage-2's probe array around T, without
inferring reachability from a different probe to a different port
on the same host.

Acceptance: a node placed behind a firewall that blocks the relay
port but not the existing UDP echo port produces a bundle in
which the relay probe consistently reports `refused` or `timeout`
while the UDP echo continues to report `ok`.

---

## 9. Per-peer dial rollup in summary (gap 9)

After this work, the post-processor's summary contains, per peer
in the run, a row listing:

- Total dials started against that peer.
- Total successful dials.
- Total failed dials.
- The last dial outcome (string) and its wall-clock millis.

The 3-event drift in the 2026-05-25 bundle (`DialStarted: 83`,
`DialOutcome: 80`) is attributable to specific peers in the
table; the reader can immediately tell which peers' dials never
completed.

This is a pure post-processor change — the raw events are already
in the bundle. No new fields, no new events.

Acceptance: re-run the post-processor against the existing
2026-05-25 bundle. The summary contains a per-peer dial table
that accounts for all 83 `DialStarted` events.

---

## 10. Gossip-receipt event (gap 10)

After this work, every time a node receives a payload through the
gossip / dissemination layer — name-registry update, SWIM
membership piggyback, anything similar — it emits a typed event
on its event stream. The event carries the source peer, the
payload kind (string, extensible), the payload size in bytes, and
the number of items inside.

The existing coarse `MessageReceived` counter remains for backward
compatibility, but the new event is the authoritative source for
"did node X ever hear about name Y from peer Z."

The post-processor's summary, per node, reports the total receipt
counts broken down by payload kind. "Stage-2 never received any
name-registry gossip from anyone" is a one-line answer.

Acceptance: in any run where one node fails to learn about
another node's registered name, the bundle distinguishes
unambiguously whether the gossip was never received vs. received
and ignored.

---

## 11. Kernel network counters (gap 11)

After this work, every host-scrape snapshot carries kernel-level
UDP and per-interface counters:

- UDP-side: aggregate packets in/out, drops attributable to
  no-listening-port, packets discarded due to errors, packets
  lost to socket buffer overflow.
- Per-interface: rx/tx bytes, rx/tx dropped, rx/tx errors.

A bundle reader can compute deltas across consecutive snapshots
to attribute packet loss to one of three layers:
- "Iroh sent and the OS dropped it" — UDP send error counters
  rise on the sender.
- "OS sent it and the path silently lost it" — sender counters
  clean, receiver counters clean.
- "It arrived and got dropped at the receiver's NIC" — receiver
  interface drop counters rise.

All counters are best-effort: absent on non-Linux hosts, absent
when the file can't be read, never silently zero. The
post-processor's summary surfaces any node whose UDP-drop or
interface-drop deltas are non-zero across the run window, so the
reader doesn't have to inspect every snapshot.

Acceptance: a node deliberately subjected to UDP-drop-rate
injection produces a bundle whose summary highlights it with the
correct counter rising.

---

## Sim cross-pollination

The behavioral contracts above also constrain the simulator. A
node simulated by the sim should produce snapshots and events
that conform to the same shape as a real node — the bundle reader
should not be able to tell from the data shape alone whether a
given snapshot came from a real deployment or the sim.

Three areas where today's sim lags this contract and must catch up
as part of the same upgrade:

- The sim must model a relay actor whose behavior produces the
  same tunnel-status field (gap 2) on simulated nodes. Without
  this, sim runs of cluster scenarios are not bundle-shape
  compatible with real ones.
- The sim must support installing a subprocess introspector fake
  (gap 4). Scenarios that want to model "a stage's worker never
  came up" wire this fake to produce a `SubprocessSpawned` with
  no following `worker_ready` Custom event.
- The sim's network failure model must allow "tunnel up,
  peer-connection-via-tunnel down" as a distinct failure case
  from "tunnel down." Without it the sim cannot reproduce the
  exact 2026-05-25 failure even after the observability lands.

These are sim-side work, not data-collection work, but they
share the data model defined here.

---

## Implementation order

Grouped by independence. Within a group, work is parallel-safe;
across groups, later groups don't depend on earlier groups
*finishing*, only on earlier groups' contracts being agreed.

**Group A — small, independent, unblock confidence elsewhere**
- 5 (host metadata) — small and pure-mechanical
- 6 (iroh version sanity) — small, but until it lands, every
  iroh-side field in the bundle has a credibility asterisk
- 9 (per-peer dial rollup) — pure post-processor
- 11 (kernel counters) — additive host-scrape extension

**Group B — relay tier**
- 1 (relay observability) — the largest single info gain
- 2 (relay-session field) — depends on having something to
  populate it from, ideally the work in 1
- 3 (relay events) — depends on 2's status field existing

**Group C — subprocess tier**
- 4 (subprocess introspector + events) — independent of B,
  parallel-safe with it

**Group D — collector robustness**
- 7 (bundle without finalize) — independent of all the above;
  land last to avoid churning the collector while other tiers
  are still moving

**Group E — polish**
- 8 (relay-port probe) — small, independent
- 10 (gossip-receipt event) — small, independent

The 2026-05-25 investigation would have been closeable with
A + B + C alone. D + E reduce future investigation cost but
weren't load-bearing for the failure we hit.
