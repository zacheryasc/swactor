//! Assertion evaluator (SIM_SPEC §10).
//!
//! Reads a finished §9 bundle from disk, evaluates every assertion the
//! scenario declared, and produces `verdicts.json` in the same bundle.
//! Also exposes a streaming side that consumes events in order and
//! resolves verdicts as soon as the prefix determines them (§10.4).
//!
//! ## Event and snapshot schema
//!
//! The §9 bundle's `events.ndjson` envelope is `(virtual_time_ns,
//! host_id, kind_tag, event)`. The evaluator looks at `event["kind"]`
//! as the inner discriminator and assumes the payload conforms to the
//! following MVP shapes:
//!
//! - `state_transition`: `{kind, peer, observed_by, from, to}`. Used by
//!   `all_alive_at`, `no_flap_while_probes_ok`, `no_dead_when_probes_ok`,
//!   `dead_peer_resurrects_within`.
//! - `message_send`: `{kind, from, to, message_kind, bytes}`. Used by
//!   `message_size_bounded`.
//! - `probe_sent` / `probe_received` / `probe_timed_out`: `{kind, from,
//!   to}`. Used by the `*_probes_ok` family.
//! - `self_incarnation_bump`: `{kind, peer, to}`. Used by
//!   `self_incarnation_bounded`.
//!
//! Snapshot payload: `{members: {peer_id: {state, incarnation}},
//! self_incarnation}`. Used by `all_alive_at`,
//! `all_alive_throughout`, and `convergence_after`.
//!
//! This is a strict subset of what production diagnostics will emit;
//! the SWIM host adapter conforms to it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::scenario::{Assertion, AssertionKind, HostKindRegistry, Scenario, load_from_str};

// ──────────────────────────────────────────────────────────────────────
// Verdict surface
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum Outcome {
    Pass,
    Fail,
    Inconclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Evidence {
    pub virtual_time_ns: u64,
    pub event_or_snapshot_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Verdict {
    pub name: String,
    pub kind: &'static str,
    pub parameters: serde_json::Value,
    pub outcome: Outcome,
    pub evidence: Vec<Evidence>,
}

// ──────────────────────────────────────────────────────────────────────
// Post-run entry point
// ──────────────────────────────────────────────────────────────────────

/// Evaluate every assertion the scenario declared against the bundle
/// at `bundle_root` and write `verdicts.json`. Returns the verdicts
/// in scenario-declaration order.
pub fn evaluate_bundle(bundle_root: &Path) -> std::io::Result<Vec<Verdict>> {
    let events_path = bundle_root.join("events.ndjson");
    let scenario_path = bundle_root.join("scenario.toml");
    let snapshots_root = bundle_root.join("snapshots");

    let scenario_text = fs::read_to_string(&scenario_path)?;
    let scenario = load_from_str(&scenario_path, &scenario_text, &HostKindRegistry::with_swim())
        .map_err(|e| std::io::Error::other(format!("scenario reload: {e}")))?;

    let events = read_events(&events_path)?;
    let snapshots = read_snapshots(&snapshots_root)?;

    let verdicts = evaluate(&scenario, &events, &snapshots);

    // Write verdicts.json.
    let out_text = serde_json::to_string_pretty(&verdicts)
        .expect("Verdict serialises to JSON by construction");
    fs::write(bundle_root.join("verdicts.json"), out_text)?;

    Ok(verdicts)
}

/// Evaluate against pre-parsed inputs. Tests use this to avoid
/// touching disk.
pub fn evaluate(
    scenario: &Scenario,
    events: &[EventLine],
    snapshots: &SnapshotIndex,
) -> Vec<Verdict> {
    scenario
        .assertions
        .iter()
        .enumerate()
        .map(|(idx, a)| evaluate_one(idx, a, events, snapshots))
        .collect()
}

// ──────────────────────────────────────────────────────────────────────
// Streaming side (SIM_SPEC §10.4)
// ──────────────────────────────────────────────────────────────────────

/// Streaming evaluator: feed it events in time order, ask for resolved
/// verdicts after each feed. The MVP implementation re-runs the
/// post-run evaluator over the accumulated prefix on each call — that
/// preserves the "streaming agrees with post-run" property without the
/// per-kind state machine each assertion would otherwise need.
///
/// Inefficient by design; an §11-era optimisation, not an MVP one.
pub struct StreamingEvaluator {
    scenario: Scenario,
    events: Vec<EventLine>,
    snapshots: SnapshotIndex,
}

impl StreamingEvaluator {
    pub fn new(scenario: Scenario) -> Self {
        Self {
            scenario,
            events: Vec::new(),
            snapshots: SnapshotIndex::default(),
        }
    }

    pub fn feed_event(&mut self, e: EventLine) {
        self.events.push(e);
    }

    pub fn feed_snapshot(&mut self, host_id: String, ev: SnapshotEntry) {
        self.snapshots
            .by_host
            .entry(host_id)
            .or_default()
            .push(ev);
    }

    /// Returns the current per-assertion verdicts. Verdicts may flip
    /// from `Inconclusive` to `Pass`/`Fail` (but never back) as more
    /// events arrive. The semantics is that a verdict reported here
    /// agrees with the eventual post-run verdict only if it is
    /// determinable from the prefix; the simple implementation
    /// follows from re-evaluating each call, which is consistent by
    /// construction.
    pub fn verdicts_now(&self) -> Vec<Verdict> {
        evaluate(&self.scenario, &self.events, &self.snapshots)
    }

    /// Are all per-assertion verdicts resolved (Pass or Fail) in the
    /// current prefix? Used by the engine for early termination.
    pub fn all_resolved(&self) -> bool {
        self.verdicts_now()
            .iter()
            .all(|v| !matches!(v.outcome, Outcome::Inconclusive))
    }
}

// ──────────────────────────────────────────────────────────────────────
// Parsed event / snapshot index
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct EventLine {
    pub virtual_time_ns: u64,
    pub host_id: Option<String>,
    pub kind_tag: String,
    pub event: serde_json::Value,
    /// The line index this event occupies in `events.ndjson`, used in
    /// evidence references.
    pub line_idx: usize,
}

impl EventLine {
    /// Build an `EventLine` from an in-engine `EventRecord`. The
    /// streaming-side engine wiring uses this to feed records into
    /// the streaming evaluator without round-tripping through disk.
    pub fn from_event_record(
        rec: &crate::bundle::EventRecord,
        line_idx: usize,
    ) -> Self {
        Self {
            virtual_time_ns: rec.virtual_time_ns,
            host_id: rec.host_id.clone(),
            kind_tag: rec.kind_tag.clone(),
            event: payload_to_json(&rec.event),
            line_idx,
        }
    }

    /// Build an `EventLine` from an in-engine `MutationRecord`.
    pub fn from_mutation_record(
        rec: &crate::bundle::MutationRecord,
        line_idx: usize,
    ) -> Self {
        Self {
            virtual_time_ns: rec.virtual_time_ns,
            host_id: None,
            kind_tag: "mutation".to_string(),
            event: serde_json::to_value(&rec.mutation).unwrap_or(serde_json::Value::Null),
            line_idx,
        }
    }
}

fn payload_to_json(p: &crate::bundle::EventPayload) -> serde_json::Value {
    use crate::bundle::{DeliveryDropReason, EventPayload};
    use crate::network::{CacheTransition, DropReason};
    match p {
        EventPayload::Bytes(b) => serde_json::from_slice(b).unwrap_or(serde_json::Value::Null),
        EventPayload::DropOnSend { from, to, reason } => serde_json::json!({
            "kind": "drop_on_send",
            "from": from,
            "to": to,
            "reason": match reason {
                DropReason::NoRoute => "no_route",
                DropReason::Partitioned => "partitioned",
                DropReason::Lossy => "lossy",
            },
        }),
        EventPayload::DropOnDelivery { to, reason } => serde_json::json!({
            "kind": "drop_on_delivery",
            "to": to,
            "reason": match reason {
                DeliveryDropReason::HostHalted => "host_halted",
                DeliveryDropReason::HostKilled => "host_killed",
                DeliveryDropReason::Partition => "partition",
            },
        }),
        EventPayload::CacheStateChange { from, to, transition } => serde_json::json!({
            "kind": "cache_state_change",
            "from": from,
            "to": to,
            "transition": match transition {
                CacheTransition::Warmed => "warmed",
                CacheTransition::IdleCooled => "idle_cooled",
                CacheTransition::Invalidated => "invalidated",
            },
        }),
        EventPayload::DialStart { from, to } => serde_json::json!({
            "kind": "dial_start",
            "from": from,
            "to": to,
        }),
        EventPayload::DialOutcome { from, to, warm } => serde_json::json!({
            "kind": "dial_outcome",
            "from": from,
            "to": to,
            "warm": warm,
        }),
    }
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotIndex {
    /// Per-host list, sorted by `virtual_time_ns`.
    pub by_host: BTreeMap<String, Vec<SnapshotEntry>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotEntry {
    pub virtual_time_ns: u64,
    pub seq: u32,
    pub members: BTreeMap<String, MemberView>,
    pub self_incarnation: u64,
}

impl SnapshotEntry {
    /// Build a `SnapshotEntry` from an in-engine `SnapshotRecord`.
    /// Used by the streaming-side engine wiring. The seq field is
    /// per-host monotonic; the engine assigns it via a separate
    /// counter.
    pub fn from_snapshot_record(rec: &crate::bundle::SnapshotRecord, seq: u32) -> Self {
        let parsed: serde_json::Value =
            serde_json::from_slice(&rec.snapshot).unwrap_or(serde_json::Value::Null);
        let members = parse_members(&parsed["members"]);
        let self_incarnation = parsed["self_incarnation"].as_u64().unwrap_or(0);
        Self {
            virtual_time_ns: rec.virtual_time_ns,
            seq,
            members,
            self_incarnation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberView {
    pub state: String, // "Alive" | "Suspect" | "Dead"
    pub incarnation: u64,
}

fn read_events(path: &Path) -> std::io::Result<Vec<EventLine>> {
    let text = fs::read_to_string(path)?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| std::io::Error::other(format!("events.ndjson line {i}: {e}")))?;
        out.push(parse_event_line(i, &v));
    }
    Ok(out)
}

fn parse_event_line(idx: usize, v: &serde_json::Value) -> EventLine {
    let virtual_time_ns = v["virtual_time_ns"].as_u64().unwrap_or(0);
    let host_id = v["host_id"].as_str().map(|s| s.to_string());
    let kind_tag = v["kind_tag"].as_str().unwrap_or("").to_string();
    let event = v["event"].clone();
    EventLine {
        virtual_time_ns,
        host_id,
        kind_tag,
        event,
        line_idx: idx,
    }
}

fn read_snapshots(root: &Path) -> std::io::Result<SnapshotIndex> {
    let mut idx = SnapshotIndex::default();
    if !root.exists() {
        return Ok(idx);
    }
    for host_entry in fs::read_dir(root)? {
        let host_entry = host_entry?;
        if !host_entry.file_type()?.is_dir() {
            continue;
        }
        let host_id = host_entry.file_name().into_string().unwrap_or_default();
        let mut entries: Vec<(u32, PathBuf)> = Vec::new();
        for snap in fs::read_dir(host_entry.path())? {
            let snap = snap?;
            if !snap.file_type()?.is_file() {
                continue;
            }
            let stem = snap
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
                .unwrap_or_default();
            let Ok(seq) = stem.parse::<u32>() else {
                continue;
            };
            entries.push((seq, snap.path()));
        }
        entries.sort_by_key(|(seq, _)| *seq);
        let mut list = Vec::new();
        for (seq, path) in entries {
            let text = fs::read_to_string(&path)?;
            let outer: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| std::io::Error::other(format!("snapshot {path:?}: {e}")))?;
            let virtual_time_ns = outer["virtual_time_ns"].as_u64().unwrap_or(0);
            let snapshot = &outer["snapshot"];
            let members = parse_members(&snapshot["members"]);
            let self_incarnation = snapshot["self_incarnation"].as_u64().unwrap_or(0);
            list.push(SnapshotEntry {
                virtual_time_ns,
                seq,
                members,
                self_incarnation,
            });
        }
        list.sort_by_key(|e| e.virtual_time_ns);
        idx.by_host.insert(host_id, list);
    }
    Ok(idx)
}

fn parse_members(v: &serde_json::Value) -> BTreeMap<String, MemberView> {
    let mut out = BTreeMap::new();
    if let Some(obj) = v.as_object() {
        for (peer, mv) in obj {
            let state = mv["state"].as_str().unwrap_or("").to_string();
            let incarnation = mv["incarnation"].as_u64().unwrap_or(0);
            out.insert(peer.clone(), MemberView { state, incarnation });
        }
    }
    out
}

// ──────────────────────────────────────────────────────────────────────
// Per-assertion evaluation
// ──────────────────────────────────────────────────────────────────────

fn evaluate_one(
    idx: usize,
    a: &Assertion,
    events: &[EventLine],
    snapshots: &SnapshotIndex,
) -> Verdict {
    let name = format!("a{idx}");
    let params = serde_json::to_value(&a.kind).unwrap_or(serde_json::Value::Null);
    let (kind, (outcome, evidence)) = match &a.kind {
        AssertionKind::AllAliveAt { at_ns, peers } => {
            ("all_alive_at", eval_all_alive_at(*at_ns, peers, snapshots))
        }
        AssertionKind::AllAliveThroughout {
            window_start_ns,
            window_end_ns,
            peers,
        } => (
            "all_alive_throughout",
            eval_all_alive_throughout(*window_start_ns, *window_end_ns, peers, snapshots),
        ),
        AssertionKind::ConvergenceAfter {
            after_ns,
            within_ns,
            peers,
        } => (
            "convergence_after",
            eval_convergence_after(*after_ns, *within_ns, peers, snapshots),
        ),
        AssertionKind::NoFlapWhileProbesOk {
            peer,
            window_start_ns,
            window_end_ns,
        } => (
            "no_flap_while_probes_ok",
            eval_no_flap(peer, *window_start_ns, *window_end_ns, events),
        ),
        AssertionKind::NoDeadWhenProbesOk {
            peer,
            window_start_ns,
            window_end_ns,
        } => (
            "no_dead_when_probes_ok",
            eval_no_dead(peer, *window_start_ns, *window_end_ns, events),
        ),
        AssertionKind::SelfIncarnationBounded { peer, max_value } => (
            "self_incarnation_bounded",
            eval_self_incarnation_bounded(peer, *max_value, events, snapshots),
        ),
        AssertionKind::MessageSizeBounded {
            message_kind,
            max_bytes,
        } => (
            "message_size_bounded",
            eval_message_size_bounded(message_kind, *max_bytes, events),
        ),
        AssertionKind::DeadPeerResurrectsWithin {
            peer,
            after_ns,
            within_ns,
        } => (
            "dead_peer_resurrects_within",
            eval_dead_peer_resurrects(peer, *after_ns, *within_ns, events),
        ),
        AssertionKind::EventCount { event_kind, max } => (
            "event_count",
            eval_event_count(event_kind, *max, events),
        ),
        AssertionKind::EventRate {
            event_kind,
            window_ns,
            max_per_window,
        } => (
            "event_rate",
            eval_event_rate(event_kind, *window_ns, *max_per_window, events),
        ),
    };
    Verdict {
        name,
        kind,
        parameters: params,
        outcome,
        evidence,
    }
}

type Eval = (Outcome, Vec<Evidence>);

fn pass() -> Eval {
    (Outcome::Pass, Vec::new())
}
fn fail(evidence: Vec<Evidence>) -> Eval {
    (Outcome::Fail, evidence)
}
fn inconclusive() -> Eval {
    (Outcome::Inconclusive, Vec::new())
}

fn ev_event(e: &EventLine) -> Evidence {
    Evidence {
        virtual_time_ns: e.virtual_time_ns,
        event_or_snapshot_ref: format!("events.ndjson:{}", e.line_idx),
    }
}
fn ev_snapshot(host: &str, s: &SnapshotEntry) -> Evidence {
    Evidence {
        virtual_time_ns: s.virtual_time_ns,
        event_or_snapshot_ref: format!("snapshots/{host}/{}.json", s.seq),
    }
}

// ── all_alive_at ────────────────────────────────────────────────────

fn eval_all_alive_at(at_ns: u64, peers: &[String], snapshots: &SnapshotIndex) -> Eval {
    // Take the latest snapshot from each named peer at or before at_ns.
    // If any peer never produced a snapshot ≤ at_ns ⇒ Inconclusive.
    // If any membership view names any peer in `peers` as non-Alive ⇒ Fail.
    let mut evidence_fail = Vec::new();
    let mut any_seen = false;
    for observer in peers {
        let Some(list) = snapshots.by_host.get(observer) else {
            return inconclusive();
        };
        let snap = list.iter().filter(|s| s.virtual_time_ns <= at_ns).next_back();
        let Some(snap) = snap else {
            return inconclusive();
        };
        any_seen = true;
        for subject in peers {
            if observer == subject {
                continue;
            }
            match snap.members.get(subject) {
                Some(mv) if mv.state == "Alive" => {}
                _ => {
                    evidence_fail.push(ev_snapshot(observer, snap));
                }
            }
        }
    }
    if !any_seen {
        return inconclusive();
    }
    if evidence_fail.is_empty() {
        pass()
    } else {
        fail(evidence_fail)
    }
}

// ── all_alive_throughout ────────────────────────────────────────────

fn eval_all_alive_throughout(
    start: u64,
    end: u64,
    peers: &[String],
    snapshots: &SnapshotIndex,
) -> Eval {
    let mut any_in_window = false;
    let mut evidence_fail = Vec::new();
    for observer in peers {
        let Some(list) = snapshots.by_host.get(observer) else {
            return inconclusive();
        };
        for snap in list.iter().filter(|s| s.virtual_time_ns >= start && s.virtual_time_ns <= end) {
            any_in_window = true;
            for subject in peers {
                if observer == subject {
                    continue;
                }
                match snap.members.get(subject) {
                    Some(mv) if mv.state == "Alive" => {}
                    _ => evidence_fail.push(ev_snapshot(observer, snap)),
                }
            }
        }
    }
    if !any_in_window {
        return inconclusive();
    }
    if evidence_fail.is_empty() {
        pass()
    } else {
        fail(evidence_fail)
    }
}

// ── convergence_after ───────────────────────────────────────────────

fn eval_convergence_after(
    after: u64,
    within: u64,
    peers: &[String],
    snapshots: &SnapshotIndex,
) -> Eval {
    // Find a virtual time t in (after, after+within] such that every
    // peer's snapshot at-or-before t has the same membership *view*
    // over `peers`. If found ⇒ Pass; if no peer has any snapshot in
    // the window ⇒ Inconclusive; otherwise Fail.
    let deadline = after.saturating_add(within);
    let mut peer_snapshots: BTreeMap<&String, Vec<&SnapshotEntry>> = BTreeMap::new();
    for p in peers {
        let Some(list) = snapshots.by_host.get(p) else {
            return inconclusive();
        };
        let in_window: Vec<&SnapshotEntry> = list
            .iter()
            .filter(|s| s.virtual_time_ns > after && s.virtual_time_ns <= deadline)
            .collect();
        peer_snapshots.insert(p, in_window);
    }
    if peer_snapshots.values().all(|v| v.is_empty()) {
        return inconclusive();
    }
    // Collect every distinct time we have a snapshot at.
    let mut times: BTreeSet<u64> = BTreeSet::new();
    for v in peer_snapshots.values() {
        for s in v {
            times.insert(s.virtual_time_ns);
        }
    }
    for t in &times {
        // For each subject, do all observers (peers other than the
        // subject) agree on the subject's state at-or-before t?
        let mut all_present = true;
        let mut converged = true;
        for subject in peers {
            let mut observed_states: Vec<String> = Vec::new();
            for observer in peers {
                if observer == subject {
                    continue;
                }
                let Some(list) = snapshots.by_host.get(observer) else {
                    all_present = false;
                    break;
                };
                let snap = list.iter().filter(|s| s.virtual_time_ns <= *t).next_back();
                let Some(snap) = snap else {
                    all_present = false;
                    break;
                };
                let state = snap
                    .members
                    .get(subject)
                    .map(|m| m.state.clone())
                    .unwrap_or_else(|| "Unknown".to_string());
                observed_states.push(state);
            }
            if !all_present {
                break;
            }
            if observed_states.windows(2).any(|w| w[0] != w[1]) {
                converged = false;
                break;
            }
        }
        if all_present && converged {
            return pass();
        }
    }
    // No convergence in window.
    let mut evidence = Vec::new();
    for p in peers {
        if let Some(list) = snapshots.by_host.get(p) {
            if let Some(last) = list.iter().filter(|s| s.virtual_time_ns <= deadline).next_back() {
                evidence.push(ev_snapshot(p, last));
            }
        }
    }
    fail(evidence)
}

// ── no_flap_while_probes_ok / no_dead_when_probes_ok ────────────────

fn eval_no_flap(peer: &str, start: u64, end: u64, events: &[EventLine]) -> Eval {
    // Probes-ok ⇔ for every probe_sent there is a probe_received (or
    // probe_timed_out is absent) in the window. Flap = Suspect → Alive
    // → Suspect on `peer`. If no probes happened ⇒ Inconclusive.
    let (probes_ok, any_probe) = probes_ok_in_window(peer, start, end, events);
    if !any_probe {
        return inconclusive();
    }
    if !probes_ok {
        return inconclusive();
    }
    let mut seq: Vec<(u64, &EventLine, &str)> = Vec::new();
    for e in events
        .iter()
        .filter(|e| e.virtual_time_ns >= start && e.virtual_time_ns <= end)
    {
        if e.event["kind"] == "state_transition" && e.event["peer"] == peer {
            let to = e.event["to"].as_str().unwrap_or("");
            seq.push((e.virtual_time_ns, e, to));
        }
    }
    // Walk for Suspect → Alive → Suspect.
    let mut last_two: Vec<&str> = Vec::new();
    let mut evidence = Vec::new();
    for (_t, e, to) in &seq {
        last_two.push(to);
        if last_two.len() >= 3 {
            let n = last_two.len();
            if last_two[n - 3] == "Suspect" && last_two[n - 2] == "Alive" && last_two[n - 1] == "Suspect" {
                evidence.push(ev_event(e));
            }
        }
    }
    if evidence.is_empty() {
        pass()
    } else {
        fail(evidence)
    }
}

fn eval_no_dead(peer: &str, start: u64, end: u64, events: &[EventLine]) -> Eval {
    let (probes_ok, any_probe) = probes_ok_in_window(peer, start, end, events);
    if !any_probe {
        return inconclusive();
    }
    if !probes_ok {
        return inconclusive();
    }
    let mut evidence = Vec::new();
    for e in events
        .iter()
        .filter(|e| e.virtual_time_ns >= start && e.virtual_time_ns <= end)
    {
        if e.event["kind"] == "state_transition" && e.event["peer"] == peer && e.event["to"] == "Dead" {
            evidence.push(ev_event(e));
        }
    }
    if evidence.is_empty() {
        pass()
    } else {
        fail(evidence)
    }
}

fn probes_ok_in_window(peer: &str, start: u64, end: u64, events: &[EventLine]) -> (bool, bool) {
    // bidirectional: probes_sent_to_peer == probes_received_from_peer
    // and no probe_timed_out for peer in the window.
    let mut any = false;
    let mut sent_to = 0u64;
    let mut received_from = 0u64;
    let mut timed_out = 0u64;
    for e in events
        .iter()
        .filter(|e| e.virtual_time_ns >= start && e.virtual_time_ns <= end)
    {
        match e.event["kind"].as_str() {
            Some("probe_sent") if e.event["to"] == peer => {
                sent_to += 1;
                any = true;
            }
            Some("probe_received") if e.event["from"] == peer => {
                received_from += 1;
                any = true;
            }
            Some("probe_timed_out") if e.event["to"] == peer || e.event["from"] == peer => {
                timed_out += 1;
                any = true;
            }
            _ => {}
        }
    }
    let ok = timed_out == 0 && sent_to == received_from && sent_to > 0;
    (ok, any)
}

// ── self_incarnation_bounded ────────────────────────────────────────

fn eval_self_incarnation_bounded(
    peer: &str,
    max_value: u64,
    events: &[EventLine],
    snapshots: &SnapshotIndex,
) -> Eval {
    let mut evidence = Vec::new();
    let mut any = false;
    for e in events {
        if e.event["kind"] == "self_incarnation_bump" && e.event["peer"] == peer {
            any = true;
            let to = e.event["to"].as_u64().unwrap_or(0);
            if to > max_value {
                evidence.push(ev_event(e));
            }
        }
    }
    if let Some(list) = snapshots.by_host.get(peer) {
        for snap in list {
            any = true;
            if snap.self_incarnation > max_value {
                evidence.push(ev_snapshot(peer, snap));
            }
        }
    }
    if !any {
        return inconclusive();
    }
    if evidence.is_empty() {
        pass()
    } else {
        fail(evidence)
    }
}

// ── message_size_bounded ────────────────────────────────────────────

fn eval_message_size_bounded(message_kind: &str, max_bytes: u64, events: &[EventLine]) -> Eval {
    let mut evidence = Vec::new();
    let mut any = false;
    for e in events {
        if e.event["kind"] == "message_send" && e.event["message_kind"] == message_kind {
            any = true;
            let bytes = e.event["bytes"].as_u64().unwrap_or(0);
            if bytes > max_bytes {
                evidence.push(ev_event(e));
            }
        }
    }
    if !any {
        return inconclusive();
    }
    if evidence.is_empty() {
        pass()
    } else {
        fail(evidence)
    }
}

// ── dead_peer_resurrects_within ─────────────────────────────────────

fn eval_dead_peer_resurrects(peer: &str, after: u64, within: u64, events: &[EventLine]) -> Eval {
    // Find the last time peer transitioned to Dead after `after`; then
    // look for a transition to Alive within `within` ns.
    let mut last_dead_t: Option<&EventLine> = None;
    for e in events {
        if e.virtual_time_ns >= after
            && e.event["kind"] == "state_transition"
            && e.event["peer"] == peer
            && e.event["to"] == "Dead"
        {
            last_dead_t = Some(e);
        }
    }
    let Some(dead_e) = last_dead_t else {
        return inconclusive();
    };
    let deadline = dead_e.virtual_time_ns.saturating_add(within);
    for e in events {
        if e.virtual_time_ns > dead_e.virtual_time_ns
            && e.virtual_time_ns <= deadline
            && e.event["kind"] == "state_transition"
            && e.event["peer"] == peer
            && e.event["to"] == "Alive"
        {
            return pass();
        }
    }
    fail(vec![ev_event(dead_e)])
}

// ── event_count ─────────────────────────────────────────────────────

fn eval_event_count(event_kind: &str, max: u64, events: &[EventLine]) -> Eval {
    let count: u64 = events
        .iter()
        .filter(|e| e.event["kind"] == event_kind)
        .count() as u64;
    if count <= max {
        pass()
    } else {
        let evidence: Vec<Evidence> = events
            .iter()
            .filter(|e| e.event["kind"] == event_kind)
            .map(ev_event)
            .collect();
        fail(evidence)
    }
}

// ── event_rate ──────────────────────────────────────────────────────

fn eval_event_rate(
    event_kind: &str,
    window_ns: u64,
    max_per_window: u64,
    events: &[EventLine],
) -> Eval {
    if window_ns == 0 {
        return inconclusive();
    }
    let matching: Vec<&EventLine> = events
        .iter()
        .filter(|e| e.event["kind"] == event_kind)
        .collect();
    if matching.is_empty() {
        // No firings ⇒ rate is trivially 0 ⇒ Pass.
        return pass();
    }
    // Sliding window: for each event, count matching events with time in
    // [t, t + window_ns).
    let mut evidence = Vec::new();
    for (i, lead) in matching.iter().enumerate() {
        let window_end = lead.virtual_time_ns.saturating_add(window_ns);
        let count: u64 = matching[i..]
            .iter()
            .take_while(|e| e.virtual_time_ns < window_end)
            .count() as u64;
        if count > max_per_window {
            evidence.push(ev_event(lead));
        }
    }
    if evidence.is_empty() {
        pass()
    } else {
        fail(evidence)
    }
}
