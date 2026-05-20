//! Renderers — bundle in, plain text out.
//!
//! Three target formats:
//!
//! - **`render_summary`**: a one-page Markdown bulletin that names the
//!   first peer to go Dead (and why), each side's iroh `conn_type` at
//!   that moment, and whether raw UDP probes were succeeding.
//! - **`render_reachability_tsv`**: a row per (observer, snapshot)
//!   with the observer's SWIM opinion of every peer in the run. Lets
//!   the reader trace "when did node A start thinking node B was
//!   Dead?"
//! - **`render_timeline_tsv`**: per ordered peer-pair (`a` → `b`), the
//!   chronological list of events where either side touched the other.
//!
//! Plus **`render_diff`**: a textual comparison of two bundles —
//! useful for "did this rerun reproduce the failure?"

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

use crate::diagnostics::event::{Event, PeerState};
use crate::diagnostics::reachability::node_id_hex;
use crate::diagnostics::snapshot::Snapshot;

use super::parse::{Bundle, NodeData};

// ============================================================
// Summary
// ============================================================

/// Render the one-page Markdown bulletin.
///
/// The rendered text is intentionally pure ASCII (no fancy unicode
/// glyphs) so it diffs cleanly between runs and renders identically
/// in any terminal.
pub fn render_summary(bundle: &Bundle) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Diagnostics summary — `{}`", bundle.run_id);
    let _ = writeln!(out);

    // -- Run-level facts --
    let _ = writeln!(out, "## Run");
    let _ = writeln!(
        out,
        "- nodes:                {}",
        bundle.manifest.nodes.len()
    );
    let _ = writeln!(
        out,
        "- finalize_received:   {}",
        bundle.manifest.finalize_received
    );
    if let Some(start) = bundle.manifest.run_start_collector_ms {
        let _ = writeln!(out, "- run_start_ms:        {start}");
    }
    if let Some(end) = bundle.manifest.run_end_collector_ms {
        let _ = writeln!(out, "- run_end_ms:          {end}");
    }
    if let (Some(start), Some(end)) = (
        bundle.manifest.run_start_collector_ms,
        bundle.manifest.run_end_collector_ms,
    ) {
        let _ = writeln!(out, "- duration_ms:         {}", end.saturating_sub(start));
    }
    let _ = writeln!(out);

    // -- Per-node summary --
    let _ = writeln!(out, "## Nodes");
    for node in &bundle.manifest.nodes {
        let data = bundle.nodes.get(&node.label);
        let snaps = data.map(|d| d.snapshots.len()).unwrap_or(0);
        let events = data.map(|d| d.events.len()).unwrap_or(0);
        let finalize = node.finalize_recorded;
        let role = node.role.as_deref().unwrap_or("?");
        let _ = writeln!(
            out,
            "- **{}** (role={role}, node_id={}…)",
            node.label,
            short_hex(&node.node_id_hex)
        );
        let _ = writeln!(
            out,
            "  snapshots={snaps}, events={events}, finalize_recorded={finalize}"
        );
    }
    let _ = writeln!(out);

    // -- First-Dead analysis --
    let _ = writeln!(out, "## First peer to go Dead");
    match first_dead_transition(bundle) {
        Some(d) => render_first_dead_block(bundle, &d, &mut out),
        None => {
            let _ = writeln!(
                out,
                "- No `SwimTransition -> Dead` was observed in this bundle."
            );
        }
    }
    let _ = writeln!(out);

    // -- Probe summary --
    let _ = writeln!(out, "## Probe outcomes");
    let probe_lines = probe_summary_lines(bundle);
    if probe_lines.is_empty() {
        let _ = writeln!(out, "- No tier-3 probe data captured.");
    } else {
        for line in probe_lines {
            let _ = writeln!(out, "- {line}");
        }
    }
    let _ = writeln!(out);

    // -- Event totals by type --
    let _ = writeln!(out, "## Event totals (by type)");
    let totals = event_totals(bundle);
    if totals.is_empty() {
        let _ = writeln!(out, "- No events captured.");
    } else {
        for (kind, count) in totals {
            let _ = writeln!(out, "- {kind}: {count}");
        }
    }

    out
}

/// What we learned from the first SWIM `-> Dead` transition.
#[derive(Debug, Clone)]
struct FirstDead {
    observer_label: String,
    /// Peer label (or hex fallback).
    peer_label: String,
    /// Hex of the peer the observer marked Dead.
    peer_hex: String,
    at_ms: u64,
    reason: String,
}

fn first_dead_transition(bundle: &Bundle) -> Option<FirstDead> {
    let mut earliest: Option<FirstDead> = None;
    for (label, node) in &bundle.nodes {
        for rec in &node.events {
            let Event::SwimTransition { peer, to, reason, .. } = &rec.event else {
                continue;
            };
            if *to != PeerState::Dead {
                continue;
            }
            let peer_hex = node_id_hex(peer);
            let candidate = FirstDead {
                observer_label: label.clone(),
                peer_label: bundle.label_for_hex(&peer_hex),
                peer_hex,
                at_ms: rec.wall_ms,
                reason: reason.clone(),
            };
            earliest = Some(match earliest {
                Some(prev) if prev.at_ms <= candidate.at_ms => prev,
                _ => candidate,
            });
        }
    }
    earliest
}

fn render_first_dead_block(bundle: &Bundle, d: &FirstDead, out: &mut String) {
    let _ = writeln!(
        out,
        "- **{observer}** marked **{peer}** ({peer_hex}…) Dead at t={at_ms} ms",
        observer = d.observer_label,
        peer = d.peer_label,
        peer_hex = short_hex(&d.peer_hex),
        at_ms = d.at_ms,
    );
    let _ = writeln!(out, "  reason: \"{}\"", d.reason);
    // Each side's iroh conn_type at the moment of the transition,
    // looked up from the nearest snapshot at or before `at_ms`.
    let observer_view = side_view(bundle, &d.observer_label, &d.peer_hex, d.at_ms);
    let _ = writeln!(
        out,
        "  observer side ({}): conn_type={}",
        d.observer_label, observer_view.conn_type
    );
    let peer_view = side_view(bundle, &d.peer_label, &observer_hex(bundle, &d.observer_label), d.at_ms);
    let _ = writeln!(
        out,
        "  peer side ({}): conn_type={}",
        d.peer_label, peer_view.conn_type
    );
    let _ = writeln!(
        out,
        "  observer probes_ok_at_transition={}",
        yes_no_unknown(observer_view.any_probe_ok)
    );
    let _ = writeln!(
        out,
        "  peer probes_ok_at_transition={}",
        yes_no_unknown(peer_view.any_probe_ok)
    );
}

fn observer_hex(bundle: &Bundle, observer_label: &str) -> String {
    bundle
        .nodes
        .get(observer_label)
        .map(|n| n.node_id_hex.clone())
        .unwrap_or_default()
}

#[derive(Debug, Default)]
struct SideView {
    /// `"Direct" | "Relay" | "Mixed" | "None" | "unknown"`.
    conn_type: String,
    /// True/False/None: was *any* probe `last_outcome == "ok"` at the
    /// nearest snapshot at or before the moment of interest?
    any_probe_ok: Option<bool>,
}

fn side_view(bundle: &Bundle, observer_label: &str, peer_hex: &str, at_ms: u64) -> SideView {
    let Some(node) = bundle.nodes.get(observer_label) else {
        return SideView {
            conn_type: "unknown".into(),
            any_probe_ok: None,
        };
    };
    let snap = nearest_snapshot(&node.snapshots, at_ms);
    let conn_type = snap
        .and_then(|s| s.body.iroh.as_ref())
        .and_then(|iroh| {
            iroh.peers
                .iter()
                .find(|p| p.peer_node_id_hex.eq_ignore_ascii_case(peer_hex))
                .and_then(|p| p.conn_type)
                .map(|c| format!("{c:?}"))
        })
        .unwrap_or_else(|| "unknown".to_string());
    let any_probe_ok = snap
        .and_then(|s| s.body.probes.as_ref())
        .map(|p| p.probes.iter().any(|probe| probe.last_outcome == "ok"));
    SideView {
        conn_type,
        any_probe_ok,
    }
}

fn nearest_snapshot(snaps: &[Snapshot], t: u64) -> Option<&Snapshot> {
    snaps.iter().min_by_key(|s| {
        if s.wall_ms >= t {
            s.wall_ms - t
        } else {
            t - s.wall_ms
        }
    })
}

fn probe_summary_lines(bundle: &Bundle) -> Vec<String> {
    let mut out = Vec::new();
    for (label, node) in &bundle.nodes {
        let Some(latest) = node.snapshots.iter().rev().find(|s| s.body.probes.is_some()) else {
            continue;
        };
        let Some(probes) = latest.body.probes.as_ref() else {
            continue;
        };
        if probes.probes.is_empty() {
            out.push(format!("{label}: no probe targets registered"));
            continue;
        }
        for probe in &probes.probes {
            let rtt = match probe.last_rtt_ms {
                Some(ms) => format!("{ms}ms"),
                None => "-".to_string(),
            };
            out.push(format!(
                "{label}: {kind}/{target} → {outcome} (rtt={rtt}, {ok}/{attempts} ok)",
                kind = probe.kind,
                target = probe.target,
                outcome = probe.last_outcome,
                ok = probe.successes,
                attempts = probe.attempts,
            ));
        }
    }
    out.sort();
    out
}

fn event_totals(bundle: &Bundle) -> Vec<(String, u64)> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for node in bundle.nodes.values() {
        for rec in &node.events {
            *counts.entry(event_kind(&rec.event)).or_insert(0) += 1;
        }
    }
    counts.into_iter().collect()
}

fn event_kind(event: &Event) -> String {
    match event {
        Event::SwimTransition { .. } => "SwimTransition".into(),
        Event::DialStarted { .. } => "DialStarted".into(),
        Event::DialOutcome { .. } => "DialOutcome".into(),
        Event::IrohConnTypeChanged { .. } => "IrohConnTypeChanged".into(),
        Event::RelayChanged { .. } => "RelayChanged".into(),
        Event::SwimMetadataSent { .. } => "SwimMetadataSent".into(),
        Event::SwimMetadataReceived { .. } => "SwimMetadataReceived".into(),
        Event::ConnectionCacheHit { .. } => "ConnectionCacheHit".into(),
        Event::ConnectionCacheMiss { .. } => "ConnectionCacheMiss".into(),
        Event::ConnectionCacheInvalidated { .. } => "ConnectionCacheInvalidated".into(),
        Event::NodeMapUpdate { .. } => "NodeMapUpdate".into(),
        Event::MessageSent { .. } => "MessageSent".into(),
        Event::MessageReceived { .. } => "MessageReceived".into(),
        Event::ProbeSent { .. } => "ProbeSent".into(),
        Event::ProbeReceived { .. } => "ProbeReceived".into(),
        Event::Error { .. } => "Error".into(),
        Event::Custom { kind, .. } => format!("Custom({kind})"),
    }
}

fn short_hex(hex: &str) -> String {
    if hex.len() >= 8 {
        hex[..8].to_string()
    } else {
        hex.to_string()
    }
}

fn yes_no_unknown(b: Option<bool>) -> &'static str {
    match b {
        Some(true) => "yes",
        Some(false) => "no",
        None => "unknown",
    }
}

// ============================================================
// Reachability matrix
// ============================================================

/// Render the N×N reachability matrix as TSV.
///
/// Rows are sorted by `time_ms` (the snapshot's `wall_ms`), then by
/// observer label. Each row carries the observer's SWIM opinion of
/// every peer that appears anywhere in the bundle (so columns are
/// stable across rows).
///
/// First row is a header beginning with `time_ms\tobserver\t...`.
pub fn render_reachability_tsv(bundle: &Bundle) -> String {
    let peer_labels: Vec<String> = bundle
        .manifest
        .nodes
        .iter()
        .map(|n| n.label.clone())
        .collect();
    let label_by_hex: BTreeMap<String, String> = bundle
        .manifest
        .nodes
        .iter()
        .map(|n| (n.node_id_hex.to_lowercase(), n.label.clone()))
        .collect();

    let mut rows: Vec<MatrixRow> = Vec::new();
    for (label, node) in &bundle.nodes {
        for snap in &node.snapshots {
            let mut opinions: BTreeMap<String, String> = BTreeMap::new();
            for peer in &snap.body.reachability {
                let hex = peer.peer_node_id_hex.to_lowercase();
                let peer_label = label_by_hex
                    .get(&hex)
                    .cloned()
                    .unwrap_or_else(|| format!("node-{}", short_hex(&hex)));
                opinions.insert(peer_label, format!("{:?}", peer.current_swim_opinion));
            }
            rows.push(MatrixRow {
                time_ms: snap.wall_ms,
                observer: label.clone(),
                opinions,
            });
        }
    }
    rows.sort_by(|a, b| a.time_ms.cmp(&b.time_ms).then(a.observer.cmp(&b.observer)));

    let mut out = String::new();
    out.push_str("time_ms\tobserver");
    for peer in &peer_labels {
        out.push('\t');
        out.push_str(peer);
    }
    out.push('\n');
    for row in &rows {
        let _ = write!(out, "{}\t{}", row.time_ms, row.observer);
        for peer in &peer_labels {
            out.push('\t');
            let cell = if peer == &row.observer {
                "-".to_string()
            } else {
                row.opinions.get(peer).cloned().unwrap_or_else(|| "?".into())
            };
            out.push_str(&cell);
        }
        out.push('\n');
    }
    out
}

#[derive(Debug)]
struct MatrixRow {
    time_ms: u64,
    observer: String,
    /// peer_label -> swim opinion
    opinions: BTreeMap<String, String>,
}

// ============================================================
// Per-peer-pair timeline
// ============================================================

/// Render the timeline of events between `a_label` and `b_label`.
///
/// Includes events recorded by either side that name the other; rows
/// are sorted chronologically by `wall_ms`. Columns:
/// `time_ms\tsource\tkind\tpeer\tdetails`.
pub fn render_timeline_tsv(bundle: &Bundle, a_label: &str, b_label: &str) -> String {
    let mut out = String::new();
    out.push_str("time_ms\tsource\tkind\tpeer\tdetails\n");
    let Some(a) = bundle.nodes.get(a_label) else {
        return out;
    };
    let Some(b) = bundle.nodes.get(b_label) else {
        return out;
    };
    let b_hex = b.node_id_hex.to_lowercase();
    let a_hex = a.node_id_hex.to_lowercase();

    let mut rows: Vec<TimelineRow> = Vec::new();
    extract_pair_events(a, &b_hex, b_label, &mut rows);
    extract_pair_events(b, &a_hex, a_label, &mut rows);
    rows.sort_by(|x, y| x.time_ms.cmp(&y.time_ms).then(x.source.cmp(&y.source)));

    for row in &rows {
        let _ = writeln!(
            out,
            "{}\t{}\t{}\t{}\t{}",
            row.time_ms, row.source, row.kind, row.peer, row.details
        );
    }
    out
}

#[derive(Debug)]
struct TimelineRow {
    time_ms: u64,
    source: String,
    kind: String,
    peer: String,
    details: String,
}

fn extract_pair_events(
    source_node: &NodeData,
    other_hex: &str,
    other_label: &str,
    out: &mut Vec<TimelineRow>,
) {
    for rec in &source_node.events {
        if let Some((kind, details)) = describe_event_about(&rec.event, other_hex) {
            out.push(TimelineRow {
                time_ms: rec.wall_ms,
                source: source_node.label.clone(),
                kind,
                peer: other_label.to_string(),
                details,
            });
        }
    }
}

/// Returns `Some((kind, details))` if the event involves `peer_hex`.
fn describe_event_about(event: &Event, peer_hex: &str) -> Option<(String, String)> {
    fn matches(peer_id: &crate::types::NodeId, target: &str) -> bool {
        node_id_hex(peer_id).eq_ignore_ascii_case(target)
    }
    match event {
        Event::SwimTransition {
            peer, from, to, reason,
        } if matches(peer, peer_hex) => Some((
            "SwimTransition".into(),
            format!("{from:?} -> {to:?} ({reason})"),
        )),
        Event::DialStarted { peer, attempt, timeout_ms } if matches(peer, peer_hex) => Some((
            "DialStarted".into(),
            format!("attempt={attempt} timeout_ms={timeout_ms}"),
        )),
        Event::DialOutcome {
            peer, attempt, outcome, duration_ms,
        } if matches(peer, peer_hex) => Some((
            "DialOutcome".into(),
            format!("attempt={attempt} outcome={outcome:?} duration_ms={duration_ms}"),
        )),
        Event::IrohConnTypeChanged { peer, old, new } if matches(peer, peer_hex) => Some((
            "IrohConnTypeChanged".into(),
            format!("{old:?} -> {new:?}"),
        )),
        Event::SwimMetadataReceived {
            peer, version, payload_hash,
        } if matches(peer, peer_hex) => Some((
            "SwimMetadataReceived".into(),
            format!("version={version} hash={payload_hash:#x}"),
        )),
        Event::ConnectionCacheHit { peer, generation } if matches(peer, peer_hex) => Some((
            "ConnectionCacheHit".into(),
            format!("generation={generation}"),
        )),
        Event::ConnectionCacheMiss { peer } if matches(peer, peer_hex) => {
            Some(("ConnectionCacheMiss".into(), String::new()))
        }
        Event::ConnectionCacheInvalidated {
            peer, generation, reason,
        } if matches(peer, peer_hex) => Some((
            "ConnectionCacheInvalidated".into(),
            format!("generation={generation} reason={reason}"),
        )),
        Event::NodeMapUpdate {
            peer, from_source, accepted,
        } if matches(peer, peer_hex) => Some((
            "NodeMapUpdate".into(),
            format!("from={from_source} accepted={accepted}"),
        )),
        Event::MessageSent { peer, kind, size } if matches(peer, peer_hex) => Some((
            "MessageSent".into(),
            format!("kind={kind} size={size}"),
        )),
        Event::MessageReceived { peer, kind, size } if matches(peer, peer_hex) => Some((
            "MessageReceived".into(),
            format!("kind={kind} size={size}"),
        )),
        Event::Error { peer: Some(peer), component, message } if matches(peer, peer_hex) => Some((
            "Error".into(),
            format!("component={component} message={message}"),
        )),
        _ => None,
    }
}

// ============================================================
// Diff
// ============================================================

/// Diff two bundles.
///
/// What this picks up:
/// - Manifest-level differences (node set, finalize_received).
/// - Per-node event-type counts (which event types appeared more or
///   fewer times).
/// - The summary of the first-Dead transition on each side.
///
/// Output is plain text, one finding per line, prefixed with `=`,
/// `+`, or `-` so it diffs cleanly.
pub fn render_diff(a: &Bundle, b: &Bundle) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "## Bundle diff: `{}` vs `{}`",
        a.run_id, b.run_id
    );

    // Node set diff.
    let labels_a: BTreeSet<&str> = a.manifest.nodes.iter().map(|n| n.label.as_str()).collect();
    let labels_b: BTreeSet<&str> = b.manifest.nodes.iter().map(|n| n.label.as_str()).collect();
    let only_a: Vec<&&str> = labels_a.difference(&labels_b).collect();
    let only_b: Vec<&&str> = labels_b.difference(&labels_a).collect();
    if only_a.is_empty() && only_b.is_empty() {
        let _ = writeln!(out, "= nodes: same set ({:?})", labels_a);
    } else {
        for label in only_a {
            let _ = writeln!(out, "- node only in `{}`: {}", a.run_id, label);
        }
        for label in only_b {
            let _ = writeln!(out, "+ node only in `{}`: {}", b.run_id, label);
        }
    }

    // Finalize bit.
    if a.manifest.finalize_received != b.manifest.finalize_received {
        let _ = writeln!(
            out,
            "* finalize_received differs: {} -> {}",
            a.manifest.finalize_received, b.manifest.finalize_received
        );
    }

    // Event totals per node label.
    let shared: BTreeSet<&str> = labels_a.intersection(&labels_b).copied().collect();
    for label in &shared {
        let na = a.nodes.get(*label);
        let nb = b.nodes.get(*label);
        let counts_a = node_event_counts(na);
        let counts_b = node_event_counts(nb);
        let kinds: BTreeSet<&str> = counts_a
            .keys()
            .chain(counts_b.keys())
            .map(String::as_str)
            .collect();
        for kind in kinds {
            let ca = counts_a.get(kind).copied().unwrap_or(0);
            let cb = counts_b.get(kind).copied().unwrap_or(0);
            if ca != cb {
                let _ = writeln!(out, "* {label}.{kind}: {ca} -> {cb}");
            }
        }
    }

    // First-Dead.
    let fd_a = first_dead_transition(a);
    let fd_b = first_dead_transition(b);
    match (&fd_a, &fd_b) {
        (None, None) => {
            let _ = writeln!(out, "= first_dead: neither bundle observed a Dead transition");
        }
        (Some(d), None) => {
            let _ = writeln!(
                out,
                "- first_dead only in `{}`: {} marked {} Dead at t={}ms",
                a.run_id, d.observer_label, d.peer_label, d.at_ms
            );
        }
        (None, Some(d)) => {
            let _ = writeln!(
                out,
                "+ first_dead only in `{}`: {} marked {} Dead at t={}ms",
                b.run_id, d.observer_label, d.peer_label, d.at_ms
            );
        }
        (Some(da), Some(db)) => {
            if da.observer_label != db.observer_label || da.peer_label != db.peer_label {
                let _ = writeln!(
                    out,
                    "* first_dead pair changed: ({}->{}) -> ({}->{})",
                    da.observer_label, da.peer_label, db.observer_label, db.peer_label
                );
            } else if da.at_ms != db.at_ms {
                let _ = writeln!(
                    out,
                    "* first_dead at_ms shifted: {} -> {} ({}->{})",
                    da.at_ms, db.at_ms, da.observer_label, da.peer_label
                );
            } else {
                let _ = writeln!(
                    out,
                    "= first_dead: {} marked {} Dead at t={}ms (both bundles)",
                    da.observer_label, da.peer_label, da.at_ms
                );
            }
        }
    }

    out
}

fn node_event_counts(node: Option<&NodeData>) -> BTreeMap<String, u64> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let Some(node) = node else {
        return counts;
    };
    for rec in &node.events {
        *counts.entry(event_kind(&rec.event)).or_insert(0) += 1;
    }
    counts
}

