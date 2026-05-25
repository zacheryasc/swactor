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

    // -- Host context per node (spec §5) --
    let _ = writeln!(out, "## Hosts");
    let host_lines = host_context_lines(bundle);
    if host_lines.is_empty() {
        let _ = writeln!(out, "- No boot identities captured.");
    } else {
        for line in host_lines {
            let _ = writeln!(out, "- {line}");
        }
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

    // -- Relay sessions (spec §1) --
    // Always rendered: when no relay observability data is in the
    // bundle, the section explains the gap and points the reader at
    // it instead of silently omitting itself.
    let _ = writeln!(out, "## Relay sessions");
    let relay_lines = relay_session_lines(bundle);
    for line in relay_lines {
        let _ = writeln!(out, "- {line}");
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

    // -- Kernel-level UDP / interface drops across the run window
    //    (spec §11). A line per (node, counter) only when the delta is
    //    non-zero; nothing rendered when every counter is clean.
    let _ = writeln!(out, "## Kernel network drops");
    let drops = kernel_drop_lines(bundle);
    if drops.is_empty() {
        let _ = writeln!(out, "- No non-zero UDP/interface drop deltas observed.");
    } else {
        for line in drops {
            let _ = writeln!(out, "- {line}");
        }
    }
    let _ = writeln!(out);

    // -- Gossip receipts per node, broken down by payload kind
    //    (spec §10). "Stage-2 never received any name-registry gossip
    //    from anyone" is supposed to be a one-line answer.
    let _ = writeln!(out, "## Gossip receipts (by node, by kind)");
    let gossip_lines = gossip_receipt_lines(bundle);
    if gossip_lines.is_empty() {
        let _ = writeln!(
            out,
            "- No GossipReceived events captured (no node ran a gossip-emitting source)."
        );
    } else {
        for line in gossip_lines {
            let _ = writeln!(out, "- {line}");
        }
    }
    let _ = writeln!(out);

    // -- Per-peer dial rollup --
    let _ = writeln!(out, "## Per-peer dials");
    let rollups = per_peer_dial_rollup(bundle);
    if rollups.is_empty() {
        let _ = writeln!(out, "- No DialStarted events captured.");
    } else {
        let totals = rollups_totals(&rollups);
        let _ = writeln!(
            out,
            "- totals: started={}, succeeded={}, failed={}, in-flight={}",
            totals.started, totals.succeeded, totals.failed, totals.in_flight,
        );
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "| peer | started | succeeded | failed | in-flight | last_outcome | last_outcome_at_ms |"
        );
        let _ = writeln!(
            out,
            "|------|---------|-----------|--------|-----------|--------------|--------------------|"
        );
        for row in &rollups {
            let last_outcome = row
                .last_outcome
                .as_deref()
                .unwrap_or("-")
                .to_string();
            let last_at = row
                .last_outcome_at_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            let _ = writeln!(
                out,
                "| {peer} | {started} | {succeeded} | {failed} | {in_flight} | {last_outcome} | {last_at} |",
                peer = row.peer_label,
                started = row.started,
                succeeded = row.succeeded,
                failed = row.failed,
                in_flight = row.in_flight(),
            );
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

/// Per-target-peer dial-event rollup
/// (spec §9 / `N3_OBSERVABILITY_UPGRADE_SPEC.md` gap 9).
///
/// Aggregates `DialStarted` / `DialOutcome` events across every
/// observer in the bundle. `in_flight = started - succeeded - failed`
/// surfaces the dials that never completed — the 3-event drift
/// (`DialStarted: 83`, `DialOutcome: 80`) attributed to a specific
/// peer in the table.
#[derive(Debug, Clone)]
pub struct PerPeerDialRollup {
    pub peer_hex: String,
    pub peer_label: String,
    pub started: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub last_outcome: Option<String>,
    pub last_outcome_at_ms: Option<u64>,
}

impl PerPeerDialRollup {
    pub fn in_flight(&self) -> u64 {
        self.started
            .saturating_sub(self.succeeded.saturating_add(self.failed))
    }
}

pub fn per_peer_dial_rollup(bundle: &Bundle) -> Vec<PerPeerDialRollup> {
    use crate::diagnostics::event::DialOutcome as DialOutcomeKind;
    let mut by_peer: BTreeMap<String, PerPeerDialRollup> = BTreeMap::new();
    for node in bundle.nodes.values() {
        for rec in &node.events {
            match &rec.event {
                Event::DialStarted { peer, .. } => {
                    let hex = node_id_hex(peer);
                    let entry = by_peer.entry(hex.clone()).or_insert_with(|| {
                        PerPeerDialRollup {
                            peer_label: bundle.label_for_hex(&hex),
                            peer_hex: hex,
                            started: 0,
                            succeeded: 0,
                            failed: 0,
                            last_outcome: None,
                            last_outcome_at_ms: None,
                        }
                    });
                    entry.started += 1;
                }
                Event::DialOutcome { peer, outcome, .. } => {
                    let hex = node_id_hex(peer);
                    let entry = by_peer.entry(hex.clone()).or_insert_with(|| {
                        PerPeerDialRollup {
                            peer_label: bundle.label_for_hex(&hex),
                            peer_hex: hex,
                            started: 0,
                            succeeded: 0,
                            failed: 0,
                            last_outcome: None,
                            last_outcome_at_ms: None,
                        }
                    });
                    match outcome {
                        DialOutcomeKind::Success => entry.succeeded += 1,
                        _ => entry.failed += 1,
                    }
                    let outcome_str = format!("{outcome:?}");
                    let stamp_better = match entry.last_outcome_at_ms {
                        Some(prev) => rec.wall_ms >= prev,
                        None => true,
                    };
                    if stamp_better {
                        entry.last_outcome = Some(outcome_str);
                        entry.last_outcome_at_ms = Some(rec.wall_ms);
                    }
                }
                _ => {}
            }
        }
    }
    let mut out: Vec<PerPeerDialRollup> = by_peer.into_values().collect();
    out.sort_by(|a, b| a.peer_label.cmp(&b.peer_label).then(a.peer_hex.cmp(&b.peer_hex)));
    out
}

#[derive(Debug, Default)]
struct DialTotals {
    started: u64,
    succeeded: u64,
    failed: u64,
    in_flight: u64,
}

fn rollups_totals(rollups: &[PerPeerDialRollup]) -> DialTotals {
    let mut t = DialTotals::default();
    for r in rollups {
        t.started = t.started.saturating_add(r.started);
        t.succeeded = t.succeeded.saturating_add(r.succeeded);
        t.failed = t.failed.saturating_add(r.failed);
        t.in_flight = t.in_flight.saturating_add(r.in_flight());
    }
    t
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

/// One compact line per node summarising the host context the boot
/// record carries (spec §5). Missing fields render as `?` so the bundle
/// reader can tell "absent" from "blank" at a glance.
fn host_context_lines(bundle: &Bundle) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for node in &bundle.manifest.nodes {
        let Some(data) = bundle.nodes.get(&node.label) else {
            continue;
        };
        let Some(id) = data.identity.as_ref() else {
            out.push(format!("{}: boot record absent", node.label));
            continue;
        };
        let contract = id.vastai_contract_id.as_deref().unwrap_or("?");
        let ip = id.host_ip_public.as_deref().unwrap_or("?");
        let dc = id.datacenter_id.as_deref().unwrap_or("?");
        let country = id.host_country.as_deref().unwrap_or("?");
        let container = id.container_id.as_deref().unwrap_or("?");
        let hostname = id.hostname.as_deref().unwrap_or("?");
        let relay = id.home_relay_url_at_boot.as_deref().unwrap_or("?");
        let iroh = id.iroh_version.as_deref().unwrap_or("?");
        let git = id.git_sha.as_deref().unwrap_or("?");
        out.push(format!(
            "{label}: rental={contract} ip={ip} dc={dc} country={country} container={container} \
             hostname={hostname} relay={relay} iroh={iroh} git={git}",
            label = node.label,
        ));
    }
    out
}

/// One line per (node, counter) where the delta between the first and
/// last snapshot of the run is non-zero (spec §11). Counters that came
/// back `None` are skipped — the bundle reader should never see a
/// silent zero for "kernel didn't expose this".
fn kernel_drop_lines(bundle: &Bundle) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (label, node) in &bundle.nodes {
        let mut snaps = node.snapshots.iter().filter_map(|s| s.body.host.as_ref());
        let first = snaps.next();
        let mut last_with_data = first;
        for s in snaps {
            if s.network.is_some() {
                last_with_data = Some(s);
            }
        }
        let (Some(first), Some(last)) = (first, last_with_data) else {
            continue;
        };
        let first_net = first.network.as_ref();
        let last_net = last.network.as_ref();
        if let (Some(a), Some(b)) = (first_net, last_net) {
            // UDP-side deltas
            if let (Some(au), Some(bu)) = (a.udp_kernel_stats.as_ref(), b.udp_kernel_stats.as_ref()) {
                let entries: [(&str, Option<u64>, Option<u64>); 4] = [
                    ("udp.no_ports", au.no_ports, bu.no_ports),
                    ("udp.in_errors", au.in_errors, bu.in_errors),
                    ("udp.rcvbuf_errors", au.rcvbuf_errors, bu.rcvbuf_errors),
                    ("udp.sndbuf_errors", au.sndbuf_errors, bu.sndbuf_errors),
                ];
                for (name, before, after) in entries {
                    let (Some(before), Some(after)) = (before, after) else {
                        continue;
                    };
                    let delta = after.saturating_sub(before);
                    if delta > 0 {
                        out.push(format!("{label}: {name} +{delta}"));
                    }
                }
            }
            // Per-interface drop deltas. A counter absent in the
            // baseline is treated as zero — the interface either just
            // came up or we simply weren't capturing yet, and either
            // way the delta is upper-bounded by the late value.
            let zero = crate::diagnostics::snapshot::Tier3InterfaceCounters::default();
            for iface_b in &b.interfaces {
                let Some(cb) = iface_b.counters.as_ref() else {
                    continue;
                };
                let ca = a
                    .interfaces
                    .iter()
                    .find(|i| i.name == iface_b.name)
                    .and_then(|i| i.counters.as_ref())
                    .unwrap_or(&zero);
                let rx_drop = cb.rx_dropped.saturating_sub(ca.rx_dropped);
                let tx_drop = cb.tx_dropped.saturating_sub(ca.tx_dropped);
                let rx_err = cb.rx_errors.saturating_sub(ca.rx_errors);
                let tx_err = cb.tx_errors.saturating_sub(ca.tx_errors);
                if rx_drop > 0 {
                    out.push(format!("{label}: {}.rx_dropped +{rx_drop}", iface_b.name));
                }
                if tx_drop > 0 {
                    out.push(format!("{label}: {}.tx_dropped +{tx_drop}", iface_b.name));
                }
                if rx_err > 0 {
                    out.push(format!("{label}: {}.rx_errors +{rx_err}", iface_b.name));
                }
                if tx_err > 0 {
                    out.push(format!("{label}: {}.tx_errors +{tx_err}", iface_b.name));
                }
            }
        }
    }
    out.sort();
    out
}

/// Per-peer "relay sessions" correlation (spec §1).
///
/// Walks every node in the bundle:
/// - relay-role nodes contribute `RelaySessionClosed` events plus the
///   end-of-run `Tier3RelayServer` totals;
/// - non-relay nodes contribute their `iroh.connection_cache[peer]`
///   tail, specifically `last_failure_reason`.
///
/// Output: one summary line per (peer, last close), suffixed with the
/// node-side `last_failure_reason` when one is present. When no
/// relay-role node is in the bundle, returns a single line that names
/// the gap explicitly so the bundle reader is never left wondering
/// whether the relay was quiet or unobserved.
fn relay_session_lines(bundle: &Bundle) -> Vec<String> {
    let mut relay_labels: Vec<&str> = bundle
        .manifest
        .nodes
        .iter()
        .filter(|n| n.role.as_deref() == Some("relay"))
        .map(|n| n.label.as_str())
        .collect();
    relay_labels.sort();

    if relay_labels.is_empty() {
        return vec![
            "No relay observability data in this bundle (gap 1). To enable: run \
             `swactor-iroh-relay` with `SWACTOR_DIAG_COLLECTOR_URL` set so the relay \
             reports into the same bundle as the nodes."
                .to_string(),
        ];
    }

    let mut out: Vec<String> = Vec::new();

    // Node-side cache map: peer_hex -> (node_label, last_failure_reason).
    let mut node_cache_failure: BTreeMap<String, (String, String)> = BTreeMap::new();
    for (label, node) in &bundle.nodes {
        // Skip the relay's own snapshot — its iroh cache is irrelevant
        // here; we want the *clients'* view of what they saw.
        if relay_labels.contains(&label.as_str()) {
            continue;
        }
        // Use the latest snapshot's iroh.connection_cache entries.
        let Some(snap) = node.snapshots.last() else {
            continue;
        };
        let Some(iroh) = snap.body.iroh.as_ref() else {
            continue;
        };
        for entry in &iroh.connection_cache {
            if let Some(reason) = entry.last_failure_reason.as_ref() {
                node_cache_failure
                    .entry(entry.peer_node_id_hex.to_lowercase())
                    .or_insert_with(|| (label.clone(), reason.clone()));
            }
        }
    }

    // Per-relay aggregate totals.
    for relay_label in &relay_labels {
        let Some(node) = bundle.nodes.get(*relay_label) else {
            continue;
        };
        if let Some(latest) = node
            .snapshots
            .iter()
            .rev()
            .find(|s| s.body.relay_server.is_some())
        {
            if let Some(rs) = latest.body.relay_server.as_ref() {
                let reasons = if rs.closes_by_reason.is_empty() {
                    "(no classified closes)".to_string()
                } else {
                    rs.closes_by_reason
                        .iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                out.push(format!(
                    "relay {relay_label}: active={active} opens={opens} closes={closes} \
                     rx={rx}B tx={tx}B closes_by_reason=[{reasons}]",
                    active = rs.active_sessions,
                    opens = rs.total_opens,
                    closes = rs.total_closes,
                    rx = rs.bytes_rx_total,
                    tx = rs.bytes_tx_total,
                ));
            }
        }

        // Per-session closed events keyed by peer; latest close wins.
        let mut last_close: BTreeMap<String, RelayCloseDetail> = BTreeMap::new();
        for rec in &node.events {
            if let Event::RelaySessionClosed {
                peer_node_id_hex,
                opened_at_ms,
                closed_at_ms,
                duration_ms,
                close_initiator,
                close_reason,
                bytes_rx,
                bytes_tx,
            } = &rec.event
            {
                let hex_lower = peer_node_id_hex.to_lowercase();
                let detail = RelayCloseDetail {
                    opened_at_ms: *opened_at_ms,
                    closed_at_ms: *closed_at_ms,
                    duration_ms: *duration_ms,
                    close_initiator: close_initiator.clone(),
                    close_reason: close_reason.clone(),
                    bytes_rx: *bytes_rx,
                    bytes_tx: *bytes_tx,
                };
                let replace = last_close
                    .get(&hex_lower)
                    .map(|prev| prev.closed_at_ms < detail.closed_at_ms)
                    .unwrap_or(true);
                if replace {
                    last_close.insert(hex_lower, detail);
                }
            }
        }

        if last_close.is_empty() {
            out.push(format!(
                "relay {relay_label}: no RelaySessionClosed events captured (relay binary may \
                 not be wired to emit per-session lifecycle yet)"
            ));
            continue;
        }
        for (peer_hex, d) in &last_close {
            let peer_label = bundle.label_for_hex(peer_hex);
            let node_view = node_cache_failure
                .get(peer_hex)
                .map(|(observer, reason)| {
                    format!(" | node-side cache ({observer}): last_failure_reason=\"{reason}\"")
                })
                .unwrap_or_else(|| " | node-side cache: no last_failure_reason recorded".into());
            out.push(format!(
                "relay {relay_label} → {peer_label} ({peer_hex_short}…): closed by \
                 {initiator} reason=\"{reason}\" duration={duration}ms rx={rx}B tx={tx}B{node_view}",
                peer_hex_short = short_hex(peer_hex),
                initiator = d.close_initiator,
                reason = d.close_reason,
                duration = d.duration_ms,
                rx = d.bytes_rx,
                tx = d.bytes_tx,
            ));
        }
    }
    out
}

#[derive(Debug, Clone)]
struct RelayCloseDetail {
    #[allow(dead_code)]
    opened_at_ms: u64,
    closed_at_ms: u64,
    duration_ms: u64,
    close_initiator: String,
    close_reason: String,
    bytes_rx: u64,
    bytes_tx: u64,
}

/// Per-node breakdown of `GossipReceived` events by payload kind
/// (spec §10). Lines look like
/// `stage-2: swim_piggyback × 17 (12345 bytes, 34 items)`. Empty when
/// no node observed any gossip; rendered as a single zero-line
/// elsewhere.
fn gossip_receipt_lines(bundle: &Bundle) -> Vec<String> {
    use std::collections::BTreeMap;
    let mut totals: BTreeMap<(String, String), GossipTotals> = BTreeMap::new();
    for (label, node) in &bundle.nodes {
        for rec in &node.events {
            if let Event::GossipReceived {
                payload_kind,
                payload_bytes,
                item_count,
                ..
            } = &rec.event
            {
                let entry = totals
                    .entry((label.clone(), payload_kind.clone()))
                    .or_default();
                entry.receipts = entry.receipts.saturating_add(1);
                entry.bytes = entry.bytes.saturating_add(*payload_bytes as u64);
                entry.items = entry.items.saturating_add(*item_count as u64);
            }
        }
    }
    totals
        .into_iter()
        .map(|((label, kind), t)| {
            format!(
                "{label}: {kind} × {receipts} ({bytes} bytes, {items} items)",
                receipts = t.receipts,
                bytes = t.bytes,
                items = t.items,
            )
        })
        .collect()
}

#[derive(Debug, Default)]
struct GossipTotals {
    receipts: u64,
    bytes: u64,
    items: u64,
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
        Event::RelaySessionStateChanged { .. } => "RelaySessionStateChanged".into(),
        Event::RelaySessionOpened { .. } => "RelaySessionOpened".into(),
        Event::RelaySessionClosed { .. } => "RelaySessionClosed".into(),
        Event::SubprocessSpawned { .. } => "SubprocessSpawned".into(),
        Event::SubprocessExited { .. } => "SubprocessExited".into(),
        Event::GossipReceived { .. } => "GossipReceived".into(),
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

