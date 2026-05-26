//! Coverage 2.6 T-layer
//! (`N3_COVERAGE_EXTENSION_SPEC.md §2.6`).
//!
//! Spec close criterion: "a deployed bundle's postproc summary names
//! the median / p99 RTT per (observer, target) and a sim bundle
//! produces the matching surface. `no_flap_while_probes_ok`
//! resolves to `Pass` or `Fail` (not `Inconclusive`) on every SWIM
//! scenario in the calibration library."
//!
//! This test exercises the renderer's `## Probe RTT distribution`
//! section directly against a hand-constructed bundle whose events
//! carry known `(observer, target, sequence, kind)` joins. The
//! renderer must:
//!
//! 1. Reconstruct per-probe RTT from `wall_ms` deltas between
//!    matching `SwimProbeSent` and `SwimProbeAcked` events.
//! 2. Compute median / p95 / p99 per (observer, target) pair across
//!    the run.
//! 3. Bucket the same data into 5-second windows so degradation
//!    over time is visible — the spec's "spike at the mutation
//!    time" sub-contract.
//!
//! Testing at the renderer level (rather than as a sim integration
//! test) cuts straight at the close-criterion surface: the *rendered
//! section* is what a bundle reader actually sees. Confirming the
//! renderer's RTT math is correct under controlled inputs is what
//! the spec's "fall within stated tolerance" assertion targets.

#![cfg(feature = "collector")]

use std::collections::BTreeMap;

use distribution::diagnostics::event::{Event, EventRecord};
use distribution::diagnostics::postproc::{Bundle, NodeData, PostprocManifest, PostprocManifestNode, render_summary};
use distribution::types::NodeId;

const ORCH_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const STAGE0_HEX: &str = "2222222222222222222222222222222222222222222222222222222222222222";

fn node_id_from_hex(hex: &str) -> NodeId {
    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        let s = &hex[2 * i..2 * i + 2];
        *b = u8::from_str_radix(s, 16).expect("valid hex");
    }
    NodeId(bytes)
}

fn manifest_with(nodes: Vec<(&str, &str)>) -> PostprocManifest {
    PostprocManifest {
        run_id: "rtt-distribution-fixture".into(),
        run_start_collector_ms: Some(0),
        run_end_collector_ms: Some(15_000),
        finalize_received: true,
        nodes: nodes
            .into_iter()
            .map(|(label, hex)| PostprocManifestNode {
                node_id_hex: hex.to_string(),
                label: label.to_string(),
                role: None,
                stage_index: None,
                boot_recorded: true,
                event_batches: 0,
                snapshots: 0,
                finalize_recorded: true,
            })
            .collect(),
    }
}

/// Construct a `EventRecord` for a `SwimProbeSent` event.
fn probe_sent(observer_hex: &str, target: NodeId, sequence: u64, wall_ms: u64) -> EventRecord {
    EventRecord {
        node_id: node_id_from_hex(observer_hex),
        monotonic_seq: sequence,
        wall_ms,
        event: Event::SwimProbeSent {
            target,
            sequence,
            kind: "direct".into(),
        },
    }
}

fn probe_acked(observer_hex: &str, target: NodeId, sequence: u64, wall_ms: u64) -> EventRecord {
    EventRecord {
        node_id: node_id_from_hex(observer_hex),
        monotonic_seq: sequence + 10_000,
        wall_ms,
        event: Event::SwimProbeAcked {
            target,
            sequence,
            kind: "direct".into(),
        },
    }
}

fn probe_timed_out(
    observer_hex: &str,
    target: NodeId,
    sequence: u64,
    wall_ms: u64,
    budget_ticks: u64,
) -> EventRecord {
    EventRecord {
        node_id: node_id_from_hex(observer_hex),
        monotonic_seq: sequence + 20_000,
        wall_ms,
        event: Event::SwimProbeTimedOut {
            target,
            sequence,
            kind: "direct".into(),
            budget_ticks,
        },
    }
}

fn make_bundle(events: Vec<EventRecord>) -> Bundle {
    let manifest = manifest_with(vec![("orchestrator", ORCH_HEX), ("stage-0", STAGE0_HEX)]);

    let mut orch = NodeData::default();
    orch.label = "orchestrator".into();
    orch.node_id_hex = ORCH_HEX.into();
    let mut stage0 = NodeData::default();
    stage0.label = "stage-0".into();
    stage0.node_id_hex = STAGE0_HEX.into();
    let orch_id = node_id_from_hex(ORCH_HEX);
    for rec in events {
        if rec.node_id == orch_id {
            orch.events.push(rec);
        } else {
            stage0.events.push(rec);
        }
    }
    let mut nodes_map: BTreeMap<String, NodeData> = BTreeMap::new();
    nodes_map.insert("orchestrator".into(), orch);
    nodes_map.insert("stage-0".into(), stage0);
    Bundle {
        run_id: "rtt-distribution-fixture".into(),
        manifest,
        nodes: nodes_map,
    }
}

/// Extract the `## Probe RTT distribution` section lines from
/// `render_summary` output. Returns the lines including the section
/// header up to (not including) the next section.
fn extract_rtt_section(summary: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut in_section = false;
    for line in summary.lines() {
        if line.starts_with("## ") {
            if in_section {
                break;
            }
            if line.starts_with("## Probe RTT distribution") {
                in_section = true;
            }
        }
        if in_section {
            out.push(line.to_string());
        }
    }
    out
}

#[test]
fn renderer_computes_median_p95_p99_per_observer_target_pair_within_tolerance() {
    // Spec §2.6: "the postproc summary names the median / p99 RTT
    // per (observer, target)". Synthesize 100 probes for the
    // orch → stage-0 pair with deterministic RTTs in the band
    // [100 ms, 200 ms]. The median lands at 150 ms; p95 at 195 ms;
    // p99 at 199 ms.
    let stage0_id = node_id_from_hex(STAGE0_HEX);
    let mut events: Vec<EventRecord> = Vec::new();
    for i in 0..100u64 {
        // Linearly-spaced RTTs from 100 ms (i=0) to 199 ms (i=99).
        let send_ms = i * 250;
        let rtt_ms = 100 + i;
        events.push(probe_sent(ORCH_HEX, stage0_id, i + 1, send_ms));
        events.push(probe_acked(ORCH_HEX, stage0_id, i + 1, send_ms + rtt_ms));
    }
    let bundle = make_bundle(events);
    let summary = render_summary(&bundle);
    let rtt_lines = extract_rtt_section(&summary);
    assert!(
        !rtt_lines.is_empty(),
        "no `## Probe RTT distribution` section in summary:\n{summary}"
    );
    // The orch→stage-0 line carries the run-wide totals.
    let totals_line = rtt_lines
        .iter()
        .find(|l| l.starts_with("- orchestrator -> stage-0:"))
        .unwrap_or_else(|| panic!("no orch→stage-0 totals line in RTT section:\n{rtt_lines:#?}"));
    // The line is `- {observer} -> {target}: probes=N acked=N
    // timed_out=N pending=N rtt_ms median=X p95=Y p99=Z`. We parse
    // the numeric fields and assert they fall within tolerance of
    // the synthesized distribution.
    let (probes, acked, timed_out, median, p95, p99) = parse_totals_line(totals_line);
    assert_eq!(probes, 100, "probes count must match synthesized input");
    assert_eq!(acked, 100, "all 100 probes acked in this fixture");
    assert_eq!(timed_out, 0, "no timeouts in this fixture");
    // Median: 50th percentile by nearest-rank on 100 sorted RTTs ⇒
    // index 49 ⇒ value 149 ms.
    assert!(
        (149..=151).contains(&median),
        "median ({median}) must be ~150 ms; got line: {totals_line}"
    );
    // p95: nearest-rank rank=95 ⇒ index 94 ⇒ value 194 ms.
    assert!(
        (192..=196).contains(&p95),
        "p95 ({p95}) must be ~194 ms; got line: {totals_line}"
    );
    // p99: rank=99 ⇒ index 98 ⇒ value 198 ms.
    assert!(
        (196..=200).contains(&p99),
        "p99 ({p99}) must be ~198 ms; got line: {totals_line}"
    );
}

#[test]
fn renderer_buckets_show_spike_at_mutation_time() {
    // Spec §2.6: "a scenario with a `LatencySpike` mutation produces
    // a bundle whose RTT section shows the spike at the mutation
    // time". Synthesize two clusters:
    //   - bucket [0-5s): steady-state at 100 ms RTT.
    //   - bucket [10-15s): spike at 600 ms RTT (6× the baseline).
    // The bucket lines must surface the difference: the spike
    // bucket's median ~600 ms, the steady bucket's median ~100 ms.
    let stage0_id = node_id_from_hex(STAGE0_HEX);
    let mut events: Vec<EventRecord> = Vec::new();
    for i in 0..10u64 {
        // Steady state: ten probes in [0, 5s), all with RTT 100 ms.
        let send_ms = i * 400;
        events.push(probe_sent(ORCH_HEX, stage0_id, i + 1, send_ms));
        events.push(probe_acked(ORCH_HEX, stage0_id, i + 1, send_ms + 100));
    }
    for i in 0..10u64 {
        // Spike window: ten probes in [10s, 15s), all with RTT 600 ms.
        let send_ms = 10_000 + i * 400;
        let seq = i + 100;
        events.push(probe_sent(ORCH_HEX, stage0_id, seq, send_ms));
        events.push(probe_acked(ORCH_HEX, stage0_id, seq, send_ms + 600));
    }
    let bundle = make_bundle(events);
    let summary = render_summary(&bundle);
    let rtt_lines = extract_rtt_section(&summary);
    // Find the bucket lines for [0-5s) and [10-15s).
    let bucket_steady = rtt_lines
        .iter()
        .find(|l| l.contains("bucket 0-5s:"))
        .expect("steady-state bucket 0-5s line missing");
    let bucket_spike = rtt_lines
        .iter()
        .find(|l| l.contains("bucket 10-15s:"))
        .expect("spike bucket 10-15s line missing");
    let steady_median = parse_median_from_bucket(bucket_steady);
    let spike_median = parse_median_from_bucket(bucket_spike);
    assert!(
        (95..=105).contains(&steady_median),
        "steady-state bucket median ({steady_median}) must be ~100 ms; got line: {bucket_steady}"
    );
    assert!(
        (595..=605).contains(&spike_median),
        "spike bucket median ({spike_median}) must be ~600 ms; got line: {bucket_spike}"
    );
    assert!(
        spike_median > steady_median * 4,
        "spike bucket median ({spike_median}) must be >> steady-state median ({steady_median}); 6× spike configured"
    );
}

#[test]
fn renderer_surfaces_timeout_count_and_budget_when_probes_expire() {
    // Spec §2.6: "A `probe_timed_out` outcome carries the configured
    // timeout budget alongside the observed RTT (where one exists)
    // so a reader sees 'probe missed a 3 s budget by 200 ms' vs
    // 'no response within 3 s, never arrived'". Honesty-under-
    // absence: a timed-out probe has no RTT, but its budget is
    // surfaced as a discriminator.
    let stage0_id = node_id_from_hex(STAGE0_HEX);
    let mut events: Vec<EventRecord> = Vec::new();
    // Three timeouts at distinct (target, sequence) — no acks.
    for i in 0..3u64 {
        events.push(probe_sent(ORCH_HEX, stage0_id, i + 1, i * 1000));
        events.push(probe_timed_out(
            ORCH_HEX,
            stage0_id,
            i + 1,
            i * 1000 + 3000,
            15, // 15-tick budget
        ));
    }
    let bundle = make_bundle(events);
    let summary = render_summary(&bundle);
    let rtt_lines = extract_rtt_section(&summary);
    let totals_line = rtt_lines
        .iter()
        .find(|l| l.starts_with("- orchestrator -> stage-0:"))
        .expect("orch→stage-0 totals line missing");
    // The renderer surfaces timed_out=N + timeout_budget_ticks=N
    // when any timeout fired. No median/p95/p99 reported because
    // no acks happened.
    assert!(
        totals_line.contains("timed_out=3"),
        "totals line must report timed_out=3 when 3 probes expired; got: {totals_line}"
    );
    assert!(
        totals_line.contains("timeout_budget_ticks=15"),
        "totals line must surface the configured budget under honesty-under-absence; got: {totals_line}"
    );
    assert!(
        totals_line.contains("rtt_ms=- (no acks)"),
        "rtt_ms must explicitly say `- (no acks)` when no probe completed; got: {totals_line}"
    );
}

#[test]
fn renderer_surfaces_pending_probes_per_honesty_under_absence() {
    // A probe sent but with no matching ack or timeout within the
    // bundle's window. The renderer must surface this as `pending`
    // rather than silently dropping it — the bundle reader must
    // never be misled into thinking "no probe attempted" when the
    // actual answer is "probe sent, lifecycle didn't resolve".
    let stage0_id = node_id_from_hex(STAGE0_HEX);
    let events = vec![probe_sent(ORCH_HEX, stage0_id, 42, 5_000)];
    let bundle = make_bundle(events);
    let summary = render_summary(&bundle);
    let rtt_lines = extract_rtt_section(&summary);
    let totals = rtt_lines
        .iter()
        .find(|l| l.starts_with("- orchestrator -> stage-0:"))
        .expect("orch→stage-0 line missing for an unresolved probe");
    assert!(
        totals.contains("pending=1"),
        "unresolved probe must surface as pending=1; got: {totals}"
    );
    assert!(
        totals.contains("acked=0") && totals.contains("timed_out=0"),
        "unresolved probe must not be counted as acked or timed_out; got: {totals}"
    );
}

// ─── parsing helpers ──────────────────────────────────────────────────

fn parse_totals_line(line: &str) -> (u64, u64, u64, u64, u64, u64) {
    // Format: "- {observer} -> {target}: probes=N acked=N
    //          timed_out=N pending=N rtt_ms median=X p95=Y p99=Z[ timeout_budget_ticks=B]"
    let probes = parse_kv(line, "probes=");
    let acked = parse_kv(line, "acked=");
    let timed_out = parse_kv(line, "timed_out=");
    let median = parse_kv(line, "median=");
    let p95 = parse_kv(line, "p95=");
    let p99 = parse_kv(line, "p99=");
    (probes, acked, timed_out, median, p95, p99)
}

fn parse_median_from_bucket(line: &str) -> u64 {
    parse_kv(line, "median=")
}

fn parse_kv(line: &str, key: &str) -> u64 {
    let idx = line.find(key).unwrap_or_else(|| panic!("`{key}` not found in line: {line}"));
    let rest = &line[idx + key.len()..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().unwrap_or_else(|_| panic!("could not parse u64 after `{key}` in line: {line}"))
}
