//! SIM_SPEC §10.5 behavioural property tests for the assertion
//! evaluator. The tests use the in-memory `evaluate` entry point so
//! they don't touch disk; one end-to-end test exercises
//! `evaluate_bundle` to verify the on-disk `verdicts.json` shape.

use std::collections::BTreeMap;
use std::path::Path;

use tempfile::TempDir;

use simulation::bundle::{
    BundleRecord, BundleWriter, EventPayload, EventRecord, SnapshotRecord,
};
use simulation::bundle_file::FileBundleWriter;
use simulation::evaluator::{
    EventLine, MemberView, Outcome, SnapshotEntry, SnapshotIndex, StreamingEvaluator,
    evaluate, evaluate_bundle,
};
use simulation::scenario::{
    AssertionKind, HostKindRegistry, Scenario, load_from_str,
};

// ──────────────────────────────────────────────────────────────────────
// Fixtures
// ──────────────────────────────────────────────────────────────────────

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn fake_path() -> &'static Path {
    Path::new("test://evaluator.toml")
}

fn scenario_with(assertions: Vec<AssertionKind>) -> Scenario {
    scenario_with_peers(&["a", "b", "c"], assertions)
}

fn scenario_with_peers(peers: &[&str], assertions: Vec<AssertionKind>) -> Scenario {
    let assertions_toml: String = assertions
        .iter()
        .map(|a| {
            let v = serde_json::to_value(a).unwrap();
            let obj = v.as_object().unwrap();
            let mut entries: Vec<String> = obj
                .iter()
                .map(|(k, v)| {
                    let val = match v {
                        serde_json::Value::String(s) => format!("{s:?}"),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Array(arr) => {
                            let items: Vec<String> = arr
                                .iter()
                                .map(|v| format!("{v:?}").replace("String(", "").replace(")", ""))
                                .collect();
                            format!("[{}]", items.join(", "))
                        }
                        _ => "null".to_string(),
                    };
                    format!("{k} = {val}")
                })
                .collect();
            entries.sort();
            format!("[[assertions]]\n{}", entries.join("\n"))
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let body = format!(
        r#"
name = "ev"
seed = 1
duration_ns = 1_000_000_000

[default_tick]
period_ns = 1_000_000

[default_link]
latency_ns = 1000
jitter_stddev_ns = 0
loss_prob_ppm = 0
reorder_prob_ppm = 0
bandwidth_bps = 1_000_000_000
cold_dial_penalty_ns = 0
cache_warm_after_ns = 0
cache_invalidate_after_idle_ns = 10_000_000_000

{peers_toml}

{links_toml}

{assertions_toml}
"#,
        peers_toml = peers
            .iter()
            .map(|p| format!(
                r#"[[peers]]
id = "{p}"
kind = "swim"
initial_state = "alive"
kind_config = {{ probe_interval_ns = 1, suspicion_timeout_ns = 10 }}"#
            ))
            .collect::<Vec<_>>()
            .join("\n\n"),
        links_toml = peers
            .iter()
            .flat_map(|from| {
                peers.iter().filter(move |to| to != &from).map(move |to| {
                    format!(
                        r#"[[links]]
from = "{from}"
to = "{to}""#
                    )
                })
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
    );
    load_from_str(fake_path(), &body, &registry())
        .unwrap_or_else(|e| panic!("scenario should parse: {e}\n---\n{body}"))
}

fn evt(t: u64, host: Option<&str>, kind_tag: &str, event: serde_json::Value, idx: usize) -> EventLine {
    EventLine {
        virtual_time_ns: t,
        host_id: host.map(|s| s.into()),
        kind_tag: kind_tag.into(),
        event,
        line_idx: idx,
    }
}

fn snap(t: u64, seq: u32, members: &[(&str, &str, u64)], self_incarnation: u64) -> SnapshotEntry {
    SnapshotEntry {
        virtual_time_ns: t,
        seq,
        members: members
            .iter()
            .map(|(id, st, inc)| {
                (
                    (*id).into(),
                    MemberView {
                        state: (*st).into(),
                        incarnation: *inc,
                    },
                )
            })
            .collect(),
        self_incarnation,
        name_registry: std::collections::BTreeMap::new(),
    }
}

fn idx(host_snaps: &[(&str, Vec<SnapshotEntry>)]) -> SnapshotIndex {
    let mut by_host = BTreeMap::new();
    for (h, list) in host_snaps {
        by_host.insert((*h).to_string(), list.clone());
    }
    SnapshotIndex { by_host }
}

// ──────────────────────────────────────────────────────────────────────
// §10.5 Per-kind soundness — Pass + Fail + Inconclusive for each kind
// ──────────────────────────────────────────────────────────────────────

#[test]
fn all_alive_at_pass_fail_inconclusive() {
    let scen = scenario_with(vec![AssertionKind::AllAliveAt {
        at_ns: 100,
        peers: vec!["a".into(), "b".into()],
    }]);
    // PASS: both views Alive.
    let snaps = idx(&[
        ("a", vec![snap(100, 0, &[("b", "Alive", 0)], 0)]),
        ("b", vec![snap(100, 0, &[("a", "Alive", 0)], 0)]),
    ]);
    assert_eq!(evaluate(&scen, &[], &snaps)[0].outcome, Outcome::Pass);
    // FAIL: one sees Suspect.
    let snaps_fail = idx(&[
        ("a", vec![snap(100, 0, &[("b", "Suspect", 0)], 0)]),
        ("b", vec![snap(100, 0, &[("a", "Alive", 0)], 0)]),
    ]);
    let v = evaluate(&scen, &[], &snaps_fail);
    assert_eq!(v[0].outcome, Outcome::Fail);
    assert!(!v[0].evidence.is_empty());
    // INCONCLUSIVE: no snapshots for one peer.
    let snaps_inc = idx(&[("a", vec![snap(100, 0, &[("b", "Alive", 0)], 0)])]);
    assert_eq!(evaluate(&scen, &[], &snaps_inc)[0].outcome, Outcome::Inconclusive);
}

#[test]
fn all_alive_throughout_pass_fail_inconclusive() {
    let scen = scenario_with(vec![AssertionKind::AllAliveThroughout {
        window_start_ns: 100,
        window_end_ns: 200,
        peers: vec!["a".into(), "b".into()],
    }]);
    let pass_snaps = idx(&[
        ("a", vec![
            snap(110, 0, &[("b", "Alive", 0)], 0),
            snap(190, 1, &[("b", "Alive", 0)], 0),
        ]),
        ("b", vec![
            snap(120, 0, &[("a", "Alive", 0)], 0),
            snap(180, 1, &[("a", "Alive", 0)], 0),
        ]),
    ]);
    assert_eq!(evaluate(&scen, &[], &pass_snaps)[0].outcome, Outcome::Pass);

    let fail_snaps = idx(&[
        ("a", vec![snap(150, 0, &[("b", "Dead", 0)], 0)]),
        ("b", vec![snap(150, 0, &[("a", "Alive", 0)], 0)]),
    ]);
    assert_eq!(evaluate(&scen, &[], &fail_snaps)[0].outcome, Outcome::Fail);

    let inc_snaps = idx(&[
        ("a", vec![snap(50, 0, &[("b", "Alive", 0)], 0)]), // before window
        ("b", vec![snap(50, 0, &[("a", "Alive", 0)], 0)]),
    ]);
    assert_eq!(evaluate(&scen, &[], &inc_snaps)[0].outcome, Outcome::Inconclusive);
}

#[test]
fn convergence_after_pass_fail_inconclusive() {
    // 3 peers so each subject has ≥2 observers — only then can
    // observers actually disagree about a subject's state.
    let scen = scenario_with(vec![AssertionKind::ConvergenceAfter {
        after_ns: 100,
        within_ns: 100,
        peers: vec!["a".into(), "b".into(), "c".into()],
    }]);
    // PASS: every observer's view of every other peer agrees.
    let pass_snaps = idx(&[
        ("a", vec![snap(150, 0, &[("b", "Alive", 0), ("c", "Alive", 0)], 0)]),
        ("b", vec![snap(150, 0, &[("a", "Alive", 0), ("c", "Alive", 0)], 0)]),
        ("c", vec![snap(150, 0, &[("a", "Alive", 0), ("b", "Alive", 0)], 0)]),
    ]);
    assert_eq!(evaluate(&scen, &[], &pass_snaps)[0].outcome, Outcome::Pass);
    // FAIL: a and c disagree about b's state throughout the window.
    let fail_snaps = idx(&[
        ("a", vec![snap(150, 0, &[("b", "Suspect", 0), ("c", "Alive", 0)], 0)]),
        ("b", vec![snap(150, 0, &[("a", "Alive", 0), ("c", "Alive", 0)], 0)]),
        ("c", vec![snap(150, 0, &[("a", "Alive", 0), ("b", "Alive", 0)], 0)]),
    ]);
    assert_eq!(evaluate(&scen, &[], &fail_snaps)[0].outcome, Outcome::Fail);
    // INCONCLUSIVE: no snapshots in window.
    let inc_snaps = idx(&[
        ("a", vec![snap(50, 0, &[("b", "Alive", 0), ("c", "Alive", 0)], 0)]),
        ("b", vec![snap(50, 0, &[("a", "Alive", 0), ("c", "Alive", 0)], 0)]),
        ("c", vec![snap(50, 0, &[("a", "Alive", 0), ("b", "Alive", 0)], 0)]),
    ]);
    assert_eq!(evaluate(&scen, &[], &inc_snaps)[0].outcome, Outcome::Inconclusive);
}

#[test]
fn no_flap_while_probes_ok_pass_fail_inconclusive() {
    let scen = scenario_with(vec![AssertionKind::NoFlapWhileProbesOk {
        peer: "b".into(),
        window_start_ns: 0,
        window_end_ns: 1000,
    }]);
    let probe = |t: u64, kind: &str, from: &str, to: &str, i: usize| {
        evt(t, None, "swim", serde_json::json!({"kind": kind, "from": from, "to": to}), i)
    };
    let st = |t: u64, peer: &str, from: &str, to: &str, i: usize| {
        evt(
            t,
            None,
            "swim",
            serde_json::json!({"kind": "state_transition", "peer": peer, "from": from, "to": to}),
            i,
        )
    };
    let snaps = SnapshotIndex::default();

    // PASS: probes balanced, no flap on b.
    let events_pass = vec![
        probe(10, "probe_sent", "a", "b", 0),
        probe(15, "probe_received", "b", "a", 1),
        probe(20, "probe_sent", "a", "b", 2),
        probe(25, "probe_received", "b", "a", 3),
    ];
    assert_eq!(evaluate(&scen, &events_pass, &snaps)[0].outcome, Outcome::Pass);

    // FAIL: Suspect→Alive→Suspect on b while probes balanced.
    let events_fail = vec![
        probe(5, "probe_sent", "a", "b", 0),
        probe(6, "probe_received", "b", "a", 1),
        st(10, "b", "Alive", "Suspect", 2),
        st(20, "b", "Suspect", "Alive", 3),
        st(30, "b", "Alive", "Suspect", 4),
        probe(40, "probe_sent", "a", "b", 5),
        probe(41, "probe_received", "b", "a", 6),
    ];
    assert_eq!(evaluate(&scen, &events_fail, &snaps)[0].outcome, Outcome::Fail);

    // INCONCLUSIVE: no probes in window.
    let events_inc = vec![st(10, "b", "Alive", "Suspect", 0)];
    assert_eq!(evaluate(&scen, &events_inc, &snaps)[0].outcome, Outcome::Inconclusive);
}

#[test]
fn no_dead_when_probes_ok_pass_fail_inconclusive() {
    let scen = scenario_with(vec![AssertionKind::NoDeadWhenProbesOk {
        peer: "b".into(),
        window_start_ns: 0,
        window_end_ns: 1000,
    }]);
    let probe = |t: u64, kind: &str, from: &str, to: &str, i: usize| {
        evt(t, None, "swim", serde_json::json!({"kind": kind, "from": from, "to": to}), i)
    };
    let st = |t: u64, peer: &str, from: &str, to: &str, i: usize| {
        evt(
            t,
            None,
            "swim",
            serde_json::json!({"kind": "state_transition", "peer": peer, "from": from, "to": to}),
            i,
        )
    };
    let snaps = SnapshotIndex::default();

    let pass = vec![
        probe(10, "probe_sent", "a", "b", 0),
        probe(11, "probe_received", "b", "a", 1),
    ];
    assert_eq!(evaluate(&scen, &pass, &snaps)[0].outcome, Outcome::Pass);
    let fail = vec![
        probe(10, "probe_sent", "a", "b", 0),
        probe(11, "probe_received", "b", "a", 1),
        st(20, "b", "Alive", "Dead", 2),
    ];
    assert_eq!(evaluate(&scen, &fail, &snaps)[0].outcome, Outcome::Fail);
    let inc = vec![st(20, "b", "Alive", "Dead", 0)]; // no probe in window
    assert_eq!(evaluate(&scen, &inc, &snaps)[0].outcome, Outcome::Inconclusive);
}

#[test]
fn self_incarnation_bounded_pass_fail_inconclusive() {
    let scen = scenario_with(vec![AssertionKind::SelfIncarnationBounded {
        peer: "a".into(),
        max_value: 5,
    }]);
    let bump = |t: u64, peer: &str, to: u64, i: usize| {
        evt(
            t,
            None,
            "swim",
            serde_json::json!({"kind": "self_incarnation_bump", "peer": peer, "to": to}),
            i,
        )
    };
    let no_snaps = SnapshotIndex::default();

    let pass = vec![bump(10, "a", 3, 0)];
    assert_eq!(evaluate(&scen, &pass, &no_snaps)[0].outcome, Outcome::Pass);

    let fail = vec![bump(10, "a", 10, 0)];
    assert_eq!(evaluate(&scen, &fail, &no_snaps)[0].outcome, Outcome::Fail);

    // INCONCLUSIVE: no events and no snapshots for a.
    assert_eq!(
        evaluate(&scen, &[], &no_snaps)[0].outcome,
        Outcome::Inconclusive
    );
}

#[test]
fn message_size_bounded_pass_fail_inconclusive() {
    let scen = scenario_with(vec![AssertionKind::MessageSizeBounded {
        message_kind: "ping".into(),
        max_bytes: 1024,
    }]);
    let msg = |t: u64, kind: &str, bytes: u64, i: usize| {
        evt(
            t,
            None,
            "swim",
            serde_json::json!({"kind": "message_send", "from": "a", "to": "b", "message_kind": kind, "bytes": bytes}),
            i,
        )
    };
    let no_snaps = SnapshotIndex::default();
    let pass = vec![msg(10, "ping", 64, 0), msg(20, "ping", 512, 1)];
    assert_eq!(evaluate(&scen, &pass, &no_snaps)[0].outcome, Outcome::Pass);
    let fail = vec![msg(10, "ping", 2000, 0)];
    assert_eq!(evaluate(&scen, &fail, &no_snaps)[0].outcome, Outcome::Fail);
    // INCONCLUSIVE: no ping messages.
    let inc = vec![msg(10, "ack", 9999, 0)];
    assert_eq!(evaluate(&scen, &inc, &no_snaps)[0].outcome, Outcome::Inconclusive);
}

#[test]
fn dead_peer_resurrects_within_pass_fail_inconclusive() {
    let scen = scenario_with(vec![AssertionKind::DeadPeerResurrectsWithin {
        peer: "b".into(),
        after_ns: 0,
        within_ns: 100,
    }]);
    let st = |t: u64, peer: &str, to: &str, i: usize| {
        evt(
            t,
            None,
            "swim",
            serde_json::json!({"kind": "state_transition", "peer": peer, "from": "Alive", "to": to}),
            i,
        )
    };
    let no_snaps = SnapshotIndex::default();
    let pass = vec![st(10, "b", "Dead", 0), st(50, "b", "Alive", 1)];
    assert_eq!(evaluate(&scen, &pass, &no_snaps)[0].outcome, Outcome::Pass);
    let fail = vec![st(10, "b", "Dead", 0)];
    assert_eq!(evaluate(&scen, &fail, &no_snaps)[0].outcome, Outcome::Fail);
    let inc: Vec<EventLine> = vec![]; // never Dead
    assert_eq!(evaluate(&scen, &inc, &no_snaps)[0].outcome, Outcome::Inconclusive);
}

#[test]
fn event_count_pass_fail() {
    let scen = scenario_with(vec![AssertionKind::EventCount {
        event_kind: "probe_sent".into(),
        max: 3,
    }]);
    let probe = |t: u64, i: usize| {
        evt(
            t,
            None,
            "swim",
            serde_json::json!({"kind": "probe_sent", "from": "a", "to": "b"}),
            i,
        )
    };
    let no_snaps = SnapshotIndex::default();
    let pass = (0..3).map(|i| probe(i as u64 * 10, i)).collect::<Vec<_>>();
    assert_eq!(evaluate(&scen, &pass, &no_snaps)[0].outcome, Outcome::Pass);
    let fail = (0..5).map(|i| probe(i as u64 * 10, i)).collect::<Vec<_>>();
    assert_eq!(evaluate(&scen, &fail, &no_snaps)[0].outcome, Outcome::Fail);
    // No events ⇒ count 0 ≤ max ⇒ Pass (per the catalog's literal wording
    // — "bounds the absolute count"; 0 is within bound).
    assert_eq!(evaluate(&scen, &[], &no_snaps)[0].outcome, Outcome::Pass);
}

#[test]
fn event_rate_pass_fail() {
    let scen = scenario_with(vec![AssertionKind::EventRate {
        event_kind: "probe_sent".into(),
        window_ns: 100,
        max_per_window: 2,
    }]);
    let probe = |t: u64, i: usize| {
        evt(
            t,
            None,
            "swim",
            serde_json::json!({"kind": "probe_sent", "from": "a", "to": "b"}),
            i,
        )
    };
    let no_snaps = SnapshotIndex::default();
    // PASS: 2 probes per 100ns window, never exceeded.
    let pass = vec![probe(0, 0), probe(50, 1), probe(150, 2), probe(200, 3)];
    assert_eq!(evaluate(&scen, &pass, &no_snaps)[0].outcome, Outcome::Pass);
    // FAIL: 3 probes in a 100ns window.
    let fail = vec![probe(0, 0), probe(20, 1), probe(40, 2)];
    assert_eq!(evaluate(&scen, &fail, &no_snaps)[0].outcome, Outcome::Fail);
}

// ──────────────────────────────────────────────────────────────────────
// §10.5 Verdict shape
// ──────────────────────────────────────────────────────────────────────

#[test]
fn verdict_shape_includes_kind_parameters_outcome_and_evidence_on_fail() {
    let scen = scenario_with(vec![AssertionKind::EventCount {
        event_kind: "probe_sent".into(),
        max: 0,
    }]);
    let events = vec![evt(
        10,
        None,
        "swim",
        serde_json::json!({"kind": "probe_sent", "from": "a", "to": "b"}),
        0,
    )];
    let v = evaluate(&scen, &events, &SnapshotIndex::default());
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].outcome, Outcome::Fail);
    assert_eq!(v[0].kind, "event_count");
    assert!(!v[0].evidence.is_empty());
    assert_eq!(v[0].evidence[0].virtual_time_ns, 10);
    assert!(v[0].evidence[0].event_or_snapshot_ref.starts_with("events.ndjson:"));
}

// ──────────────────────────────────────────────────────────────────────
// §10.5 Verdict order is scenario-declared
// ──────────────────────────────────────────────────────────────────────

#[test]
fn verdicts_listed_in_scenario_declaration_order() {
    let scen = scenario_with(vec![
        AssertionKind::EventCount { event_kind: "alpha".into(), max: 0 },
        AssertionKind::EventCount { event_kind: "beta".into(), max: 0 },
        AssertionKind::EventCount { event_kind: "gamma".into(), max: 0 },
    ]);
    let v = evaluate(&scen, &[], &SnapshotIndex::default());
    assert_eq!(v.len(), 3);
    let kinds_obs: Vec<&str> = v.iter().map(|x| x.kind).collect();
    assert_eq!(kinds_obs, vec!["event_count", "event_count", "event_count"]);
    let names: Vec<String> = v.iter().map(|x| x.name.clone()).collect();
    assert_eq!(names, vec!["a0".to_string(), "a1".to_string(), "a2".to_string()]);
}

// ──────────────────────────────────────────────────────────────────────
// §10.5 Streaming agrees with post-run
// ──────────────────────────────────────────────────────────────────────

#[test]
fn streaming_resolves_to_same_verdict_as_post_run() {
    let scen = scenario_with(vec![AssertionKind::EventCount {
        event_kind: "probe_sent".into(),
        max: 1,
    }]);
    let events = vec![
        evt(10, None, "swim", serde_json::json!({"kind": "probe_sent", "from": "a", "to": "b"}), 0),
        evt(20, None, "swim", serde_json::json!({"kind": "probe_sent", "from": "a", "to": "b"}), 1),
        evt(30, None, "swim", serde_json::json!({"kind": "probe_sent", "from": "a", "to": "b"}), 2),
    ];
    let post = evaluate(&scen, &events, &SnapshotIndex::default());
    let mut stream = StreamingEvaluator::new(scen);
    for e in &events {
        stream.feed_event(e.clone());
    }
    let now = stream.verdicts_now();
    assert_eq!(now, post);
}

// ──────────────────────────────────────────────────────────────────────
// §10.5 Streaming resolves as early as possible
// ──────────────────────────────────────────────────────────────────────

#[test]
fn streaming_resolves_event_count_fail_at_first_overshoot() {
    let scen = scenario_with(vec![AssertionKind::EventCount {
        event_kind: "probe_sent".into(),
        max: 1,
    }]);
    let mut stream = StreamingEvaluator::new(scen);
    stream.feed_event(evt(
        10,
        None,
        "swim",
        serde_json::json!({"kind": "probe_sent"}),
        0,
    ));
    // Still within bound (count=1, max=1).
    assert!(matches!(
        stream.verdicts_now()[0].outcome,
        Outcome::Pass | Outcome::Inconclusive
    ));
    stream.feed_event(evt(
        20,
        None,
        "swim",
        serde_json::json!({"kind": "probe_sent"}),
        1,
    ));
    // Now we have 2, which exceeds max=1 → Fail.
    assert_eq!(stream.verdicts_now()[0].outcome, Outcome::Fail);
}

// ──────────────────────────────────────────────────────────────────────
// End-to-end: evaluate_bundle writes verdicts.json
// ──────────────────────────────────────────────────────────────────────

#[test]
fn evaluate_bundle_writes_verdicts_json_with_one_entry_per_assertion() {
    let tmp = TempDir::new().unwrap();
    let scen = scenario_with(vec![
        AssertionKind::EventCount { event_kind: "probe_sent".into(), max: 0 },
        AssertionKind::EventCount { event_kind: "alpha".into(), max: 100 },
    ]);
    // Write a tiny bundle: one event of kind probe_sent (which makes
    // assertion 0 fail, assertion 1 pass since alpha has 0 events).
    let out = tmp.path().join("bundle");
    let mut w = FileBundleWriter::new(&out, scen.clone());
    w.write(BundleRecord::Event(EventRecord {
        virtual_time_ns: 5,
        host_id: Some("a".into()),
        kind_tag: "swim".into(),
        event: EventPayload::Bytes(br#"{"kind":"probe_sent","from":"a","to":"b"}"#.to_vec()),
    }));
    w.write(BundleRecord::Snapshot(SnapshotRecord {
        virtual_time_ns: 1,
        host_id: "a".into(),
        kind_tag: "swim".into(),
        snapshot: br#"{"members":{},"self_incarnation":0}"#.to_vec(),
    }));
    w.finalize().unwrap();
    let verdicts = evaluate_bundle(&out).unwrap();
    assert_eq!(verdicts.len(), 2);
    assert_eq!(verdicts[0].outcome, Outcome::Fail);
    assert_eq!(verdicts[1].outcome, Outcome::Pass);
    let text = std::fs::read_to_string(out.join("verdicts.json")).unwrap();
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2);
}
