//! RELAY_SPEC §7.2 behavioural property tests for the three new
//! assertion kinds: `relay_queue_depth_bounded`,
//! `worker_alive_throughout`, and `name_resolves_within`.

use std::collections::BTreeMap;

use simulation::evaluator::{EventLine, Outcome, SnapshotEntry, SnapshotIndex, evaluate};
use simulation::scenario::{
    Assertion, AssertionKind, DefaultTick, HostRoute, Link, LinkPolicy, Peer, Scenario,
};

// ──────────────────────────────────────────────────────────────────────
// Fixtures
// ──────────────────────────────────────────────────────────────────────

fn policy() -> LinkPolicy {
    LinkPolicy {
        latency_ns: 1,
        jitter_stddev_ns: 0,
        loss_prob_ppm: 0,
        reorder_prob_ppm: 0,
        bandwidth_bps: 1_000_000_000,
        cold_dial_penalty_ns: 0,
        cache_warm_after_ns: 1,
        cache_invalidate_after_idle_ns: 1_000_000_000,
    }
}

fn peer(id: &str, kind: &str) -> Peer {
    let mut cfg = toml::value::Table::new();
    if kind == "stage" {
        cfg.insert("name".into(), toml::Value::String(format!("pp-{id}")));
        cfg.insert(
            "address".into(),
            toml::Value::String(format!("10.0.0.1:{}", 7000 + id.len())),
        );
    } else {
        cfg.insert("probe_interval_ns".into(), toml::Value::Integer(1_000));
        cfg.insert("suspicion_timeout_ns".into(), toml::Value::Integer(5_000));
    }
    Peer {
        id: id.into(),
        kind: kind.into(),
        kind_config: cfg,
        initial_state: if kind == "stage" { "cold" } else { "alive" }.into(),
        tick_period_ns_override: None,
    }
}

fn base_scenario(peers: Vec<Peer>, assertions: Vec<Assertion>) -> Scenario {
    let mut links = Vec::new();
    let mut routes = Vec::new();
    for a in &peers {
        for b in &peers {
            if a.id == b.id {
                continue;
            }
            links.push(Link {
                from: a.id.clone(),
                to: b.id.clone(),
                policy: policy(),
            });
            routes.push(HostRoute::Direct {
                from: a.id.clone(),
                to: b.id.clone(),
            });
        }
    }
    Scenario {
        name: "ev".into(),
        seed: 1,
        duration_ns: 1_000_000_000,
        early_terminate_on_all_assertions_resolved: false,
        default_tick: DefaultTick {
            period_ns: 1_000_000,
        },
        default_link: policy(),
        peers,
        relays: Vec::new(),
        links,
        mutations: Vec::new(),
        snapshots: Vec::new(),
        assertions,
        routes,
    }
}

fn relay_enqueue(virtual_time_ns: u64, relay: &str, byte_len: u64, line_idx: usize) -> EventLine {
    EventLine {
        virtual_time_ns,
        host_id: None,
        kind_tag: "relay".into(),
        event: serde_json::json!({
            "kind": "relay_enqueue",
            "relay": relay,
            "from": "a",
            "to": "b",
            "byte_len": byte_len,
        }),
        line_idx,
    }
}

fn relay_dequeue(virtual_time_ns: u64, relay: &str, byte_len: u64, line_idx: usize) -> EventLine {
    EventLine {
        virtual_time_ns,
        host_id: None,
        kind_tag: "relay".into(),
        event: serde_json::json!({
            "kind": "relay_dequeue",
            "relay": relay,
            "from": "a",
            "to": "b",
            "byte_len": byte_len,
        }),
        line_idx,
    }
}

fn stage_lifecycle(virtual_time_ns: u64, host: &str, to: &str, line_idx: usize) -> EventLine {
    EventLine {
        virtual_time_ns,
        host_id: Some(host.into()),
        kind_tag: "stage".into(),
        event: serde_json::json!({
            "kind": "stage_lifecycle",
            "from": "Running",
            "to": to,
        }),
        line_idx,
    }
}

fn snapshot_with_registry(
    virtual_time_ns: u64,
    seq: u32,
    registry: &[(&str, &str)],
) -> SnapshotEntry {
    SnapshotEntry {
        virtual_time_ns,
        seq,
        members: BTreeMap::new(),
        self_incarnation: 0,
        name_registry: registry
            .iter()
            .map(|(n, a)| ((*n).into(), (*a).into()))
            .collect(),
    }
}

fn idx(snaps: &[(&str, Vec<SnapshotEntry>)]) -> SnapshotIndex {
    let mut by_host = BTreeMap::new();
    for (h, entries) in snaps {
        by_host.insert((*h).to_string(), entries.clone());
    }
    SnapshotIndex { by_host }
}

// ────────────────────────────────────────────────────────────────────
// RELAY_SPEC §7.2
// ────────────────────────────────────────────────────────────────────

#[test]
fn relay_queue_depth_bounded_passes_when_depth_stays_under_bound() {
    // §7.2 "Relay-queue-depth soundness." Pass when the queue never
    // exceeds the bound across the window.
    let events = vec![
        relay_enqueue(100, "R", 500, 0),
        relay_dequeue(200, "R", 500, 1),
        relay_enqueue(300, "R", 800, 2),
        relay_dequeue(400, "R", 800, 3),
    ];
    let scen = base_scenario(
        vec![peer("a", "swim"), peer("b", "swim")],
        vec![Assertion {
            kind: AssertionKind::RelayQueueDepthBounded {
                relay: "R".into(),
                max_bytes: 1000,
                window_start_ns: None,
                window_end_ns: None,
            },
        }],
    );
    let verdicts = evaluate(&scen, &events, &SnapshotIndex::default());
    assert_eq!(verdicts[0].outcome, Outcome::Pass);
}

#[test]
fn relay_queue_depth_bounded_fails_on_first_enqueue_past_bound() {
    // Evidence on Fail references the exact RelayEnqueue that
    // crossed the bound.
    let events = vec![
        relay_enqueue(100, "R", 600, 0),
        relay_enqueue(150, "R", 500, 1), // pushes depth to 1100 > 1000
        relay_dequeue(200, "R", 600, 2),
    ];
    let scen = base_scenario(
        vec![peer("a", "swim"), peer("b", "swim")],
        vec![Assertion {
            kind: AssertionKind::RelayQueueDepthBounded {
                relay: "R".into(),
                max_bytes: 1000,
                window_start_ns: None,
                window_end_ns: None,
            },
        }],
    );
    let verdicts = evaluate(&scen, &events, &SnapshotIndex::default());
    assert_eq!(verdicts[0].outcome, Outcome::Fail);
    assert_eq!(verdicts[0].evidence.len(), 1);
    assert_eq!(verdicts[0].evidence[0].virtual_time_ns, 150);
}

#[test]
fn relay_queue_depth_bounded_is_inconclusive_with_no_relay_events() {
    let scen = base_scenario(
        vec![peer("a", "swim"), peer("b", "swim")],
        vec![Assertion {
            kind: AssertionKind::RelayQueueDepthBounded {
                relay: "R".into(),
                max_bytes: 100,
                window_start_ns: None,
                window_end_ns: None,
            },
        }],
    );
    let verdicts = evaluate(&scen, &[], &SnapshotIndex::default());
    assert_eq!(verdicts[0].outcome, Outcome::Inconclusive);
}

#[test]
fn relay_queue_depth_bounded_respects_window() {
    // An enqueue outside the window should not trigger a fail even
    // if it pushes the depth past the bound.
    let events = vec![
        relay_enqueue(100, "R", 2000, 0), // outside window — not a fail
        relay_dequeue(150, "R", 2000, 1),
        relay_enqueue(500, "R", 500, 2), // inside window — within bound
    ];
    let scen = base_scenario(
        vec![peer("a", "swim"), peer("b", "swim")],
        vec![Assertion {
            kind: AssertionKind::RelayQueueDepthBounded {
                relay: "R".into(),
                max_bytes: 1000,
                window_start_ns: Some(400),
                window_end_ns: Some(1000),
            },
        }],
    );
    let verdicts = evaluate(&scen, &events, &SnapshotIndex::default());
    assert_eq!(verdicts[0].outcome, Outcome::Pass);
}

#[test]
fn worker_alive_throughout_passes_on_no_halt_in_window() {
    // §7.2 "Worker-alive soundness." Pass when no stage_lifecycle
    // event into "Halted" appears in the window.
    let events = vec![
        // A non-Halt lifecycle keeps us out of the inconclusive arm.
        EventLine {
            virtual_time_ns: 100,
            host_id: Some("s".into()),
            kind_tag: "stage".into(),
            event: serde_json::json!({
                "kind": "stage_lifecycle",
                "from": "Cold",
                "to": "Registering",
            }),
            line_idx: 0,
        },
    ];
    let scen = base_scenario(
        vec![peer("s", "stage")],
        vec![Assertion {
            kind: AssertionKind::WorkerAliveThroughout {
                peer: "s".into(),
                window_start_ns: 0,
                window_end_ns: 1_000,
            },
        }],
    );
    let verdicts = evaluate(&scen, &events, &SnapshotIndex::default());
    assert_eq!(verdicts[0].outcome, Outcome::Pass);
}

#[test]
fn worker_alive_throughout_fails_on_halt_in_window() {
    let events = vec![
        stage_lifecycle(50, "s", "Registering", 0),
        stage_lifecycle(60, "s", "Running", 1),
        stage_lifecycle(500, "s", "Halted", 2),
    ];
    let scen = base_scenario(
        vec![peer("s", "stage")],
        vec![Assertion {
            kind: AssertionKind::WorkerAliveThroughout {
                peer: "s".into(),
                window_start_ns: 0,
                window_end_ns: 1_000,
            },
        }],
    );
    let verdicts = evaluate(&scen, &events, &SnapshotIndex::default());
    assert_eq!(verdicts[0].outcome, Outcome::Fail);
    assert_eq!(verdicts[0].evidence[0].virtual_time_ns, 500);
}

#[test]
fn worker_alive_throughout_is_inconclusive_with_no_lifecycle_events() {
    let scen = base_scenario(
        vec![peer("s", "stage")],
        vec![Assertion {
            kind: AssertionKind::WorkerAliveThroughout {
                peer: "s".into(),
                window_start_ns: 0,
                window_end_ns: 1_000,
            },
        }],
    );
    let verdicts = evaluate(&scen, &[], &SnapshotIndex::default());
    assert_eq!(verdicts[0].outcome, Outcome::Inconclusive);
}

#[test]
fn name_resolves_within_passes_when_every_observer_sees_name_in_window() {
    let snapshots = idx(&[
        ("alpha", vec![snapshot_with_registry(100, 0, &[("pp-stage-0", "10.0.0.1:7700")])]),
        ("bravo", vec![snapshot_with_registry(150, 0, &[("pp-stage-0", "10.0.0.1:7700")])]),
    ]);
    let scen = base_scenario(
        vec![peer("alpha", "swim"), peer("bravo", "swim")],
        vec![Assertion {
            kind: AssertionKind::NameResolvesWithin {
                name: "pp-stage-0".into(),
                observers: vec!["alpha".into(), "bravo".into()],
                within_ns: 500,
                from_ns: 0,
            },
        }],
    );
    let verdicts = evaluate(&scen, &[], &snapshots);
    assert_eq!(verdicts[0].outcome, Outcome::Pass);
}

#[test]
fn name_resolves_within_fails_when_an_observer_never_sees_name() {
    let snapshots = idx(&[
        ("alpha", vec![snapshot_with_registry(100, 0, &[("pp-stage-0", "addr")])]),
        ("bravo", vec![snapshot_with_registry(150, 0, &[("other", "addr")])]),
    ]);
    let scen = base_scenario(
        vec![peer("alpha", "swim"), peer("bravo", "swim")],
        vec![Assertion {
            kind: AssertionKind::NameResolvesWithin {
                name: "pp-stage-0".into(),
                observers: vec!["alpha".into(), "bravo".into()],
                within_ns: 500,
                from_ns: 0,
            },
        }],
    );
    let verdicts = evaluate(&scen, &[], &snapshots);
    assert_eq!(verdicts[0].outcome, Outcome::Fail);
}

#[test]
fn name_resolves_within_is_inconclusive_when_no_observer_snapshots_in_window() {
    let snapshots = idx(&[
        ("alpha", vec![snapshot_with_registry(5, 0, &[("pp-stage-0", "addr")])]),
    ]);
    let scen = base_scenario(
        vec![peer("alpha", "swim")],
        vec![Assertion {
            kind: AssertionKind::NameResolvesWithin {
                name: "pp-stage-0".into(),
                observers: vec!["alpha".into()],
                within_ns: 5,
                from_ns: 100, // no snapshot at or after t=100
            },
        }],
    );
    let verdicts = evaluate(&scen, &[], &snapshots);
    assert_eq!(verdicts[0].outcome, Outcome::Inconclusive);
}
