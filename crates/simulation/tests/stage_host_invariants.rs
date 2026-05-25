//! RELAY_SPEC §5.6 behavioural tests for the stage host kind.
//!
//! The stage host is exercised both as a unit (direct calls to
//! `tick` / `recv` / `snapshot`) and as a participant in an engine
//! run (`WorkerExit` mutation routing, abort-on-wrong-kind).

use serde_json::Value;
use simulation::bundle::{BundleRecord, EventPayload, VecWriter};
use simulation::engine::{Engine, EngineAbort, TerminationReason};
use simulation::host::{Action, Host, HostMessage};
use simulation::network::Network;
use simulation::scenario::{HostKindRegistry, Mutation, MutationKind, load_from_str};
use simulation::stage_host::{StageHost, StageHostFactory, StageState};
use std::path::Path;

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn parse(text: &str) -> simulation::scenario::Scenario {
    load_from_str(Path::new("(test)"), text, &registry())
        .expect("scenario must validate")
}

fn payload_kind(b: &[u8]) -> String {
    serde_json::from_slice::<Value>(b).unwrap()["kind"]
        .as_str()
        .unwrap()
        .to_string()
}

// ────────────────────────────────────────────────────────────────────
// RELAY_SPEC §5.6
// ────────────────────────────────────────────────────────────────────

#[test]
fn trait_conformance_kind_tag_is_stage() {
    let host = StageHost::new("h1", "name", "addr");
    assert_eq!(host.kind_tag(), "stage");
    assert_eq!(host.id(), "h1");
}

#[test]
fn first_tick_emits_lifecycle_and_register_name_in_order() {
    // §5.2 — Cold → Registering, register_name, Registering → Running.
    // The three RecordEvent actions appear in declared order; the
    // host emits no `Send` or `Halt` on this tick.
    let mut host = StageHost::new("stage_0", "pp-stage-0", "10.0.0.10:7700");
    let actions = host.tick(1_000_000);
    assert_eq!(actions.len(), 3);
    let mut kinds = Vec::new();
    for action in &actions {
        match action {
            Action::RecordEvent { kind_tag, event } => {
                assert_eq!(kind_tag, "stage");
                kinds.push(payload_kind(event));
            }
            other => panic!("first-tick action must be RecordEvent, got {other:?}"),
        }
    }
    assert_eq!(
        kinds,
        vec![
            "stage_lifecycle".to_string(),
            "register_name".to_string(),
            "stage_lifecycle".to_string(),
        ]
    );
}

#[test]
fn subsequent_ticks_are_noops_once_running() {
    // §5.6 "Lifecycle linearity." After the first tick takes us to
    // Running, subsequent ticks emit no actions until WorkerExit
    // arrives.
    let mut host = StageHost::new("s", "n", "a");
    let _ = host.tick(0);
    for t in 1..10 {
        assert!(host.tick(t).is_empty());
    }
}

#[test]
fn worker_exit_emits_worker_exited_then_lifecycle_then_halt() {
    // §5.3 — recv(WorkerExit) returns exactly RecordEvent
    // (worker_exited), RecordEvent (stage_lifecycle → Halted), Halt.
    // Order is normative.
    let mut host = StageHost::new("s", "n", "a");
    let _ = host.tick(0);
    let actions = host.recv(
        HostMessage::WorkerExit {
            reason: "crashed".into(),
            status_code: Some(2),
            signal: None,
        },
        100_000,
    );
    assert_eq!(actions.len(), 3);
    match &actions[0] {
        Action::RecordEvent { kind_tag, event } => {
            assert_eq!(kind_tag, "stage");
            assert_eq!(payload_kind(event), "worker_exited");
            let parsed: Value = serde_json::from_slice(event).unwrap();
            assert_eq!(parsed["reason"], "crashed");
            assert_eq!(parsed["status_code"], 2);
        }
        other => panic!("action[0] must be RecordEvent(worker_exited), got {other:?}"),
    }
    match &actions[1] {
        Action::RecordEvent { kind_tag, event } => {
            assert_eq!(kind_tag, "stage");
            assert_eq!(payload_kind(event), "stage_lifecycle");
            let parsed: Value = serde_json::from_slice(event).unwrap();
            assert_eq!(parsed["to"], "Halted");
        }
        other => panic!("action[1] must be RecordEvent(stage_lifecycle), got {other:?}"),
    }
    assert!(matches!(actions[2], Action::Halt));
}

#[test]
fn snapshot_includes_name_registry_after_registration() {
    // §5.6 "Snapshot contains the registry." Once the stage has
    // registered, every subsequent snapshot includes the name.
    let mut host = StageHost::new("s", "my-name", "10.0.0.1:7700");
    let _ = host.tick(0);
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).unwrap();
    assert_eq!(parsed["name_registry"]["my-name"], "10.0.0.1:7700");
    assert_eq!(parsed["state"], "Running");
}

#[test]
fn snapshot_includes_last_exit_reason_after_halt() {
    let mut host = StageHost::new("s", "n", "a");
    let _ = host.tick(0);
    let _ = host.recv(
        HostMessage::WorkerExit {
            reason: "stopped".into(),
            status_code: None,
            signal: None,
        },
        100_000,
    );
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).unwrap();
    assert_eq!(parsed["state"], "Halted");
    assert_eq!(parsed["last_exit_reason"], "stopped");
}

#[test]
fn determinism_same_inputs_produce_same_action_sequence() {
    let mk = || StageHost::new("h", "n", "a");
    let go = |mut h: StageHost| -> Vec<String> {
        let mut out = Vec::new();
        for action in h.tick(0) {
            if let Action::RecordEvent { event, .. } = action {
                out.push(payload_kind(&event));
            }
        }
        for action in h.recv(
            HostMessage::WorkerExit {
                reason: "x".into(),
                status_code: None,
                signal: None,
            },
            10,
        ) {
            match action {
                Action::RecordEvent { event, .. } => out.push(payload_kind(&event)),
                Action::Halt => out.push("Halt".into()),
                _ => {}
            }
        }
        out
    };
    assert_eq!(go(mk()), go(mk()));
}

// ────────────────────────────────────────────────────────────────────
// Engine integration: WorkerExit mutation routing
// ────────────────────────────────────────────────────────────────────

fn one_stage_scenario(worker_exit_at_ns: u64) -> simulation::scenario::Scenario {
    parse(&format!(
        r#"
        name = "one_stage"
        seed = 1
        duration_ns = 1_000_000_000

        [default_tick]
        period_ns = 100_000_000

        [default_link]
        latency_ns = 1_000_000
        jitter_stddev_ns = 0
        loss_prob_ppm = 0
        reorder_prob_ppm = 0
        bandwidth_bps = 1_000_000_000
        cold_dial_penalty_ns = 0
        cache_warm_after_ns = 1_000_000_000
        cache_invalidate_after_idle_ns = 10_000_000_000

        [[peers]]
        id = "s"
        kind = "stage"
        initial_state = "cold"
        kind_config = {{ name = "pp-s", address = "10.0.0.1:7700" }}

        [[peers]]
        id = "other"
        kind = "stage"
        initial_state = "cold"
        kind_config = {{ name = "pp-other", address = "10.0.0.2:7700" }}

        [[links]]
        from = "s"
        to = "other"
        [[links]]
        from = "other"
        to = "s"

        [[mutations]]
        at_ns = {worker_exit_at_ns}
        kind = "worker_exit"
        peer = "s"
        reason = "internal crash"
        "#
    ))
}

#[test]
fn worker_exit_event_is_emitted_before_halt_takes_effect() {
    // §5.6 "Event-before-halt is observable." A scenario whose
    // duration matches the worker_exit time still contains the
    // `worker_exited` event in its bundle.
    let scen = one_stage_scenario(500_000_000);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(StageHostFactory));
    engine.auto_install_hosts();
    let _ = engine.run();
    let writer = engine.into_writer();
    let worker_exited = writer
        .records
        .iter()
        .filter_map(|r| match r {
            BundleRecord::Event(e) => {
                if let EventPayload::Bytes(b) = &e.event {
                    let v: Value = serde_json::from_slice(b).ok()?;
                    if v["kind"] == "worker_exited" {
                        return Some(e.host_id.clone());
                    }
                }
                None
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        worker_exited,
        vec![Some("s".to_string())],
        "exactly one worker_exited event from the targeted stage"
    );
}

#[test]
fn worker_exit_against_swim_aborts_run() {
    // §5.6 "WorkerExit against SWIM aborts." A WorkerExit mutation
    // targeting a SWIM-kind peer aborts the run with a structured
    // error naming the kind and the mutation index.
    //
    // We construct the scenario in code (bypassing the loader's
    // own §8.2 guard, which already rejects this at load time) so
    // the engine's runtime gate is what the test actually exercises.
    use simulation::scenario::{
        DefaultTick, HostRoute, Link, LinkPolicy, Peer, Scenario,
    };
    let policy = LinkPolicy {
        latency_ns: 1_000_000,
        jitter_stddev_ns: 0,
        loss_prob_ppm: 0,
        reorder_prob_ppm: 0,
        bandwidth_bps: 1_000_000_000,
        cold_dial_penalty_ns: 0,
        cache_warm_after_ns: 1_000_000_000,
        cache_invalidate_after_idle_ns: 10_000_000_000,
    };
    let mut config = toml::value::Table::new();
    config.insert("probe_interval_ns".into(), toml::Value::Integer(1_000_000));
    config.insert(
        "suspicion_timeout_ns".into(),
        toml::Value::Integer(5_000_000),
    );
    let scen = Scenario {
        name: "swim_worker_exit".into(),
        seed: 1,
        duration_ns: 1_000_000_000,
        early_terminate_on_all_assertions_resolved: false,
        default_tick: DefaultTick {
            period_ns: 100_000_000,
        },
        default_link: policy,
        peers: vec![Peer {
            id: "x".into(),
            kind: "swim".into(),
            kind_config: config,
            initial_state: "alive".into(),
            tick_period_ns_override: None,
        }],
        relays: Vec::new(),
        links: vec![Link {
            from: "x".into(),
            to: "x".into(),
            policy,
        }],
        mutations: vec![Mutation {
            at_ns: 10_000_000,
            kind: MutationKind::WorkerExit {
                peer: "x".into(),
                reason: "should abort".into(),
                status_code: None,
                signal: None,
            },
        }],
        snapshots: vec![],
        assertions: vec![],
        routes: vec![HostRoute::Direct {
            from: "x".into(),
            to: "x".into(),
        }],
    };
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    // Don't install any SWIM host — engine still has the peer spec
    // by kind. The abort fires at mutation dispatch, before any
    // host call.
    let result = engine.run();
    assert!(
        matches!(
            result,
            TerminationReason::Aborted(EngineAbort::WorkerExitOnWrongKind {
                ref kind,
                mutation_index: 0,
                ..
            }) if kind == "swim"
        ),
        "expected WorkerExitOnWrongKind abort, got {result:?}"
    );
}

#[test]
fn lifecycle_stages_appear_in_order_in_bundle() {
    // §5.6 "Lifecycle linearity." End-to-end through the engine, the
    // stage host emits Cold→Registering, Registering→Running, and
    // eventually Running→Halted lifecycle events in that order.
    let scen = one_stage_scenario(500_000_000);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(StageHostFactory));
    engine.auto_install_hosts();
    let _ = engine.run();
    let writer = engine.into_writer();
    let lifecycle: Vec<(String, String)> = writer
        .records
        .iter()
        .filter_map(|r| match r {
            BundleRecord::Event(e) if e.host_id.as_deref() == Some("s") => {
                if let EventPayload::Bytes(b) = &e.event {
                    let v: Value = serde_json::from_slice(b).ok()?;
                    if v["kind"] == "stage_lifecycle" {
                        return Some((
                            v["from"].as_str().unwrap_or("").to_string(),
                            v["to"].as_str().unwrap_or("").to_string(),
                        ));
                    }
                }
                None
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        lifecycle,
        vec![
            ("Cold".to_string(), "Registering".to_string()),
            ("Registering".to_string(), "Running".to_string()),
            ("Running".to_string(), "Halted".to_string()),
        ]
    );
}

#[test]
fn cold_state_string_is_well_known() {
    // Sanity: the lifecycle state strings are stable contract.
    // RELAY_SPEC §5.2 names them; tests outside this file
    // (assertion evaluators, calibration scenarios) match on them.
    assert_eq!(StageState::Cold.as_str(), "Cold");
    assert_eq!(StageState::Registering.as_str(), "Registering");
    assert_eq!(StageState::Running.as_str(), "Running");
    assert_eq!(StageState::Halted.as_str(), "Halted");
}
