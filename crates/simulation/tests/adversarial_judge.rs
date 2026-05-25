//! Adversarial tests written by the judge.
//!
//! Each test hunts a specific gap the spec/code might have on the
//! purpose the simulator exists to serve.

use serde_json::Value;
use simulation::bundle::{BundleRecord, EventPayload, SnapshotRecord, VecWriter};
use simulation::engine::Engine;
use simulation::evaluator::{
    EventLine, Outcome, SnapshotEntry, SnapshotIndex, evaluate,
};
use simulation::network::{Network, SendOutcome};
use simulation::scenario::{HostKindRegistry, load_from_str};
use simulation::stage_host::StageHostFactory;
use std::path::Path;

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn parse(text: &str) -> simulation::scenario::Scenario {
    load_from_str(Path::new("(test)"), text, &registry())
        .expect("scenario must validate")
}

fn payload_kind(b: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(b).ok()?;
    v["kind"].as_str().map(|s| s.to_string())
}

// ───────────────────────────────────────────────────────────────────
// Gap 1: BOUNDARY TIMING.
// SIM_SPEC §6A.6 (and the file's own existing test) names:
//   "Event-before-halt is observable. A scenario whose `duration_ns`
//   is the same nanosecond as a `WorkerExit` mutation's `at_ns`
//   produces a bundle containing the `worker_exited` event."
// The existing test (worker_exit_event_is_emitted_before_halt_takes_effect)
// sets worker_exit at half the duration, NOT at the boundary; this
// adversarial test exercises the actual at_ns == duration_ns case.
// ───────────────────────────────────────────────────────────────────
#[test]
fn worker_exit_at_exact_duration_still_emits_worker_exited_event() {
    let text = r#"
        name = "boundary"
        seed = 1
        duration_ns = 500_000_000

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
        kind_config = { name = "pp-s", address = "10.0.0.1:7700" }

        [[peers]]
        id = "other"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-other", address = "10.0.0.2:7700" }

        [[links]]
        from = "s"
        to = "other"
        [[links]]
        from = "other"
        to = "s"

        # Worker exit at exactly the run's duration. Per §6A.6 the
        # event-before-halt rule must still surface the worker_exited
        # event in the bundle.
        [[mutations]]
        at_ns = 500_000_000
        kind = "worker_exit"
        peer = "s"
        reason = "boundary crash"
    "#;
    let scen = parse(text);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(StageHostFactory));
    engine.auto_install_hosts();
    let _ = engine.run();
    let writer = engine.into_writer();
    let worker_exited: Vec<_> = writer
        .records
        .iter()
        .filter_map(|r| match r {
            BundleRecord::Event(e) => {
                if let EventPayload::Bytes(b) = &e.event {
                    payload_kind(b)
                        .filter(|k| k == "worker_exited")
                        .map(|_| e.host_id.clone())
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        worker_exited,
        vec![Some("s".to_string())],
        "boundary worker_exit must still produce worker_exited; the spec §6A.6 names this exact case"
    );
}

// ───────────────────────────────────────────────────────────────────
// Gap 2: SUBSTREAM ISOLATION UNDER STRUCTURAL EDITS.
// SIM_SPEC §7.2 says: "editing one link's policy must not perturb the
// draws on any other link, or every test edit becomes a new random
// universe and bisection is impossible." The companion test in
// network_invariants asserts policy-edit isolation; nothing tests
// that ADDING a new peer/link leaves existing edges' draws unchanged.
// If a sim run's bundle changes when an unrelated peer is added, the
// judge's "structural edit" attack succeeds — the gap §7.2 names is
// real.
// ───────────────────────────────────────────────────────────────────
fn three_peer_scenario_text(extra_peer: bool) -> String {
    let extra_peer_block = if extra_peer {
        r#"
        [[peers]]
        id = "z_unrelated"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-z", address = "10.0.0.99:7700" }
        "#
    } else {
        ""
    };
    let extra_link_block = if extra_peer {
        r#"
        [[links]]
        from = "z_unrelated"
        to = "a"
        [[links]]
        from = "a"
        to = "z_unrelated"
        "#
    } else {
        ""
    };
    format!(
        r#"
        name = "structural_edit"
        seed = 42
        duration_ns = 500_000_000

        [default_tick]
        period_ns = 100_000_000

        [default_link]
        latency_ns = 10_000_000
        jitter_stddev_ns = 5_000_000
        loss_prob_ppm = 100_000
        reorder_prob_ppm = 0
        bandwidth_bps = 1_000_000_000
        cold_dial_penalty_ns = 0
        cache_warm_after_ns = 1_000_000_000
        cache_invalidate_after_idle_ns = 10_000_000_000

        [[peers]]
        id = "a"
        kind = "stage"
        initial_state = "cold"
        kind_config = {{ name = "pp-a", address = "10.0.0.1:7700" }}

        [[peers]]
        id = "b"
        kind = "stage"
        initial_state = "cold"
        kind_config = {{ name = "pp-b", address = "10.0.0.2:7700" }}
        {extra_peer_block}
        [[links]]
        from = "a"
        to = "b"
        [[links]]
        from = "b"
        to = "a"
        {extra_link_block}
    "#
    )
}

fn run_collect_relevant_records(text: &str) -> Vec<String> {
    let scen = parse(text);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(StageHostFactory));
    engine.auto_install_hosts();
    let _ = engine.run();
    let writer = engine.into_writer();
    // Project to a host-id-filtered slice that excludes the added
    // peer entirely. The §7.2 claim is that the "alphabet a vs b"
    // behaviour is unchanged.
    let mut out = Vec::new();
    for r in writer.records.iter() {
        match r {
            BundleRecord::Event(e) => {
                let host = e.host_id.as_deref().unwrap_or("");
                if host == "a" || host == "b" || host.is_empty() {
                    // Bring in the event so we can compare.
                    let kind = match &e.event {
                        EventPayload::Bytes(b) => payload_kind(b).unwrap_or_default(),
                        EventPayload::CacheStateChange { .. } => "cache_state_change".into(),
                        EventPayload::DialStart { .. } => "dial_start".into(),
                        EventPayload::DialOutcome { .. } => "dial_outcome".into(),
                        EventPayload::DropOnSend { from, to, .. } => {
                            if from == "a" || from == "b" || to == "a" || to == "b" {
                                "drop_on_send".into()
                            } else {
                                continue;
                            }
                        }
                        EventPayload::DropOnDelivery { to, .. } => {
                            if to == "a" || to == "b" {
                                "drop_on_delivery".into()
                            } else {
                                continue;
                            }
                        }
                        EventPayload::RelayEnqueue { .. }
                        | EventPayload::RelayDequeue { .. }
                        | EventPayload::RelayDrop { .. } => continue,
                    };
                    out.push(format!("{}@{}|{}", host, e.virtual_time_ns, kind));
                }
            }
            _ => {}
        }
    }
    out
}

#[test]
fn adding_unrelated_peer_does_not_perturb_existing_peers_records() {
    let baseline = run_collect_relevant_records(&three_peer_scenario_text(false));
    let with_extra = run_collect_relevant_records(&three_peer_scenario_text(true));
    assert_eq!(
        baseline, with_extra,
        "Adding an unrelated peer must not perturb the records emitted by `a` and `b` \
         (substream isolation §7.2)."
    );
}


// Companion: prove the failure is specifically at the boundary, not
// a bug in the WorkerExit plumbing more broadly. With at_ns one ns
// less than duration_ns, the LocalRecv generated by the mutation
// pops at a strictly earlier instant than Terminate and the event
// is recorded — proving the implementation works correctly off
// the boundary and FAILS exactly on the §6A.6 boundary case.
#[test]
fn worker_exit_one_ns_below_duration_works_correctly() {
    let text = r#"
        name = "near_boundary"
        seed = 1
        duration_ns = 500_000_000

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
        kind_config = { name = "pp-s", address = "10.0.0.1:7700" }

        [[peers]]
        id = "other"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-other", address = "10.0.0.2:7700" }

        [[links]]
        from = "s"
        to = "other"
        [[links]]
        from = "other"
        to = "s"

        [[mutations]]
        at_ns = 499_999_999
        kind = "worker_exit"
        peer = "s"
        reason = "near-boundary crash"
    "#;
    let scen = parse(text);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(StageHostFactory));
    engine.auto_install_hosts();
    let _ = engine.run();
    let writer = engine.into_writer();
    let worker_exited: Vec<_> = writer
        .records
        .iter()
        .filter_map(|r| match r {
            BundleRecord::Event(e) => {
                if let EventPayload::Bytes(b) = &e.event {
                    payload_kind(b)
                        .filter(|k| k == "worker_exited")
                        .map(|_| e.host_id.clone())
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        worker_exited,
        vec![Some("s".to_string())],
        "off-boundary case must emit worker_exited — confirms the boundary failure is the bug"
    );
}

// ───────────────────────────────────────────────────────────────────
// Gap 3 (NEW, iter 4): END-TO-END BOUNDARY ENFORCEMENT for the
// `worker_alive_throughout` assertion (SIM_SPEC §10.1).
//
// Spec text (§10.1): "The window is inclusive on both ends: a halt
// at window_end_ns (or at duration_ns when the window spans the
// whole run) fails the assertion, because the §6A.3 synchronous
// dispatch rule guarantees the stage_lifecycle → Halted event
// appears in the bundle even at the boundary."
//
// The previous judge round only verified that the *event* survives
// the boundary. This test ties the two halves together: the
// assertion must surface the boundary halt as `Fail`. If the engine
// drops the lifecycle event (or the evaluator's window comparison
// is exclusive on the upper end), this assertion silently passes
// when a stage actually died at the deadline — exactly the
// invisibility failure the N3 report names.
// ───────────────────────────────────────────────────────────────────
#[test]
fn worker_alive_throughout_fails_at_exact_duration_boundary() {
    let text = r#"
        name = "alive_throughout_boundary"
        seed = 1
        duration_ns = 500_000_000

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
        kind_config = { name = "pp-s", address = "10.0.0.1:7700" }

        [[peers]]
        id = "other"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-other", address = "10.0.0.2:7700" }

        [[links]]
        from = "s"
        to = "other"
        [[links]]
        from = "other"
        to = "s"

        # Whole-run window plus a halt at the closing instant. The
        # spec §10.1 boundary clause says this must Fail.
        [[assertions]]
        kind = "worker_alive_throughout"
        peer = "s"
        window_start_ns = 0
        window_end_ns = 500_000_000

        [[mutations]]
        at_ns = 500_000_000
        kind = "worker_exit"
        peer = "s"
        reason = "boundary crash"
    "#;
    let scen = parse(text);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(StageHostFactory));
    engine.auto_install_hosts();
    let _ = engine.run();
    let writer = engine.into_writer();

    // Reconstruct the evaluator's view from the bundle so the test
    // exercises the same path a real run would.
    let mut events: Vec<EventLine> = Vec::new();
    let mut snapshots = SnapshotIndex::default();
    let mut line_idx = 0usize;
    let mut next_seq: std::collections::BTreeMap<String, u32> =
        std::collections::BTreeMap::new();
    for r in writer.records.iter() {
        match r {
            BundleRecord::Event(e) => {
                events.push(EventLine::from_event_record(e, line_idx));
                line_idx += 1;
            }
            BundleRecord::Mutation(m) => {
                events.push(EventLine::from_mutation_record(m, line_idx));
                line_idx += 1;
            }
            BundleRecord::Snapshot(s) => {
                let seq = next_seq.entry(s.host_id.clone()).or_insert(0);
                let entry = SnapshotEntry::from_snapshot_record(s, *seq);
                *seq += 1;
                snapshots
                    .by_host
                    .entry(s.host_id.clone())
                    .or_default()
                    .push(entry);
            }
        }
    }
    let verdicts = evaluate(&scen, &events, &snapshots);
    assert_eq!(
        verdicts.len(),
        1,
        "exactly one verdict from one assertion"
    );
    assert_eq!(
        verdicts[0].outcome,
        Outcome::Fail,
        "spec §10.1 boundary clause: halt at window_end_ns must Fail; \
         got {:?}. If this passes, either the bundle is missing the \
         stage_lifecycle→Halted event at duration_ns (the §6A.3 \
         synchronous-dispatch guarantee failed) or the evaluator's \
         window comparison is exclusive on the upper end (the §10.1 \
         inclusive clause failed). Either way, a stage that died at \
         the resolve deadline is now an invisible failure — the bug \
         class the simulator exists to make visible.",
        verdicts[0].outcome,
    );
}

// ───────────────────────────────────────────────────────────────────
// Gap 4 (NEW, iter 4): SNAPSHOT AT THE BOUNDARY captures the
// post-halt state.
//
// Spec §4.5 says snapshot "asks every live host for its snapshot()"
// at the scheduled time; §6A.4 says the stage snapshot includes
// `state` and `last_exit_reason` (the latter present only when
// `state == Halted`). Spec §4.1 names mutations as enqueued before
// snapshots — so at the same virtual time a mutation pops first.
// Combined with §6A.3's synchronous WorkerExit dispatch, a snapshot
// at `duration_ns` paired with a `WorkerExit` at `duration_ns` must
// show the stage in `Halted` with `last_exit_reason` populated.
//
// This is the operationally-interesting case: a calibration scenario
// asking "what state did the stage land in at the deadline?" gets a
// truthful answer only if both the synchronous dispatch and the
// snapshot's "live host" inclusion at the boundary work together.
// A killed-but-not-snapshot path would mark the host's terminal
// state as `Running` in the last snapshot — a silent diagnostic
// loss.
// ───────────────────────────────────────────────────────────────────
#[test]
fn snapshot_at_duration_captures_halted_state_after_worker_exit() {
    let text = r#"
        name = "snapshot_boundary"
        seed = 1
        duration_ns = 500_000_000

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
        kind_config = { name = "pp-s", address = "10.0.0.1:7700" }

        [[peers]]
        id = "other"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-other", address = "10.0.0.2:7700" }

        [[links]]
        from = "s"
        to = "other"
        [[links]]
        from = "other"
        to = "s"

        [[mutations]]
        at_ns = 500_000_000
        kind = "worker_exit"
        peer = "s"
        reason = "deadline crash"

        [[snapshots]]
        at_ns = 500_000_000
    "#;
    let scen = parse(text);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(StageHostFactory));
    engine.auto_install_hosts();
    let _ = engine.run();
    let writer = engine.into_writer();

    // Find the snapshot of `s` at duration_ns and check its `state`
    // and `last_exit_reason`.
    let snap: Option<&SnapshotRecord> = writer.records.iter().find_map(|r| match r {
        BundleRecord::Snapshot(s)
            if s.host_id == "s" && s.virtual_time_ns == 500_000_000 =>
        {
            Some(s)
        }
        _ => None,
    });
    let snap = snap.expect(
        "spec §4.5: snapshot at duration_ns must fire for every live host; \
         no snapshot for `s` found in the bundle.",
    );
    let parsed: Value = serde_json::from_slice(&snap.snapshot).expect("snapshot bytes JSON");
    assert_eq!(
        parsed["state"].as_str(),
        Some("Halted"),
        "spec §6A.4: snapshot must reflect post-WorkerExit state. The mutation pops \
         before the snapshot (lower construction-time seq) and is dispatched \
         synchronously (§6A.3), so by the time the snapshot fires the host is in \
         Halted. Got: {:?}",
        parsed["state"],
    );
    assert_eq!(
        parsed["last_exit_reason"].as_str(),
        Some("deadline crash"),
        "spec §6A.4: snapshot in Halted state must include last_exit_reason."
    );
}

// ───────────────────────────────────────────────────────────────────
// Gap 5 (NEW, iter 4): TWO WORKER EXITS AT THE EXACT BOUNDARY.
//
// The iter-3 fix processes WorkerExit synchronously. With multiple
// WorkerExit mutations at the same virtual time (the boundary), each
// must dispatch in turn and each must record its `worker_exited`
// event before `Terminate` pops. If the synchronous dispatch
// short-circuits on the first mutation (or if a same-time second
// mutation is silently dropped because the engine already considers
// time advanced), Layer C's "multi-stage simultaneous death" gets
// silently truncated to "first stage dies, others vanish."
// ───────────────────────────────────────────────────────────────────
#[test]
fn two_worker_exits_at_exact_duration_each_emit_worker_exited() {
    let text = r#"
        name = "two_at_boundary"
        seed = 1
        duration_ns = 500_000_000

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
        id = "s1"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-s1", address = "10.0.0.1:7700" }

        [[peers]]
        id = "s2"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-s2", address = "10.0.0.2:7700" }

        [[links]]
        from = "s1"
        to = "s2"
        [[links]]
        from = "s2"
        to = "s1"

        [[mutations]]
        at_ns = 500_000_000
        kind = "worker_exit"
        peer = "s1"
        reason = "joint crash 1"

        [[mutations]]
        at_ns = 500_000_000
        kind = "worker_exit"
        peer = "s2"
        reason = "joint crash 2"
    "#;
    let scen = parse(text);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(StageHostFactory));
    engine.auto_install_hosts();
    let _ = engine.run();
    let writer = engine.into_writer();

    let worker_exited_for: std::collections::BTreeSet<String> = writer
        .records
        .iter()
        .filter_map(|r| match r {
            BundleRecord::Event(e) => {
                if let EventPayload::Bytes(b) = &e.event {
                    payload_kind(b)
                        .filter(|k| k == "worker_exited")
                        .and_then(|_| e.host_id.clone())
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    let expected: std::collections::BTreeSet<String> =
        ["s1".to_string(), "s2".to_string()].into_iter().collect();
    assert_eq!(
        worker_exited_for, expected,
        "spec §6A.6 boundary case generalised to two same-time WorkerExits: both \
         stages must contribute one worker_exited record. If a same-time second \
         mutation is dropped (e.g. because the engine treats `now == duration_ns` \
         as terminal after the first sync dispatch), Layer C's multi-stage \
         deadline-collapse failure mode becomes invisible."
    );
}

// ───────────────────────────────────────────────────────────────────
// Gap 6 (NEW, iter 4): RELAY HOL across INDEPENDENT SENDERS to the
// same destination — the exact shape Layer A names.
//
// The N3 report (§A): "A shared queue servicing multiple peers
// couples otherwise-independent traffic: a 9.8 KB Ack from one peer
// delays every probe behind it on the same egress." The existing
// relay HOL test (`head_of_line_is_observable_on_a_shared_egress`)
// only exercises *same source, same destination* — it does not prove
// that the relay's shared egress couples *different* senders. If the
// implementation accidentally partitioned the egress queue per-
// (from, to) pair instead of per-to, the existing test still passes
// while the failure mode the simulator exists to reproduce is
// silently absent.
//
// This test: alpha sends a big message (slow egress to charlie),
// then bravo sends a small message (also to charlie). The small
// message must wait for the big one's egress serialization, because
// they share the egress link relay→charlie.
// ───────────────────────────────────────────────────────────────────
#[test]
fn relay_egress_hol_couples_independent_senders_on_shared_egress() {
    // Counterfactual HOL test: send bravo's small probe alone, then
    // re-run the same scenario with alpha's big message preceding
    // bravo's. The DIFFERENCE in bravo's arrivals must be at least
    // alpha's egress serialization time. This isolates HOL from
    // bravo's own egress and from latency.
    let text = r#"
        name = "egress_hol_two_senders"
        seed = 1
        duration_ns = 5_000_000_000

        [default_tick]
        period_ns = 100_000_000

        [default_link]
        # Fast inbound and outbound link bandwidth so the relay's
        # egress capacity is the dominant serialization term.
        latency_ns = 1_000
        jitter_stddev_ns = 0
        loss_prob_ppm = 0
        reorder_prob_ppm = 0
        bandwidth_bps = 10_000_000_000
        cold_dial_penalty_ns = 0
        cache_warm_after_ns = 1_000_000_000
        cache_invalidate_after_idle_ns = 100_000_000_000

        [[relays]]
        id = "R"
        ingress_capacity_bps = 10_000_000_000
        egress_capacity_bps_per_link = 8_000_000   # 8 MB/s per outbound link
        queue_depth_bytes = 10_000_000
        cold_start_penalty_ns = 0

        [[peers]]
        id = "alpha"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-a", address = "10.0.0.1:7700" }
        [[peers]]
        id = "bravo"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-b", address = "10.0.0.2:7700" }
        [[peers]]
        id = "charlie"
        kind = "stage"
        initial_state = "cold"
        kind_config = { name = "pp-c", address = "10.0.0.3:7700" }

        [[links]]
        from = "alpha"
        to = "charlie"
        via = "R"
        [[links]]
        from = "bravo"
        to = "charlie"
        via = "R"
    "#;
    let scen = parse(text);

    let big = 100_000u64; // alpha: 100 KB. Egress at 8 MB/s ⇒ 12.5 ms.
    let small = 100u64;

    // Run A: bravo alone.
    let mut net_alone = Network::new(&scen);
    let SendOutcome::Arrive { at_ns: bravo_alone, .. } =
        net_alone.send("bravo", "charlie", small, 0)
    else {
        panic!("bravo→charlie must arrive (alone)");
    };

    // Run B: alpha first, then bravo on the same Network.
    let mut net_coupled = Network::new(&scen);
    let SendOutcome::Arrive { .. } = net_coupled.send("alpha", "charlie", big, 0) else {
        panic!("alpha→charlie must arrive (coupled)");
    };
    let SendOutcome::Arrive { at_ns: bravo_coupled, .. } =
        net_coupled.send("bravo", "charlie", small, 0)
    else {
        panic!("bravo→charlie must arrive (coupled)");
    };

    let alpha_egress_serialization_ns = 12_500_000u64; // 100_000 / 8_000_000 * 1e9
    let hol_delay = bravo_coupled - bravo_alone;
    assert!(
        hol_delay >= alpha_egress_serialization_ns - 100_000,
        "spec §5A.3 / Layer A: the shared egress relay→charlie must serialize \
         independent senders. Adding a preceding 100KB alpha→charlie message must \
         delay bravo's small probe by at least alpha's egress serialization \
         (~{alpha_egress_serialization_ns}ns); got hol_delay={hol_delay} \
         (bravo_alone={bravo_alone}, bravo_coupled={bravo_coupled}). \
         If hol_delay is ≪ the floor, the relay's egress queue is \
         (incorrectly) partitioned per source, and the Layer A failure mode \
         (per-peer-independent traffic coupled at a shared egress) cannot be \
         reproduced — defeating the simulator's purpose.",
    );
}
