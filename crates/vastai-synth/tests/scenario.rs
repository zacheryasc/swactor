//! Scenario-level checks on the pure generator. These assert on the *story* a
//! consumer would see (a deploy with a stall+replace, an OOM, teardowns) and on
//! invariants (monotonic cost, bounded utilization, determinism, schema
//! round-trip) rather than on internal record counts, so they survive a refactor
//! of how the scenario is assembled.

use distribution::diagnostics::vastai::record::{HostSample, LifecycleEvent, VastaiBody};
use vastai_synth::{PlannedRecord, Producer, ScenarioParams, generate};

fn default_run() -> Vec<vastai_synth::PlannedRecord> {
    generate(&ScenarioParams::new(4, 180_000, 42))
}

fn eight_node_run() -> Vec<PlannedRecord> {
    generate(&ScenarioParams::new(8, 180_000, 42))
}

/// All in-VM host samples emitted by a given stage's monitor.
fn stage_samples(recs: &[PlannedRecord], stage: usize) -> Vec<&HostSample> {
    recs.iter()
        .filter_map(|r| match (&r.producer, &r.body) {
            (Producer::Stage(s), VastaiBody::HostSample(h)) if *s == stage => Some(h),
            _ => None,
        })
        .collect()
}

fn lifecycle_events(recs: &[vastai_synth::PlannedRecord]) -> Vec<&LifecycleEvent> {
    recs.iter()
        .filter_map(|r| match &r.body {
            VastaiBody::Lifecycle(e) => Some(e),
            _ => None,
        })
        .collect()
}

#[test]
fn deploy_starts_once_and_leases_every_stage() {
    let recs = default_run();
    let events = lifecycle_events(&recs);

    let deploy_starts = events
        .iter()
        .filter(|e| matches!(e, LifecycleEvent::DeployStart { .. }))
        .count();
    assert_eq!(deploy_starts, 1, "exactly one deploy-start marker");

    // Every stage's primary contract (41000+i) must be leased at least once.
    for stage in 0..4u64 {
        let leased = events.iter().any(|e| {
            matches!(e, LifecycleEvent::ContractLeased { contract_id, .. } if *contract_id == 41_000 + stage)
        });
        assert!(leased, "stage {stage} primary contract should be leased");
    }
}

#[test]
fn stalled_stage_is_torn_down_and_replaced() {
    let recs = default_run();
    let events = lifecycle_events(&recs);

    // Stage 1 stalls: its primary (41001) is torn down with a pull-related reason,
    // and a distinct replacement contract is leased for the same stage.
    let primary_teardown = events.iter().any(|e| {
        matches!(e, LifecycleEvent::Teardown { contract_id, reason }
            if *contract_id == 41_001 && reason.as_deref() == Some("stalled image pull"))
    });
    assert!(primary_teardown, "stalled primary should be torn down");

    let replacement_leased = events.iter().any(|e| {
        matches!(e, LifecycleEvent::ContractLeased { contract_id, .. } if *contract_id == 41_501)
    });
    assert!(replacement_leased, "a replacement contract should be leased");
}

#[test]
fn oom_stage_reports_exited_with_oom_message() {
    let recs = default_run();

    // Stage 2 (41002) OOMs: at least one external observation reports it exited
    // with an OOM status message, and its in-VM stream carries an OOM traceback.
    let exited_oom = recs.iter().any(|r| match &r.body {
        VastaiBody::Instance(o) => {
            o.id == 41_002
                && o.actual_status.as_deref() == Some("exited")
                && o.status_msg.as_deref().is_some_and(|m| m.contains("OOM"))
        }
        _ => false,
    });
    assert!(exited_oom, "OOM stage should report an exited+OOM observation");

    let oom_log = recs.iter().any(|r| match (&r.producer, &r.body) {
        (Producer::Stage(2), VastaiBody::Logs(b)) => {
            b.lines.iter().any(|l| l.text.contains("OutOfMemoryError"))
        }
        _ => false,
    });
    assert!(oom_log, "OOM stage should emit an OOM traceback on its log stream");
}

#[test]
fn every_live_contract_is_torn_down_by_the_end() {
    let recs = default_run();
    let events = lifecycle_events(&recs);

    // Each leased contract id eventually has a matching teardown.
    let leased: std::collections::BTreeSet<u64> = events
        .iter()
        .filter_map(|e| match e {
            LifecycleEvent::ContractLeased { contract_id, .. } => Some(*contract_id),
            _ => None,
        })
        .collect();
    let torn: std::collections::BTreeSet<u64> = events
        .iter()
        .filter_map(|e| match e {
            LifecycleEvent::Teardown { contract_id, .. } => Some(*contract_id),
            _ => None,
        })
        .collect();
    assert_eq!(leased, torn, "every leased contract should be torn down");
}

#[test]
fn accumulated_cost_is_monotonic_per_contract() {
    let recs = default_run();
    let mut last: std::collections::BTreeMap<u64, f64> = std::collections::BTreeMap::new();
    for r in &recs {
        if let VastaiBody::Instance(o) = &r.body {
            if let Some(cost) = o.accumulated_cost {
                let prev = last.entry(o.id).or_insert(f64::NEG_INFINITY);
                assert!(
                    cost + 1e-9 >= *prev,
                    "cost for contract {} went backwards: {} -> {}",
                    o.id,
                    *prev,
                    cost
                );
                *prev = cost;
            }
        }
    }
    assert!(!last.is_empty(), "expected some cost observations");
}

#[test]
fn gpu_utilization_and_temperature_stay_in_plausible_bounds() {
    let recs = default_run();
    let mut saw_sample = false;
    for r in &recs {
        if let VastaiBody::HostSample(s) = &r.body {
            for g in &s.gpus {
                saw_sample = true;
                if let Some(u) = g.util_pct {
                    assert!((0.0..=100.0).contains(&u), "util out of range: {u}");
                }
                if let Some(t) = g.temp_c {
                    assert!((30.0..=100.0).contains(&t), "temp implausible: {t}");
                }
                if let (Some(p), Some(limit)) = (g.power_w, g.power_limit_w) {
                    assert!(p >= 0.0 && p <= limit + 1.0, "power {p} exceeds limit {limit}");
                }
            }
        }
    }
    assert!(saw_sample, "expected in-VM GPU samples");
}

#[test]
fn records_are_returned_in_emission_order() {
    let recs = default_run();
    assert!(
        recs.windows(2).all(|w| w[0].at_ms <= w[1].at_ms),
        "records must be sorted by at_ms"
    );
}

#[test]
fn same_seed_reproduces_identical_bodies() {
    let a = generate(&ScenarioParams::new(4, 180_000, 7));
    let b = generate(&ScenarioParams::new(4, 180_000, 7));
    assert_eq!(a.len(), b.len());
    for (x, y) in a.iter().zip(b.iter()) {
        assert_eq!(x.at_ms, y.at_ms);
        assert_eq!(x.producer, y.producer);
        // Bodies are equal iff their serialized JSON matches.
        let jx = serde_json::to_string(&x.body).unwrap();
        let jy = serde_json::to_string(&y.body).unwrap();
        assert_eq!(jx, jy, "body diverged at at_ms={}", x.at_ms);
    }
}

#[test]
fn different_seeds_diverge_but_keep_the_same_structure() {
    let a = generate(&ScenarioParams::new(4, 180_000, 1));
    let b = generate(&ScenarioParams::new(4, 180_000, 2));
    // Same scenario skeleton (same number/placement of records)...
    assert_eq!(a.len(), b.len());
    // ...but the noisy measurements differ somewhere.
    let differ = a.iter().zip(b.iter()).any(|(x, y)| {
        serde_json::to_string(&x.body).unwrap() != serde_json::to_string(&y.body).unwrap()
    });
    assert!(differ, "different seeds should produce different measurements");
}

#[test]
fn eight_node_deploy_leases_every_stage() {
    let recs = eight_node_run();
    let events = lifecycle_events(&recs);

    let started = events.iter().any(
        |e| matches!(e, LifecycleEvent::DeployStart { num_stages } if *num_stages == Some(8)),
    );
    assert!(started, "deploy-start should announce 8 stages");

    for stage in 0..8u64 {
        let leased = events.iter().any(|e| {
            matches!(e, LifecycleEvent::ContractLeased { contract_id, .. } if *contract_id == 41_000 + stage)
        });
        assert!(leased, "stage {stage} primary contract should be leased");
    }
}

#[test]
fn each_node_personality_shows_up_in_steady_state_telemetry() {
    // In the 8-node deploy, several nodes misbehave in characteristic ways. By the
    // time the run reaches steady state, each one's signature should be visible in
    // its in-VM telemetry — and distinguishable from the healthy baseline (stage 0).
    let recs = eight_node_run();

    let baseline = stage_samples(&recs, 0);
    let baseline_last = *baseline.last().expect("baseline node should emit samples");

    let disk_read = |h: &HostSample| h.disk.first().and_then(|d| d.read_bytes_per_s).unwrap_or(0.0);
    let net_rx = |h: &HostSample| h.net.first().and_then(|n| n.rx_bytes_per_s).unwrap_or(0.0);
    let cpu = |h: &HostSample| h.cpu.util_pct.unwrap_or(0.0);
    let gpu = |h: &HostSample| h.gpus.first().and_then(|g| g.util_pct).unwrap_or(0.0);

    // Disk-bound node (4) is still streaming from NVMe long after warmup.
    let disk = *stage_samples(&recs, 4).last().expect("disk node samples");
    assert!(
        disk_read(disk) > 10.0 * disk_read(baseline_last).max(1.0),
        "disk-bound node should sustain far higher disk read than the baseline"
    );

    // Net-bound node (5) is still pulling weights, saturating the NIC.
    let net = *stage_samples(&recs, 5).last().expect("net node samples");
    assert!(
        net_rx(net) > 10.0 * net_rx(baseline_last).max(1.0),
        "net-bound node should sustain far higher network rx than the baseline"
    );

    // Underutilized node (6) leaves its expensive GPU mostly idle.
    let idle = *stage_samples(&recs, 6).last().expect("underutilized node samples");
    assert!(
        gpu(idle) < 0.5 * gpu(baseline_last),
        "underutilized node's GPU should sit well below the baseline's"
    );

    // CPU-bound node (7) has its cores pinned while the GPU coasts.
    let cpu_bound = *stage_samples(&recs, 7).last().expect("cpu-bound node samples");
    assert!(
        cpu(cpu_bound) > 2.0 * cpu(baseline_last),
        "cpu-bound node should run its CPU far hotter than the baseline"
    );
}

#[test]
fn every_body_round_trips_through_the_collector_wire_format() {
    // Each planned body must serialize and parse back unchanged — the contract the
    // collector relies on when it stores the JSON and re-serves it over SSE.
    let recs = default_run();
    for r in &recs {
        let json = serde_json::to_value(&r.body).expect("serialize body");
        let back: VastaiBody = serde_json::from_value(json.clone()).expect("parse body");
        assert_eq!(
            serde_json::to_value(&back).unwrap(),
            json,
            "body did not round-trip at at_ms={}",
            r.at_ms
        );
    }
}
