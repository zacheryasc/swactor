//! T-topology: SWIM name registration + per-stage neighbour resolution.
//!
//! Covers TEST_SPEC §5. Names are computed from `STAGE` + `NUM_STAGES`
//! alone — no central topology config. Stage 0 registers `pp-entry` and
//! `pp-stage-0`; the last stage (stage 1 in the two-stage MVP) registers
//! `pp-exit` and `pp-stage-{N-1}`.

use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::actor::ActorAddress;

use pipeline_parallel_inference::topology::{
    ENTRY_NAME, EXIT_NAME, next_stage_name, prev_stage_name, register_stage_names, stage_name,
};

// ── SWIM / driver helpers (mirror t_cluster.rs from single-gpu-inference) ──

fn test_node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: 1,
            probe_timeout: 3,
            indirect_probes: 1,
            suspicion_timeout: 5,
            dead_reprobe_interval: 0,
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        republish_interval: 50,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

fn make_driver() -> IrohDriver {
    IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: test_node_config(),
        peer_auth: None,
        additional_alpns: vec![],
    })
    .expect("failed to create iroh driver")
}

fn pump_one(driver: &mut IrohDriver) {
    driver.recv();
    driver.tick();
}

fn sees_alive(driver: &IrohDriver, peer_key: &PublicKey) -> bool {
    let snap = driver.snapshot();
    let peer_hex: String = peer_key
        .as_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    snap.members
        .iter()
        .any(|m| m.node_id == peer_hex && m.state == "alive")
}

fn pubkey_of(driver: &IrohDriver) -> PublicKey {
    PublicKey::from_bytes(&driver.node_id().0).unwrap()
}

/// Build a three-node cluster (orchestrator + stage 0 + stage 1) joined via
/// the orchestrator as the seed. Returns the drivers in `[orchestrator,
/// stage0, stage1]` order once every node sees every other node alive.
fn make_three_node_cluster() -> [IrohDriver; 3] {
    let mut orch = make_driver();
    let mut s0 = make_driver();
    let mut s1 = make_driver();

    let seed = orch.endpoint_addr();
    s0.join(&[seed.clone()]);
    s1.join(&[seed]);

    let orch_key = pubkey_of(&orch);
    let s0_key = pubkey_of(&s0);
    let s1_key = pubkey_of(&s1);

    let start = Instant::now();
    let mut converged = false;
    while start.elapsed() < Duration::from_secs(10) {
        pump_one(&mut orch);
        pump_one(&mut s0);
        pump_one(&mut s1);

        let orch_sees_all = sees_alive(&orch, &s0_key) && sees_alive(&orch, &s1_key);
        let s0_sees_all = sees_alive(&s0, &orch_key) && sees_alive(&s0, &s1_key);
        let s1_sees_all = sees_alive(&s1, &orch_key) && sees_alive(&s1, &s0_key);
        if orch_sees_all && s0_sees_all && s1_sees_all {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(converged, "3-node cluster did not converge within 10s");
    [orch, s0, s1]
}

/// Pump all three drivers until `cond(s0, s1)` returns true (used for waiting
/// out registry gossip propagation between stages).
fn pump_until(
    drivers: &mut [IrohDriver; 3],
    timeout: Duration,
    cond: impl Fn(&IrohDriver, &IrohDriver) -> bool,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        for d in drivers.iter_mut() {
            pump_one(d);
        }
        if cond(&drivers[1], &drivers[2]) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

// ── §5 tests ──────────────────────────────────────────────────────────────

/// Stage 0 boot registers both `pp-entry` and `pp-stage-0`, and both names
/// resolve to the same `ActorAddress` on the local node.
#[test]
fn stage_0_registers_pp_entry_and_pp_stage_0() {
    let mut driver = make_driver();
    let stage_addr = ActorAddress::new_random();

    let registered = register_stage_names(driver.node_mut(), 0, 2, stage_addr);

    assert!(registered.contains(&ENTRY_NAME.to_string()));
    assert!(registered.contains(&stage_name(0)));

    let entry = driver.node().resolve_name(ENTRY_NAME).expect("pp-entry");
    let by_index = driver.node().resolve_name(&stage_name(0)).expect("pp-stage-0");

    assert_eq!(entry.0, stage_addr);
    assert_eq!(by_index.0, stage_addr);
    assert_eq!(entry, by_index);

    driver.shutdown();
}

/// Stage 1 boot registers both `pp-exit` and `pp-stage-1`, and both names
/// resolve to the same `ActorAddress` on the local node.
#[test]
fn stage_1_registers_pp_exit_and_pp_stage_1() {
    let mut driver = make_driver();
    let stage_addr = ActorAddress::new_random();

    let registered = register_stage_names(driver.node_mut(), 1, 2, stage_addr);

    assert!(registered.contains(&EXIT_NAME.to_string()));
    assert!(registered.contains(&stage_name(1)));

    let exit = driver.node().resolve_name(EXIT_NAME).expect("pp-exit");
    let by_index = driver.node().resolve_name(&stage_name(1)).expect("pp-stage-1");

    assert_eq!(exit.0, stage_addr);
    assert_eq!(by_index.0, stage_addr);
    assert_eq!(exit, by_index);

    driver.shutdown();
}

/// After a 3-node cluster converges, stage 0 can resolve `pp-stage-1` to a
/// non-empty address via gossiped registry entries.
#[test]
fn stage_resolves_next_neighbor_after_cluster_join() {
    let mut drivers = make_three_node_cluster();
    let stage0_addr = ActorAddress::new_random();
    let stage1_addr = ActorAddress::new_random();

    register_stage_names(drivers[1].node_mut(), 0, 2, stage0_addr);
    register_stage_names(drivers[2].node_mut(), 1, 2, stage1_addr);

    let next_name = next_stage_name(0, 2).expect("stage 0 has a next neighbour");
    assert_eq!(next_name, "pp-stage-1");

    let propagated = pump_until(
        &mut drivers,
        Duration::from_secs(10),
        |s0, _s1| s0.node().resolve_name("pp-stage-1").is_some(),
    );
    assert!(
        propagated,
        "stage 0 should resolve pp-stage-1 after gossip within 10s"
    );

    let (addr, _node_id) = drivers[1]
        .node()
        .resolve_name(&next_name)
        .expect("stage 0 resolves next neighbour");
    assert_eq!(addr, stage1_addr);

    for d in drivers.iter_mut() {
        d.shutdown();
    }
}

/// After a 3-node cluster converges, stage 1 can resolve `pp-stage-0` to a
/// non-empty address via gossiped registry entries.
#[test]
fn stage_resolves_prev_neighbor_after_cluster_join() {
    let mut drivers = make_three_node_cluster();
    let stage0_addr = ActorAddress::new_random();
    let stage1_addr = ActorAddress::new_random();

    register_stage_names(drivers[1].node_mut(), 0, 2, stage0_addr);
    register_stage_names(drivers[2].node_mut(), 1, 2, stage1_addr);

    let prev_name = prev_stage_name(1).expect("stage 1 has a prev neighbour");
    assert_eq!(prev_name, "pp-stage-0");

    let propagated = pump_until(
        &mut drivers,
        Duration::from_secs(10),
        |_s0, s1| s1.node().resolve_name("pp-stage-0").is_some(),
    );
    assert!(
        propagated,
        "stage 1 should resolve pp-stage-0 after gossip within 10s"
    );

    let (addr, _node_id) = drivers[2]
        .node()
        .resolve_name(&prev_name)
        .expect("stage 1 resolves prev neighbour");
    assert_eq!(addr, stage0_addr);

    for d in drivers.iter_mut() {
        d.shutdown();
    }
}

/// A stage given `STAGE=0`, `NUM_STAGES=2` computes its outbound target name
/// without consulting any external config: pure function of those two values.
#[test]
fn next_stage_name_computed_from_env_alone() {
    // Stage 0 of 2 → next is "pp-stage-1".
    assert_eq!(next_stage_name(0, 2).as_deref(), Some("pp-stage-1"));

    // Same answer no matter how many times you call it (no shared state).
    assert_eq!(next_stage_name(0, 2).as_deref(), Some("pp-stage-1"));

    // Generalises to longer chains without any extra config.
    assert_eq!(next_stage_name(0, 4).as_deref(), Some("pp-stage-1"));
    assert_eq!(next_stage_name(2, 4).as_deref(), Some("pp-stage-3"));
}

/// The last stage (`STAGE == NUM_STAGES - 1`) has no next neighbour: the
/// helper returns `None` so callers do not even attempt resolution.
#[test]
fn last_stage_has_no_next_neighbor() {
    // Two-stage MVP: stage 1 is the last.
    assert_eq!(next_stage_name(1, 2), None);

    // Generalises to longer chains.
    assert_eq!(next_stage_name(3, 4), None);
    assert_eq!(next_stage_name(7, 8), None);
}

// ── §4 N-generic helper tests (TEST_SPEC §4) ──────────────────────────────
//
// The 2-stage tests above remain unchanged (the SPEC's invariant is that we
// do not delete the existing scaffolding). These additional tests cover the
// same surface at the N-generic shape the SPEC requires.

/// `stage_name(i)` is purely formatted from `i`; no chain length involved.
/// Exercise the full range we plan to test against (up to N=16).
#[test]
fn stage_name_uses_pp_prefix() {
    for i in 0..16 {
        assert_eq!(stage_name(i), format!("pp-stage-{i}"));
    }
}

#[test]
fn first_stage_has_no_prev() {
    assert_eq!(prev_stage_name(0), None);
}

/// `next_stage_name(N-1, N) == None` for every N in our supported range —
/// the last stage of a chain never produces a successor name.
#[test]
fn last_stage_has_no_next() {
    for n in 2..=8u32 {
        assert_eq!(
            next_stage_name(n - 1, n),
            None,
            "last stage of {n} should have no next",
        );
    }
}

/// Every interior stage has both a `prev` and a `next` neighbour, and the
/// two names differ. The first-and-last asymmetry shows up only at the
/// chain ends.
#[test]
fn middle_stage_has_both_neighbours() {
    for n in 3..=8u32 {
        for i in 1..(n - 1) {
            let prev = prev_stage_name(i).unwrap_or_else(|| {
                panic!("interior stage {i} of {n} should have a prev")
            });
            let next = next_stage_name(i, n).unwrap_or_else(|| {
                panic!("interior stage {i} of {n} should have a next")
            });
            assert_ne!(
                prev, next,
                "interior stage {i} of {n}: prev and next must differ",
            );
            assert_eq!(prev, stage_name(i - 1));
            assert_eq!(next, stage_name(i + 1));
        }
    }
}

/// First stage registers both `pp-entry` and its per-index name, and both
/// resolve to the same address. Generalised over N up to 8.
#[test]
fn first_stage_registers_entry_and_index() {
    for n in 2..=8 {
        let mut driver = make_driver();
        let stage_addr = ActorAddress::new_random();

        let registered = register_stage_names(driver.node_mut(), 0, n, stage_addr);

        assert!(
            registered.contains(&ENTRY_NAME.to_string()),
            "N={n}: first stage should register pp-entry",
        );
        assert!(
            registered.contains(&stage_name(0)),
            "N={n}: first stage should register pp-stage-0",
        );

        let entry = driver.node().resolve_name(ENTRY_NAME).expect("pp-entry");
        let idx = driver.node().resolve_name(&stage_name(0)).expect("pp-stage-0");
        assert_eq!(entry.0, stage_addr);
        assert_eq!(idx.0, stage_addr);
        assert_eq!(entry, idx);

        driver.shutdown();
    }
}

/// Last stage registers both `pp-exit` and its per-index name, and both
/// resolve to the same address. Generalised over N up to 8.
#[test]
fn last_stage_registers_exit_and_index() {
    for n in 2..=8u32 {
        let last = n - 1;
        let mut driver = make_driver();
        let stage_addr = ActorAddress::new_random();

        let registered = register_stage_names(driver.node_mut(), last, n, stage_addr);

        assert!(
            registered.contains(&EXIT_NAME.to_string()),
            "N={n}: last stage should register pp-exit",
        );
        assert!(
            registered.contains(&stage_name(last)),
            "N={n}: last stage should register pp-stage-{last}",
        );

        let exit = driver.node().resolve_name(EXIT_NAME).expect("pp-exit");
        let idx = driver
            .node()
            .resolve_name(&stage_name(last))
            .expect("per-index name");
        assert_eq!(exit.0, stage_addr);
        assert_eq!(idx.0, stage_addr);
        assert_eq!(exit, idx);

        driver.shutdown();
    }
}

/// A middle stage registers only its per-index name — never `pp-entry` or
/// `pp-exit`. Exercised across every interior index for N ∈ {3..=8}.
#[test]
fn middle_stage_registers_index_only() {
    for n in 3..=8u32 {
        for i in 1..(n - 1) {
            let mut driver = make_driver();
            let stage_addr = ActorAddress::new_random();

            let registered = register_stage_names(driver.node_mut(), i, n, stage_addr);

            assert!(
                registered.contains(&stage_name(i)),
                "N={n} stage {i}: middle should register its per-index name",
            );
            assert!(
                !registered.contains(&ENTRY_NAME.to_string()),
                "N={n} stage {i}: middle must not register pp-entry",
            );
            assert!(
                !registered.contains(&EXIT_NAME.to_string()),
                "N={n} stage {i}: middle must not register pp-exit",
            );
            assert_eq!(
                driver.node().resolve_name(ENTRY_NAME),
                None,
                "N={n} stage {i}: middle must not expose pp-entry",
            );
            assert_eq!(
                driver.node().resolve_name(EXIT_NAME),
                None,
                "N={n} stage {i}: middle must not expose pp-exit",
            );
            let idx = driver
                .node()
                .resolve_name(&stage_name(i))
                .expect("per-index name");
            assert_eq!(idx.0, stage_addr);

            driver.shutdown();
        }
    }
}

/// The returned `Vec<String>` length tracks the role: 2 for First and Last
/// (per-index + entry/exit), 1 for Middle.
#[test]
fn register_stage_names_count_matches_role() {
    for n in 3..=8u32 {
        for stage in 0..n {
            let mut driver = make_driver();
            let addr = ActorAddress::new_random();
            let registered = register_stage_names(driver.node_mut(), stage, n, addr);

            let expected_len = if stage == 0 || stage == n - 1 { 2 } else { 1 };
            assert_eq!(
                registered.len(),
                expected_len,
                "N={n} stage {stage}: expected {expected_len} registered names, got {:?}",
                registered,
            );
            driver.shutdown();
        }
    }
}

/// Across every stage in a chain, the per-index names are pairwise distinct,
/// `pp-entry` is registered exactly once (by stage 0), and `pp-exit` is
/// registered exactly once (by the last stage).
#[test]
fn register_stage_names_uniqueness() {
    use std::collections::HashSet;

    for n in 2..=8u32 {
        let mut idx_names = HashSet::new();
        let mut entry_count = 0u32;
        let mut exit_count = 0u32;
        for stage in 0..n {
            let mut driver = make_driver();
            let addr = ActorAddress::new_random();
            let names = register_stage_names(driver.node_mut(), stage, n, addr);

            // The per-index name is always included; track it for the
            // pairwise-distinct check across stages.
            let inserted = idx_names.insert(stage_name(stage));
            assert!(
                inserted,
                "N={n}: per-index name pp-stage-{stage} must be unique across stages",
            );

            if names.contains(&ENTRY_NAME.to_string()) {
                entry_count += 1;
            }
            if names.contains(&EXIT_NAME.to_string()) {
                exit_count += 1;
            }
            driver.shutdown();
        }
        assert_eq!(idx_names.len() as u32, n, "N={n}: every stage owns a unique pp-stage-i");
        assert_eq!(entry_count, 1, "N={n}: pp-entry must be registered exactly once");
        assert_eq!(exit_count, 1, "N={n}: pp-exit must be registered exactly once");
    }
}
