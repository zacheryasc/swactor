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
