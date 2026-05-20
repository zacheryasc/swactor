//! T-cluster: N-node iroh cluster + actor-level message exchange.
//!
//! Covers TEST_SPEC §8. One `DistributedNode` per orchestrator and one per
//! pipeline stage, joined via the orchestrator as the seed, exercised for
//! `NUM_STAGES ∈ {2, 3, 4}` in `RelayMode::Disabled` so the tests run
//! offline and without a GPU.
//!
//! For each N the tests assert:
//!
//! 1. The cluster converges (every node sees every other peer alive).
//! 2. Every adjacent stage pair `(i → i+1)` carries `StageActivation`
//!    intact in the pipeline's forward direction.
//! 3. `NextToken` reaches stage 0 from the last stage (the autoregressive
//!    feedback edge — middle stages are skipped on the wire).
//! 4. `InferenceResponse` reaches the orchestrator from the last stage.
//! 5. SWIM detects the death of a stage regardless of role (first,
//!    middle, last).

use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

/// Process-wide lock that serialises whole test bodies in this binary.
/// SWIM convergence at N≥4 is fast in isolation but degrades sharply
/// when several parallel tests are also pumping their own clusters of
/// iroh drivers at `probe_interval=1` tick. Each test holds the guard
/// for its full lifetime (cluster build + send/receive + shutdown), so
/// fan-out across the binary is at most one cluster at a time. Total
/// wall-clock is bounded by the per-test cost (~3s × 14 tests).
static CLUSTER_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn acquire_cluster_lock() -> MutexGuard<'static, ()> {
    CLUSTER_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::actor::{ActorAddress, Message};
use swactor::runtime::{Runtime, RuntimeConfig};
use swactor::transport::{CodecRegistry, TransportRouter};

use pipeline_parallel_inference::iroh_transport::{
    drain_actor_messages, IrohActorTransport, ACTOR_ALPN,
};
use pipeline_parallel_inference::messages::{
    inference_codec_registry, InferenceResponse, NextToken, StageActivation,
};

// ── Driver config (mirrors single-GPU t_cluster) ─────────────────────────

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
        additional_alpns: vec![ACTOR_ALPN.to_vec()],
    })
    .expect("failed to create iroh driver")
}

fn pump_one(driver: &mut IrohDriver) {
    driver.recv();
    driver.tick();
}

fn pubkey_of(driver: &IrohDriver) -> PublicKey {
    PublicKey::from_bytes(&driver.node_id().0).unwrap()
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

/// Build a `num_stages + 1`-node cluster: index 0 is the orchestrator,
/// indices `1..=num_stages` are pipeline stages 0..num_stages-1. Every
/// non-orchestrator node joins via the orchestrator's seed address.
/// Returns once every node sees every other node alive, or panics on
/// timeout.
/// Returned tuple's second field is the test-body lock guard — keep it
/// bound in the test (`let (drivers, _lock) = make_cluster(N);`) so
/// the lock is released only when the test function returns. See
/// `CLUSTER_LOCK` for the concurrency story.
fn make_cluster(num_stages: u32) -> (Vec<IrohDriver>, MutexGuard<'static, ()>) {
    let test_lock = acquire_cluster_lock();

    assert!(num_stages >= 2, "cluster tests require num_stages >= 2");
    let total = num_stages as usize + 1;

    let mut drivers: Vec<IrohDriver> = (0..total).map(|_| make_driver()).collect();
    let seed = drivers[0].endpoint_addr();
    for d in drivers.iter_mut().skip(1) {
        d.join(&[seed.clone()]);
    }

    let keys: Vec<PublicKey> = drivers.iter().map(pubkey_of).collect();

    // 30s is plenty under exclusive access — convergence finishes in
    // well under 5s on this box. Cap exists for a slow CI runner.
    let timeout = Duration::from_secs(30);
    let start = Instant::now();
    let mut converged = false;
    while start.elapsed() < timeout {
        for d in drivers.iter_mut() {
            pump_one(d);
        }
        let all_see_all = drivers.iter().enumerate().all(|(i, d)| {
            keys.iter()
                .enumerate()
                .all(|(j, k)| i == j || sees_alive(d, k))
        });
        if all_see_all {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        converged,
        "N={num_stages} cluster did not converge within {}s",
        timeout.as_secs(),
    );
    (drivers, test_lock)
}

/// Stage index `s` (0-based) lives at driver index `s + 1`; the
/// orchestrator is at driver index 0.
fn stage_idx(s: u32) -> usize {
    s as usize + 1
}

/// Send one `T` from `sender` to `receiver` over an iroh transport route,
/// drain the wire, and return the message the receiver inbox saw. Panics
/// if nothing arrived. Generic over any message type the inference codec
/// knows about.
fn send_and_receive<T: Message>(
    sender: &IrohDriver,
    receiver: &IrohDriver,
    payload: T,
    codecs: Arc<CodecRegistry>,
) -> T {
    let mut rt_send = Runtime::new(RuntimeConfig::default());
    let mut rt_recv = Runtime::new(RuntimeConfig::default());

    let inbox = rt_recv.new_inbox::<T>().unwrap();
    let inbox_addr: ActorAddress = *inbox.addr();

    let transport = Arc::new(IrohActorTransport::new(
        sender.endpoint().clone(),
        receiver.endpoint_addr(),
        sender.tokio_handle(),
    ));
    let router = TransportRouter::new();
    router.add_route(inbox_addr, transport);
    rt_send.set_codec_registry(codecs.clone());
    rt_send.set_transport_router(Arc::new(router));
    rt_recv.set_codec_registry(codecs.clone());

    rt_send.send_to(inbox_addr, payload).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    drain_actor_messages(receiver, &codecs, &rt_recv, Duration::from_millis(500));
    inbox
        .try_recv()
        .expect("payload did not arrive at the receiver inbox")
}

fn shutdown_all(drivers: &mut [IrohDriver]) {
    for d in drivers.iter_mut() {
        d.shutdown();
    }
}

// ── §8 — cluster convergence at N ∈ {2, 3, 4} ───────────────────────────

fn convergence_case(num_stages: u32) {
    let (mut drivers, _test_lock) = make_cluster(num_stages);
    // orch + N stages → every node should see N alive peers.
    for (i, d) in drivers.iter().enumerate() {
        assert_eq!(
            d.snapshot().alive_count as u32,
            num_stages,
            "node {i} should see {num_stages} alive peers after convergence at N={num_stages}",
        );
    }
    shutdown_all(&mut drivers);
}

#[test]
fn n_node_cluster_converges_via_iroh_seed_join_n_2() {
    convergence_case(2);
}

#[test]
fn n_node_cluster_converges_via_iroh_seed_join_n_3() {
    convergence_case(3);
}

#[test]
fn n_node_cluster_converges_via_iroh_seed_join_n_4() {
    convergence_case(4);
}

// ── §8 — StageActivation across every adjacent pair ──────────────────────

fn stage_activation_each_hop_case(num_stages: u32) {
    let (mut drivers, _test_lock) = make_cluster(num_stages);
    let codecs = Arc::new(inference_codec_registry());

    for hop in 0..(num_stages - 1) {
        let payload = StageActivation {
            request_id: 100 + hop as u64,
            position: hop * 4,
            hidden: (0u8..(32 + hop as u8)).collect(),
            seq_len: 4 + hop,
            is_prefill: hop == 0,
        };

        let (sender_idx, receiver_idx) = (stage_idx(hop), stage_idx(hop + 1));
        let (sender_part, receiver_part) = if sender_idx < receiver_idx {
            let (left, right) = drivers.split_at_mut(receiver_idx);
            (&left[sender_idx], &right[0])
        } else {
            unreachable!("hop sender_idx < receiver_idx by construction")
        };

        let received = send_and_receive::<StageActivation>(
            sender_part,
            receiver_part,
            payload.clone(),
            codecs.clone(),
        );
        assert_eq!(
            received, payload,
            "StageActivation hop ({hop} -> {}) at N={num_stages} must roundtrip intact",
            hop + 1,
        );
    }

    shutdown_all(&mut drivers);
}

#[test]
fn stage_activation_roundtrips_between_each_adjacent_pair_n_2() {
    stage_activation_each_hop_case(2);
}

#[test]
fn stage_activation_roundtrips_between_each_adjacent_pair_n_3() {
    stage_activation_each_hop_case(3);
}

#[test]
fn stage_activation_roundtrips_between_each_adjacent_pair_n_4() {
    stage_activation_each_hop_case(4);
}

// ── §8 — NextToken from last stage to stage 0 ────────────────────────────

fn next_token_last_to_first_case(num_stages: u32) {
    let (mut drivers, _test_lock) = make_cluster(num_stages);
    let codecs = Arc::new(inference_codec_registry());

    let payload = NextToken {
        request_id: 42,
        token_id: 1337,
        position: 7,
        done: false,
    };

    let last_idx = stage_idx(num_stages - 1);
    let first_idx = stage_idx(0);
    let (first_part, last_part) = {
        let (left, right) = drivers.split_at_mut(last_idx);
        (&left[first_idx], &right[0])
    };
    let received = send_and_receive::<NextToken>(
        last_part,
        first_part,
        payload.clone(),
        codecs.clone(),
    );
    assert_eq!(
        received, payload,
        "NextToken from last -> first at N={num_stages} must roundtrip intact",
    );

    shutdown_all(&mut drivers);
}

#[test]
fn next_token_roundtrips_last_to_first_n_2() {
    next_token_last_to_first_case(2);
}

#[test]
fn next_token_roundtrips_last_to_first_n_3() {
    next_token_last_to_first_case(3);
}

#[test]
fn next_token_roundtrips_last_to_first_n_4() {
    next_token_last_to_first_case(4);
}

// ── §8 — InferenceResponse from last stage to orchestrator ───────────────

fn inference_response_last_to_orch_case(num_stages: u32) {
    let (mut drivers, _test_lock) = make_cluster(num_stages);
    let codecs = Arc::new(inference_codec_registry());

    let payload = InferenceResponse {
        text: format!("tokens: [N={num_stages}, ✓, 世界]"),
    };

    let last_idx = stage_idx(num_stages - 1);
    let (orch_part, last_part) = {
        let (left, right) = drivers.split_at_mut(last_idx);
        (&left[0], &right[0])
    };
    let received = send_and_receive::<InferenceResponse>(
        last_part,
        orch_part,
        payload.clone(),
        codecs.clone(),
    );
    assert_eq!(
        received, payload,
        "InferenceResponse from last -> orchestrator at N={num_stages} must roundtrip intact",
    );

    shutdown_all(&mut drivers);
}

#[test]
fn inference_response_roundtrips_last_to_orchestrator_n_2() {
    inference_response_last_to_orch_case(2);
}

#[test]
fn inference_response_roundtrips_last_to_orchestrator_n_3() {
    inference_response_last_to_orch_case(3);
}

#[test]
fn inference_response_roundtrips_last_to_orchestrator_n_4() {
    inference_response_last_to_orch_case(4);
}

// ── §8 — SWIM death detection for each role ──────────────────────────────

/// Shut down the driver at `victim_idx`, then pump the remaining drivers
/// until they all stop seeing the victim alive (or the timeout expires).
/// Returns whether detection succeeded.
fn wait_for_death(
    drivers: &mut [IrohDriver],
    victim_idx: usize,
    timeout: Duration,
) -> bool {
    let victim_key = pubkey_of(&drivers[victim_idx]);
    drivers[victim_idx].shutdown();

    let start = Instant::now();
    while start.elapsed() < timeout {
        for (i, d) in drivers.iter_mut().enumerate() {
            if i != victim_idx {
                pump_one(d);
            }
        }
        let all_dropped = drivers.iter().enumerate().all(|(i, d)| {
            i == victim_idx || !sees_alive(d, &victim_key)
        });
        if all_dropped {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// Middle-stage death requires N >= 3 to even have a middle. We pick N=4
/// and kill stage 1 (one of the two middle stages).
#[test]
fn node_death_detected_via_swim_after_middle_stage_shutdown() {
    let (mut drivers, _test_lock) = make_cluster(4);
    let middle_idx = stage_idx(1);
    let detected = wait_for_death(&mut drivers, middle_idx, Duration::from_secs(15));
    assert!(
        detected,
        "every surviving node should detect the middle stage's death via SWIM within the suspicion window",
    );
    // Avoid double-shutdown: the victim is already shut down.
    for (i, d) in drivers.iter_mut().enumerate() {
        if i != middle_idx {
            d.shutdown();
        }
    }
}

/// First and last stage deaths are detected via SWIM the same way. We
/// run both at N=3 in one test — the per-iteration cost dominates, so
/// folding them in one test keeps the suite fast.
#[test]
fn node_death_detected_via_swim_after_first_or_last_stage_shutdown() {
    // First-stage death scenario.
    {
        let (mut drivers, _test_lock) = make_cluster(3);
        let first_idx = stage_idx(0);
        let detected = wait_for_death(&mut drivers, first_idx, Duration::from_secs(15));
        assert!(
            detected,
            "every surviving node should detect first stage's death via SWIM",
        );
        for (i, d) in drivers.iter_mut().enumerate() {
            if i != first_idx {
                d.shutdown();
            }
        }
    }

    // Last-stage death scenario.
    {
        let (mut drivers, _test_lock) = make_cluster(3);
        let last_idx = stage_idx(2);
        let detected = wait_for_death(&mut drivers, last_idx, Duration::from_secs(15));
        assert!(
            detected,
            "every surviving node should detect last stage's death via SWIM",
        );
        for (i, d) in drivers.iter_mut().enumerate() {
            if i != last_idx {
                d.shutdown();
            }
        }
    }
}
