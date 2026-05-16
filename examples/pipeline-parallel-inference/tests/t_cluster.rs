//! T-cluster: three-node iroh cluster + actor-level message exchange.
//!
//! Covers TEST_SPEC §6. Three `DistributedNode`s in one process — orchestrator
//! plus stage 0 plus stage 1 — joined via the orchestrator as the seed. The
//! actor messages defined in `messages.rs` (`StageActivation`, `NextToken`,
//! `InferenceResponse`) flow over real iroh QUIC streams, exercised in the
//! same direction the pipeline runs them.
//!
//! Helpers mirror `single-gpu-inference::tests::t_cluster` (driver config,
//! convergence pump, drain cadence) so the only thing new here is the
//! three-node topology and the pipeline-specific message types.

use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::runtime::{Runtime, RuntimeConfig};
use swactor::transport::TransportRouter;

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

// ── §6 tests ─────────────────────────────────────────────────────────────

/// All three nodes (orchestrator + stage 0 + stage 1) join via the
/// orchestrator's seed address and converge in the SWIM membership view —
/// each driver reports two alive peers within the configured window.
#[test]
fn three_node_cluster_converges_via_iroh_seed_join() {
    let mut drivers = make_three_node_cluster();

    for d in drivers.iter() {
        assert_eq!(
            d.snapshot().alive_count,
            2,
            "every node should see two alive peers after convergence"
        );
    }

    for d in drivers.iter_mut() {
        d.shutdown();
    }
}

/// A `StageActivation` sent from stage 0's runtime to an inbox on stage 1's
/// runtime arrives intact — every scalar field plus the `hidden` byte payload
/// match what the sender produced.
#[test]
fn stage_activation_roundtrips_stage_0_to_stage_1() {
    let [mut orch, mut s0, mut s1] = make_three_node_cluster();

    let codecs = Arc::new(inference_codec_registry());
    let mut rt_s0 = Runtime::new(RuntimeConfig::default());
    let mut rt_s1 = Runtime::new(RuntimeConfig::default());

    let inbox = rt_s1.new_inbox::<StageActivation>().unwrap();
    let inbox_addr = *inbox.addr();

    let s0_to_s1 = Arc::new(IrohActorTransport::new(
        s0.endpoint().clone(),
        s1.endpoint_addr(),
        s0.tokio_handle(),
    ));
    let router_s0 = TransportRouter::new();
    router_s0.add_route(inbox_addr, s0_to_s1);
    rt_s0.set_codec_registry(codecs.clone());
    rt_s0.set_transport_router(Arc::new(router_s0));
    rt_s1.set_codec_registry(codecs.clone());

    let payload = StageActivation {
        request_id: 7,
        position: 0,
        hidden: (0u8..128u8).collect(),
        seq_len: 4,
        is_prefill: true,
    };

    rt_s0.send_to(inbox_addr, payload.clone()).unwrap();

    std::thread::sleep(Duration::from_millis(200));
    drain_actor_messages(&s1, &codecs, &rt_s1, Duration::from_millis(500));

    let received = inbox
        .try_recv()
        .expect("StageActivation should arrive at stage 1");
    assert_eq!(received, payload, "stage activation must roundtrip intact");

    for d in [&mut orch, &mut s0, &mut s1] {
        d.shutdown();
    }
}

/// A `NextToken` sent from stage 1's runtime to an inbox on stage 0's runtime
/// arrives intact — `done` flag, token id, position, and request id all
/// preserved.
#[test]
fn next_token_roundtrips_stage_1_to_stage_0() {
    let [mut orch, mut s0, mut s1] = make_three_node_cluster();

    let codecs = Arc::new(inference_codec_registry());
    let mut rt_s0 = Runtime::new(RuntimeConfig::default());
    let mut rt_s1 = Runtime::new(RuntimeConfig::default());

    let inbox = rt_s0.new_inbox::<NextToken>().unwrap();
    let inbox_addr = *inbox.addr();

    let s1_to_s0 = Arc::new(IrohActorTransport::new(
        s1.endpoint().clone(),
        s0.endpoint_addr(),
        s1.tokio_handle(),
    ));
    let router_s1 = TransportRouter::new();
    router_s1.add_route(inbox_addr, s1_to_s0);
    rt_s1.set_codec_registry(codecs.clone());
    rt_s1.set_transport_router(Arc::new(router_s1));
    rt_s0.set_codec_registry(codecs.clone());

    let payload = NextToken {
        request_id: 11,
        token_id: 1337,
        position: 5,
        done: true,
    };

    rt_s1.send_to(inbox_addr, payload.clone()).unwrap();

    std::thread::sleep(Duration::from_millis(200));
    drain_actor_messages(&s0, &codecs, &rt_s0, Duration::from_millis(500));

    let received = inbox
        .try_recv()
        .expect("NextToken should arrive at stage 0");
    assert_eq!(received, payload, "next token must roundtrip intact");

    for d in [&mut orch, &mut s0, &mut s1] {
        d.shutdown();
    }
}

/// An `InferenceResponse` sent from stage 1's runtime to an inbox on the
/// orchestrator's runtime arrives intact — final detokenized text preserved
/// byte-for-byte (including multibyte unicode).
#[test]
fn inference_response_roundtrips_stage_1_to_orchestrator() {
    let [mut orch, mut s0, mut s1] = make_three_node_cluster();

    let codecs = Arc::new(inference_codec_registry());
    let mut rt_orch = Runtime::new(RuntimeConfig::default());
    let mut rt_s1 = Runtime::new(RuntimeConfig::default());

    let inbox = rt_orch.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *inbox.addr();

    let s1_to_orch = Arc::new(IrohActorTransport::new(
        s1.endpoint().clone(),
        orch.endpoint_addr(),
        s1.tokio_handle(),
    ));
    let router_s1 = TransportRouter::new();
    router_s1.add_route(inbox_addr, s1_to_orch);
    rt_s1.set_codec_registry(codecs.clone());
    rt_s1.set_transport_router(Arc::new(router_s1));
    rt_orch.set_codec_registry(codecs.clone());

    let payload = InferenceResponse {
        text: "tokens: [hello 世界 ✓]".into(),
    };

    rt_s1.send_to(inbox_addr, payload.clone()).unwrap();

    std::thread::sleep(Duration::from_millis(200));
    drain_actor_messages(&orch, &codecs, &rt_orch, Duration::from_millis(500));

    let received = inbox
        .try_recv()
        .expect("InferenceResponse should arrive at orchestrator");
    assert_eq!(received, payload, "inference response must roundtrip intact");

    for d in [&mut orch, &mut s0, &mut s1] {
        d.shutdown();
    }
}

/// After a stage's host node shuts down, the surviving nodes mark it dead via
/// SWIM within the suspicion window. Verifies failure detection across the
/// 3-node topology, not just a pair.
#[test]
fn node_death_detected_via_swim_after_stage_shutdown() {
    let [mut orch, mut s0, mut s1] = make_three_node_cluster();

    let s1_key = pubkey_of(&s1);
    assert!(sees_alive(&orch, &s1_key));
    assert!(sees_alive(&s0, &s1_key));

    // Shut down stage 1.
    s1.shutdown();

    let start = Instant::now();
    let timeout = Duration::from_secs(15);
    let mut detected = false;
    while start.elapsed() < timeout {
        pump_one(&mut orch);
        pump_one(&mut s0);

        let orch_dropped = !sees_alive(&orch, &s1_key);
        let s0_dropped = !sees_alive(&s0, &s1_key);
        if orch_dropped && s0_dropped {
            detected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        detected,
        "orchestrator and stage 0 should detect stage 1's death via SWIM within the suspicion window"
    );

    orch.shutdown();
    s0.shutdown();
}
