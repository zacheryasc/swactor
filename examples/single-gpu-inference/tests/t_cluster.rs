//! T-cluster: Cross-node messaging component tests.
//!
//! Two swactor nodes on localhost via iroh. Tests SWIM convergence,
//! actor-level InferenceRequest/InferenceResponse exchange, and
//! SWIM death detection after node shutdown.

use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::actor::ActorInterface;
use swactor::runtime::{Ctx, Runtime, RuntimeConfig};
use swactor::transport::TransportRouter;

use single_gpu_inference::iroh_transport::{drain_actor_messages, IrohActorTransport, ACTOR_ALPN};
use single_gpu_inference::messages::{inference_codec_registry, InferenceRequest, InferenceResponse};

// ── Test SWIM config ─────────────────────────────────────────────────────

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

// ── Pump helpers ─────────────────────────────────────────────────────────

fn pump_one(driver: &mut IrohDriver) {
    driver.recv();
    driver.tick();
}

fn pump_until_pair(
    a: &mut IrohDriver,
    b: &mut IrohDriver,
    timeout: Duration,
    check_fn: fn(&IrohDriver, &IrohDriver) -> bool,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        pump_one(a);
        pump_one(b);
        if check_fn(a, b) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
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

fn both_alive(a: &IrohDriver, b: &IrohDriver) -> bool {
    let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
    let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
    sees_alive(a, &b_key) && sees_alive(b, &a_key)
}

/// Create two IrohDrivers and converge them via seed join.
fn make_converged_pair() -> (IrohDriver, IrohDriver) {
    let mut driver_a = make_driver();
    let mut driver_b = make_driver();

    let a_addr = driver_a.endpoint_addr();
    driver_b.join(&[a_addr]);

    let converged = pump_until_pair(
        &mut driver_a,
        &mut driver_b,
        Duration::from_secs(5),
        both_alive,
    );
    assert!(converged, "cluster setup: nodes did not converge within 5s");

    (driver_a, driver_b)
}

// ── Echo actor (replies InferenceResponse for any InferenceRequest) ─────

struct EchoInferenceActor;

impl ActorInterface for EchoInferenceActor {
    type Incoming = InferenceRequest;
    type Response = InferenceResponse;

    fn handle(&mut self, ctx: &Ctx, msg: InferenceRequest) {
        let _ = ctx.send(
            msg.reply_to,
            InferenceResponse {
                text: format!("echo: {}", msg.prompt),
            },
        );
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

/// Node B joins node A via seed address. SWIM converges — both nodes see
/// each other alive within 5 seconds.
#[test]
fn cluster_converges_via_iroh_seed_join() {
    let mut driver_a = make_driver();
    let mut driver_b = make_driver();

    let a_addr = driver_a.endpoint_addr();
    driver_b.join(&[a_addr]);

    let converged = pump_until_pair(
        &mut driver_a,
        &mut driver_b,
        Duration::from_secs(5),
        both_alive,
    );

    assert!(converged, "nodes did not converge within 5s");
    assert_eq!(driver_a.snapshot().alive_count, 1);
    assert_eq!(driver_b.snapshot().alive_count, 1);

    driver_a.shutdown();
    driver_b.shutdown();
}

/// Actor on node A sends InferenceRequest to actor on node B via the
/// transport router + codec. InferenceResponse arrives back at node A.
#[test]
fn inference_request_roundtrips_across_two_nodes() {
    let (mut driver_a, mut driver_b) = make_converged_pair();

    let codecs = Arc::new(inference_codec_registry());

    let mut rt_a = Runtime::new(RuntimeConfig::default());
    let mut rt_b = Runtime::new(RuntimeConfig::default());

    // Spawn echo actor on node B
    let echo_addr = rt_b.spawn(EchoInferenceActor).unwrap();
    rt_b.tick();

    // Inbox on node A for responses
    let response_inbox = rt_a.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    // Build iroh-backed transports for actor messages
    let transport_a_to_b = Arc::new(IrohActorTransport::new(
        driver_a.endpoint().clone(),
        driver_b.endpoint_addr(),
        driver_a.tokio_handle(),
    ));
    let transport_b_to_a = Arc::new(IrohActorTransport::new(
        driver_b.endpoint().clone(),
        driver_a.endpoint_addr(),
        driver_b.tokio_handle(),
    ));

    // Wire routes: A knows echo_addr is on B, B knows inbox_addr is on A
    let router_a = TransportRouter::new();
    router_a.add_route(echo_addr, transport_a_to_b);
    let router_b = TransportRouter::new();
    router_b.add_route(inbox_addr, transport_b_to_a);

    rt_a.set_codec_registry(codecs.clone());
    rt_a.set_transport_router(Arc::new(router_a));
    rt_b.set_codec_registry(codecs.clone());
    rt_b.set_transport_router(Arc::new(router_b));

    // Send InferenceRequest from node A → actor on node B
    rt_a.send_to(
        echo_addr,
        InferenceRequest {
            prompt: "Hello from node A".into(),
            max_tokens: 8,
            temperature: 0.7,
            reply_to: inbox_addr,
        },
    )
    .unwrap();

    // Allow iroh transport to deliver, then drain into runtime B
    std::thread::sleep(Duration::from_millis(200));
    drain_actor_messages(&driver_b, &codecs, &rt_b, Duration::from_millis(500));
    rt_b.tick();

    // Actor replied — allow transport to deliver, then drain into runtime A
    std::thread::sleep(Duration::from_millis(200));
    drain_actor_messages(&driver_a, &codecs, &rt_a, Duration::from_millis(500));

    let response = response_inbox
        .try_recv()
        .expect("InferenceResponse should arrive at node A");
    assert!(
        response.text.contains("Hello from node A"),
        "expected echo of prompt, got: {:?}",
        response.text
    );

    driver_a.shutdown();
    driver_b.shutdown();
}

/// When node B shuts down, node A detects the death via SWIM within the
/// configured suspicion window.
#[test]
fn node_death_detected_via_swim_after_shutdown() {
    let (mut driver_a, mut driver_b) = make_converged_pair();

    assert_eq!(
        driver_a.snapshot().alive_count, 1,
        "precondition: A sees B alive"
    );

    // Kill node B
    driver_b.shutdown();

    // Pump node A until it sees zero alive peers
    let start = Instant::now();
    let timeout = Duration::from_secs(10);
    let mut detected = false;
    while start.elapsed() < timeout {
        driver_a.recv();
        driver_a.tick();
        if driver_a.snapshot().alive_count == 0 {
            detected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(
        detected,
        "node A should detect node B's death via SWIM within the suspicion window"
    );

    driver_a.shutdown();
}
