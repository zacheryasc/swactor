//! T-cluster: Cross-node messaging component tests.
//!
//! Two swactor nodes on localhost via iroh. Tests SWIM convergence,
//! actor-level InferenceRequest/InferenceResponse exchange, and
//! SWIM death detection after node shutdown.
//!
//! Each node is a [`ClusterNode`] — the actorized distribution stack (driver
//! transport bridge + per-node swactor runtime hosting the SWIM / registry /
//! metadata / directory actors). App actors share the node's runtime.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use iroh::{PublicKey, RelayMode};
use tokio::runtime::{Handle, Runtime as TokioRuntime};

use distribution::iroh_driver::IrohDriverConfig;
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;

use swactor::actor::ActorInterface;
use swactor::runtime::Ctx;

use single_gpu_inference::cluster::ClusterNode;
use single_gpu_inference::iroh_transport::{drain_actor_messages, IrohActorTransport, ACTOR_ALPN};
use single_gpu_inference::messages::{inference_codec_registry, InferenceRequest, InferenceResponse};

// ── Test SWIM config ─────────────────────────────────────────────────────

const TICK: Duration = Duration::from_millis(10);

fn test_node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: TICK,
            probe_timeout: TICK * 3,
            indirect_probes: 1,
            suspicion_timeout: TICK * 5,
            dead_reprobe_interval: Duration::ZERO,
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

/// Shared tokio runtime for sync `#[test]`s — iroh needs a tokio context, and
/// each test creating its own multi-thread runtime would balloon thread count.
fn test_tokio_handle() -> Handle {
    static RT: OnceLock<TokioRuntime> = OnceLock::new();
    RT.get_or_init(|| TokioRuntime::new().expect("build test tokio runtime"))
        .handle()
        .clone()
}

fn make_node() -> ClusterNode {
    ClusterNode::with_handle(
        test_tokio_handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: test_node_config(),
            peer_auth: None,
            additional_alpns: vec![ACTOR_ALPN.to_vec()],
        },
        test_node_config(),
        inference_codec_registry(),
        |_| {},
    )
    .expect("failed to create cluster node")
}

// ── Pump helpers ─────────────────────────────────────────────────────────

fn pump_until_pair(
    a: &mut ClusterNode,
    b: &mut ClusterNode,
    timeout: Duration,
    check_fn: fn(&ClusterNode, &ClusterNode) -> bool,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        a.pump_once();
        b.pump_once();
        if check_fn(a, b) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

fn sees_alive(node: &ClusterNode, peer_key: &PublicKey) -> bool {
    let peer = distribution::types::NodeId(*peer_key.as_bytes());
    node.sees_alive(&peer)
}

fn both_alive(a: &ClusterNode, b: &ClusterNode) -> bool {
    let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
    let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
    sees_alive(a, &b_key) && sees_alive(b, &a_key)
}

/// Create two cluster nodes and converge them via seed join.
fn make_converged_pair() -> (ClusterNode, ClusterNode) {
    let mut node_a = make_node();
    let mut node_b = make_node();

    let a_addr = node_a.endpoint_addr();
    node_b.join(&[a_addr]);

    let converged = pump_until_pair(
        &mut node_a,
        &mut node_b,
        Duration::from_secs(5),
        both_alive,
    );
    assert!(converged, "cluster setup: nodes did not converge within 5s");

    (node_a, node_b)
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
    let mut node_a = make_node();
    let mut node_b = make_node();

    let a_addr = node_a.endpoint_addr();
    node_b.join(&[a_addr]);

    let converged = pump_until_pair(
        &mut node_a,
        &mut node_b,
        Duration::from_secs(5),
        both_alive,
    );

    assert!(converged, "nodes did not converge within 5s");
    assert_eq!(node_a.alive_count(), 1);
    assert_eq!(node_b.alive_count(), 1);

    node_a.driver.shutdown();
    node_b.driver.shutdown();
}

/// Actor on node A sends InferenceRequest to actor on node B via the
/// transport router + codec. InferenceResponse arrives back at node A.
#[test]
fn inference_request_roundtrips_across_two_nodes() {
    let (mut node_a, mut node_b) = make_converged_pair();

    let codecs = Arc::new(inference_codec_registry());

    // Spawn echo actor on node B's runtime
    let echo_addr = node_b.rt.spawn(EchoInferenceActor).unwrap();
    node_b.rt.tick();

    // Inbox on node A for responses
    let response_inbox = node_a.rt.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    // Build iroh-backed transports for actor messages
    let transport_a_to_b = Arc::new(IrohActorTransport::new(
        node_a.driver.endpoint().clone(),
        node_b.endpoint_addr(),
        node_a.driver.tokio_handle(),
    ));
    let transport_b_to_a = Arc::new(IrohActorTransport::new(
        node_b.driver.endpoint().clone(),
        node_a.endpoint_addr(),
        node_b.driver.tokio_handle(),
    ));

    // Wire routes on each node's shared transport router: A knows echo_addr is
    // on B, B knows inbox_addr is on A.
    node_a.transport_router.add_route(echo_addr, transport_a_to_b);
    node_b.transport_router.add_route(inbox_addr, transport_b_to_a);

    // Send InferenceRequest from node A → actor on node B
    node_a
        .rt
        .send_to(
            echo_addr,
            InferenceRequest {
                prompt: "Hello from node A".into(),
                max_tokens: 8,
                temperature: 0.7,
                reply_to: inbox_addr,
            },
        )
        .unwrap();
    node_a.rt.tick();

    // Allow iroh transport to deliver, then drain into node B's runtime
    std::thread::sleep(Duration::from_millis(200));
    drain_actor_messages(&node_b.driver, &codecs, &node_b.rt, Duration::from_millis(500));
    node_b.rt.tick();

    // Actor replied — allow transport to deliver, then drain into node A's runtime
    std::thread::sleep(Duration::from_millis(200));
    drain_actor_messages(&node_a.driver, &codecs, &node_a.rt, Duration::from_millis(500));
    node_a.rt.tick();

    let response = response_inbox
        .try_recv()
        .expect("InferenceResponse should arrive at node A");
    assert!(
        response.text.contains("Hello from node A"),
        "expected echo of prompt, got: {:?}",
        response.text
    );

    node_a.driver.shutdown();
    node_b.driver.shutdown();
}

/// When node B shuts down, node A detects the death via SWIM within the
/// configured suspicion window.
#[test]
fn node_death_detected_via_swim_after_shutdown() {
    let (mut node_a, mut node_b) = make_converged_pair();

    assert_eq!(
        node_a.alive_count(), 1,
        "precondition: A sees B alive"
    );

    // Kill node B
    node_b.driver.shutdown();

    // Pump node A until it sees zero alive peers
    let start = Instant::now();
    let timeout = Duration::from_secs(10);
    let mut detected = false;
    while start.elapsed() < timeout {
        node_a.pump_once();
        if node_a.alive_count() == 0 {
            detected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(
        detected,
        "node A should detect node B's death via SWIM within the suspicion window"
    );

    node_a.driver.shutdown();
}
