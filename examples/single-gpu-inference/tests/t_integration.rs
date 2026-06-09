//! T-integration: End-to-end distributed inference tests.
//!
//! Two swactor nodes on localhost via iroh/QUIC. Node B runs an
//! `InferenceActor` backed by a Python worker. Node A sends an
//! `InferenceRequest` across the network, through the `RequestBridge`,
//! into the actor, through the child process, and back.
//!
//! Each node is a [`ClusterNode`] (driver transport bridge + per-node swactor
//! runtime hosting the protocol actors); app actors share that runtime.
//!
//! - `distributed_inference_through_echo_worker` — fast, uses canned echo responses.
//! - `distributed_inference_through_tinygrad` — slow (#[ignore]), downloads a real
//!   ~1B GGUF model and runs real tinygrad inference on CPU.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use iroh::{PublicKey, RelayMode};
use tokio::runtime::{Handle, Runtime as TokioRuntime};

use distribution::iroh_driver::IrohDriverConfig;
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;

use single_gpu_inference::cluster::ClusterNode;
use single_gpu_inference::inference_actor::{InferenceActor, InferenceActorStatus, RequestBridge};
use single_gpu_inference::iroh_transport::{drain_actor_messages, IrohActorTransport, ACTOR_ALPN};
use single_gpu_inference::messages::{inference_codec_registry, InferenceRequest, InferenceResponse};
use swactor_process::{ProcessMode, ProcessSpec};

// ── Iroh cluster helpers ─────────────────────────────────────────────────

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

/// Shared tokio runtime for sync `#[test]`s — iroh needs a tokio context.
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

fn sees_alive(node: &ClusterNode, peer_key: &PublicKey) -> bool {
    let peer = distribution::types::NodeId(*peer_key.as_bytes());
    node.sees_alive(&peer)
}

fn both_alive(a: &ClusterNode, b: &ClusterNode) -> bool {
    let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
    let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
    sees_alive(a, &b_key) && sees_alive(b, &a_key)
}

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

// ── Process helpers (from t_actor.rs) ────────────────────────────────────

fn echo_worker_spec() -> ProcessSpec {
    ProcessSpec {
        command: "python3".into(),
        args: vec![format!("{}/echo_worker.py", env!("CARGO_MANIFEST_DIR"))],
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: Some(Duration::from_secs(2)),
        stdin_buffer_limit: None,
    }
}

fn tinygrad_worker_spec() -> ProcessSpec {
    let manifest = env!("CARGO_MANIFEST_DIR");
    ProcessSpec {
        command: format!("{manifest}/.venv/bin/python"),
        args: vec![format!("{manifest}/tinygrad_worker.py")],
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: Some(Duration::from_secs(5)),
        stdin_buffer_limit: None,
    }
}

fn is_process_alive(pid: u32) -> bool {
    std::fs::metadata(format!("/proc/{}", pid)).is_ok()
}

// ── Test ─────────────────────────────────────────────────────────────────

#[test]
fn distributed_inference_through_echo_worker() {
    // 1. Converge two cluster nodes
    let (mut node_a, mut node_b) = make_converged_pair();

    let codecs = Arc::new(inference_codec_registry());

    // 2. On node B's runtime: spawn InferenceActor with echo_worker + RequestBridge
    let status_inbox = node_b.rt.new_inbox::<InferenceActorStatus>().unwrap();
    let sender = node_b.rt.create_sender();
    let actor = InferenceActor::new(echo_worker_spec(), sender)
        .with_status_addr(*status_inbox.addr());
    let inference_addr = node_b.rt.spawn(actor).unwrap();

    let bridge = RequestBridge { target: inference_addr };
    let bridge_addr = node_b.rt.spawn(bridge).unwrap();

    // 3. On node A's runtime: create response inbox
    let response_inbox = node_a.rt.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    // 4. Build iroh transports
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

    // Wire routes on each node's shared transport router: A sends to bridge_addr
    // on B, B sends to inbox_addr on A.
    node_a.transport_router.add_route(bridge_addr, transport_a_to_b);
    node_b.transport_router.add_route(inbox_addr, transport_b_to_a);

    // 5. Tick node B until InferenceActor reports WorkerReady
    let start = Instant::now();
    let mut worker_pid = None;
    while start.elapsed() < Duration::from_secs(10) {
        node_b.rt.tick();
        if let Some(status) = status_inbox.try_recv() {
            match status {
                InferenceActorStatus::WorkerReady { pid } => {
                    worker_pid = pid;
                    break;
                }
                InferenceActorStatus::ProcessStarted => {}
                other => panic!("unexpected status during startup: {:?}", other),
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let worker_pid = worker_pid.expect("echo_worker.py should report ready within 10s");

    // 6. Send InferenceRequest from node A → bridge on node B
    node_a
        .rt
        .send_to(
            bridge_addr,
            InferenceRequest {
                prompt: "Hello from node A".into(),
                max_tokens: 8,
                temperature: 0.7,
                reply_to: inbox_addr,
            },
        )
        .unwrap();

    // 7. Pump loop: drain messages on both sides, tick both runtimes
    let start = Instant::now();
    let mut got_response = false;
    while start.elapsed() < Duration::from_secs(10) {
        // Let transport deliver
        std::thread::sleep(Duration::from_millis(100));

        // Drain incoming actor messages on both sides
        drain_actor_messages(&node_b.driver, &codecs, &node_b.rt, Duration::from_millis(100));
        drain_actor_messages(&node_a.driver, &codecs, &node_a.rt, Duration::from_millis(100));

        // Tick both runtimes
        node_b.rt.tick();
        node_a.rt.tick();

        // Check for response
        if let Some(response) = response_inbox.try_recv() {
            // 8. Assert
            assert!(
                response.text.contains("Hello from node A"),
                "expected echo of prompt, got: {:?}",
                response.text
            );
            got_response = true;
            break;
        }
    }
    assert!(got_response, "should receive InferenceResponse within 10s");

    // 9. Cleanup: stop inference actor, wait for process death, shutdown drivers
    node_b.rt.stop_actor(inference_addr).unwrap();

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        node_b.rt.tick();
        std::thread::sleep(Duration::from_millis(10));
        if !is_process_alive(worker_pid) {
            break;
        }
    }
    assert!(
        !is_process_alive(worker_pid),
        "echo_worker.py (pid {}) should be dead after actor stop",
        worker_pid
    );

    node_a.driver.shutdown();
    node_b.driver.shutdown();
}

/// Full distributed inference through tinygrad with a real ~1B GGUF model.
///
/// Ignored by default — requires the `.venv` with tinygrad installed and
/// downloads a ~1 GB model on first run. Run explicitly with:
///   cargo test --package smoke-test distributed_inference -- --ignored
#[test]
#[ignore]
fn distributed_inference_through_tinygrad() {
    // 1. Converge two cluster nodes
    let (mut node_a, mut node_b) = make_converged_pair();

    let codecs = Arc::new(inference_codec_registry());

    // 2. On node B's runtime: spawn InferenceActor with tinygrad_worker + RequestBridge
    let status_inbox = node_b.rt.new_inbox::<InferenceActorStatus>().unwrap();
    let sender = node_b.rt.create_sender();
    let actor = InferenceActor::new(tinygrad_worker_spec(), sender)
        .with_status_addr(*status_inbox.addr());
    let inference_addr = node_b.rt.spawn(actor).unwrap();

    let bridge = RequestBridge { target: inference_addr };
    let bridge_addr = node_b.rt.spawn(bridge).unwrap();

    // 3. On node A's runtime: create response inbox
    let response_inbox = node_a.rt.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    // 4. Build iroh transports
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

    node_a.transport_router.add_route(bridge_addr, transport_a_to_b);
    node_b.transport_router.add_route(inbox_addr, transport_b_to_a);

    // 5. Wait for tinygrad model download + load (generous timeout)
    let start = Instant::now();
    let mut worker_pid = None;
    while start.elapsed() < Duration::from_secs(600) {
        node_b.rt.tick();
        if let Some(status) = status_inbox.try_recv() {
            match status {
                InferenceActorStatus::WorkerReady { pid } => {
                    worker_pid = pid;
                    break;
                }
                InferenceActorStatus::ProcessStarted => {}
                other => panic!("unexpected status during startup: {:?}", other),
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let worker_pid = worker_pid.expect("tinygrad_worker.py should report ready (model loaded)");

    // 6. Send InferenceRequest from node A → bridge on node B
    node_a
        .rt
        .send_to(
            bridge_addr,
            InferenceRequest {
                prompt: "Say hello".into(),
                max_tokens: 32,
                temperature: 0.7,
                reply_to: inbox_addr,
            },
        )
        .unwrap();

    // 7. Pump loop — generation on CPU can be slow
    let start = Instant::now();
    let mut got_response = false;
    while start.elapsed() < Duration::from_secs(300) {
        std::thread::sleep(Duration::from_millis(200));

        drain_actor_messages(&node_b.driver, &codecs, &node_b.rt, Duration::from_millis(100));
        drain_actor_messages(&node_a.driver, &codecs, &node_a.rt, Duration::from_millis(100));

        node_b.rt.tick();
        node_a.rt.tick();

        if let Some(response) = response_inbox.try_recv() {
            assert!(
                !response.text.is_empty(),
                "expected non-empty generated text from tinygrad, got empty string"
            );
            eprintln!("tinygrad response: {:?}", response.text);
            got_response = true;
            break;
        }
    }
    assert!(
        got_response,
        "should receive InferenceResponse from tinygrad within timeout"
    );

    // 8. Cleanup
    node_b.rt.stop_actor(inference_addr).unwrap();

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        node_b.rt.tick();
        std::thread::sleep(Duration::from_millis(50));
        if !is_process_alive(worker_pid) {
            break;
        }
    }
    assert!(
        !is_process_alive(worker_pid),
        "tinygrad_worker.py (pid {}) should be dead after actor stop",
        worker_pid
    );

    node_a.driver.shutdown();
    node_b.driver.shutdown();
}
