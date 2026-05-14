//! T-integration: End-to-end distributed inference tests.
//!
//! Two swactor nodes on localhost via iroh/QUIC. Node B runs an
//! `InferenceActor` backed by a Python worker. Node A sends an
//! `InferenceRequest` across the network, through the `RequestBridge`,
//! into the actor, through the child process, and back.
//!
//! - `distributed_inference_through_echo_worker` — fast, uses canned echo responses.
//! - `distributed_inference_through_tinygrad` — slow (#[ignore]), downloads a real
//!   ~1B GGUF model and runs real tinygrad inference on CPU.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::runtime::{Runtime, RuntimeConfig};
use swactor::transport::TransportRouter;

use single_gpu_inference::inference_actor::{InferenceActor, InferenceActorStatus, RequestBridge};
use single_gpu_inference::iroh_transport::{drain_actor_messages, IrohActorTransport, ACTOR_ALPN};
use single_gpu_inference::messages::{inference_codec_registry, InferenceRequest, InferenceResponse};
use swactor_process::{ProcessMode, ProcessSpec};

// ── Iroh helpers (from t_cluster.rs) ────────────────────────────────────

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
    // 1. Converge two iroh drivers
    let (mut driver_a, mut driver_b) = make_converged_pair();

    let codecs = Arc::new(inference_codec_registry());

    // 2. Create runtimes
    let mut rt_a = Runtime::new(RuntimeConfig::default());
    let mut rt_b = Runtime::new(RuntimeConfig::default());

    // 3. On rt_b: spawn InferenceActor with echo_worker + RequestBridge
    let status_inbox = rt_b.new_inbox::<InferenceActorStatus>().unwrap();
    let sender = rt_b.create_sender();
    let actor = InferenceActor::new(echo_worker_spec(), sender)
        .with_status_addr(*status_inbox.addr());
    let inference_addr = rt_b.spawn(actor).unwrap();

    let bridge = RequestBridge { target: inference_addr };
    let bridge_addr = rt_b.spawn(bridge).unwrap();

    // 4. On rt_a: create response inbox
    let response_inbox = rt_a.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    // 5. Build iroh transports
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

    // Wire routes: A sends to bridge_addr on B, B sends to inbox_addr on A
    let router_a = TransportRouter::new();
    router_a.add_route(bridge_addr, transport_a_to_b);
    let router_b = TransportRouter::new();
    router_b.add_route(inbox_addr, transport_b_to_a);

    // 6. Install codecs and transport routers
    rt_a.set_codec_registry(codecs.clone());
    rt_a.set_transport_router(Arc::new(router_a));
    rt_b.set_codec_registry(codecs.clone());
    rt_b.set_transport_router(Arc::new(router_b));

    // 7. Tick rt_b until InferenceActor reports WorkerReady
    let start = Instant::now();
    let mut worker_pid = None;
    while start.elapsed() < Duration::from_secs(10) {
        rt_b.tick();
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

    // 8. Send InferenceRequest from node A → bridge on node B
    rt_a.send_to(
        bridge_addr,
        InferenceRequest {
            prompt: "Hello from node A".into(),
            max_tokens: 8,
            temperature: 0.7,
            reply_to: inbox_addr,
        },
    )
    .unwrap();

    // 9. Pump loop: drain messages on both sides, tick both runtimes
    let start = Instant::now();
    let mut got_response = false;
    while start.elapsed() < Duration::from_secs(10) {
        // Let transport deliver
        std::thread::sleep(Duration::from_millis(100));

        // Drain incoming actor messages on both sides
        drain_actor_messages(&driver_b, &codecs, &rt_b, Duration::from_millis(100));
        drain_actor_messages(&driver_a, &codecs, &rt_a, Duration::from_millis(100));

        // Tick both runtimes
        rt_b.tick();
        rt_a.tick();

        // Check for response
        if let Some(response) = response_inbox.try_recv() {
            // 10. Assert
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

    // 11. Cleanup: stop inference actor, wait for process death, shutdown drivers
    rt_b.stop_actor(inference_addr).unwrap();

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        rt_b.tick();
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

    driver_a.shutdown();
    driver_b.shutdown();
}

/// Full distributed inference through tinygrad with a real ~1B GGUF model.
///
/// Ignored by default — requires the `.venv` with tinygrad installed and
/// downloads a ~1 GB model on first run. Run explicitly with:
///   cargo test --package smoke-test distributed_inference -- --ignored
#[test]
#[ignore]
fn distributed_inference_through_tinygrad() {
    // 1. Converge two iroh drivers
    let (mut driver_a, mut driver_b) = make_converged_pair();

    let codecs = Arc::new(inference_codec_registry());

    // 2. Create runtimes
    let mut rt_a = Runtime::new(RuntimeConfig::default());
    let mut rt_b = Runtime::new(RuntimeConfig::default());

    // 3. On rt_b: spawn InferenceActor with tinygrad_worker + RequestBridge
    let status_inbox = rt_b.new_inbox::<InferenceActorStatus>().unwrap();
    let sender = rt_b.create_sender();
    let actor = InferenceActor::new(tinygrad_worker_spec(), sender)
        .with_status_addr(*status_inbox.addr());
    let inference_addr = rt_b.spawn(actor).unwrap();

    let bridge = RequestBridge { target: inference_addr };
    let bridge_addr = rt_b.spawn(bridge).unwrap();

    // 4. On rt_a: create response inbox
    let response_inbox = rt_a.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    // 5. Build iroh transports
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

    let router_a = TransportRouter::new();
    router_a.add_route(bridge_addr, transport_a_to_b);
    let router_b = TransportRouter::new();
    router_b.add_route(inbox_addr, transport_b_to_a);

    // 6. Install codecs and transport routers
    rt_a.set_codec_registry(codecs.clone());
    rt_a.set_transport_router(Arc::new(router_a));
    rt_b.set_codec_registry(codecs.clone());
    rt_b.set_transport_router(Arc::new(router_b));

    // 7. Wait for tinygrad model download + load (generous timeout)
    let start = Instant::now();
    let mut worker_pid = None;
    while start.elapsed() < Duration::from_secs(600) {
        rt_b.tick();
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

    // 8. Send InferenceRequest from node A → bridge on node B
    rt_a.send_to(
        bridge_addr,
        InferenceRequest {
            prompt: "Say hello".into(),
            max_tokens: 32,
            temperature: 0.7,
            reply_to: inbox_addr,
        },
    )
    .unwrap();

    // 9. Pump loop — generation on CPU can be slow
    let start = Instant::now();
    let mut got_response = false;
    while start.elapsed() < Duration::from_secs(300) {
        std::thread::sleep(Duration::from_millis(200));

        drain_actor_messages(&driver_b, &codecs, &rt_b, Duration::from_millis(100));
        drain_actor_messages(&driver_a, &codecs, &rt_a, Duration::from_millis(100));

        rt_b.tick();
        rt_a.tick();

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

    // 10. Cleanup
    rt_b.stop_actor(inference_addr).unwrap();

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        rt_b.tick();
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

    driver_a.shutdown();
    driver_b.shutdown();
}
