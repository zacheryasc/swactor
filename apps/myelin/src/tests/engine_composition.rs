//! Production execution-composition smoke test (ENGINE_SPEC.md).
//!
//! Verifies the real process-local execution composition for one Myelin node:
//! a swactor `Engine` over a Tokio substrate owns the core runtime and drives
//! it; the production distribution runtime/actors are constructed on that
//! engine; a real `IrohDriver` receives explicit task authority with
//! relay-disabled networking; transport-observed actor-bridge progression is
//! installed on that same engine; and observable actor progress happens with no
//! ambient Tokio runtime and no application call to `tick`, `try_tick`,
//! `has_work`, or a manual network pump.
//!
//! The composition shares the production wiring (`DistributionRuntimeStack` +
//! `IrohDriver`); it does not duplicate a fake version of it. Observations await
//! inbox notifications and actual HTTP listener readiness.

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, EngineHandle, TaskSpawner, TokioBackend, TokioConfig};

use distribution::node::DistributedNodeConfig;
use iroh::RelayMode;
use iroh_driver::{IrohDriver, IrohDriverConfig};

use crate::orchestration::distribution_stack::DistributionRuntimeStack;

// ── Local probe actor ──────────────────────────────────────────────────────

/// Probe message: replies `ProbePong` to a captured address.
#[derive(Clone)]
struct ProbePing;
#[derive(Clone, PartialEq, Eq, Debug)]
struct ProbePong;

struct EchoProbe {
    reply_to: ActorAddress,
}

impl ActorInterface for EchoProbe {
    type Incoming = ProbePing;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _: ProbePing) {
        let _ = ctx.send(self.reply_to, ProbePong);
    }
}

// ── Production composition ─────────────────────────────────────────────────

/// Build the production composition for one node, mirroring the node boot
/// sequence: build the core runtime → the engine owns and drives it → the iroh
/// driver receives task authority → the distribution stack is built
/// on the same runtime → the actor bridge is installed on that one engine.
fn prepare_composition() -> (
    RuntimeParts,
    impl FnOnce(TaskSpawner, EngineHandle) -> (IrohDriver, DistributionRuntimeStack),
) {
    let (parts, runtime, codec, transport_router) =
        DistributionRuntimeStack::build_runtime(|_| {}, None);
    (parts, move |tasks, engine| {
        let mut driver = IrohDriver::with_engine(
            tasks,
            engine.clone(),
            IrohDriverConfig {
                secret_key: None,
                relay_mode: RelayMode::Disabled,
                bind_port: None,
                node: DistributedNodeConfig::default(),
                peer_auth: None,
                additional_alpns: vec![],
            },
        )
        .expect("build iroh driver with task capability");

        let stack = DistributionRuntimeStack::new_from_runtime(
            runtime.clone(),
            codec,
            transport_router,
            driver.node_id(),
            DistributedNodeConfig::default(),
            engine,
        );

        driver.enable_actor_bridge(iroh_driver::ActorBridgeConfig {
            runtime: stack.runtime.clone(),
            codec: stack.codec.clone(),
            routes: stack.actor_bridge_routes(),
            swim: stack.actors.swim,
            relay_mirror: stack.relay_mirror.clone(),
            route_view: stack.route_view.clone(),
            outbox: stack.outbox.clone(),
        });
        driver.install_actor_bridge();

        (driver, stack)
    })
}

#[test]
fn engine_drives_actor_progress_without_manual_tick() {
    let (parts, compose) = prepare_composition();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("Tokio backend"),
    )
    .expect("engine");
    let (_driver, stack) = compose(engine.task_spawner(), engine.handle());

    // A probe actor plus an external inbox observe its reply. Delivery and the
    // reply are processed entirely by engine-driven core progression — this
    // test never calls tick / try_tick / has_work and pumps no network queue.
    let pong_inbox = stack
        .runtime
        .new_inbox::<ProbePong>()
        .expect("create pong inbox");
    let echo = stack
        .runtime
        .spawn(EchoProbe {
            reply_to: *pong_inbox.addr(),
        })
        .expect("spawn echo probe");
    stack
        .runtime
        .send_to(echo, ProbePing)
        .expect("send probe ping");

    let pong = futures_lite::future::block_on(pong_inbox.recv());
    // Keep the engine alive until the observation completes.
    drop(engine);
    assert_eq!(pong, ProbePong, "engine did not drive actor progress");
}

#[cfg(feature = "dashboard")]
#[test]
fn dashboard_server_is_scheduled_through_the_engine() {
    use std::io::{Read, Write};

    // The dashboard server future is scheduled through the Myelin engine path
    // (tasks.spawn(handle.http_server())), exactly as in production, without
    // constructing another runtime (ENGINE_SPEC.md).
    let (parts, compose) = prepare_composition();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("Tokio backend"),
    )
    .expect("engine");
    let tasks = engine.task_spawner();
    let (_driver, _stack) = compose(tasks.clone(), engine.handle());
    let config = dashboard::DashboardConfig {
        port: 0,
        ..dashboard::DashboardConfig::default()
    };
    let handle = dashboard::DashboardHandle::new(config);
    handle.spawn(&tasks);

    // Readiness is published only after the engine-hosted server binds its
    // listener. Connect once to that exact address, without reserving a port
    // early or retrying until an unrelated service happens to accept.
    let address = futures_lite::future::block_on(handle.wait_until_ready())
        .expect("dashboard listener readiness");
    assert_eq!(
        futures_lite::future::block_on(handle.wait_until_ready())
            .expect("readiness remains observable after startup"),
        address,
    );
    let mut connection =
        std::net::TcpStream::connect(address).expect("dashboard server accepts connections");
    connection
        .write_all(b"GET /api/views HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("request dashboard views");
    let mut response = String::new();
    connection
        .read_to_string(&mut response)
        .expect("read dashboard response");
    assert!(response.starts_with("HTTP/1.1 200 "), "{response}");
    drop(engine);
}
