//! Production execution-composition smoke test (ENGINE_SPEC.md).
//!
//! Verifies the real process-local execution composition for one Myelin node:
//! a swactor `Engine` over a Tokio substrate owns the core runtime and drives
//! it; the production distribution runtime/actors are constructed on that
//! engine; a real `IrohDriver` is bound through `EngineHandle` with
//! relay-disabled networking; actor-bridge and protocol-ticker progression are
//! installed on that same engine; and observable actor progress happens with no
//! ambient Tokio runtime and no application call to `tick`, `try_tick`,
//! `has_work`, or a manual network pump.
//!
//! The composition shares the production wiring (`DistributionRuntimeStack` +
//! `IrohDriver`); it does not duplicate a fake version of it. The only blocking
//! here is test-side observation polling — never engine work.

use std::time::{Duration, Instant};
use swactor::actor::{ActorAddress, ActorInterface, Ctx, Message};
use swactor::runtime::Inbox;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

use distribution::node::DistributedNodeConfig;
use iroh::RelayMode;
use iroh_driver::{IrohDriver, IrohDriverConfig};

use crate::orchestration::distribution_stack::DistributionRuntimeStack;

const PROBE_TICK: Duration = Duration::from_millis(10);
const PROBE_DEADLINE: Duration = Duration::from_secs(8);

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
/// driver is bound through the engine handle → the distribution stack is built
/// on the same runtime → actor bridge, protocol ticker, and adapter pump are
/// installed on that one engine.
fn build_composition() -> (Engine, IrohDriver, DistributionRuntimeStack) {
    let (parts, runtime, codec, transport_router) =
        DistributionRuntimeStack::build_runtime(|_| {}, None);
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("build tokio backend"),
    )
    .expect("build engine");

    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![],
        },
    )
    .expect("build iroh driver with engine handle");

    let stack = DistributionRuntimeStack::new_from_runtime(
        runtime.clone(),
        codec,
        transport_router,
        driver.node_id(),
        DistributedNodeConfig::default(),
        engine.handle(),
    );

    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
        stack.outbox.clone(),
    );
    // Engine-hosted protocol tick injection + adapter progression — no manual
    // pump is wired anywhere.
    stack.spawn_protocol_ticker(PROBE_TICK);
    driver.install_actor_bridge_pump(PROBE_TICK);

    (engine, driver, stack)
}

/// Poll an inbox until a value arrives or the deadline elapses. The only
/// `thread::sleep` in this module: test observation, not engine work.
#[allow(clippy::disallowed_methods)]
fn recv_within<T: Message>(inbox: &Inbox<T>, deadline: Duration) -> Option<T> {
    let started = Instant::now();
    loop {
        if let Some(value) = inbox.try_recv() {
            return Some(value);
        }
        if started.elapsed() >= deadline {
            return None;
        }
        std::thread::sleep(PROBE_TICK);
    }
}

#[test]
fn engine_drives_actor_progress_without_manual_tick() {
    let (engine, _driver, stack) = build_composition();

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

    let pong = recv_within(&pong_inbox, PROBE_DEADLINE);
    // Keep the engine alive until the observation completes.
    drop(engine);
    assert_eq!(pong, Some(ProbePong), "engine did not drive actor progress");
}

#[cfg(feature = "dashboard")]
#[test]
fn dashboard_server_is_scheduled_through_the_engine() {
    // The dashboard server future is scheduled through the Myelin engine path
    // (engine.spawn(handle.http_server())), exactly as in production, without
    // constructing another runtime (ENGINE_SPEC.md).
    let free_port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("probe bind for free port")
        .local_addr()
        .expect("probe local addr")
        .port();

    let (engine, _driver, _stack) = build_composition();
    let mut config = dashboard::DashboardConfig::default();
    config.port = free_port;
    let handle = dashboard::DashboardHandle::new(config);
    engine.handle().spawn(handle.http_server());

    // Behavioral proof the server future is actually running on the engine:
    // the bound port accepts a TCP connection. No second runtime is involved.
    let connected = poll_connect(("127.0.0.1", free_port), PROBE_DEADLINE);
    drop(engine);
    assert!(connected, "dashboard server did not accept connections");
}

#[cfg(feature = "dashboard")]
#[allow(clippy::disallowed_methods)]
fn poll_connect(addr: (&str, u16), deadline: Duration) -> bool {
    use std::net::TcpStream;
    let started = Instant::now();
    loop {
        if TcpStream::connect(addr).is_ok() {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        std::thread::sleep(PROBE_TICK);
    }
}
