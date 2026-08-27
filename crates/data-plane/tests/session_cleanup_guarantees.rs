#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::bootstrap::{BootstrapSpec, prepare_arena};
use data_plane::host::{HostDataPlaneConfig, HostDataPlaneSessionActor, HostRouteRegistrar};
use data_plane::path::SessionAccess;
use data_plane::protocol::{
    AttachmentFailure, ChildSessionIn, DataPlaneError, HostSessionIn, SessionCapability,
};
use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime, RuntimeConfig, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

const CAPABILITY: SessionCapability = SessionCapability::new([0x44; 32]);

#[derive(Default)]
struct RecordingRoutes {
    registered: AtomicUsize,
    revoked: AtomicUsize,
    active: AtomicBool,
}

impl HostRouteRegistrar for RecordingRoutes {
    fn register_child(
        &self,
        _child_session: ActorAddress,
        _child_node: [u8; 32],
    ) -> Result<(), String> {
        self.registered.fetch_add(1, Ordering::AcqRel);
        self.active.store(true, Ordering::Release);
        Ok(())
    }

    fn revoke_child(&self, _child_session: ActorAddress) -> Result<(), String> {
        self.revoked.fetch_add(1, Ordering::AcqRel);
        self.active.store(false, Ordering::Release);
        Ok(())
    }
}

struct Harness {
    runtime: Runtime,
    _engine: Engine,
    session: ActorAddress,
    child: Inbox<ChildSessionIn>,
    routes: Arc<RecordingRoutes>,
}

fn harness(generation: u64) -> Harness {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("Tokio backend"),
    )
    .expect("session engine");
    let mut arena = ArenaManager::boot(ArenaConfig {
        node_id: NodeId(generation),
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .expect("session arena");
    let _prepared = prepare_arena(
        &mut arena,
        BootstrapSpec {
            arena_generation: generation,
            alignment: 64,
        },
    )
    .expect("prepared arena");
    let routes = Arc::new(RecordingRoutes::default());
    let route_port: Arc<dyn HostRouteRegistrar> = routes.clone();
    let session = runtime
        .spawn(
            HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                runtime: runtime.clone(),
                engine: engine.handle(),
                arena,
                arena_generation: generation,
                session_generation: generation,
                capability: CAPABILITY,
                session_access: SessionAccess {
                    execution_id: format!("execution-{generation}"),
                    read_prefixes: Vec::new(),
                    write_prefixes: Vec::new(),
                },
                namespace: None,
                transfer_receiver: None,
                source_sender: None,
                source_publisher: None,
                route_registrar: Some(route_port),
                stream_transport: None,
            })
            .expect("host session config"),
        )
        .expect("spawn host session");
    let child = runtime.new_inbox::<ChildSessionIn>().expect("child inbox");
    Harness {
        runtime,
        _engine: engine,
        session,
        child,
        routes,
    }
}

fn drive_until<T>(mut observe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Some(value) = observe() {
            return value;
        }
        std::thread::yield_now();
    }
    panic!("runtime condition was not reached")
}

fn close(harness: &mut Harness) {
    let reply = harness
        .runtime
        .new_inbox::<Result<(), DataPlaneError>>()
        .expect("close reply");
    harness
        .runtime
        .send_to(
            harness.session,
            HostSessionIn::Close {
                reply_to: Some(*reply.addr()),
            },
        )
        .expect("send close");
    let result = drive_until(|| reply.try_recv());
    assert_eq!(result, Ok(()));
    drive_until(|| {
        (!harness
            .runtime
            .stats()
            .actors
            .iter()
            .any(|(address, _)| *address == harness.session))
        .then_some(())
    });
}

#[test]
fn authorized_attachment_route_is_revoked_before_final_arena_release() {
    let mut harness = harness(1);
    harness
        .runtime
        .send_to(
            harness.session,
            HostSessionIn::Attach {
                child_session: *harness.child.addr(),
                arena_generation: 1,
                session_capability: CAPABILITY,
                child_node: Some([7; 32]),
            },
        )
        .expect("send attach");
    let attached = drive_until(|| harness.child.try_recv());
    assert!(matches!(attached, ChildSessionIn::Attached { .. }));
    assert_eq!(harness.routes.registered.load(Ordering::Acquire), 1);
    assert!(harness.routes.active.load(Ordering::Acquire));

    harness
        .runtime
        .send_to(harness.session, HostSessionIn::Revoke)
        .expect("send revoke");
    drive_until(|| (!harness.routes.active.load(Ordering::Acquire)).then_some(()));
    assert_eq!(harness.routes.revoked.load(Ordering::Acquire), 1);
    assert!(
        harness
            .runtime
            .stats()
            .actors
            .iter()
            .any(|(address, _)| *address == harness.session)
    );

    close(&mut harness);
}

#[test]
fn rejected_capability_revokes_its_temporary_response_route() {
    let mut harness = harness(2);
    harness
        .runtime
        .send_to(
            harness.session,
            HostSessionIn::Attach {
                child_session: *harness.child.addr(),
                arena_generation: 2,
                session_capability: SessionCapability::new([0; 32]),
                child_node: Some([8; 32]),
            },
        )
        .expect("send rejected attach");
    let rejected = drive_until(|| harness.child.try_recv());
    assert!(matches!(
        rejected,
        ChildSessionIn::AttachmentFailed {
            error: DataPlaneError::Attachment(AttachmentFailure::CapabilityRejected)
        }
    ));
    drive_until(|| (harness.routes.revoked.load(Ordering::Acquire) == 1).then_some(()));
    assert_eq!(harness.routes.registered.load(Ordering::Acquire), 1);
    assert_eq!(harness.routes.revoked.load(Ordering::Acquire), 1);
    assert!(!harness.routes.active.load(Ordering::Acquire));
    close(&mut harness);
}
