use std::any::Any;
use std::sync::Arc;

use parking_lot::Mutex;

use swactor::actor::{
    ActorAddress, ActorInterface, Ctx, Environment, EnvironmentBuilder, ExitValue, StopReason,
};
use swactor::config::RuntimeConfig;
use swactor::extension::{RuntimeExtension, WorkerExtension};
use swactor::runtime::Runtime;

#[derive(Clone, Debug, PartialEq, Eq)]
struct SpawnMarker(&'static str);

#[derive(Clone, Debug, PartialEq, Eq)]
struct SpawnMarkerSeen(Option<&'static str>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct DeathSeen(ActorAddress);

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkerExtFired;

struct WorkerRequest {
    target: ActorAddress,
}

#[derive(Default)]
struct SeamState {
    death_report_to: Mutex<Option<ActorAddress>>,
    cleaned: Mutex<Vec<ActorAddress>>,
    worker_pending: Mutex<Vec<ActorAddress>>,
}

struct SeamExtension {
    state: Arc<SeamState>,
    inject_spawn_marker: bool,
    enable_worker_extension: bool,
}

impl SeamExtension {
    fn new(state: Arc<SeamState>) -> Self {
        Self {
            state,
            inject_spawn_marker: false,
            enable_worker_extension: false,
        }
    }

    fn with_spawn_marker(mut self) -> Self {
        self.inject_spawn_marker = true;
        self
    }

    fn with_worker_extension(mut self) -> Self {
        self.enable_worker_extension = true;
        self
    }
}

impl RuntimeExtension for SeamExtension {
    fn on_actor_death(
        &self,
        dead: &[(ActorAddress, StopReason, Option<ExitValue>)],
    ) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        let _ = dead
            .iter()
            .map(|(_, reason, value)| (reason, value))
            .count();
        let Some(report_to) = *self.state.death_report_to.lock() else {
            return Vec::new();
        };

        dead.iter()
            .map(|(addr, _, _)| (report_to, Box::new(DeathSeen(*addr)) as Box<dyn Any + Send>))
            .collect()
    }

    fn cleanup_dead(&self, dead: &[ActorAddress]) {
        self.state.cleaned.lock().extend_from_slice(dead);
    }

    fn on_spawn(
        &self,
        _child: ActorAddress,
        _parent: Option<ActorAddress>,
        env: Environment,
        _uptime_ms: u64,
    ) -> Environment {
        if self.inject_spawn_marker {
            EnvironmentBuilder::from_env(&env)
                .set(SpawnMarker("from-extension"))
                .build()
        } else {
            env
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_worker_extension(&self) -> Option<Box<dyn WorkerExtension>> {
        self.enable_worker_extension.then(|| {
            Box::new(SeamWorkerExtension {
                state: Arc::clone(&self.state),
            }) as Box<dyn WorkerExtension>
        })
    }
}

struct SeamWorkerExtension {
    state: Arc<SeamState>,
}

impl WorkerExtension for SeamWorkerExtension {
    fn has_pending_work(&self) -> bool {
        !self.state.worker_pending.lock().is_empty()
    }

    fn on_tick(&mut self) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        self.state
            .worker_pending
            .lock()
            .pop()
            .map(|target| (target, Box::new(WorkerExtFired) as Box<dyn Any + Send>))
            .into_iter()
            .collect()
    }

    fn handle_request(&mut self, request: Box<dyn Any + Send>) {
        if let Ok(request) = request.downcast::<WorkerRequest>() {
            self.state.worker_pending.lock().push(request.target);
        }
    }

    fn gc_dead(&mut self, dead: &[ActorAddress]) {
        self.state
            .worker_pending
            .lock()
            .retain(|target| !dead.contains(target));
    }
}

fn tick_n(rt: &Runtime, n: usize) {
    for _ in 0..n {
        rt.tick();
    }
}

struct MarkerReporter {
    report_to: ActorAddress,
}

impl ActorInterface for MarkerReporter {
    type Incoming = ();
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let marker = ctx.env::<SpawnMarker>().map(|marker| marker.0);
        ctx.send(self.report_to, SpawnMarkerSeen(marker)).unwrap();
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
}

#[test]
fn on_spawn_environment_mutation_is_visible_to_actor() {
    let state = Arc::new(SeamState::default());
    let rt = Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(
        SeamExtension::new(Arc::clone(&state)).with_spawn_marker(),
    ));
    let inbox = rt.new_inbox::<SpawnMarkerSeen>().unwrap();

    rt.spawn(MarkerReporter {
        report_to: *inbox.addr(),
    })
    .unwrap();
    tick_n(&rt, 2);

    assert_eq!(
        inbox.try_recv(),
        Some(SpawnMarkerSeen(Some("from-extension")))
    );
}

struct PanicOnPing;

impl ActorInterface for PanicOnPing {
    type Incoming = ();
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: ()) {
        panic!("intentional seam-test panic");
    }
}

#[test]
fn on_actor_death_messages_are_routed() {
    let state = Arc::new(SeamState::default());
    let rt = Runtime::new(RuntimeConfig::default())
        .with_extension(Arc::new(SeamExtension::new(Arc::clone(&state))));
    let inbox = rt.new_inbox::<DeathSeen>().unwrap();
    *state.death_report_to.lock() = Some(*inbox.addr());

    let target = rt.spawn(PanicOnPing).unwrap();
    rt.send_to(target, ()).unwrap();
    tick_n(&rt, 4);

    assert_eq!(inbox.try_recv(), Some(DeathSeen(target)));
}

#[test]
fn cleanup_dead_receives_dead_actor_batch() {
    let state = Arc::new(SeamState::default());
    let rt = Runtime::new(RuntimeConfig::default())
        .with_extension(Arc::new(SeamExtension::new(Arc::clone(&state))));

    let target = rt.spawn(PanicOnPing).unwrap();
    rt.send_to(target, ()).unwrap();
    tick_n(&rt, 4);

    assert!(state.cleaned.lock().contains(&target));
}

struct WorkerRequestActor {
    target: ActorAddress,
}

impl ActorInterface for WorkerRequestActor {
    type Incoming = ();
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _msg: ()) {
        ctx.raw_inner().post_worker_request(Box::new(WorkerRequest {
            target: self.target,
        }));
    }
}

#[test]
fn worker_extension_request_is_handled_and_emits_message() {
    let state = Arc::new(SeamState::default());
    let rt = Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(
        SeamExtension::new(Arc::clone(&state)).with_worker_extension(),
    ));
    let inbox = rt.new_inbox::<WorkerExtFired>().unwrap();
    let actor = rt
        .spawn(WorkerRequestActor {
            target: *inbox.addr(),
        })
        .unwrap();

    rt.send_to(actor, ()).unwrap();
    tick_n(&rt, 4);

    assert_eq!(inbox.try_recv(), Some(WorkerExtFired));
}

#[test]
fn worker_extension_pending_work_keeps_runtime_progressing() {
    let state = Arc::new(SeamState::default());
    let rt = Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(
        SeamExtension::new(Arc::clone(&state)).with_worker_extension(),
    ));
    let inbox = rt.new_inbox::<WorkerExtFired>().unwrap();
    state.worker_pending.lock().push(*inbox.addr());

    rt.tick();

    assert_eq!(inbox.try_recv(), Some(WorkerExtFired));
}
