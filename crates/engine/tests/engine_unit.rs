//! Unit tests for engine logic — capability binding, time semantics, and the
//! non-Tokio portability proof.
//!
//! Deliberately separate from `engine_contract.rs` (the behavioral contract).
//! These tests exercise internal logic directly and use the [`SteppingBackend`]
//! to prove substrate independence without Tokio (ENGINE_SPEC.md).

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Duration;

use parking_lot::Mutex;
use proptest::prelude::*;
use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::{
    ActorCompletion, ActorTimer, Capabilities, Engine, EngineError, ExecutionBackend,
    SteppingBackend,
};

#[cfg(feature = "tokio")]
use swactor_engine::{TokioBackend, TokioConfig};

// ═══════════════════════════════════════════════════════════════════════════════
// §1  Capabilities::satisfies logic
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn satisfies_all_true_meets_all_requirements() {
    let full = Capabilities::ALL;
    assert!(full.satisfies(Capabilities::ALL));
    assert!(full.satisfies(Capabilities::TASKS_ONLY));
    assert!(full.satisfies(Capabilities {
        tasks: true,
        timers: true,
        blocking: true,
        io: true,
    }));
}

#[test]
fn satisfies_empty_required_always_passes() {
    let none_required = Capabilities {
        tasks: false,
        timers: false,
        blocking: false,
        io: false,
    };
    let weak = Capabilities::TASKS_ONLY;
    assert!(weak.satisfies(none_required));
    assert!(Capabilities::ALL.satisfies(none_required));
}

#[test]
fn satisfies_missing_single_capability_fails() {
    let backend = Capabilities {
        tasks: true,
        timers: true,
        blocking: true,
        io: false,
    };
    assert!(!backend.satisfies(Capabilities::ALL));
    assert!(backend.satisfies(Capabilities {
        tasks: true,
        timers: true,
        blocking: true,
        io: false,
    }));
}

#[test]
fn satisfies_tasks_only_does_not_imply_timers() {
    let tasks_only = Capabilities::TASKS_ONLY;
    assert!(tasks_only.satisfies(Capabilities::TASKS_ONLY));
    assert!(!tasks_only.satisfies(Capabilities {
        tasks: true,
        timers: true,
        blocking: false,
        io: false,
    }));
}

#[test]
fn capabilities_constants_are_correct() {
    assert_eq!(
        Capabilities::TASKS_ONLY,
        Capabilities {
            tasks: true,
            timers: false,
            blocking: false,
            io: false,
        }
    );
    assert_eq!(
        Capabilities::ALL,
        Capabilities {
            tasks: true,
            timers: true,
            blocking: true,
            io: true,
        }
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// §2  Engine construction / capability rejection
// ═══════════════════════════════════════════════════════════════════════════════

/// A minimal backend that advertises no capabilities — used to verify
/// `Engine::new` rejects it.
struct NoCapBackend;

impl swactor_engine::ExecutionBackend for NoCapBackend {
    fn spawn(&self, _: swactor_engine::BoxTask) {}
    fn spawn_blocking(&self, _: swactor_engine::BoxWork) {}
    fn timer(&self, _: Duration) -> swactor_engine::BoxTimer {
        unreachable!("no capabilities")
    }
    fn now(&self) -> swactor_engine::EngineInstant {
        swactor_engine::EngineInstant::now()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            tasks: false,
            timers: false,
            blocking: false,
            io: false,
        }
    }
}

#[test]
fn engine_new_rejects_backend_without_tasks() {
    let parts = default_parts();
    let result = Engine::new(parts, NoCapBackend);
    assert!(
        matches!(result, Err(EngineError::MissingRequiredCapability)),
        "Engine::new must reject a backend that cannot schedule tasks"
    );
}

#[test]
fn engine_new_accepts_stepping_backend() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend).expect("stepping backend has tasks");
    drop(engine);
}

// ═══════════════════════════════════════════════════════════════════════════════
// §3  EngineHandle::require — capability binding
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn require_accepts_when_all_capabilities_present() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend).unwrap();
    let handle = engine.handle();

    // Stepping provides tasks + timers + blocking.
    let required = Capabilities {
        tasks: true,
        timers: true,
        blocking: true,
        io: false,
    };
    assert!(handle.require(required).is_ok());
}

#[test]
fn require_rejects_when_io_missing() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend).unwrap();
    let handle = engine.handle();

    // Stepping does NOT provide io.
    assert!(handle.require(Capabilities::ALL).is_err());
}

#[test]
fn require_rejects_when_timers_missing() {
    struct TaskOnlyBackend;
    impl swactor_engine::ExecutionBackend for TaskOnlyBackend {
        fn spawn(&self, _: swactor_engine::BoxTask) {}
        fn spawn_blocking(&self, _: swactor_engine::BoxWork) {}
        fn timer(&self, _: Duration) -> swactor_engine::BoxTimer {
            unreachable!()
        }
        fn now(&self) -> swactor_engine::EngineInstant {
            swactor_engine::EngineInstant::now()
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::TASKS_ONLY
        }
    }

    let parts = default_parts();
    let engine = Engine::new(parts, TaskOnlyBackend).unwrap();
    let handle = engine.handle();

    assert!(
        handle
            .require(Capabilities {
                tasks: true,
                timers: true,
                blocking: false,
                io: false,
            })
            .is_err(),
        "must reject when timers required but not provided"
    );
    assert!(handle.require(Capabilities::TASKS_ONLY).is_ok());
}

#[test]
fn require_can_be_called_multiple_times() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend).unwrap();
    let handle = engine.handle();

    assert!(handle.require(Capabilities::TASKS_ONLY).is_ok());
    assert!(handle.require(Capabilities::TASKS_ONLY).is_ok());
    assert!(handle.require(Capabilities::ALL).is_err());
    assert!(handle.require(Capabilities::ALL).is_err());
}

// ═══════════════════════════════════════════════════════════════════════════════
// §4  Truthful capability reporting
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn stepping_backend_reports_no_io() {
    let backend = SteppingBackend::new();
    let caps = backend.capabilities();
    assert!(caps.tasks);
    assert!(caps.timers);
    assert!(caps.blocking);
    assert!(!caps.io, "stepping backend must not advertise io");
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_backend_reports_all_capabilities() {
    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let caps = backend.capabilities();
    assert!(caps.tasks, "tokio must advertise tasks");
    assert!(caps.timers, "tokio must advertise timers");
    assert!(caps.blocking, "tokio must advertise blocking");
    assert!(
        caps.io,
        "tokio must advertise io — enable_all starts the I/O reactor"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// §5  Portability proof: SteppingBackend (ENGINE_SPEC.md)
// ═══════════════════════════════════════════════════════════════════════════════

/// Enough steps to let the driver loop tick and process queued work.
const STEPS: usize = 30;

#[test]
fn stepping_core_progresses_without_tokio() {
    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let addr = runtime
        .spawn(RecordingProbe {
            received: received.clone(),
        })
        .expect("spawn probe");

    let backend = SteppingBackend::new();
    let _engine = Engine::new(parts, backend.clone()).expect("construct engine");

    // Deliver AFTER engine construction — a later tick must observe it.
    runtime.send_to(addr, Probe).expect("deliver probe");

    for _ in 0..STEPS {
        backend.step();
    }

    assert!(
        received.load(SeqCst) >= 1,
        "actor must process a message without any application tick or tokio"
    );
}

#[test]
fn stepping_engine_installs_one_driver_per_worker() {
    let (parts, _runtime) = runtime_parts_with_workers(3);
    let backend = SteppingBackend::new();

    let _engine = Engine::new(parts, backend.clone()).expect("construct engine");

    assert_eq!(
        backend.pending_task_count(),
        3,
        "one core-driving task is installed per worker"
    );
}

#[test]
fn stepping_engine_drives_every_worker() {
    let (parts, runtime) = runtime_parts_with_workers(3);
    let counters: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let addrs: Vec<_> = counters
        .iter()
        .map(|received| {
            runtime
                .spawn(RecordingProbe {
                    received: received.clone(),
                })
                .expect("spawn probe")
        })
        .collect();

    let backend = SteppingBackend::new();
    let _engine = Engine::new(parts, backend.clone()).expect("construct engine");

    for addr in addrs {
        runtime.send_to(addr, Probe).expect("deliver probe");
    }
    for _ in 0..STEPS {
        backend.step();
    }

    for (worker, received) in counters.iter().enumerate() {
        assert_eq!(
            received.load(SeqCst),
            1,
            "worker {worker} must be driven by its own core driver"
        );
    }
}

#[test]
fn stepping_supporting_work_progresses() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");
    let handle = engine.handle();

    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();
    handle.spawn(async move {
        done_clone.store(true, SeqCst);
    });

    for _ in 0..STEPS {
        backend.step();
    }

    assert!(
        done.load(SeqCst),
        "spawned supporting work must complete without tokio"
    );
}

#[test]
fn stepping_core_and_supporting_work_both_progress() {
    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let addr = runtime
        .spawn(RecordingProbe {
            received: received.clone(),
        })
        .expect("spawn probe");

    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");
    let handle = engine.handle();

    // Long-lived cooperative supporting work that yields between steps.
    let steps = Arc::new(AtomicUsize::new(0));
    let steps_clone = steps.clone();
    handle.spawn(async move {
        for _ in 0..10 {
            steps_clone.fetch_add(1, SeqCst);
            yield_once().await;
        }
    });

    runtime.send_to(addr, Probe).expect("deliver probe");

    for _ in 0..(STEPS * 2) {
        backend.step();
    }

    assert!(steps.load(SeqCst) >= 10, "supporting work must finish");
    assert!(
        received.load(SeqCst) >= 1,
        "actor message must be processed"
    );
}

#[test]
fn stepping_virtual_time_is_monotonic() {
    let backend = SteppingBackend::new();
    let mut prev = backend.virtual_now();
    for _ in 0..100 {
        backend.advance_time(Duration::from_millis(1));
        let cur = backend.virtual_now();
        assert!(cur > prev, "virtual clock must advance");
        prev = cur;
    }
}

#[test]
fn stepping_virtual_now_matches_engine_now() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).unwrap();
    let handle = engine.handle();

    assert_eq!(handle.now(), backend.virtual_now());

    backend.advance_time(Duration::from_secs(5));
    assert_eq!(handle.now(), backend.virtual_now());
}

#[test]
fn stepping_timer_does_not_fire_before_advance() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");
    let handle = engine.handle();

    let fired = Arc::new(AtomicBool::new(false));
    let fired_clone = fired.clone();
    let timer_handle = handle.clone();
    handle.spawn(async move {
        timer_handle.timer(Duration::from_secs(1)).await;
        fired_clone.store(true, SeqCst);
    });

    for _ in 0..10 {
        backend.step();
    }
    assert!(
        !fired.load(SeqCst),
        "timer must NOT fire before virtual time reaches the deadline"
    );
}

#[test]
fn stepping_timer_fires_after_virtual_time_advance() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");
    let handle = engine.handle();

    let fired = Arc::new(AtomicBool::new(false));
    let fired_clone = fired.clone();
    let timer_handle = handle.clone();
    handle.spawn(async move {
        timer_handle.timer(Duration::from_millis(500)).await;
        fired_clone.store(true, SeqCst);
    });

    for _ in 0..10 {
        backend.step();
    }
    assert!(!fired.load(SeqCst));

    backend.advance_time(Duration::from_secs(1));

    for _ in 0..10 {
        backend.step();
    }
    assert!(
        fired.load(SeqCst),
        "timer must fire after virtual time advances past the deadline"
    );
}

#[test]
fn actor_message_timer_uses_engine_clock() {
    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let actor = runtime
        .spawn(RecordingProbe {
            received: Arc::clone(&received),
        })
        .expect("spawn recording actor");
    let sender = runtime.create_sender();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");

    engine
        .handle()
        .send_after(Duration::from_secs(1), sender, actor, Probe);
    for _ in 0..4 {
        backend.step();
    }
    assert_eq!(received.load(SeqCst), 0);

    backend.advance_time(Duration::from_secs(1));
    for _ in 0..4 {
        backend.step();
    }
    assert_eq!(received.load(SeqCst), 1);
}

#[test]
fn cancelling_actor_message_timer_prevents_delivery() {
    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let actor = runtime
        .spawn(RecordingProbe {
            received: Arc::clone(&received),
        })
        .expect("spawn recording actor");
    let sender = runtime.create_sender();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");

    let timer = engine
        .handle()
        .send_after(Duration::from_secs(1), sender, actor, Probe);
    timer.cancel();
    backend.advance_time(Duration::from_secs(1));
    for _ in 0..4 {
        backend.step();
    }

    assert!(timer.is_cancelled());
    assert_eq!(received.load(SeqCst), 0);
}

#[test]
fn stepping_blocking_work_runs_isolated() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");
    let handle = engine.handle();

    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();
    assert!(
        handle
            .blocking_work_sender()
            .submit(Box::new(move || {
                std::thread::sleep(Duration::from_millis(10));
                done_clone.store(true, SeqCst);
            }))
            .is_ok(),
        "submit stepping work"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if done.load(SeqCst) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("blocking work did not complete within deadline");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// §6  Generated control-flow contracts
// ═══════════════════════════════════════════════════════════════════════════════

const LIFECYCLE_DRAIN_STEPS: usize = 4;
const MAX_GENERATED_TIMER_DELAY: Duration = Duration::from_millis(4);

#[derive(Clone, Debug)]
struct TimerFuzzMessage {
    token: usize,
    generation: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TimerObservation {
    token: usize,
    message_generation: usize,
    actor_generation: usize,
}

struct TimerFuzzProbe {
    generation: Arc<AtomicUsize>,
    observations: Arc<Mutex<Vec<TimerObservation>>>,
    deliveries: Arc<Mutex<Vec<TimerObservation>>>,
}

impl ActorInterface for TimerFuzzProbe {
    type Incoming = TimerFuzzMessage;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, message: TimerFuzzMessage) {
        let observation = TimerObservation {
            token: message.token,
            message_generation: message.generation,
            actor_generation: self.generation.load(SeqCst),
        };
        self.observations.lock().push(observation.clone());
        if observation.message_generation == observation.actor_generation {
            self.deliveries.lock().push(observation);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum TimerKind {
    Once,
    Periodic,
}

#[derive(Debug)]
struct TimerRecord {
    timer: ActorTimer,
    token: usize,
    kind: TimerKind,
    scheduled_while_live: bool,
    cancelled_at: Option<usize>,
}

type TerminalOutcome = Result<u8, u8>;

#[derive(Debug, Default)]
struct CompletionCensus {
    attempts: Vec<TerminalOutcome>,
    accepted: Vec<TerminalOutcome>,
    rejected: Vec<TerminalOutcome>,
}

#[derive(Clone, Copy, Debug)]
struct LifecycleCensus {
    pending_tasks: usize,
    task_limit: usize,
    timer_handles: usize,
    timer_limit: usize,
    uncancelled_periodic: usize,
    quiesced: bool,
}

fn record_completion_attempt(
    completion: &ActorCompletion<TerminalOutcome>,
    census: &mut CompletionCensus,
    outcome: TerminalOutcome,
) {
    census.attempts.push(outcome);
    match completion.complete(outcome) {
        Ok(()) => census.accepted.push(outcome),
        Err(rejected) => census.rejected.push(rejected),
    }
}

fn check_lifecycle_invariants(
    completions: &CompletionCensus,
    census: LifecycleCensus,
) -> Result<(), String> {
    let expected_accepted = usize::from(!completions.attempts.is_empty());
    if completions.accepted.len() != expected_accepted {
        return Err(format!(
            "completion accepted {} terminal observations, expected {}; \
             completion_census={completions:?}, lifecycle_census={census:?}",
            completions.accepted.len(),
            expected_accepted,
        ));
    }
    if completions.accepted.first() != completions.attempts.first() {
        return Err(format!(
            "completion did not preserve its first terminal observation; \
             completion_census={completions:?}, lifecycle_census={census:?}",
        ));
    }
    if completions.rejected.as_slice() != &completions.attempts[expected_accepted..] {
        return Err(format!(
            "completion did not reject every duplicate terminal observation; \
             completion_census={completions:?}, lifecycle_census={census:?}",
        ));
    }
    if census.quiesced && census.uncancelled_periodic != 0 {
        return Err(format!(
            "{} uncancelled periodic timer(s) survived quiescence; \
             completion_census={completions:?}, lifecycle_census={census:?}",
            census.uncancelled_periodic,
        ));
    }
    if census.pending_tasks > census.task_limit {
        return Err(format!(
            "task cardinality {} exceeded generated-operation bound {}; \
             completion_census={completions:?}, lifecycle_census={census:?}",
            census.pending_tasks, census.task_limit,
        ));
    }
    if census.timer_handles > census.timer_limit {
        return Err(format!(
            "timer cardinality {} exceeded generated-operation bound {}; \
             completion_census={completions:?}, lifecycle_census={census:?}",
            census.timer_handles, census.timer_limit,
        ));
    }
    Ok(())
}

fn observations_for_token(observations: &[TimerObservation], token: usize) -> usize {
    observations
        .iter()
        .filter(|observation| observation.token == token)
        .count()
}

#[derive(Clone, Debug)]
enum TimerAction {
    ScheduleOnce(u8),
    ScheduleEvery(u8),
    Cancel(u8),
    Advance(u8),
    BumpGeneration,
    StopActor,
    DropEngine,
    CompleteSuccess(u8),
    CompleteError(u8),
}

fn timer_actions() -> impl Strategy<Value = Vec<TimerAction>> {
    proptest::collection::vec(
        prop_oneof![
            3 => (1_u8..=4).prop_map(TimerAction::ScheduleOnce),
            2 => (1_u8..=4).prop_map(TimerAction::ScheduleEvery),
            2 => any::<u8>().prop_map(TimerAction::Cancel),
            3 => (0_u8..=4).prop_map(TimerAction::Advance),
            1 => Just(TimerAction::BumpGeneration),
            1 => Just(TimerAction::StopActor),
            1 => Just(TimerAction::DropEngine),
            1 => any::<u8>().prop_map(TimerAction::CompleteSuccess),
            1 => any::<u8>().prop_map(TimerAction::CompleteError),
        ],
        0..=32,
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        max_shrink_iters: 2_000,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_actor_timers_and_completion_are_bounded(actions in timer_actions()) {
        let (parts, runtime) = default_runtime_parts();
        let baseline = runtime.stats().actors.len();
        let generation = Arc::new(AtomicUsize::new(0));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let deliveries = Arc::new(Mutex::new(Vec::new()));
        let actor = runtime
            .spawn(TimerFuzzProbe {
                generation: Arc::clone(&generation),
                observations: Arc::clone(&observations),
                deliveries: Arc::clone(&deliveries),
            })
            .expect("spawn timer fuzz probe");
        let sender = runtime.create_sender();
        let backend = SteppingBackend::new();
        let mut engine = Some(Engine::new(parts, backend.clone()).expect("construct engine"));
        let handle = engine.as_ref().unwrap().handle();
        let driver_task_count = backend.pending_task_count();
        let completion = ActorCompletion::new();
        let mut completion_census = CompletionCensus::default();
        let mut timers = Vec::<TimerRecord>::new();
        let mut scheduled_while_live = 0;
        let mut generated_timer_actions = 0;
        let mut stopped_at = None;

        let assert_post_drop_scheduling_is_inert = || -> Result<(), String> {
            let pending_before = backend.pending_task_count();
            let generation = generation.load(SeqCst);
            let one_shot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handle.send_after(
                    Duration::from_millis(1),
                    sender.clone(),
                    actor,
                    TimerFuzzMessage {
                        token: usize::MAX,
                        generation,
                    },
                )
            }));
            let periodic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handle.send_every(
                    Duration::from_millis(1),
                    sender.clone(),
                    actor,
                    TimerFuzzMessage {
                        token: usize::MAX,
                        generation,
                    },
                )
            }));
            if one_shot.is_err() || periodic.is_err() {
                return Err("post-drop one-shot or periodic scheduling panicked".to_owned());
            }
            let pending_after = backend.pending_task_count();
            if pending_after != pending_before {
                return Err(format!(
                    "post-drop scheduling changed pending tasks from {pending_before} to \
                     {pending_after}",
                ));
            }
            if handle.capabilities() != Capabilities::NONE
                || handle.require(Capabilities::TASKS_ONLY).is_ok()
            {
                return Err("dropped engine still advertised scheduling capabilities".to_owned());
            }
            Ok(())
        };

        drive_steps(&backend, LIFECYCLE_DRAIN_STEPS);
        for action in &actions {
            match *action {
                TimerAction::ScheduleOnce(delay) => {
                    let token = timers.len();
                    let engine_was_live = engine.is_some();
                    let tasks_before = backend.pending_task_count();
                    let scheduled =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            handle.send_after(
                                Duration::from_millis(u64::from(delay)),
                                sender.clone(),
                                actor,
                                TimerFuzzMessage {
                                    token,
                                    generation: generation.load(SeqCst),
                                },
                            )
                        }));
                    prop_assert!(
                        scheduled.is_ok(),
                        "one-shot scheduling panicked; actions={:?}, action={:?}, stats={:?}",
                        actions,
                        action,
                        runtime.stats(),
                    );
                    let timer = scheduled.unwrap();
                    generated_timer_actions += 1;
                    if engine_was_live {
                        scheduled_while_live += 1;
                        prop_assert_eq!(
                            backend.pending_task_count(),
                            tasks_before + 1,
                            "live one-shot did not create exactly one task; actions={:?}, \
                             action={:?}, stats={:?}",
                            actions,
                            action,
                            runtime.stats(),
                        );
                    } else {
                        prop_assert_eq!(
                            backend.pending_task_count(),
                            tasks_before,
                            "post-drop one-shot was not inert; actions={:?}, action={:?}, \
                             stats={:?}",
                            actions,
                            action,
                            runtime.stats(),
                        );
                    }
                    timers.push(TimerRecord {
                        timer,
                        token,
                        kind: TimerKind::Once,
                        scheduled_while_live: engine_was_live,
                        cancelled_at: None,
                    });
                }
                TimerAction::ScheduleEvery(period) => {
                    let token = timers.len();
                    let engine_was_live = engine.is_some();
                    let tasks_before = backend.pending_task_count();
                    let scheduled =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            handle.send_every(
                                Duration::from_millis(u64::from(period)),
                                sender.clone(),
                                actor,
                                TimerFuzzMessage {
                                    token,
                                    generation: generation.load(SeqCst),
                                },
                            )
                        }));
                    prop_assert!(
                        scheduled.is_ok(),
                        "periodic scheduling panicked; actions={:?}, action={:?}, stats={:?}",
                        actions,
                        action,
                        runtime.stats(),
                    );
                    let timer = scheduled.unwrap();
                    generated_timer_actions += 1;
                    if engine_was_live {
                        scheduled_while_live += 1;
                        prop_assert_eq!(
                            backend.pending_task_count(),
                            tasks_before + 1,
                            "live periodic send did not create exactly one task; actions={:?}, \
                             action={:?}, stats={:?}",
                            actions,
                            action,
                            runtime.stats(),
                        );
                    } else {
                        prop_assert_eq!(
                            backend.pending_task_count(),
                            tasks_before,
                            "post-drop periodic send was not inert; actions={:?}, action={:?}, \
                             stats={:?}",
                            actions,
                            action,
                            runtime.stats(),
                        );
                    }
                    timers.push(TimerRecord {
                        timer,
                        token,
                        kind: TimerKind::Periodic,
                        scheduled_while_live: engine_was_live,
                        cancelled_at: None,
                    });
                }
                TimerAction::Cancel(index) => {
                    if !timers.is_empty() {
                        drive_steps(&backend, LIFECYCLE_DRAIN_STEPS);
                        let index = usize::from(index) % timers.len();
                        let delivered =
                            observations_for_token(&observations.lock(), timers[index].token);
                        timers[index].timer.cancel();
                        timers[index].timer.cancel();
                        timers[index].cancelled_at.get_or_insert(delivered);
                        prop_assert!(
                            timers[index].timer.is_cancelled(),
                            "repeated cancellation was not idempotent; actions={:?}, \
                             action={:?}, timer_census={:?}",
                            actions,
                            action,
                            timers,
                        );
                    }
                }
                TimerAction::Advance(milliseconds) => {
                    advance_and_drive(
                        &backend,
                        Duration::from_millis(u64::from(milliseconds)),
                        LIFECYCLE_DRAIN_STEPS,
                    );
                }
                TimerAction::BumpGeneration => {
                    generation.fetch_add(1, SeqCst);
                }
                TimerAction::StopActor => {
                    drive_steps(&backend, LIFECYCLE_DRAIN_STEPS);
                    let observed = observations.lock().len();
                    let _ = runtime.stop_actor(actor);
                    stopped_at.get_or_insert(observed);
                }
                TimerAction::DropEngine => {
                    drop(engine.take());
                    let post_drop = assert_post_drop_scheduling_is_inert();
                    prop_assert!(
                        post_drop.is_ok(),
                        "post-drop invariant failed: {}; actions={:?}, action={:?}, \
                         timer_census={:?}, completion_census={:?}, stats={:?}",
                        post_drop.as_ref().unwrap_err(),
                        actions,
                        action,
                        timers,
                        completion_census,
                        runtime.stats(),
                    );
                }
                TimerAction::CompleteSuccess(value) => {
                    record_completion_attempt(&completion, &mut completion_census, Ok(value));
                }
                TimerAction::CompleteError(value) => {
                    record_completion_attempt(&completion, &mut completion_census, Err(value));
                }
            }

            drive_steps(&backend, LIFECYCLE_DRAIN_STEPS);
            let observed = observations.lock().clone();
            let delivered = deliveries.lock().clone();
            let expected_deliveries = observed
                .iter()
                .filter(|observation| {
                    observation.message_generation == observation.actor_generation
                })
                .cloned()
                .collect::<Vec<_>>();
            prop_assert_eq!(
                delivered,
                expected_deliveries,
                "stale generation was delivered or a current generation was lost; actions={:?}, \
                 action={:?}, observations={:?}, timer_census={:?}, \
                 completion_census={:?}, stats={:?}",
                actions,
                action,
                observed,
                timers,
                completion_census,
                runtime.stats(),
            );
            for timer in &timers {
                if let Some(cancelled_at) = timer.cancelled_at {
                    let current = observations_for_token(&observed, timer.token);
                    prop_assert_eq!(
                        current,
                        cancelled_at,
                        "timer {} delivered after cancellation; actions={:?}, action={:?}, \
                         observations={:?}, timer_census={:?}, completion_census={:?}, \
                         stats={:?}",
                        timer.token,
                        actions,
                        action,
                        observed,
                        timers,
                        completion_census,
                        runtime.stats(),
                    );
                }
            }
            if let Some(stopped_at) = stopped_at {
                prop_assert_eq!(
                    observed.len(),
                    stopped_at,
                    "message delivered after actor stop; actions={:?}, action={:?}, \
                     observations={:?}, timer_census={:?}, completion_census={:?}, stats={:?}",
                    actions,
                    action,
                    observed,
                    timers,
                    completion_census,
                    runtime.stats(),
                );
            }
            let lifecycle_census = LifecycleCensus {
                pending_tasks: backend.pending_task_count(),
                task_limit: driver_task_count + scheduled_while_live,
                timer_handles: timers.len(),
                timer_limit: generated_timer_actions,
                uncancelled_periodic: timers
                    .iter()
                    .filter(|timer| {
                        timer.scheduled_while_live
                            && matches!(timer.kind, TimerKind::Periodic)
                            && !timer.timer.is_cancelled()
                    })
                    .count(),
                quiesced: false,
            };
            let invariant =
                check_lifecycle_invariants(&completion_census, lifecycle_census);
            prop_assert!(
                invariant.is_ok(),
                "lifecycle invariant failed: {}; actions={:?}, action={:?}, \
                 observations={:?}, timer_census={:?}, completion_census={:?}, stats={:?}",
                invariant.as_ref().unwrap_err(),
                actions,
                action,
                observed,
                timers,
                completion_census,
                runtime.stats(),
            );
            prop_assert!(
                runtime.stats().actors.len() <= baseline + 1,
                "actor cardinality exceeded fixed bound; actions={:?}, action={:?}, \
                 timer_census={:?}, completion_census={:?}, stats={:?}",
                actions,
                action,
                timers,
                completion_census,
                runtime.stats(),
            );
        }

        if let Some(expected) = completion_census.accepted.first().copied() {
            let observed = completion.wait();
            prop_assert_eq!(
                observed,
                expected,
                "completion wait returned a different terminal observation; actions={:?}, \
                 timer_census={:?}, completion_census={:?}, stats={:?}",
                actions,
                timers,
                completion_census,
                runtime.stats(),
            );
            record_completion_attempt(&completion, &mut completion_census, expected);
        }

        drive_steps(&backend, LIFECYCLE_DRAIN_STEPS);
        let observed = observations.lock().clone();
        for timer in &mut timers {
            let delivered = observations_for_token(&observed, timer.token);
            timer.timer.cancel();
            timer.timer.cancel();
            timer.cancelled_at.get_or_insert(delivered);
        }
        advance_and_drive(
            &backend,
            MAX_GENERATED_TIMER_DELAY,
            LIFECYCLE_DRAIN_STEPS,
        );
        drop(engine.take());
        let post_drop = assert_post_drop_scheduling_is_inert();
        prop_assert!(
            post_drop.is_ok(),
            "final post-drop invariant failed: {}; actions={:?}, timer_census={:?}, \
             completion_census={:?}, stats={:?}",
            post_drop.as_ref().unwrap_err(),
            actions,
            timers,
            completion_census,
            runtime.stats(),
        );
        drive_steps(&backend, LIFECYCLE_DRAIN_STEPS);
        let final_observations = observations.lock().clone();
        let final_deliveries = deliveries.lock().clone();
        let expected_final_deliveries = final_observations
            .iter()
            .filter(|observation| {
                observation.message_generation == observation.actor_generation
            })
            .cloned()
            .collect::<Vec<_>>();
        prop_assert_eq!(
            final_deliveries,
            expected_final_deliveries,
            "final drain delivered a stale generation or lost a current generation; \
             actions={:?}, observations={:?}, timer_census={:?}, completion_census={:?}, \
             stats={:?}",
            actions,
            final_observations,
            timers,
            completion_census,
            runtime.stats(),
        );
        for timer in &timers {
            let delivered = observations_for_token(&final_observations, timer.token);
            prop_assert_eq!(
                delivered,
                timer.cancelled_at.expect("all timers cancelled during final drain"),
                "timer {} delivered during the final post-cancellation drain; actions={:?}, \
                 observations={:?}, timer_census={:?}, completion_census={:?}, stats={:?}",
                timer.token,
                actions,
                final_observations,
                timers,
                completion_census,
                runtime.stats(),
            );
        }

        let lifecycle_census = LifecycleCensus {
            pending_tasks: backend.pending_task_count(),
            task_limit: driver_task_count,
            timer_handles: timers.len(),
            timer_limit: generated_timer_actions,
            uncancelled_periodic: timers
                .iter()
                .filter(|timer| {
                    timer.scheduled_while_live
                        && matches!(timer.kind, TimerKind::Periodic)
                        && !timer.timer.is_cancelled()
                })
                .count(),
            quiesced: true,
        };
        let invariant = check_lifecycle_invariants(&completion_census, lifecycle_census);
        prop_assert!(
            invariant.is_ok(),
            "final lifecycle invariant failed: {}; actions={:?}, observations={:?}, \
             timer_census={:?}, completion_census={:?}, lifecycle_census={:?}, stats={:?}",
            invariant.as_ref().unwrap_err(),
            actions,
            observations.lock(),
            timers,
            completion_census,
            lifecycle_census,
            runtime.stats(),
        );

        let final_stats = runtime.stats();
        let worker_panics = final_stats
            .workers
            .iter()
            .map(|worker| worker.panics)
            .sum::<u64>();
        let poisoned = final_stats
            .actor_details
            .iter()
            .filter(|actor| actor.poisoned)
            .collect::<Vec<_>>();
        prop_assert!(
            worker_panics == 0 && poisoned.is_empty(),
            "runtime poisoned during generated lifecycle; actions={:?}, observations={:?}, \
             timer_census={:?}, completion_census={:?}, lifecycle_census={:?}, stats={:?}",
            actions,
            final_observations,
            timers,
            completion_census,
            lifecycle_census,
            final_stats,
        );
        prop_assert!(
            final_stats.actors.len() <= baseline + 1,
            "final actor cardinality exceeded fixed bound; actions={:?}, observations={:?}, \
             timer_census={:?}, completion_census={:?}, lifecycle_census={:?}, stats={:?}",
            actions,
            final_observations,
            timers,
            completion_census,
            lifecycle_census,
            final_stats,
        );
    }
}

#[test]
fn lifecycle_invariant_detects_injected_duplicate_completion() {
    let completions = CompletionCensus {
        attempts: vec![Ok(7), Err(9)],
        accepted: vec![Ok(7), Err(9)],
        rejected: Vec::new(),
    };
    let census = LifecycleCensus {
        pending_tasks: 1,
        task_limit: 1,
        timer_handles: 0,
        timer_limit: 0,
        uncancelled_periodic: 0,
        quiesced: false,
    };

    let violation = check_lifecycle_invariants(&completions, census)
        .expect_err("the exactly-once invariant must reject an injected duplicate completion");
    assert!(
        violation.contains("completion accepted 2 terminal observations"),
        "wrong detector failure for injected actions=[CompleteSuccess(7), CompleteError(9)]: \
         violation={violation}, completion_census={completions:?}, lifecycle_census={census:?}",
    );
}

#[test]
fn lifecycle_invariant_detects_injected_uncancelled_periodic_timer() {
    let completions = CompletionCensus::default();
    let census = LifecycleCensus {
        pending_tasks: 2,
        task_limit: 1,
        timer_handles: 1,
        timer_limit: 1,
        uncancelled_periodic: 1,
        quiesced: true,
    };

    let violation = check_lifecycle_invariants(&completions, census)
        .expect_err("the leak invariant must reject an injected uncancelled periodic timer");
    assert!(
        violation.contains("uncancelled periodic timer(s) survived quiescence"),
        "wrong detector failure for injected actions=[ScheduleEvery(1), DropEngine]: \
         violation={violation}, completion_census={completions:?}, lifecycle_census={census:?}",
    );
}

#[test]
fn stepping_spawned_task_completing_is_removed_from_queue() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");
    let handle = engine.handle();

    handle.spawn(async {});
    for _ in 0..5 {
        backend.step();
    }
    assert_eq!(
        backend.pending_task_count(),
        1,
        "only the core-driving loop should remain after spawned tasks complete"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// §6  Time types
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn engine_instant_is_ordered() {
    let backend = SteppingBackend::new();
    let t0 = backend.virtual_now();

    backend.advance_time(Duration::from_secs(1));
    let t1 = backend.virtual_now();

    backend.advance_time(Duration::from_secs(1));
    let t2 = backend.virtual_now();

    assert!(t0 < t1);
    assert!(t1 < t2);
    assert!(t0 < t2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// §7  Engine ownership: handles never keep the backend alive (§4.1)
// ═══════════════════════════════════════════════════════════════════════════════

/// A tasks-only probe backend that shares a sentinel `Arc<()>` so the test can
/// observe exactly when the engine's strong backend reference is released.
struct SentinelBackend {
    #[allow(dead_code)]
    sentinel: Arc<()>,
}

impl ExecutionBackend for SentinelBackend {
    fn spawn(&self, _: swactor_engine::BoxTask) {}
    fn spawn_blocking(&self, _: swactor_engine::BoxWork) {}
    fn timer(&self, _: Duration) -> swactor_engine::BoxTimer {
        Box::pin(std::future::pending())
    }
    fn now(&self) -> swactor_engine::EngineInstant {
        swactor_engine::EngineInstant::now()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::TASKS_ONLY
    }
}

#[test]
fn dropping_engine_releases_backend_even_with_live_handles() {
    // The engine is the sole strong owner of its backend. EngineHandle and
    // interval state hold weak references, so the backend (and the runtime /
    // core-driver task it owns) is released once the engine drops — even while
    // handles remain alive (ENGINE_SPEC.md).
    let sentinel = Arc::new(());
    let parts = default_parts();
    let engine = Engine::new(
        parts,
        SentinelBackend {
            sentinel: sentinel.clone(),
        },
    )
    .expect("tasks capability present");
    let handle = engine.handle();
    let _handle_clone = handle.clone();

    // Two strong refs: the test's `sentinel` and the backend's clone.
    assert_eq!(Arc::strong_count(&sentinel), 2);

    drop(engine);
    // The engine was the sole strong backend owner; handles are weak, so the
    // backend (and its sentinel clone) is gone.
    assert_eq!(
        Arc::strong_count(&sentinel),
        1,
        "backend retained after the owning engine was dropped"
    );

    // Dropping the surviving handles changes nothing — they never held a strong
    // reference.
    drop(handle);
    drop(_handle_clone);
    assert_eq!(Arc::strong_count(&sentinel), 1);
}

#[test]
fn handle_used_after_engine_drop_degrades_gracefully() {
    // Behavior beyond the engine's lifetime is out of spec, but a handle must
    // not retain the backend and should degrade through the smallest practical
    // API rather than panic (ENGINE_SPEC.md).
    let parts = default_parts();
    let engine = Engine::new(parts, SteppingBackend::default()).unwrap();
    let handle = engine.handle();
    // Live handle reports the stepping backend's capabilities.
    assert!(handle.capabilities().tasks);
    assert!(handle.capabilities().timers);

    drop(engine);

    // Closed handle reports no capabilities and rejects every requirement.
    assert_eq!(handle.capabilities(), Capabilities::NONE);
    assert!(handle.require(Capabilities::TASKS_ONLY).is_err());

    // Time falls back to the wall clock without panicking.
    let _ = handle.now();

    // Scheduling work and creating primitives reject / no-op / never fire,
    // never panic, and never retain the backend.
    handle.spawn(async {});
    assert!(
        handle
            .blocking_work_sender()
            .submit(Box::new(|| {}))
            .is_err()
    );
    let _never_fires = handle.timer(Duration::from_secs(1));
    let _never_ticks = handle.interval(Duration::from_secs(1));
}
