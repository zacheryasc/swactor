//! Unit tests for engine logic — capability binding, time semantics, and the
//! non-Tokio portability proof.
//!
//! Deliberately separate from `engine_contract.rs` (the behavioral contract).
//! These tests exercise internal logic directly and use the [`SteppingBackend`]
//! to prove substrate independence without Tokio (ENGINE_SPEC.md).

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;


use swactor_engine::{
    Capabilities, Engine, EngineError, ExecutionBackend, SteppingBackend,
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
    let counters: Vec<_> = (0..3)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
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

    assert!(
        steps.load(SeqCst) >= 10,
        "supporting work must finish"
    );
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
fn stepping_blocking_work_runs_isolated() {
    let parts = default_parts();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).expect("construct engine");
    let handle = engine.handle();

    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();
    handle.spawn_blocking(move || {
        std::thread::sleep(Duration::from_millis(10));
        done_clone.store(true, SeqCst);
    });

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
        SentinelBackend { sentinel: sentinel.clone() },
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

    // Scheduling work and creating primitives are no-ops / never fire, never
    // panic, and never retain the backend.
    handle.spawn(async {});
    handle.spawn_blocking(|| {});
    let _never_fires = handle.timer(Duration::from_secs(1));
    let _never_ticks = handle.interval(Duration::from_secs(1));
}