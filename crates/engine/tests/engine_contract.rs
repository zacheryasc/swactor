#![cfg(feature = "tokio")]
//! Black-box contract tests for the swactor engine: baseline driving/tasks,
//! time, blocking, and non-reentrancy.
//!
//! These are ordinary synchronous `#[test]`s. They construct and own their
//! engine explicitly, never call `tick()`/`try_tick()`, never use
//! `#[tokio::test]`, and observe behavior through atomics and bounded channels
//! with finite deadlines. See `ENGINE_SPEC.md`.
//!
//! These tests exercise the native Tokio backend specifically; the
//! non-Tokio portability proof lives in `engine_unit.rs`.

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Duration;

use swactor_engine::{Engine, TokioBackend, TokioConfig};

/// Outer deadline shared across tests: generous enough to absorb scheduler
/// jitter, short enough that a hung test terminates.
const DEADLINE: Duration = Duration::from_secs(5);

// ── 7.1 ──────────────────────────────────────────────────────────────────────

#[test]
fn engine_runs_without_an_ambient_tokio_runtime() {
    // No outer Tokio runtime, no `#[tokio::test]`. The engine owns its runtime.
    let parts = default_parts();
    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let engine = Engine::new(parts, backend).expect("construct engine");
    let handle = engine.handle();

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    handle.spawn(async move {
        let _ = tx.send(());
    });

    rx.recv_timeout(DEADLINE)
        .expect("spawned work must signal without an ambient runtime");
}

// ── 7.2 ──────────────────────────────────────────────────────────────────────

#[test]
fn engine_drives_core_without_application_ticks() {
    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let addr = runtime
        .spawn(RecordingProbe {
            received: received.clone(),
        })
        .expect("spawn probe actor");

    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let _engine = Engine::new(parts, backend).expect("construct engine");

    // Deliver AFTER engine construction: a later tick must observe it.
    runtime.send_to(addr, Probe).expect("deliver probe message");

    assert!(
        wait_for(|| received.load(SeqCst) >= 1, DEADLINE),
        "actor must process a message without any application tick"
    );
}

// ── 7.3 ──────────────────────────────────────────────────────────────────────

#[test]
fn spawned_supporting_work_runs() {
    let parts = default_parts();
    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let engine = Engine::new(parts, backend).expect("construct engine");
    let handle = engine.handle();

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    handle.spawn(async move {
        let _ = tx.send(());
    });

    rx.recv_timeout(DEADLINE)
        .expect("opaque spawned task must signal");
}

// ── 7.4 ──────────────────────────────────────────────────────────────────────

#[test]
fn actor_ticks_and_supporting_work_both_progress() {
    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let addr = runtime
        .spawn(RecordingProbe {
            received: received.clone(),
        })
        .expect("spawn probe actor");

    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let engine = Engine::new(parts, backend).expect("construct engine");
    let handle = engine.handle();

    // Long-lived cooperative supporting work that yields between steps so it
    // stays active while the actor message is processed.
    let steps = Arc::new(AtomicUsize::new(0));
    let steps_for_task = steps.clone();
    handle.spawn(async move {
        for _ in 0..200 {
            steps_for_task.fetch_add(1, SeqCst);
            yield_once().await;
        }
    });

    // Deliver an actor message while the supporting work is still active.
    runtime.send_to(addr, Probe).expect("deliver probe message");

    assert!(
        wait_for(
            || steps.load(SeqCst) >= 200 && received.load(SeqCst) >= 1,
            DEADLINE,
        ),
        "both actor ticks and supporting work must progress"
    );
}

// ── 7.6 ──────────────────────────────────────────────────────────────────────

#[test]
fn runtime_ticks_are_never_concurrent() {
    let (parts, runtime) = default_runtime_parts();
    let entered = Arc::new(AtomicBool::new(false));
    let violations = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));
    let addr = runtime
        .spawn(ReentrancyGuardProbe {
            entered: entered.clone(),
            violations: violations.clone(),
            handled: handled.clone(),
        })
        .expect("spawn reentrancy probe");

    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let _engine = Engine::new(parts, backend).expect("construct engine");

    let sender = runtime.create_sender();
    const SENDERS: usize = 4;
    const PER_SENDER: usize = 250;
    const TOTAL: usize = SENDERS * PER_SENDER;

    // Many messages from multiple external threads, all concurrent with the
    // engine's single driving loop.
    let mut threads = Vec::new();
    for _ in 0..SENDERS {
        let sender = sender.clone();
        threads.push(std::thread::spawn(move || {
            for _ in 0..PER_SENDER {
                let _ = sender.send_to(addr, Probe);
            }
        }));
    }
    for t in threads {
        t.join().expect("sender thread panicked");
    }

    assert!(
        wait_for(|| handled.load(SeqCst) >= TOTAL, Duration::from_secs(10)),
        "all messages must be processed"
    );
    assert_eq!(
        violations.load(SeqCst),
        0,
        "detected a concurrent or reentrant tick"
    );
}

// ── 7.5 ──────────────────────────────────────────────────────────────────────

/// Releases a [`Barrier`](std::sync::Barrier) on drop so blocking test work can
/// finish even when an assertion fails before explicit cleanup.
struct BarrierRelease(Arc<std::sync::Barrier>);

impl Drop for BarrierRelease {
    fn drop(&mut self) {
        self.0.wait();
    }
}

#[test]
fn blocking_work_does_not_stop_actor_ticks() {
    // A blocking-capability test, not part of the baseline tasks-plus-time
    // contract. Configure a small async worker pool so passing cannot be an
    // accident of excessive worker count.
    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let addr = runtime
        .spawn(RecordingProbe {
            received: received.clone(),
        })
        .expect("spawn probe actor");

    let backend = TokioBackend::new(TokioConfig {
        worker_threads: 1,
        ..Default::default()
    })
    .expect("build tokio backend");
    let engine = Engine::new(parts, backend).expect("construct engine");
    let handle = engine.handle();

    // Blocking work that waits on a barrier; it stays stuck for the whole test
    // body. It runs on the blocking pool, not the single async worker, so actor
    // ticks must still progress (ENGINE_SPEC.md §8 progress independence).
    let barrier = Arc::new(std::sync::Barrier::new(2));
    // `_release` drops at scope end — even on panic — to release the blocking
    // task so the owned runtime shuts down deterministically.
    let _release = BarrierRelease(barrier.clone());
    let barrier_for_work = barrier.clone();
    assert!(
        handle
            .blocking_work_sender()
            .submit(Box::new(move || {
                barrier_for_work.wait();
            }))
            .is_ok(),
        "submit blocking work"
    );

    // Deliver an actor message while the blocking work remains blocked.
    runtime.send_to(addr, Probe).expect("deliver probe message");

    assert!(
        wait_for(|| received.load(SeqCst) >= 1, DEADLINE),
        "actor ticks must progress while blocking work is stuck"
    );
}

// ── 7.7 ──────────────────────────────────────────────────────────────────────

#[test]
fn engine_clock_is_monotonic() {
    let parts = default_parts();
    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let engine = Engine::new(parts, backend).expect("construct engine");
    let handle = engine.handle();

    let mut prev = handle.now();
    for _ in 0..10_000 {
        let cur = handle.now();
        assert!(cur >= prev, "engine clock moved backwards");
        prev = cur;
    }
}

// ── 7.8 ──────────────────────────────────────────────────────────────────────

#[test]
fn engine_timer_fires() {
    let parts = default_parts();
    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let engine = Engine::new(parts, backend).expect("construct engine");
    let handle = engine.handle();

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let timer_handle = handle.clone();
    handle.spawn(async move {
        timer_handle.timer(Duration::from_millis(20)).await;
        let _ = tx.send(());
    });

    rx.recv_timeout(DEADLINE).expect("engine timer must fire");
}

// ── 7.9 ──────────────────────────────────────────────────────────────────────

#[test]
fn engine_interval_recurs() {
    let parts = default_parts();
    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let engine = Engine::new(parts, backend).expect("construct engine");
    let handle = engine.handle();

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let interval_handle = handle.clone();
    handle.spawn(async move {
        // `Interval` re-arms on each `Ready`, so awaiting it repeatedly yields
        // one ready per period. `Box::pin` lets us poll it in a loop.
        let mut interval = Box::pin(interval_handle.interval(Duration::from_millis(5)));
        for _ in 0..3 {
            interval.as_mut().await;
        }
        let _ = tx.send(());
    });

    rx.recv_timeout(DEADLINE)
        .expect("interval must recur several times");
}

// ── 7.10 ─────────────────────────────────────────────────────────────────────

#[test]
fn engine_timer_can_be_created_off_runtime() {
    // ENGINE_SPEC.md §7: creating an engine timer
    // must not require the caller to enter or possess the raw substrate runtime.
    // Construct the timer directly in the test body — no spawned task, no ambient
    // runtime — then await it on an engine task. If the tokio backend's timer
    // needed runtime context at construction, this would panic.
    let parts = default_parts();
    let backend = TokioBackend::new(TokioConfig::default()).expect("build tokio backend");
    let engine = Engine::new(parts, backend).expect("construct engine");
    let handle = engine.handle();

    // Constructed off-runtime: must not panic.
    let timer = handle.timer(Duration::from_millis(10));

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    handle.spawn(async move {
        timer.await;
        let _ = tx.send(());
    });

    rx.recv_timeout(DEADLINE)
        .expect("off-runtime-constructed timer must fire when awaited on a task");
}

// ── 7.11 ─────────────────────────────────────────────────────────────────────

// This test exercises `TokioBackend::from_runtime`, so it must build a real
// Tokio runtime to hand the engine — the one test-only use of the substrate
// constructor (ENGINE_SPEC.md §2).
#[test]
fn engine_adopts_caller_tuned_tokio_runtime() {
    // ENGINE_SPEC.md §9: the native engine supports
    // consuming an explicitly tuned Tokio runtime rather than always building
    // its own. Build a runtime with a non-default worker count, transfer it,
    // and confirm the engine still drives core and reports full capabilities.
    let tuned = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .expect("build tuned tokio runtime");
    let backend = TokioBackend::from_runtime(tuned);

    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let addr = runtime
        .spawn(RecordingProbe {
            received: received.clone(),
        })
        .expect("spawn probe actor");

    let engine = Engine::new(parts, backend).expect("construct engine");
    // The adopted substrate still exposes every native capability (§9).
    assert_eq!(
        engine.handle().capabilities(),
        swactor_engine::Capabilities::ALL
    );

    runtime.send_to(addr, Probe).expect("deliver probe");

    assert!(
        wait_for(|| received.load(SeqCst) >= 1, DEADLINE),
        "engine must drive core through an adopted runtime"
    );
}

// ── 7.12 ──────────────────────────────────────────────────────────────────────

#[test]
fn idle_core_observes_late_external_work_within_idle_interval() {
    // A core driver parks on a timer once its worker goes idle. Work can then
    // arrive through paths the engine cannot see (an external send from this
    // thread). The parked driver must wake within its idle interval and tick.
    // A driver that parks and never re-arms would hang this test.
    let (parts, runtime) = default_runtime_parts();
    let received = Arc::new(AtomicUsize::new(0));
    let addr = runtime
        .spawn(RecordingProbe {
            received: received.clone(),
        })
        .expect("spawn probe actor");

    let idle_poll = Duration::from_millis(50);
    let backend = TokioBackend::new(TokioConfig {
        core_idle_poll: idle_poll,
        ..Default::default()
    })
    .expect("build tokio backend");
    let _engine = Engine::new(parts, backend).expect("construct engine");

    // First message proves the driver ran before parking.
    runtime.send_to(addr, Probe).expect("deliver first probe");
    assert!(
        wait_for(|| received.load(SeqCst) >= 1, DEADLINE),
        "driver must process work before going idle"
    );
    // Long enough for every worker driver to observe no work and park.
    std::thread::sleep(Duration::from_millis(300));

    // External send from outside the engine: invisible to any engine-side
    // wake path, observed only by the parked driver's timer.
    runtime.send_to(addr, Probe).expect("deliver late probe");

    let start = std::time::Instant::now();
    assert!(
        wait_for(|| received.load(SeqCst) >= 2, DEADLINE),
        "a parked core driver must still observe external work"
    );
    // The idle timer is free-running (armed once per idle transition, re-armed
    // on each firing), so the send lands at a random phase within one
    // interval: observed latency is uniform in [0, idle_poll). Bound it
    // generously to absorb scheduler jitter without asserting the phase.
    let elapsed = start.elapsed();
    assert!(
        elapsed < idle_poll * 10,
        "late work observed in {elapsed:?}, far beyond the idle interval"
    );
}

#[test]
fn tokio_backend_reports_configured_core_idle_poll() {
    use swactor_engine::ExecutionBackend;

    let configured = Duration::from_millis(5);
    let backend = TokioBackend::new(TokioConfig {
        core_idle_poll: configured,
        ..Default::default()
    })
    .expect("build tokio backend");
    assert_eq!(
        backend.core_idle_poll(),
        configured,
        "backend must report its configured idle interval"
    );
}
