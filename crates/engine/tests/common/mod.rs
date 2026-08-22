//! Shared probe actors and bounded-wait helpers for the engine contract tests.
//!
//! Imports only public `swactor` APIs and exposes no private engine state. See
//! `ENGINE_SPEC.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use swactor::actor::{ActorInterface, Ctx};
use swactor::runtime::{Runtime, RuntimeConfig, RuntimeParts};
use swactor_engine::SteppingBackend;

pub fn runtime_parts(config: RuntimeConfig) -> (RuntimeParts, Runtime) {
    let parts = RuntimeParts::new(config);
    let runtime = parts.runtime().clone();
    (parts, runtime)
}

pub fn default_runtime_parts() -> (RuntimeParts, Runtime) {
    runtime_parts(RuntimeConfig::default())
}
pub fn runtime_parts_with_workers(worker_count: usize) -> (RuntimeParts, Runtime) {
    runtime_parts(RuntimeConfig {
        worker_count,
        ..RuntimeConfig::default()
    })
}

pub fn default_parts() -> RuntimeParts {
    RuntimeParts::new(RuntimeConfig::default())
}

// ── Probe message ───────────────────────────────────────────────────────────

/// A minimal message delivered to probe actors.
#[derive(Clone, Debug)]
pub struct Probe;

// ── Probe actors ────────────────────────────────────────────────────────────

/// Records how many messages it has received into a shared counter.
pub struct RecordingProbe {
    pub received: Arc<AtomicUsize>,
}

impl ActorInterface for RecordingProbe {
    type Incoming = Probe;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Probe) {
        self.received.fetch_add(1, Ordering::SeqCst);
    }
}

/// Detects concurrent or reentrant handler entry. A violation is recorded if
/// `handle` is entered while a previous invocation is still in flight — which
/// can only happen if two ticks run the same worker concurrently.
pub struct ReentrancyGuardProbe {
    pub entered: Arc<AtomicBool>,
    pub violations: Arc<AtomicUsize>,
    pub handled: Arc<AtomicUsize>,
}

impl ActorInterface for ReentrancyGuardProbe {
    type Incoming = Probe;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Probe) {
        if self.entered.swap(true, Ordering::SeqCst) {
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
        self.handled.fetch_add(1, Ordering::SeqCst);
        self.entered.store(false, Ordering::SeqCst);
    }
}

// ── Wait helpers ────────────────────────────────────────────────────────────

/// Block until `cond` holds, polling every 2 ms up to `timeout`. Returns the
/// final value of `cond` (true on success).
pub fn wait_for<F: Fn() -> bool>(cond: F, timeout: Duration) -> bool {
    const POLL: Duration = Duration::from_millis(2);
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return cond();
        }
        std::thread::sleep(POLL);
    }
}

/// A substrate-agnostic cooperative yield: suspend the current task for one
/// scheduler turn (giving other engine work — including the core driver — a
/// chance to run), then resume. Uses only `std`, so it works on any substrate
/// without coupling the test to Tokio.
pub async fn yield_once() {
    // The closure is stored inside `poll_fn`'s future and polled via `&mut`,
    // so its captured `yielded` flag persists across polls: the first poll
    // reschedules and suspends, the next poll resumes.
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

pub fn drive_steps(backend: &SteppingBackend, count: usize) {
    for _ in 0..count {
        backend.step();
    }
}

pub fn advance_and_drive(backend: &SteppingBackend, duration: Duration, count: usize) {
    backend.advance_time(duration);
    drive_steps(backend, count);
}

pub fn assert_no_poison(runtime: &Runtime) {
    let stats = runtime.stats();
    let panics = stats
        .workers
        .iter()
        .map(|worker| worker.panics)
        .sum::<u64>();
    let poisoned = stats
        .actor_details
        .iter()
        .filter(|actor| actor.poisoned)
        .collect::<Vec<_>>();
    assert!(
        panics == 0 && poisoned.is_empty(),
        "runtime contains poisoned actors: worker_panics={panics}, poisoned={poisoned:?}, \
         actors={:?}, details={:?}",
        stats.actors,
        stats.actor_details,
    );
}

pub fn assert_actor_delta_at_most(runtime: &Runtime, baseline: usize, limit: usize) {
    let stats = runtime.stats();
    assert!(
        stats.actors.len() <= baseline.saturating_add(limit),
        "actor count grew from {baseline} to {}, limit={limit}: {:?}",
        stats.actors.len(),
        stats.actors,
    );
}

pub fn assert_mailboxes_drained(runtime: &Runtime) {
    let stats = runtime.stats();
    let mailbox_depth = stats
        .workers
        .iter()
        .map(|worker| worker.mailbox_depth)
        .sum::<usize>();
    assert_eq!(
        mailbox_depth, 0,
        "mailboxes did not drain: workers={:?}, details={:?}",
        stats.workers, stats.actor_details,
    );
}
