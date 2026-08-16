//! Shared probe actors and bounded-wait helpers for the engine contract tests.
//!
//! Imports only public `swactor` APIs and exposes no private engine state. See
//! `ENGINE_SPEC.md`.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use swactor::actor::{ActorInterface, Ctx};
use swactor::runtime::{Runtime, RuntimeConfig, RuntimeParts};

pub fn runtime_parts(config: RuntimeConfig) -> (RuntimeParts, Runtime) {
    let parts = RuntimeParts::new(config);
    let runtime = parts.runtime().clone();
    (parts, runtime)
}

pub fn default_runtime_parts() -> (RuntimeParts, Runtime) {
    runtime_parts(RuntimeConfig::default())
}
pub fn runtime_parts_with_workers(worker_count: usize) -> (RuntimeParts, Runtime) {
    let mut config = RuntimeConfig::default();
    config.worker_count = worker_count;
    runtime_parts(config)
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
