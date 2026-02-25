use std::any::Any;

use crate::actor::{ActorAddress, Message};
use crate::extension::WorkerExtension;

// ─── Cloneable Message Trait ────────────────────────────────────────────────

/// Type-erased cloneable message for interval timers.
/// Since `Message: Clone`, all actor messages implement this.
pub(crate) trait CloneMsg: Send {
    fn clone_boxed(&self) -> Box<dyn Any + Send>;
}

impl<M: Message> CloneMsg for M {
    fn clone_boxed(&self) -> Box<dyn Any + Send> {
        Box::new(self.clone())
    }
}

// ─── Timer Request ──────────────────────────────────────────────────────────

/// Timer request from a handler, queued for processing after tick_all.
pub(crate) enum TimerRequest {
    /// One-shot: deliver `msg` to `dest` after `ticks` worker ticks.
    Once {
        dest: ActorAddress,
        msg: Box<dyn Any + Send>,
        ticks: u64,
    },
    /// Repeating: deliver a clone of `msg` to `dest` every `period` ticks.
    Interval {
        dest: ActorAddress,
        msg: Box<dyn CloneMsg>,
        period: u64,
    },
}

// ─── Timer Wheel ────────────────────────────────────────────────────────────

struct OnceTimer {
    fire_at: u64,
    dest: ActorAddress,
    msg: Box<dyn Any + Send>,
}

struct IntervalTimer {
    next_fire: u64,
    period: u64,
    dest: ActorAddress,
    msg: Box<dyn CloneMsg>,
}

/// Per-worker tick-counting timer wheel.
///
/// Timers are deterministic (tick-counted, not wall-clock). One-shot timers
/// fire once and are consumed; interval timers fire repeatedly every N ticks.
pub struct TimerWheel {
    current_tick: u64,
    once_timers: Vec<OnceTimer>,
    interval_timers: Vec<IntervalTimer>,
}

impl TimerWheel {
    pub fn new() -> Self {
        Self {
            current_tick: 0,
            once_timers: Vec::new(),
            interval_timers: Vec::new(),
        }
    }

    /// Advance the tick counter and collect all due timer messages.
    fn fire(&mut self) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        self.current_tick += 1;
        let tick = self.current_tick;
        let mut result = Vec::new();

        // Fire one-shot timers (swap-remove for O(1) removal)
        let mut i = 0;
        while i < self.once_timers.len() {
            if self.once_timers[i].fire_at <= tick {
                let timer = self.once_timers.swap_remove(i);
                result.push((timer.dest, timer.msg));
            } else {
                i += 1;
            }
        }

        // Fire interval timers
        for timer in &mut self.interval_timers {
            if timer.next_fire <= tick {
                let msg = timer.msg.clone_boxed();
                result.push((timer.dest, msg));
                timer.next_fire = tick + timer.period;
            }
        }

        result
    }

    /// Remove interval timers whose target was just removed from the worker.
    fn gc_dead_intervals(&mut self, dead: &[ActorAddress]) {
        if dead.is_empty() {
            return;
        }
        self.interval_timers
            .retain(|t| !dead.contains(&t.dest));
    }

    fn add_once(&mut self, dest: ActorAddress, msg: Box<dyn Any + Send>, ticks: u64) {
        self.once_timers.push(OnceTimer {
            fire_at: self.current_tick + ticks,
            dest,
            msg,
        });
    }

    fn add_interval(&mut self, dest: ActorAddress, msg: Box<dyn CloneMsg>, period: u64) {
        let period = period.max(1); // prevent zero-period infinite loop
        self.interval_timers.push(IntervalTimer {
            next_fire: self.current_tick + period,
            period,
            dest,
            msg,
        });
    }
}

impl WorkerExtension for TimerWheel {
    fn has_pending_work(&self) -> bool {
        !self.once_timers.is_empty() || !self.interval_timers.is_empty()
    }

    fn on_tick(&mut self) -> Vec<(ActorAddress, Box<dyn Any + Send>)> {
        if self.once_timers.is_empty() && self.interval_timers.is_empty() {
            return Vec::new();
        }
        self.fire()
    }

    fn handle_request(&mut self, request: Box<dyn Any + Send>) {
        if let Ok(req) = request.downcast::<TimerRequest>() {
            match *req {
                TimerRequest::Once { dest, msg, ticks } => self.add_once(dest, msg, ticks),
                TimerRequest::Interval { dest, msg, period } => {
                    self.add_interval(dest, msg, period)
                }
            }
        }
    }

    fn gc_dead(&mut self, dead: &[ActorAddress]) {
        self.gc_dead_intervals(dead);
    }
}
