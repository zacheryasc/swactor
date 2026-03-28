//! Property-based tests for G5: Fault Isolation.
//!
//! An actor panic must never affect any other actor. Sibling actors must
//! continue to process messages and complete their lifecycle normally.

use proptest::prelude::*;

use crate::actor::{ActorAddress, ActorInterface};
use crate::config::RuntimeConfig;
use crate::runtime::{Ctx, Runtime};

// ─── Message Types ──────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Count(u64);

#[derive(Clone, Debug)]
struct Started(ActorAddress);

#[derive(Clone, Debug)]
struct Stopped(ActorAddress);

// ─── Actor Types ────────────────────────────────────────────────────────────

/// Counts messages received, sends final count to inbox on stop.
struct CountingActor {
    count: u64,
    report_to: ActorAddress,
}

impl ActorInterface for CountingActor {
    type Incoming = Count;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Count) {
        self.count += 1;
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.report_to, Count(self.count));
    }
}

/// Panics after receiving exactly `panic_at` messages.
struct DelayedPanicActor {
    count: u64,
    panic_at: u64,
}

impl ActorInterface for DelayedPanicActor {
    type Incoming = Count;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Count) {
        self.count += 1;
        if self.count == self.panic_at {
            panic!("intentional panic at message {}", self.panic_at);
        }
    }
}

/// Panics in on_start.
struct StartPanicActor;

impl ActorInterface for StartPanicActor {
    type Incoming = Count;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Count) {}

    fn on_start(&mut self, _ctx: &Ctx) {
        panic!("intentional panic in on_start");
    }
}

/// Reports lifecycle events to an inbox so we can verify them externally.
struct LifecycleActor {
    start_report: ActorAddress,
    stop_report: ActorAddress,
    count: u64,
    count_report: ActorAddress,
}

impl ActorInterface for LifecycleActor {
    type Incoming = Count;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.start_report, Started(ctx.self_addr()));
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Count) {
        self.count += 1;
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.count_report, Count(self.count));
        let _ = ctx.send(self.stop_report, Stopped(ctx.self_addr()));
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn tick_many(rt: &Runtime, n: usize) {
    for _ in 0..n {
        rt.tick();
    }
}

// ─── Property Tests ─────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(80))]

    /// Spawn N healthy actors and 1 that panics at a random message index.
    /// Send `msg_count` messages to every actor. Assert all healthy actors
    /// receive exactly `msg_count` messages and call on_stop normally.
    #[test]
    fn panic_at_random_index_isolates(
        n in 2usize..=12,
        msg_count in 1u64..=200,
        panic_at in 1u64..=200,
    ) {
        let rt = Runtime::new(RuntimeConfig::default());
        let report_inbox = rt.new_inbox::<Count>().unwrap();
        let report_addr = *report_inbox.addr();

        // Spawn N healthy counting actors
        let mut healthy: Vec<ActorAddress> = Vec::with_capacity(n);
        for _ in 0..n {
            let addr = rt.spawn(CountingActor {
                count: 0,
                report_to: report_addr,
            }).unwrap();
            healthy.push(addr);
        }

        // Spawn the panicking actor (clamp panic_at to msg_count so it fires)
        let actual_panic_at = (panic_at % msg_count) + 1;
        let panic_addr = rt.spawn(DelayedPanicActor {
            count: 0,
            panic_at: actual_panic_at,
        }).unwrap();

        // Deliver on_start
        rt.tick();

        // Send msg_count messages to every actor (healthy + panicker)
        let all_addrs: Vec<ActorAddress> = healthy.iter()
            .copied()
            .chain(std::iter::once(panic_addr))
            .collect();

        for i in 0..msg_count {
            for &addr in &all_addrs {
                rt.send_to(addr, Count(i)).unwrap();
            }
        }

        // Tick enough to drain everything (budget=64 default)
        let ticks = (msg_count as usize / 64) + 10;
        tick_many(&rt, ticks);

        // Stop healthy actors so they report
        for &addr in &healthy {
            rt.stop_actor(addr).unwrap();
        }
        tick_many(&rt, 3);

        // Collect reports
        let mut reports = Vec::new();
        while let Some(msg) = report_inbox.try_recv() {
            reports.push(msg.0);
        }

        // Every healthy actor must have processed exactly msg_count messages
        prop_assert_eq!(
            reports.len(), n,
            "expected {} reports, got {}", n, reports.len()
        );
        for (i, &count) in reports.iter().enumerate() {
            prop_assert_eq!(
                count, msg_count,
                "actor {} processed {} messages, expected {}",
                i, count, msg_count
            );
        }
    }

    /// An actor that panics in on_start must not affect siblings spawned
    /// before or after it.
    #[test]
    fn on_start_panic_isolates_siblings(
        before_count in 1usize..=8,
        after_count in 1usize..=8,
        msgs_each in 1u64..=100,
    ) {
        let rt = Runtime::new(RuntimeConfig::default());
        let start_inbox = rt.new_inbox::<Started>().unwrap();
        let stop_inbox = rt.new_inbox::<Stopped>().unwrap();
        let count_inbox = rt.new_inbox::<Count>().unwrap();

        let start_addr = *start_inbox.addr();
        let stop_addr = *stop_inbox.addr();
        let count_addr = *count_inbox.addr();

        // Spawn "before" siblings
        let mut before_addrs = Vec::new();
        for _ in 0..before_count {
            let addr = rt.spawn(LifecycleActor {
                start_report: start_addr,
                stop_report: stop_addr,
                count: 0,
                count_report: count_addr,
            }).unwrap();
            before_addrs.push(addr);
        }

        // Spawn the on_start panicker
        let _panic_addr = rt.spawn(StartPanicActor).unwrap();

        // Spawn "after" siblings
        let mut after_addrs = Vec::new();
        for _ in 0..after_count {
            let addr = rt.spawn(LifecycleActor {
                start_report: start_addr,
                stop_report: stop_addr,
                count: 0,
                count_report: count_addr,
            }).unwrap();
            after_addrs.push(addr);
        }

        // Tick to run on_start for everyone
        tick_many(&rt, 3);

        // Verify all siblings started successfully
        let mut started = Vec::new();
        while let Some(Started(addr)) = start_inbox.try_recv() {
            started.push(addr);
        }
        let total_siblings = before_count + after_count;
        prop_assert_eq!(
            started.len(), total_siblings,
            "expected {} on_start reports, got {}", total_siblings, started.len()
        );

        // Send messages to all siblings
        let all_siblings: Vec<ActorAddress> = before_addrs.iter()
            .chain(after_addrs.iter())
            .copied()
            .collect();
        for i in 0..msgs_each {
            for &addr in &all_siblings {
                rt.send_to(addr, Count(i)).unwrap();
            }
        }

        let ticks = (msgs_each as usize / 64) + 5;
        tick_many(&rt, ticks);

        // Stop all siblings
        for &addr in &all_siblings {
            rt.stop_actor(addr).unwrap();
        }
        tick_many(&rt, 3);

        // Check stop reports
        let mut stopped = Vec::new();
        while let Some(Stopped(addr)) = stop_inbox.try_recv() {
            stopped.push(addr);
        }
        prop_assert_eq!(
            stopped.len(), total_siblings,
            "expected {} on_stop reports, got {}", total_siblings, stopped.len()
        );

        // Check message counts
        let mut counts = Vec::new();
        while let Some(Count(c)) = count_inbox.try_recv() {
            counts.push(c);
        }
        prop_assert_eq!(counts.len(), total_siblings);
        for (i, &c) in counts.iter().enumerate() {
            prop_assert_eq!(
                c, msgs_each,
                "sibling {} processed {} messages, expected {}", i, c, msgs_each
            );
        }
    }

    /// Multiple actors panic in the same tick. Non-panicking actors must be
    /// completely unaffected.
    #[test]
    fn multiple_panics_same_tick_isolates(
        healthy_count in 2usize..=10,
        panic_count in 2usize..=6,
        msgs_each in 1u64..=150,
    ) {
        let rt = Runtime::new(RuntimeConfig::default());
        let report_inbox = rt.new_inbox::<Count>().unwrap();
        let report_addr = *report_inbox.addr();

        // Spawn healthy actors
        let mut healthy = Vec::new();
        for _ in 0..healthy_count {
            let addr = rt.spawn(CountingActor {
                count: 0,
                report_to: report_addr,
            }).unwrap();
            healthy.push(addr);
        }

        // Spawn panicking actors — they all panic on message 1
        let mut panickers = Vec::new();
        for _ in 0..panic_count {
            let addr = rt.spawn(DelayedPanicActor {
                count: 0,
                panic_at: 1,
            }).unwrap();
            panickers.push(addr);
        }

        // Tick to process on_start
        rt.tick();

        // Send messages to everyone — panickers get at least 1 so they
        // all panic during the same tick
        let all: Vec<ActorAddress> = healthy.iter()
            .chain(panickers.iter())
            .copied()
            .collect();
        for i in 0..msgs_each {
            for &addr in &all {
                rt.send_to(addr, Count(i)).unwrap();
            }
        }

        // Tick enough to drain all messages
        let ticks = (msgs_each as usize / 64) + 10;
        tick_many(&rt, ticks);

        // Stop healthy actors to trigger on_stop reports
        for &addr in &healthy {
            rt.stop_actor(addr).unwrap();
        }
        tick_many(&rt, 3);

        // Every healthy actor must have processed all messages
        let mut reports = Vec::new();
        while let Some(Count(c)) = report_inbox.try_recv() {
            reports.push(c);
        }
        prop_assert_eq!(
            reports.len(), healthy_count,
            "expected {} reports, got {}", healthy_count, reports.len()
        );
        for (i, &c) in reports.iter().enumerate() {
            prop_assert_eq!(
                c, msgs_each,
                "healthy actor {} processed {} messages, expected {}",
                i, c, msgs_each
            );
        }
    }
}
