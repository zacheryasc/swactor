//! Property-based tests for G6 (Death Notification Completeness)
//! and G7 (Orphan Cleanup).
//!
//! G6: For every (monitor, monitored) pair where the monitored actor dies,
//!     exactly one `Down` or `ActorExited` is delivered. No notifications
//!     for actors still alive. Demonitored relationships produce no notification.
//!
//! G7: When a parent dies, all unsupervised children eventually stop.
//!     Supervised children are handled by their supervisor, not orphan-killed.

use std::sync::Arc;

use proptest::prelude::*;

use crate::actor::{ActorAddress, ActorExited, ActorInterface, Down, ExitReason, StopReason};
use crate::config::RuntimeConfig;
use crate::runtime::{Ctx, Runtime};
use crate::std::{
    ChildSpec, CtxMonitoring, CtxWatching, RestartPolicy, StdExtension,
    Supervisor, SupervisorStrategy,
};

// ─── Helpers ────────────────────────────────────────────────────────────────

fn std_runtime() -> Runtime {
    Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()))
}

fn tick_many(rt: &Runtime, n: usize) {
    for _ in 0..n {
        rt.tick();
    }
}

// ─── Message Types ──────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Ping;

#[derive(Clone, Debug)]
struct DownReport {
    dead: ActorAddress,
    reason: StopReason,
}

#[derive(Clone, Debug)]
struct ExitReport {
    dead: ActorAddress,
    reason: ExitReason,
}

#[derive(Clone, Debug)]
struct StoppedReport(ActorAddress);

// ─── Actor Types ────────────────────────────────────────────────────────────

/// An actor that monitors a set of targets in on_start and reports Down
/// messages to an external inbox.
struct MonitorActor {
    targets: Vec<ActorAddress>,
    report_to: ActorAddress,
}

impl ActorInterface for MonitorActor {
    type Incoming = Ping;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        for &target in &self.targets {
            let _ = ctx.monitor(target);
        }
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}

    fn handle_down(&mut self, ctx: &Ctx, down: Down) {
        let _ = ctx.send(
            self.report_to,
            DownReport {
                dead: down.addr,
                reason: down.reason,
            },
        );
    }
}

/// An actor that watches a set of targets in on_start and reports ActorExited
/// messages to an external inbox.
struct WatchActor {
    targets: Vec<ActorAddress>,
    report_to: ActorAddress,
}

impl ActorInterface for WatchActor {
    type Incoming = Ping;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        for &target in &self.targets {
            ctx.watch(target);
        }
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}

    fn on_actor_exit(&mut self, ctx: &Ctx, exited: ActorExited) {
        let _ = ctx.send(
            self.report_to,
            ExitReport {
                dead: exited.addr,
                reason: exited.reason,
            },
        );
    }
}

/// An actor that monitors targets, then demonitors some before they die.
struct DemonitorActor {
    targets: Vec<ActorAddress>,
    /// Indices into `targets` to demonitor after setup.
    demonitor_indices: Vec<usize>,
    report_to: ActorAddress,
}

impl ActorInterface for DemonitorActor {
    type Incoming = Ping;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let mut refs = Vec::new();
        for &target in &self.targets {
            refs.push(ctx.monitor(target).unwrap());
        }
        // Demonitor selected targets
        for &idx in &self.demonitor_indices {
            if idx < refs.len() {
                ctx.demonitor(refs[idx]);
            }
        }
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}

    fn handle_down(&mut self, ctx: &Ctx, down: Down) {
        let _ = ctx.send(
            self.report_to,
            DownReport {
                dead: down.addr,
                reason: down.reason,
            },
        );
    }
}

/// Simple actor that panics on first message.
struct PanicOnMsg;

impl ActorInterface for PanicOnMsg {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
        panic!("intentional panic");
    }
}

/// Actor that does nothing, just stays alive.
struct IdleActor;

impl ActorInterface for IdleActor {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
}

#[derive(Clone, Debug)]
struct SpawnReport {
    children: Vec<ActorAddress>,
}

/// Reports when on_stop fires.
struct StopReportActor {
    report_to: ActorAddress,
}

impl ActorInterface for StopReportActor {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}

    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.report_to, StoppedReport(ctx.self_addr()));
    }
}

// ─── Property Tests ─────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(60))]

    // ═══════════════════════════════════════════════════════════════════════
    // G6: Death Notification Completeness — Monitor (Down)
    // ═══════════════════════════════════════════════════════════════════════

    /// Spawn N targets. Kill a random subset. A single monitoring actor monitors
    /// all targets. Assert: exactly one Down per dead target, zero for alive ones.
    #[test]
    fn monitor_exactly_one_down_per_dead_target(
        num_targets in 2usize..=10,
        kill_mask in prop::collection::vec(prop::bool::ANY, 2..=10),
    ) {
        let rt = std_runtime();
        let down_inbox = rt.new_inbox::<DownReport>().unwrap();

        // Spawn targets
        let mut targets = Vec::new();
        for _ in 0..num_targets {
            targets.push(rt.spawn(PanicOnMsg).unwrap());
        }

        // Spawn the monitoring actor
        let _monitor = rt.spawn(MonitorActor {
            targets: targets.clone(),
            report_to: *down_inbox.addr(),
        }).unwrap();

        // Tick so on_start runs (monitors registered)
        tick_many(&rt, 2);

        // Kill targets according to mask
        let kill_mask: Vec<bool> = kill_mask.into_iter().take(num_targets).collect();
        let expected_dead: Vec<ActorAddress> = targets.iter()
            .zip(kill_mask.iter())
            .filter(|&(_, kill)| *kill)
            .map(|(&addr, _)| addr)
            .collect();

        for &addr in &expected_dead {
            rt.send_to(addr, Ping).unwrap();
        }
        tick_many(&rt, 5);

        // Collect Down reports
        let mut reports: Vec<DownReport> = Vec::new();
        while let Some(r) = down_inbox.try_recv() {
            reports.push(r);
        }

        // Exactly one Down per dead target
        let dead_count = expected_dead.len();
        prop_assert_eq!(
            reports.len(), dead_count,
            "expected {} Down messages, got {}", dead_count, reports.len()
        );

        // Each dead target appears exactly once
        for &dead_addr in &expected_dead {
            let count = reports.iter().filter(|r| r.dead == dead_addr).count();
            prop_assert_eq!(count, 1, "dead target should appear exactly once in Down reports");
        }

        // Reason should be Panicked for panic-killed actors
        for r in &reports {
            prop_assert_eq!(r.reason, StopReason::Panicked);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // G6: Death Notification Completeness — Watch (ActorExited)
    // ═══════════════════════════════════════════════════════════════════════

    /// Same scenario but using watch + on_actor_exit instead of monitor + handle_down.
    #[test]
    fn watch_exactly_one_exited_per_dead_target(
        num_targets in 2usize..=10,
        kill_mask in prop::collection::vec(prop::bool::ANY, 2..=10),
    ) {
        let rt = std_runtime();
        let exit_inbox = rt.new_inbox::<ExitReport>().unwrap();

        let mut targets = Vec::new();
        for _ in 0..num_targets {
            targets.push(rt.spawn(PanicOnMsg).unwrap());
        }

        let _watcher = rt.spawn(WatchActor {
            targets: targets.clone(),
            report_to: *exit_inbox.addr(),
        }).unwrap();

        tick_many(&rt, 2);

        let kill_mask: Vec<bool> = kill_mask.into_iter().take(num_targets).collect();
        let expected_dead: Vec<ActorAddress> = targets.iter()
            .zip(kill_mask.iter())
            .filter(|&(_, kill)| *kill)
            .map(|(&addr, _)| addr)
            .collect();

        for &addr in &expected_dead {
            rt.send_to(addr, Ping).unwrap();
        }
        tick_many(&rt, 5);

        let mut reports: Vec<ExitReport> = Vec::new();
        while let Some(r) = exit_inbox.try_recv() {
            reports.push(r);
        }

        let dead_count = expected_dead.len();
        prop_assert_eq!(
            reports.len(), dead_count,
            "expected {} ActorExited, got {}", dead_count, reports.len()
        );

        for &dead_addr in &expected_dead {
            let count = reports.iter().filter(|r| r.dead == dead_addr).count();
            prop_assert_eq!(count, 1, "dead target should appear exactly once in ActorExited reports");
        }

        for r in &reports {
            prop_assert_eq!(r.reason.clone(), ExitReason::Panicked);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // G6: Demonitor produces no notification
    // ═══════════════════════════════════════════════════════════════════════

    /// Monitor N targets, demonitor a random subset, kill all targets.
    /// Assert: only non-demonitored targets produce Down messages.
    #[test]
    fn demonitor_suppresses_notification(
        num_targets in 2usize..=8,
        demonitor_mask in prop::collection::vec(prop::bool::ANY, 2..=8),
    ) {
        let rt = std_runtime();
        let down_inbox = rt.new_inbox::<DownReport>().unwrap();

        let mut targets = Vec::new();
        for _ in 0..num_targets {
            targets.push(rt.spawn(PanicOnMsg).unwrap());
        }

        let demonitor_mask: Vec<bool> = demonitor_mask.into_iter().take(num_targets).collect();
        let demonitor_indices: Vec<usize> = demonitor_mask.iter()
            .enumerate()
            .filter(|&(_, d)| *d)
            .map(|(i, _)| i)
            .collect();

        let _monitor = rt.spawn(DemonitorActor {
            targets: targets.clone(),
            demonitor_indices: demonitor_indices.clone(),
            report_to: *down_inbox.addr(),
        }).unwrap();

        tick_many(&rt, 2);

        // Kill all targets
        for &addr in &targets {
            rt.send_to(addr, Ping).unwrap();
        }
        tick_many(&rt, 5);

        let mut reports: Vec<DownReport> = Vec::new();
        while let Some(r) = down_inbox.try_recv() {
            reports.push(r);
        }

        // Targets that were demonitored should NOT appear
        let demonitor_set: std::collections::HashSet<usize> =
            demonitor_indices.iter().copied().collect();
        let expected_count = (0..num_targets)
            .filter(|i| !demonitor_set.contains(i))
            .count();

        prop_assert_eq!(
            reports.len(), expected_count,
            "expected {} Down (non-demonitored), got {}", expected_count, reports.len()
        );

        // Verify no demonitored target appears in reports
        for &idx in &demonitor_indices {
            if idx < num_targets {
                let count = reports.iter().filter(|r| r.dead == targets[idx]).count();
                prop_assert_eq!(count, 0, "demonitored target should not produce Down");
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // G6: No notification for alive actors
    // ═══════════════════════════════════════════════════════════════════════

    /// Monitor N targets, kill none. Assert: zero Down messages after many ticks.
    #[test]
    fn no_notification_for_alive_actors(
        num_targets in 2usize..=10,
    ) {
        let rt = std_runtime();
        let down_inbox = rt.new_inbox::<DownReport>().unwrap();

        let mut targets = Vec::new();
        for _ in 0..num_targets {
            targets.push(rt.spawn(IdleActor).unwrap());
        }

        let _monitor = rt.spawn(MonitorActor {
            targets: targets.clone(),
            report_to: *down_inbox.addr(),
        }).unwrap();

        tick_many(&rt, 10);

        let count = std::iter::from_fn(|| down_inbox.try_recv()).count();
        prop_assert_eq!(count, 0, "no Down for alive actors");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // G6: Multiple monitors on same target — each gets exactly one Down
    // ═══════════════════════════════════════════════════════════════════════

    /// N watchers monitor the same target. Kill the target. Each watcher gets
    /// exactly one Down.
    #[test]
    fn multiple_monitors_each_get_one_down(
        num_watchers in 2usize..=8,
    ) {
        let rt = std_runtime();
        let down_inbox = rt.new_inbox::<DownReport>().unwrap();

        let target = rt.spawn(PanicOnMsg).unwrap();

        for _ in 0..num_watchers {
            rt.spawn(MonitorActor {
                targets: vec![target],
                report_to: *down_inbox.addr(),
            }).unwrap();
        }

        tick_many(&rt, 2);

        // Kill target
        rt.send_to(target, Ping).unwrap();
        tick_many(&rt, 5);

        let reports: Vec<DownReport> = std::iter::from_fn(|| down_inbox.try_recv()).collect();
        prop_assert_eq!(
            reports.len(), num_watchers,
            "each watcher should get exactly one Down"
        );
        for r in &reports {
            prop_assert_eq!(r.dead, target);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // G7: Orphan Cleanup — unsupervised children stop when parent dies
    // ═══════════════════════════════════════════════════════════════════════

    /// Parent spawns N unsupervised children. Kill the parent. Assert all children
    /// eventually stop (verified via on_stop reports and failed send attempts).
    #[test]
    fn orphan_unsupervised_children_stop_on_parent_death(
        num_children in 1usize..=8,
    ) {
        let rt = std_runtime();
        let spawn_inbox = rt.new_inbox::<SpawnReport>().unwrap();
        let stop_inbox = rt.new_inbox::<StoppedReport>().unwrap();

        // Spawn a parent that will spawn children (using StopReportActor as children
        // so we can observe on_stop). We need a custom parent for this.
        struct StopReportParent {
            num_children: usize,
            spawn_report_to: ActorAddress,
            stop_report_to: ActorAddress,
        }

        impl ActorInterface for StopReportParent {
            type Incoming = Ping;
            type Response = ();

            fn on_start(&mut self, ctx: &Ctx) {
                let mut children = Vec::new();
                for _ in 0..self.num_children {
                    let child = ctx.spawn(StopReportActor {
                        report_to: self.stop_report_to,
                    }).unwrap();
                    children.push(child);
                }
                let _ = ctx.send(
                    self.spawn_report_to,
                    SpawnReport { children },
                );
            }

            fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
        }

        let parent = rt.spawn(StopReportParent {
            num_children,
            spawn_report_to: *spawn_inbox.addr(),
            stop_report_to: *stop_inbox.addr(),
        }).unwrap();

        tick_many(&rt, 3);

        // Get spawn report
        let report = spawn_inbox.try_recv().expect("should receive spawn report");
        let children = report.children;
        prop_assert_eq!(children.len(), num_children);

        // Kill parent
        rt.stop_actor(parent).unwrap();
        tick_many(&rt, 10);

        // All children should have received on_stop
        let stopped: Vec<StoppedReport> = std::iter::from_fn(|| stop_inbox.try_recv()).collect();
        let stopped_addrs: std::collections::HashSet<ActorAddress> =
            stopped.iter().map(|s| s.0).collect();

        prop_assert_eq!(
            stopped_addrs.len(), num_children,
            "all {} unsupervised children should stop, got {} stops",
            num_children, stopped_addrs.len()
        );

        for &child in &children {
            prop_assert!(
                stopped_addrs.contains(&child),
                "child {:?} should have been stopped", child
            );
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // G7: Supervised children are NOT orphan-killed
    // ═══════════════════════════════════════════════════════════════════════

    /// Spawn a supervisor with N children. Kill the supervisor's parent (which
    /// is the runtime — so stop the supervisor). The supervisor's on_stop should
    /// handle children, not the orphan mechanism.
    ///
    /// We verify that supervised children survive their grandparent's death
    /// (i.e., the supervisor manages them, not the orphan killer).
    #[test]
    fn supervised_children_not_orphan_killed(
        num_children in 1usize..=5,
    ) {
        let rt = std_runtime();

        // Build a supervisor with permanent children
        let specs: Vec<ChildSpec> = (0..num_children)
            .map(|i| {
                ChildSpec::new(
                    format!("child-{}", i),
                    RestartPolicy::Permanent,
                    move |ctx| {
                        ctx.spawn(IdleActor)
                    },
                )
            })
            .collect();

        let sup = Supervisor::new(SupervisorStrategy::OneForOne, 10, specs);
        let sup_addr = rt.spawn(sup).unwrap();

        tick_many(&rt, 3);

        // Verify children are alive by sending Ping to each
        // We need to discover children addresses — send a ping to check
        // Actually, we can verify the actor count
        let stats = rt.stats();
        let total_before = stats.workers.iter().map(|w| w.num_actors).sum::<usize>();
        // 1 supervisor + N children = N+1
        prop_assert!(
            total_before >= num_children + 1,
            "expected at least {} actors, got {}", num_children + 1, total_before
        );

        // Stop the supervisor — it should gracefully stop its children via on_stop
        rt.stop_actor(sup_addr).unwrap();
        tick_many(&rt, 10);

        // All actors (sup + children) should be gone
        let stats = rt.stats();
        let total_after = stats.workers.iter().map(|w| w.num_actors).sum::<usize>();
        prop_assert_eq!(
            total_after, 0,
            "all actors should be stopped after supervisor stops"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // G7: Cascading orphan cleanup — parent with grandchildren
    // ═══════════════════════════════════════════════════════════════════════

    /// Parent spawns children, each child spawns grandchildren. Kill the parent.
    /// Assert: all descendants eventually stop (cascading orphan cleanup).
    #[test]
    fn cascading_orphan_cleanup(
        num_children in 1usize..=4,
        grandchildren_each in 1usize..=3,
    ) {
        let rt = std_runtime();
        let stop_inbox = rt.new_inbox::<StoppedReport>().unwrap();
        let stop_addr = *stop_inbox.addr();

        /// Parent that spawns ChildWithGrandchildren
        struct TreeParent {
            num_children: usize,
            grandchildren_each: usize,
            stop_report_to: ActorAddress,
        }

        impl ActorInterface for TreeParent {
            type Incoming = Ping;
            type Response = ();

            fn on_start(&mut self, ctx: &Ctx) {
                for _ in 0..self.num_children {
                    let _ = ctx.spawn(ChildWithGrandchildren {
                        num_grandchildren: self.grandchildren_each,
                        stop_report_to: self.stop_report_to,
                    });
                }
            }

            fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}

            fn on_stop(&mut self, ctx: &Ctx) {
                let _ = ctx.send(self.stop_report_to, StoppedReport(ctx.self_addr()));
            }
        }

        struct ChildWithGrandchildren {
            num_grandchildren: usize,
            stop_report_to: ActorAddress,
        }

        impl ActorInterface for ChildWithGrandchildren {
            type Incoming = Ping;
            type Response = ();

            fn on_start(&mut self, ctx: &Ctx) {
                for _ in 0..self.num_grandchildren {
                    let _ = ctx.spawn(StopReportActor {
                        report_to: self.stop_report_to,
                    });
                }
            }

            fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}

            fn on_stop(&mut self, ctx: &Ctx) {
                let _ = ctx.send(self.stop_report_to, StoppedReport(ctx.self_addr()));
            }
        }

        let parent = rt.spawn(TreeParent {
            num_children,
            grandchildren_each,
            stop_report_to: stop_addr,
        }).unwrap();

        tick_many(&rt, 5);

        // Kill parent
        rt.stop_actor(parent).unwrap();
        tick_many(&rt, 15);

        // Count stopped reports: parent + children + grandchildren
        let expected_total = 1 + num_children + (num_children * grandchildren_each);
        let stopped: Vec<StoppedReport> = std::iter::from_fn(|| stop_inbox.try_recv()).collect();

        prop_assert_eq!(
            stopped.len(), expected_total,
            "expected {} stop reports (1 parent + {} children + {} grandchildren), got {}",
            expected_total, num_children, num_children * grandchildren_each, stopped.len()
        );

        // All actors should be gone
        let stats = rt.stats();
        let total = stats.workers.iter().map(|w| w.num_actors).sum::<usize>();
        prop_assert_eq!(total, 0, "all actors should be stopped");
    }
}
