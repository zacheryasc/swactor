//! Runtime Admin API tests — inventory, typed actor state, lifecycle control, and scheduling.

mod common;
use common::*;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use swactor::admin::{ActorStateSnapshot, AdminError, OperationResult};
use swactor::config::RuntimeConfig;

#[derive(Clone)]
struct AddAndReport {
    delta: usize,
    reply_to: ActorAddress,
}

#[derive(Clone)]
struct ReplaceProbe {
    value: usize,
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
}

impl ActorInterface for ReplaceProbe {
    type Incoming = AddAndReport;
    type Response = Count;

    fn on_start(&mut self, _ctx: &Ctx) {
        self.started.fetch_add(1, Ordering::SeqCst);
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }

    fn handle(&mut self, ctx: &Ctx, msg: AddAndReport) {
        self.value += msg.delta;
        let _ = ctx.send(msg.reply_to, Count(self.value));
    }
}

struct WrongProbe;

impl ActorInterface for WrongProbe {
    type Incoming = Ping;
    type Response = Pong;

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
}

struct StopProbe {
    started: Arc<AtomicUsize>,
    handled: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
}

impl ActorInterface for StopProbe {
    type Incoming = Ping;
    type Response = Pong;

    fn on_start(&mut self, _ctx: &Ctx) {
        self.started.fetch_add(1, Ordering::SeqCst);
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::SeqCst);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.handled.fetch_add(1, Ordering::SeqCst);
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

fn poll_admin<T: swactor::actor::Message>(
    admin: &swactor::admin::Admin<T>,
    timeout: Duration,
) -> Option<swactor::admin::AdminResult<T>> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(value) = admin.try_recv() {
            return Some(value);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    None
}

fn poll_inbox<M: swactor::actor::Message>(inbox: &Inbox<M>, timeout: Duration) -> Option<M> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(value) = inbox.try_recv() {
            return Some(value);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    None
}

fn operation_applied() -> OperationResult {
    OperationResult { applied: true }
}

#[test]
fn ask_recv_ticking_delivers_reply_through_runtime_inbox() {
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(SelfAddrActor).unwrap();

    let ask = rt
        .ask::<WhoAreYou, MyAddr>(actor, |reply_to| WhoAreYou { reply_to })
        .unwrap();

    assert_eq!(
        ask.try_recv(),
        None,
        "ask reply is not available before ticking"
    );
    assert_eq!(
        ask.recv_ticking(&rt, 5).unwrap(),
        MyAddr(actor),
        "recv_ticking drives the runtime inbox reply path"
    );
}

#[test]
fn admin_list_and_inspect_report_actor_slot_metadata() {
    let rt = std_runtime(RuntimeConfig::default());
    let ping_pong = rt.spawn(PingPongActor).unwrap();
    let counter = rt.spawn(CounterActor { count: 0 }).unwrap();
    rt.tick();

    let count_inbox = rt.new_inbox::<Count>().unwrap();
    rt.send_to(
        counter,
        Increment {
            reply_to: *count_inbox.addr(),
        },
    )
    .unwrap();
    rt.send_to(
        counter,
        Increment {
            reply_to: *count_inbox.addr(),
        },
    )
    .unwrap();

    assert_eq!(tick_until_recv(&rt, &count_inbox, 5), Some(Count(1)));
    assert_eq!(tick_until_recv(&rt, &count_inbox, 5), Some(Count(2)));

    let response = rt
        .admin()
        .list_actors()
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap();

    assert_eq!(
        response
            .actors
            .iter()
            .filter(|summary| summary.address == ping_pong)
            .count(),
        1,
        "ping-pong actor appears exactly once in inventory"
    );
    assert_eq!(
        response
            .actors
            .iter()
            .filter(|summary| summary.address == counter)
            .count(),
        1,
        "counter actor appears exactly once in inventory"
    );

    let counter_summary = response
        .actors
        .iter()
        .find(|summary| summary.address == counter)
        .expect("counter summary missing");

    assert_eq!(counter_summary.worker_id, 0);
    assert_eq!(counter_summary.parent, None);
    assert_eq!(counter_summary.mailbox_depth, 0);
    assert!(counter_summary.status.started);
    assert!(!counter_summary.status.suspended);
    assert!(!counter_summary.status.stopping);
    assert!(!counter_summary.status.poisoned);
    assert_eq!(counter_summary.messages_handled, 2);
    assert!(counter_summary.actor_type.ends_with("CounterActor"));
    assert!(counter_summary.message_type.ends_with("Increment"));

    let inspect = rt
        .admin()
        .inspect_actor(counter)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap();
    assert_eq!(inspect.summary, *counter_summary);

    let missing = ActorAddress::new_random();
    let missing_result = rt
        .admin()
        .inspect_actor(missing)
        .unwrap()
        .recv_ticking(&rt, 5);
    assert!(
        matches!(missing_result, Err(AdminError::ActorNotFound { actor }) if actor == missing),
        "missing actor is reported through AdminResult"
    );
}

#[test]
fn admin_get_and_replace_actor_state_preserves_slot_metadata() {
    let rt = std_runtime(RuntimeConfig::default());
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(ReplaceProbe {
            value: 1,
            started: started.clone(),
            stopped: stopped.clone(),
        })
        .unwrap();
    rt.tick();

    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(stopped.load(Ordering::SeqCst), 0);

    let count_inbox = rt.new_inbox::<Count>().unwrap();
    rt.send_to(
        addr,
        AddAndReport {
            delta: 1,
            reply_to: *count_inbox.addr(),
        },
    )
    .unwrap();
    assert_eq!(tick_until_recv(&rt, &count_inbox, 5), Some(Count(2)));

    let state = rt
        .admin()
        .get_actor_state::<ReplaceProbe>(addr)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap()
        .state;
    assert_eq!(state.actor, addr);
    assert!(state.actor_type.ends_with("ReplaceProbe"));
    assert!(state.message_type.ends_with("AddAndReport"));
    assert_eq!(state.actor_instance.value, 2);

    let replacement = ActorStateSnapshot::new(
        addr,
        ReplaceProbe {
            value: 100,
            started: started.clone(),
            stopped: stopped.clone(),
        },
    );
    let replace_result = rt
        .admin()
        .replace_actor_state::<ReplaceProbe>(addr, replacement)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap();
    assert_eq!(replace_result, operation_applied());
    assert_eq!(
        started.load(Ordering::SeqCst),
        1,
        "replacement does not call on_start"
    );
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        0,
        "replacement does not call on_stop"
    );

    rt.send_to(
        addr,
        AddAndReport {
            delta: 1,
            reply_to: *count_inbox.addr(),
        },
    )
    .unwrap();
    assert_eq!(tick_until_recv(&rt, &count_inbox, 5), Some(Count(101)));

    let summary = rt
        .admin()
        .inspect_actor(addr)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap()
        .summary;
    assert_eq!(summary.address, addr);
    assert_eq!(summary.worker_id, 0);
    assert_eq!(
        summary.messages_handled, 2,
        "state replacement preserves slot-owned message counters"
    );

    let stop_result = rt
        .admin()
        .stop_actor(addr)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap();
    assert_eq!(stop_result, operation_applied());
    rt.tick();
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
}

#[test]
fn admin_replace_rejects_wrong_actor_type_and_wrong_snapshot_address() {
    let rt = std_runtime(RuntimeConfig::default());
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(ReplaceProbe {
            value: 10,
            started: started.clone(),
            stopped: stopped.clone(),
        })
        .unwrap();
    rt.tick();

    let wrong_type_snapshot = ActorStateSnapshot::new(addr, WrongProbe);
    let wrong_type = rt
        .admin()
        .replace_actor_state::<WrongProbe>(addr, wrong_type_snapshot)
        .unwrap()
        .recv_ticking(&rt, 5);
    assert!(
        matches!(wrong_type, Err(AdminError::TypeMismatch { .. })),
        "wrong concrete actor type is rejected"
    );

    let wrong_addr = ActorAddress::new_random();
    let wrong_addr_snapshot = ActorStateSnapshot::new(
        wrong_addr,
        ReplaceProbe {
            value: 50,
            started: started.clone(),
            stopped: stopped.clone(),
        },
    );
    let wrong_address = rt
        .admin()
        .replace_actor_state::<ReplaceProbe>(addr, wrong_addr_snapshot)
        .unwrap()
        .recv_ticking(&rt, 5);
    assert!(
        matches!(wrong_address, Err(AdminError::AddressMismatch { requested, snapshot }) if requested == addr && snapshot == wrong_addr),
        "snapshot address must match the target address"
    );

    let count_inbox = rt.new_inbox::<Count>().unwrap();
    rt.send_to(
        addr,
        AddAndReport {
            delta: 1,
            reply_to: *count_inbox.addr(),
        },
    )
    .unwrap();
    assert_eq!(
        tick_until_recv(&rt, &count_inbox, 5),
        Some(Count(11)),
        "failed replacements do not mutate the original actor state"
    );
}

#[test]
fn admin_suspend_queues_messages_until_resume() {
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(CountingPingActor {
            counter: counter.clone(),
        })
        .unwrap();
    rt.tick();

    let suspend_result = rt
        .admin()
        .suspend_actor(addr)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap();
    assert_eq!(suspend_result, operation_applied());

    let pong_inbox = rt.new_inbox::<Pong>().unwrap();
    for _ in 0..3 {
        rt.send_to(
            addr,
            Ping {
                reply_to: *pong_inbox.addr(),
            },
        )
        .unwrap();
    }
    tick_n(&rt, 5);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(pong_inbox.try_recv(), None);

    let suspended = rt
        .admin()
        .inspect_actor(addr)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap()
        .summary;
    assert!(suspended.status.suspended);
    assert_eq!(suspended.mailbox_depth, 3);

    let resume_result = rt
        .admin()
        .resume_actor(addr)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap();
    assert_eq!(resume_result, operation_applied());
    for _ in 0..3 {
        assert_eq!(tick_until_recv(&rt, &pong_inbox, 5), Some(Pong));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 3);

    let resumed = rt
        .admin()
        .inspect_actor(addr)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap()
        .summary;
    assert!(!resumed.status.suspended);
    assert_eq!(resumed.mailbox_depth, 0);
    assert_eq!(resumed.messages_handled, 3);
}

#[test]
fn admin_stop_clears_pending_mailbox_without_calling_handle() {
    let rt = std_runtime(RuntimeConfig::default());
    let started = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(StopProbe {
            started: started.clone(),
            handled: handled.clone(),
            stopped: stopped.clone(),
        })
        .unwrap();
    rt.tick();
    assert_eq!(started.load(Ordering::SeqCst), 1);

    let pong_inbox = rt.new_inbox::<Pong>().unwrap();
    for _ in 0..5 {
        rt.send_to(
            addr,
            Ping {
                reply_to: *pong_inbox.addr(),
            },
        )
        .unwrap();
    }

    let stop_result = rt
        .admin()
        .stop_actor(addr)
        .unwrap()
        .recv_ticking(&rt, 5)
        .unwrap();
    assert_eq!(stop_result, operation_applied());
    rt.tick();

    assert_eq!(handled.load(Ordering::SeqCst), 0);
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
    assert_eq!(pong_inbox.try_recv(), None);
    assert!(
        rt.send_to(
            addr,
            Ping {
                reply_to: *pong_inbox.addr(),
            },
        )
        .is_err(),
        "admin-stopped actor is removed from normal send routing"
    );

    let inspect = rt.admin().inspect_actor(addr).unwrap().recv_ticking(&rt, 5);
    assert!(
        matches!(inspect, Err(AdminError::ActorNotFound { actor }) if actor == addr),
        "admin-stopped actor is no longer inspectable"
    );
}

#[test]
fn threaded_admin_suspend_resume_wakes_parked_worker() {
    let rt = std_runtime(RuntimeConfig {
        num_threads: 2,
        ..Default::default()
    });
    let counter = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(CountingPingActor {
            counter: counter.clone(),
        })
        .unwrap();
    let pong_inbox = rt.new_inbox::<Pong>().unwrap();

    let handle = rt.run().unwrap();
    std::thread::sleep(Duration::from_millis(50));

    let suspended = handle.runtime.admin().suspend_actor(addr).unwrap();
    let suspended = poll_admin(&suspended, Duration::from_secs(1));

    handle
        .runtime
        .send_to(
            addr,
            Ping {
                reply_to: *pong_inbox.addr(),
            },
        )
        .unwrap();
    let pong_while_suspended = poll_inbox(&pong_inbox, Duration::from_millis(100));
    let count_while_suspended = counter.load(Ordering::SeqCst);

    let resumed = handle.runtime.admin().resume_actor(addr).unwrap();
    let resumed = poll_admin(&resumed, Duration::from_secs(1));
    let pong_after_resume = poll_inbox(&pong_inbox, Duration::from_secs(1));
    let final_count = counter.load(Ordering::SeqCst);

    handle.shutdown();
    handle.join();

    assert_eq!(suspended, Some(Ok(operation_applied())));
    assert_eq!(pong_while_suspended, None);
    assert_eq!(count_while_suspended, 0);
    assert_eq!(resumed, Some(Ok(operation_applied())));
    assert_eq!(pong_after_resume, Some(Pong));
    assert_eq!(final_count, 1);
}

#[test]
fn admin_list_actors_aggregates_all_workers() {
    let rt = std_runtime(RuntimeConfig {
        num_threads: 4,
        max_actors: 100,
        ..Default::default()
    });
    let mut addrs = Vec::new();
    for _ in 0..16 {
        addrs.push(rt.spawn(CounterActor { count: 0 }).unwrap());
    }

    let handle = rt.run().unwrap();
    let start = Instant::now();
    let mut response = None;
    while start.elapsed() < Duration::from_secs(1) {
        let admin = handle.runtime.admin().list_actors().unwrap();
        if let Some(Ok(list)) = poll_admin(&admin, Duration::from_millis(100)) {
            if list.actors.len() == addrs.len() {
                response = Some(list);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    handle.shutdown();
    handle.join();

    let response = response.expect("admin list did not observe all spawned actors within timeout");
    let expected: HashSet<_> = addrs.iter().copied().collect();
    let actual: HashSet<_> = response
        .actors
        .iter()
        .map(|summary| summary.address)
        .collect();
    assert_eq!(actual, expected);
    for addr in &addrs {
        assert_eq!(
            response
                .actors
                .iter()
                .filter(|summary| summary.address == *addr)
                .count(),
            1,
            "actor {addr} appears exactly once in aggregated list"
        );
    }

    let worker_ids: HashSet<_> = response
        .actors
        .iter()
        .map(|summary| summary.worker_id)
        .collect();
    assert!(
        worker_ids.len() >= 2,
        "aggregation should include actors from at least two workers, got {worker_ids:?}"
    );
}
