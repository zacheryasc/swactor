//! Runtime Admin API tests — inventory, typed actor state, lifecycle control, and scheduling.

pub mod common;
use common::*;

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

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

fn operation_applied() -> OperationResult {
    OperationResult { applied: true }
}

#[test]
fn ask_recv_ticking_delivers_reply_through_runtime_inbox() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
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
        ask.recv_ticking(&mut host, 5).unwrap(),
        MyAddr(actor),
        "recv_ticking drives the runtime inbox reply path"
    );
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl WakeCounter {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn ask_future_wakes_after_actor_reply() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let actor = rt.spawn(SelfAddrActor).unwrap();
    let mut ask = rt
        .ask::<WhoAreYou, MyAddr>(actor, |reply_to| WhoAreYou { reply_to })
        .unwrap();
    let wakes = Arc::new(WakeCounter::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(Pin::new(&mut ask).poll(&mut cx), Poll::Pending));

    for _ in 0..5 {
        host.try_tick();
        if wakes.count() > 0 {
            break;
        }
    }

    assert!(wakes.count() > 0, "reply wakes the pending ask future");
    assert_eq!(Pin::new(&mut ask).poll(&mut cx), Poll::Ready(MyAddr(actor)));
}

#[test]
fn inbox_recv_handles_send_before_and_after_waker_registration() {
    let (rt, _host) = std_host(RuntimeConfig::default());

    let before = rt.new_inbox::<Count>().unwrap();
    rt.send_to(*before.addr(), Count(1)).unwrap();
    let mut before_recv = Box::pin(before.recv());
    let before_wakes = Arc::new(WakeCounter::default());
    let before_waker = Waker::from(before_wakes.clone());
    let mut before_cx = Context::from_waker(&before_waker);
    assert_eq!(
        before_recv.as_mut().poll(&mut before_cx),
        Poll::Ready(Count(1))
    );

    let after = rt.new_inbox::<Count>().unwrap();
    let after_addr = *after.addr();
    let mut after_recv = Box::pin(after.recv());
    let after_wakes = Arc::new(WakeCounter::default());
    let after_waker = Waker::from(after_wakes.clone());
    let mut after_cx = Context::from_waker(&after_waker);
    assert!(matches!(
        after_recv.as_mut().poll(&mut after_cx),
        Poll::Pending
    ));

    rt.send_to(after_addr, Count(2)).unwrap();
    assert!(after_wakes.count() > 0);
    assert_eq!(
        after_recv.as_mut().poll(&mut after_cx),
        Poll::Ready(Count(2))
    );
}

#[test]
fn inbox_recv_survives_spurious_polls_and_coalesced_progress() {
    let (rt, _host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Count>().unwrap();
    let addr = *inbox.addr();
    let mut recv = Box::pin(inbox.recv());
    let first_wakes = Arc::new(WakeCounter::default());
    let second_wakes = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first_wakes.clone());
    let second_waker = Waker::from(second_wakes.clone());
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);

    assert!(matches!(recv.as_mut().poll(&mut first_cx), Poll::Pending));
    assert!(matches!(recv.as_mut().poll(&mut second_cx), Poll::Pending));

    rt.send_to(addr, Count(3)).unwrap();
    rt.send_to(addr, Count(4)).unwrap();

    assert_eq!(
        first_wakes.count(),
        0,
        "latest registration replaces stale waker"
    );
    assert!(second_wakes.count() > 0);
    assert_eq!(recv.as_mut().poll(&mut second_cx), Poll::Ready(Count(3)));
    drop(recv);
    assert_eq!(inbox.try_recv(), Some(Count(4)));
}

#[test]
fn dropping_inbox_or_cancelled_ask_unregisters_address() {
    let (rt, _host) = std_host(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Count>().unwrap();
    let inbox_addr = *inbox.addr();
    drop(inbox);
    assert!(
        rt.send_to(inbox_addr, Count(1)).is_err(),
        "dropped inbox address is removed from the registry"
    );

    let actor = rt.spawn(SelfAddrActor).unwrap();
    let mut ask_addr = None;
    let ask = rt
        .ask::<WhoAreYou, MyAddr>(actor, |reply_to| {
            ask_addr = Some(reply_to);
            WhoAreYou { reply_to }
        })
        .unwrap();
    let ask_addr = ask_addr.unwrap();
    drop(ask);
    assert!(
        rt.send_to(ask_addr, MyAddr(actor)).is_err(),
        "cancelled ask address is removed from the registry"
    );
}

#[test]
fn admin_list_and_inspect_report_actor_slot_metadata() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let ping_pong = rt.spawn(PingPongActor).unwrap();
    let counter = rt.spawn(CounterActor { count: 0 }).unwrap();
    host.try_tick();

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

    assert_eq!(tick_until_recv(&mut host, &count_inbox, 5), Some(Count(1)));
    assert_eq!(tick_until_recv(&mut host, &count_inbox, 5), Some(Count(2)));

    let response = rt
        .admin()
        .list_actors()
        .unwrap()
        .recv_ticking(&mut host, 5)
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
        .recv_ticking(&mut host, 5)
        .unwrap();
    assert_eq!(inspect.summary, *counter_summary);

    let missing = ActorAddress::new_random();
    let missing_result = rt
        .admin()
        .inspect_actor(missing)
        .unwrap()
        .recv_ticking(&mut host, 5);
    assert!(
        matches!(missing_result, Err(AdminError::ActorNotFound { actor }) if actor == missing),
        "missing actor is reported through AdminResult"
    );
}

#[test]
fn admin_get_and_replace_actor_state_preserves_slot_metadata() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(ReplaceProbe {
            value: 1,
            started: started.clone(),
            stopped: stopped.clone(),
        })
        .unwrap();
    host.try_tick();

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
    assert_eq!(tick_until_recv(&mut host, &count_inbox, 5), Some(Count(2)));

    let state = rt
        .admin()
        .get_actor_state::<ReplaceProbe>(addr)
        .unwrap()
        .recv_ticking(&mut host, 5)
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
        .recv_ticking(&mut host, 5)
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
    assert_eq!(
        tick_until_recv(&mut host, &count_inbox, 5),
        Some(Count(101))
    );

    let summary = rt
        .admin()
        .inspect_actor(addr)
        .unwrap()
        .recv_ticking(&mut host, 5)
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
        .recv_ticking(&mut host, 5)
        .unwrap();
    assert_eq!(stop_result, operation_applied());
    host.try_tick();
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
}

#[test]
fn admin_replace_rejects_wrong_actor_type_and_wrong_snapshot_address() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(ReplaceProbe {
            value: 10,
            started: started.clone(),
            stopped: stopped.clone(),
        })
        .unwrap();
    host.try_tick();

    let wrong_type_snapshot = ActorStateSnapshot::new(addr, WrongProbe);
    let wrong_type = rt
        .admin()
        .replace_actor_state::<WrongProbe>(addr, wrong_type_snapshot)
        .unwrap()
        .recv_ticking(&mut host, 5);
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
        .recv_ticking(&mut host, 5);
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
        tick_until_recv(&mut host, &count_inbox, 5),
        Some(Count(11)),
        "failed replacements do not mutate the original actor state"
    );
}

#[test]
fn admin_suspend_queues_messages_until_resume() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(CountingPingActor {
            counter: counter.clone(),
        })
        .unwrap();
    host.try_tick();

    let suspend_result = rt
        .admin()
        .suspend_actor(addr)
        .unwrap()
        .recv_ticking(&mut host, 5)
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
    tick_n(&mut host, 5);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(pong_inbox.try_recv(), None);

    let suspended = rt
        .admin()
        .inspect_actor(addr)
        .unwrap()
        .recv_ticking(&mut host, 5)
        .unwrap()
        .summary;
    assert!(suspended.status.suspended);
    assert_eq!(suspended.mailbox_depth, 3);

    let resume_result = rt
        .admin()
        .resume_actor(addr)
        .unwrap()
        .recv_ticking(&mut host, 5)
        .unwrap();
    assert_eq!(resume_result, operation_applied());
    for _ in 0..3 {
        assert_eq!(tick_until_recv(&mut host, &pong_inbox, 5), Some(Pong));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 3);

    let resumed = rt
        .admin()
        .inspect_actor(addr)
        .unwrap()
        .recv_ticking(&mut host, 5)
        .unwrap()
        .summary;
    assert!(!resumed.status.suspended);
    assert_eq!(resumed.mailbox_depth, 0);
    assert_eq!(resumed.messages_handled, 3);
}

#[test]
fn admin_stop_clears_pending_mailbox_without_calling_handle() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
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
    host.try_tick();
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
        .recv_ticking(&mut host, 5)
        .unwrap();
    assert_eq!(stop_result, operation_applied());
    host.try_tick();

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

    let inspect = rt
        .admin()
        .inspect_actor(addr)
        .unwrap()
        .recv_ticking(&mut host, 5);
    assert!(
        matches!(inspect, Err(AdminError::ActorNotFound { actor }) if actor == addr),
        "admin-stopped actor is no longer inspectable"
    );
}

#[test]
fn admin_suspend_resume() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let addr = rt
        .spawn(CountingPingActor {
            counter: counter.clone(),
        })
        .unwrap();
    let pong_inbox = rt.new_inbox::<Pong>().unwrap();
    host.try_tick(); // process on_start

    // Suspend
    let suspended = rt.admin().suspend_actor(addr).unwrap();
    let suspended = suspended.recv_ticking(&mut host, 5);
    assert_eq!(suspended, Ok(operation_applied()));

    // Send while suspended — should not process
    rt.send_to(
        addr,
        Ping {
            reply_to: *pong_inbox.addr(),
        },
    )
    .unwrap();
    tick_n(&mut host, 3);
    assert!(pong_inbox.try_recv().is_none(), "no pong while suspended");
    assert_eq!(counter.load(Ordering::SeqCst), 0);

    // Resume
    let resumed = rt.admin().resume_actor(addr).unwrap();
    let resumed = resumed.recv_ticking(&mut host, 5);
    assert_eq!(resumed, Ok(operation_applied()));

    // Tick — message should now be processed
    tick_n(&mut host, 3);
    assert_eq!(
        pong_inbox.try_recv(),
        Some(Pong),
        "pong delivered after resume"
    );
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn admin_list_actors() {
    let (rt, mut host) = std_host(RuntimeConfig {
        max_actors: 100,
        ..Default::default()
    });
    let mut addrs = Vec::new();
    for _ in 0..16 {
        addrs.push(rt.spawn(CounterActor { count: 0 }).unwrap());
    }
    host.try_tick(); // process spawns

    let admin = rt.admin().list_actors().unwrap();
    let list = admin
        .recv_ticking(&mut host, 5)
        .expect("list_actors timed out");

    let expected: HashSet<_> = addrs.iter().copied().collect();
    let actual: HashSet<_> = list.actors.iter().map(|s| s.address).collect();
    assert_eq!(actual, expected);

    for addr in &addrs {
        assert_eq!(
            list.actors
                .iter()
                .filter(|summary| summary.address == *addr)
                .count(),
            1,
            "actor {addr} appears exactly once"
        );
    }
}
