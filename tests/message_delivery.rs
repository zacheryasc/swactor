//! Message Routing and Handler Behavior Tests.
//!
//! Covers: routing correctness at scale, send-from-within-handler patterns,
//! address error handling, and fairness/budgets.

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ── Local actors ────────────────────────────────────────────────────────────

/// Sends a countdown message to itself, then replies Done(0).
struct SelfSendActor;

#[derive(Clone)]
struct Countdown {
    remaining: usize,
    reply_to: ActorAddress,
}

impl ActorInterface for SelfSendActor {
    type Incoming = Countdown;
    type Response = Done;
    fn handle(&mut self, ctx: &Ctx, msg: Countdown) {
        if msg.remaining == 0 {
            let _ = ctx.send(msg.reply_to, Done(0));
        } else {
            let _ = ctx.send(
                ctx.self_addr(),
                Countdown {
                    remaining: msg.remaining - 1,
                    reply_to: msg.reply_to,
                },
            );
        }
    }
}

/// NumberedMsg/Reply for routing correctness tests.
#[derive(Clone)]
struct NumberedMsg {
    n: usize,
    reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
struct NumberedReply {
    from: ActorAddress,
    n: usize,
}

struct NumberedActor;

impl ActorInterface for NumberedActor {
    type Incoming = NumberedMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: NumberedMsg) {
        let _ = ctx.send(
            msg.reply_to,
            NumberedReply {
                from: ctx.self_addr(),
                n: msg.n,
            },
        );
    }
}

/// Ring node for routing chain test.
#[derive(Clone)]
struct RingHop {
    hops_remaining: usize,
    final_dest: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
struct RingDone(usize);

struct RingNode {
    next: ActorAddress,
}

impl ActorInterface for RingNode {
    type Incoming = RingHop;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: RingHop) {
        if msg.hops_remaining == 0 {
            let _ = ctx.send(msg.final_dest, RingDone(100));
        } else {
            let _ = ctx.send(
                self.next,
                RingHop {
                    hops_remaining: msg.hops_remaining - 1,
                    final_dest: msg.final_dest,
                },
            );
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

/// 200 actors each get a unique numbered message and reply correctly.
/// A 100-hop ring traversal completes.
#[test]
fn message_routing_at_scale() {
    // 200-actor numbered routing
    let rt = std_runtime(RuntimeConfig {
        max_actors: 300,
        channel_buffer_size: 1024,
        num_threads: 1,
        ..Default::default()
    });
    let inbox = rt.new_inbox::<NumberedReply>().unwrap();
    let inbox_addr = *inbox.addr();
    let mut addrs = Vec::new();
    for _ in 0..200 {
        addrs.push(rt.spawn(NumberedActor).unwrap());
    }
    rt.tick();
    for (i, addr) in addrs.iter().enumerate() {
        rt.send_to(
            *addr,
            NumberedMsg {
                n: i,
                reply_to: inbox_addr,
            },
        )
        .unwrap();
    }
    tick_n(&rt, 3);
    let replies: Vec<NumberedReply> = std::iter::from_fn(|| inbox.try_recv()).collect();
    assert_eq!(replies.len(), 200, "all 200 actors replied");
    for (i, addr) in addrs.iter().enumerate() {
        let reply = replies.iter().find(|r| r.n == i);
        assert!(reply.is_some(), "missing reply for actor #{i}");
        assert_eq!(
            reply.unwrap().from,
            *addr,
            "reply #{i} came from correct actor"
        );
    }

    // 100-hop ring
    let rt = std_runtime(RuntimeConfig {
        max_actors: 200,
        channel_buffer_size: 1024,
        num_threads: 1,
        ..Default::default()
    });
    let inbox = rt.new_inbox::<RingDone>().unwrap();
    let inbox_addr = *inbox.addr();
    let mut ring_addrs = Vec::new();
    let mut next = inbox_addr;
    for _ in (0..100).rev() {
        let addr = rt.spawn(RingNode { next }).unwrap();
        ring_addrs.push(addr);
        next = addr;
    }
    ring_addrs.reverse();
    rt.tick();
    rt.send_to(
        ring_addrs[0],
        RingHop {
            hops_remaining: 99,
            final_dest: inbox_addr,
        },
    )
    .unwrap();
    let result = tick_until_recv(&rt, &inbox, 110);
    assert_eq!(
        result,
        Some(RingDone(100)),
        "ring message traverses all 100 hops"
    );
}

/// Messages sent in handlers are delivered: delegation, self-send chains,
/// rapid spawn+immediate-send, multiple inbox types coexist.
#[test]
fn delivery_from_within_handlers() {
    let rt = std_runtime(RuntimeConfig::default());

    // Delegation: spawn+send in handler
    let delegator = rt.spawn(DelegatorActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();
    rt.send_to(
        delegator,
        Forward {
            value: 5,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    let reply = tick_until_recv(&rt, &inbox, 20);
    assert_eq!(
        reply,
        Some(Done(10)),
        "child spawned during handler receives message"
    );

    // Self-send countdown of 20
    let self_sender = rt.spawn(SelfSendActor).unwrap();
    rt.send_to(
        self_sender,
        Countdown {
            remaining: 20,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    let reply = tick_until_recv(&rt, &inbox, 50);
    assert_eq!(reply, Some(Done(0)), "self-send chain completes");

    // Multiple senders reach same actor
    let counter = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox_a = rt.new_inbox::<Count>().unwrap();
    let inbox_b = rt.new_inbox::<Count>().unwrap();
    rt.send_to(
        counter,
        Increment {
            reply_to: *inbox_a.addr(),
        },
    )
    .unwrap();
    rt.send_to(
        counter,
        Increment {
            reply_to: *inbox_b.addr(),
        },
    )
    .unwrap();
    tick_n(&rt, 10);
    assert!(inbox_a.try_recv().is_some());
    assert_eq!(
        inbox_b.try_recv(),
        Some(Count(2)),
        "both senders reach same actor"
    );

    // 50 rapid spawn+immediate-send pairs
    let rt = std_runtime(RuntimeConfig::default());
    let pong_inbox = rt.new_inbox::<Pong>().unwrap();
    for _ in 0..50 {
        let addr = rt.spawn(PingPongActor).unwrap();
        rt.send_to(
            addr,
            Ping {
                reply_to: *pong_inbox.addr(),
            },
        )
        .unwrap();
    }
    let replies = tick_and_drain(&rt, &pong_inbox, 50);
    assert_eq!(replies.len(), 50, "all spawn+send pairs complete");

    // Multiple inbox types coexist
    let rt = std_runtime(RuntimeConfig::default());
    let counter_addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let pinger_addr = rt.spawn(PingPongActor).unwrap();
    let count_inbox = rt.new_inbox::<Count>().unwrap();
    let pong_inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(
        counter_addr,
        Increment {
            reply_to: *count_inbox.addr(),
        },
    )
    .unwrap();
    rt.send_to(
        pinger_addr,
        Ping {
            reply_to: *pong_inbox.addr(),
        },
    )
    .unwrap();
    tick_n(&rt, 10);
    assert_eq!(count_inbox.try_recv(), Some(Count(1)));
    assert_eq!(pong_inbox.try_recv(), Some(Pong));
}

/// Sending to nonexistent address returns error, wrong type increments
/// type_mismatch counter.
#[test]
fn address_error_handling() {
    let rt = std_runtime(RuntimeConfig::default());

    // Nonexistent address
    let bogus = ActorAddress::new_random();
    assert!(
        rt.send_to(bogus, Pong).is_err(),
        "send to unknown address fails"
    );

    // Wrong type
    let addr = rt.spawn(PingPongActor).unwrap();
    rt.send_to(addr, Count(42)).unwrap(); // Count instead of Ping
    rt.send_to(addr, Count(0)).unwrap();
    rt.send_to(addr, Count(0)).unwrap();
    tick_n(&rt, 10);
    let stats = rt.stats();
    let mismatches: u64 = stats.workers.iter().map(|w| w.type_mismatches).sum();
    assert_eq!(mismatches, 3, "3 wrong-type messages counted as mismatches");
}

/// Budget fairness: hot actor doesn't starve cold actor, budget is respected
/// with self-sends, unlimited budget drains all.
#[test]
fn fairness_budget_prevents_starvation() {
    // Hot (1000 msgs) vs cold (1 msg), budget=64
    let rt = std_runtime(RuntimeConfig::default());
    let hot_counter = Arc::new(AtomicUsize::new(0));
    let cold_inbox = rt.new_inbox::<Pong>().unwrap();
    let hot = rt
        .spawn(CountingPingActor {
            counter: hot_counter.clone(),
        })
        .unwrap();
    let cold = rt.spawn(PingPongActor).unwrap();
    let dummy = rt.new_inbox::<Pong>().unwrap();
    for _ in 0..1000 {
        rt.send_to(
            hot,
            Ping {
                reply_to: *dummy.addr(),
            },
        )
        .unwrap();
    }
    rt.send_to(
        cold,
        Ping {
            reply_to: *cold_inbox.addr(),
        },
    )
    .unwrap();
    rt.tick();
    assert!(
        cold_inbox.try_recv().is_some(),
        "cold actor not starved by hot actor"
    );
    assert!(
        hot_counter.load(Ordering::SeqCst) <= 64,
        "hot capped at budget"
    );

    // Budget=4 with self-send chain of 20 → completes across multiple ticks
    let rt = std_runtime(RuntimeConfig {
        actor_message_budget: 4,
        ..Default::default()
    });
    let addr = rt.spawn(SelfSendActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();
    rt.send_to(
        addr,
        Countdown {
            remaining: 20,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    tick_n(&rt, 30);
    assert_eq!(
        inbox.try_recv(),
        Some(Done(0)),
        "self-send chain completes despite budget"
    );

    // Unlimited budget (0) drains all
    let rt = std_runtime(RuntimeConfig {
        actor_message_budget: 0,
        ..Default::default()
    });
    let counter = Arc::new(AtomicUsize::new(0));
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt
        .spawn(CountingPingActor {
            counter: counter.clone(),
        })
        .unwrap();
    for _ in 0..500 {
        rt.send_to(
            addr,
            Ping {
                reply_to: *dummy.addr(),
            },
        )
        .unwrap();
    }
    rt.tick();
    rt.tick();
    assert_eq!(
        counter.load(Ordering::SeqCst),
        500,
        "unlimited budget drains all"
    );
}

