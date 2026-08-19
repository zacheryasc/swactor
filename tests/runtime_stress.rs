//! Runtime Stress Tests — delivery at scale, panic isolation, and sustained throughput.
//!
//! Covers: high-volume delivery, panic isolation under load, and sustained
//! throughput with no message loss. All tests are tick-driven (single worker).

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ── Tests ────────────────────────────────────────────────────────────────────

/// Single-worker runtime basics.
///
/// Story: A runtime driven by tick() does nothing before the first tick,
/// then processes messages correctly.
#[test]
fn runtime_basics() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(
        addr,
        Ping {
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();

    assert!(inbox.try_recv().is_none(), "no processing before tick");
    tick_n(&mut host, 2);
    assert!(inbox.try_recv().is_some(), "tick() drives processing");

    // Delegation (spawn child from handler) works on a single worker.
    let addr2 = rt.spawn(DelegatorActor).unwrap();
    let done_inbox = rt.new_inbox::<Done>().unwrap();
    rt.send_to(
        addr2,
        Forward {
            value: 3,
            reply_to: *done_inbox.addr(),
        },
    )
    .unwrap();

    tick_n(&mut host, 3);
    assert_eq!(
        done_inbox.try_recv(),
        Some(Done(6)),
        "delegation delivers reply"
    );
}

/// High-volume delivery.
///
/// Story: We throw large workloads at the runtime — 50 senders each firing
/// 100 messages at one receiver, 200 concurrent spawn+send pairs, and a
/// 50-level chain. All messages must be accounted for.
#[test]
fn high_volume_delivery() {
    let cfg = || RuntimeConfig {
        max_actors: 5_000,
        channel_buffer_size: 10_000,
        ..Default::default()
    };

    // ── Part A: 50 senders × 100 messages → one receiver ──
    {
        let (rt, mut host) = std_host(cfg());
        let counter = Arc::new(AtomicUsize::new(0));
        let dummy = rt.new_inbox::<Pong>().unwrap();
        let receiver = rt
            .spawn(CountingPingActor {
                counter: counter.clone(),
            })
            .unwrap();

        let total_expected = 50 * 100;
        for _ in 0..50 {
            for _ in 0..100 {
                rt.send_to(
                    receiver,
                    Ping {
                        reply_to: *dummy.addr(),
                    },
                )
                .unwrap();
            }
        }

        tick_n(&mut host, 200);
        let processed = counter.load(Ordering::SeqCst);
        assert_eq!(
            processed, total_expected,
            "all 5000 messages delivered to single receiver"
        );
    }

    // ── Part B: 200 concurrent spawn+send pairs ──
    {
        let (rt, mut host) = std_host(cfg());
        let counter = Arc::new(AtomicUsize::new(0));
        let dummy = rt.new_inbox::<Pong>().unwrap();
        for _ in 0..200 {
            let a = rt
                .spawn(CountingPingActor {
                    counter: counter.clone(),
                })
                .unwrap();
            rt.send_to(
                a,
                Ping {
                    reply_to: *dummy.addr(),
                },
            )
            .unwrap();
        }

        tick_n(&mut host, 50);
        let received = counter.load(Ordering::SeqCst);
        assert_eq!(received, 200, "all 200 spawn+send pairs complete");
    }

    // ── Part C: 50-level chain ──
    {
        let (rt, mut host) = std_host(cfg());
        let addr = rt.spawn(ChainActor).unwrap();
        let inbox = rt.new_inbox::<Done>().unwrap();
        rt.send_to(
            addr,
            ChainMsg {
                remaining: 50,
                depth: 0,
                reply_to: *inbox.addr(),
            },
        )
        .unwrap();

        tick_n(&mut host, 100);
        assert_eq!(inbox.try_recv(), Some(Done(50)), "50-level chain completes");
    }
}

/// Panic isolation under load.
///
/// Story: 10 panicking actors and 10 healthy actors — every panic is isolated
/// and all 1000 healthy messages are still processed.
#[test]
fn panic_isolation_under_load() {
    let (rt, mut host) = std_host(RuntimeConfig {
        max_actors: 5_000,
        channel_buffer_size: 10_000,
        ..Default::default()
    });

    let counter = Arc::new(AtomicUsize::new(0));
    let dummy = rt.new_inbox::<Pong>().unwrap();

    let mut panic_addrs = Vec::new();
    let mut healthy_addrs = Vec::new();
    for _ in 0..10 {
        panic_addrs.push(rt.spawn(PanicActor).unwrap());
        healthy_addrs.push(
            rt.spawn(CountingPingActor {
                counter: counter.clone(),
            })
            .unwrap(),
        );
    }

    // Trigger panics and flood healthy actors.
    for &addr in &panic_addrs {
        rt.send_to(addr, PanicMsg).unwrap();
    }
    for &addr in &healthy_addrs {
        for _ in 0..100 {
            rt.send_to(
                addr,
                Ping {
                    reply_to: *dummy.addr(),
                },
            )
            .unwrap();
        }
    }

    tick_n(&mut host, 200);
    let expected = 10 * 100;
    let processed = counter.load(Ordering::SeqCst);

    assert_eq!(
        processed, expected,
        "all {expected} healthy messages processed despite panicking peers"
    );
}

/// Sustained throughput with no message loss.
///
/// Story: We send 10 batches of 100 messages, ticking between batches.
/// Each batch must make forward progress, and after draining, all 1000
/// messages are accounted for.
#[test]
fn sustained_throughput_no_message_loss() {
    let (rt, mut host) = std_host(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt
        .spawn(CountingPingActor {
            counter: counter.clone(),
        })
        .unwrap();

    for batch in 0..10 {
        for _ in 0..100 {
            rt.send_to(
                addr,
                Ping {
                    reply_to: *dummy.addr(),
                },
            )
            .unwrap();
        }
        tick_n(&mut host, 5);
        let processed = counter.load(Ordering::SeqCst);
        assert!(
            processed > batch * 50,
            "batch {batch}: expected progress, only {processed} processed"
        );
    }

    // Drain remaining.
    tick_n(&mut host, 100);
    let total = counter.load(Ordering::SeqCst);
    assert_eq!(total, 1000, "sustained load should not drop any messages");
}
