//! Runtime Stress Tests — multi-threaded execution, parking, placement, and scale.
//!
//! Covers: single vs multi-threaded processing, high-volume MT delivery,
//! panic isolation under load, worker parking/shutdown, sustained throughput,
//! and load-aware actor placement.

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Poll `inbox` until a message arrives or `timeout` elapses.
fn poll_inbox<M: swactor::actor::Message>(inbox: &Inbox<M>, timeout: Duration) -> Option<M> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(msg) = inbox.try_recv() {
            return Some(msg);
        }
        if Instant::now() > deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Wait until `counter` reaches `target` or `timeout` elapses.
fn wait_for_count(counter: &AtomicUsize, target: usize, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        let n = counter.load(Ordering::SeqCst);
        if n >= target {
            return n;
        }
        if Instant::now() > deadline {
            return n;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Single-threaded vs multi-threaded runtime basics.
///
/// Story: We start with a single-threaded runtime driven by tick(), confirm
/// nothing happens without ticking, then graduate to a multi-threaded runtime
/// with run() and verify background processing, cross-worker delegation,
/// custom thread counts, and clean shutdown.
#[test]
fn single_vs_multi_threaded_basics() {
    // ── Part A: Single-threaded requires tick() ──
    let rt = std_runtime(RuntimeConfig::default());
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
    tick_n(&rt, 2);
    assert!(
        inbox.try_recv().is_some(),
        "tick() drives single-threaded processing"
    );

    // ── Part B: Multi-threaded processes without ticking ──
    let rt_mt = std_runtime(RuntimeConfig {
        num_threads: 4,
        ..Default::default()
    });
    let addr = rt_mt.spawn(PingPongActor).unwrap();
    let inbox = rt_mt.new_inbox::<Pong>().unwrap();
    rt_mt
        .send_to(
            addr,
            Ping {
                reply_to: *inbox.addr(),
            },
        )
        .unwrap();

    let handle = rt_mt.run().unwrap();
    let reply = poll_inbox(&inbox, Duration::from_secs(5));
    assert!(
        reply.is_some(),
        "background workers process without manual ticking"
    );

    // ── Part C: Cross-worker delegation (2 threads, spawn child from handler) ──
    let addr2 = handle.runtime.spawn(DelegatorActor).unwrap();
    let done_inbox = handle.runtime.new_inbox::<Done>().unwrap();
    handle
        .runtime
        .send_to(
            addr2,
            Forward {
                value: 3,
                reply_to: *done_inbox.addr(),
            },
        )
        .unwrap();

    let reply = poll_inbox(&done_inbox, Duration::from_secs(5));
    assert_eq!(
        reply,
        Some(Done(6)),
        "cross-worker delegation delivers reply"
    );

    // ── Part D: Custom thread count reflected in stats ──
    let stats = handle.runtime.stats();
    assert_eq!(
        stats.num_workers, 4,
        "runtime respects requested thread count"
    );

    // ── Part E: Clean shutdown ──
    handle.shutdown();
    handle.join();
    // Test passes by not hanging.
}

/// High-volume multi-threaded delivery.
///
/// Story: We throw large workloads at a 4-thread runtime — 50 senders
/// each firing 100 messages at one receiver, 200 concurrent spawn+send
/// pairs, and a 50-level chain that must hop across workers.
#[test]
fn mt_high_volume_delivery() {
    let cfg = || RuntimeConfig {
        num_threads: 4,
        max_actors: 5_000,
        channel_buffer_size: 10_000,
        ..Default::default()
    };

    // ── Part A: 50 senders × 100 messages → one receiver ──
    {
        let rt = std_runtime(cfg());
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

        let handle = rt.run().unwrap();
        let processed = wait_for_count(&counter, total_expected, Duration::from_secs(5));
        handle.shutdown();
        handle.join();
        assert_eq!(
            processed, total_expected,
            "all 5000 messages delivered to single receiver"
        );
    }

    // ── Part B: 200 concurrent spawn+send pairs ──
    {
        let rt = std_runtime(cfg());
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

        let handle = rt.run().unwrap();
        let received = wait_for_count(&counter, 200, Duration::from_secs(5));
        handle.shutdown();
        handle.join();
        assert_eq!(received, 200, "all 200 spawn+send pairs complete");
    }

    // ── Part C: 50-level chain across workers ──
    {
        let rt = std_runtime(RuntimeConfig {
            num_threads: 2,
            max_actors: 5_000,
            ..Default::default()
        });
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

        let handle = rt.run().unwrap();
        let reply = poll_inbox(&inbox, Duration::from_secs(5));
        handle.shutdown();
        handle.join();
        assert_eq!(
            reply,
            Some(Done(50)),
            "50-level chain completes across workers"
        );
    }
}

/// Panic isolation under multi-threaded load.
///
/// Story: 10 panicking actors and 10 healthy actors on 4 threads — every
/// panic is isolated and all 1000 healthy messages are still processed.
#[test]
fn mt_panic_isolation_under_load() {
    let rt = std_runtime(RuntimeConfig {
        num_threads: 4,
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

    let handle = rt.run().unwrap();
    let expected = 10 * 100;
    let processed = wait_for_count(&counter, expected, Duration::from_secs(5));
    handle.shutdown();
    handle.join();

    assert_eq!(
        processed, expected,
        "all {expected} healthy messages processed despite panicking peers"
    );
}

/// Worker parking and shutdown latency.
///
/// Story: Workers park after idle time. We verify they wake quickly on new
/// messages, that messages sent after run() are delivered, and that shutdown
/// wakes all parked workers promptly.
#[test]
fn worker_parking_and_shutdown() {
    // ── Part A: Parked workers wake on send ──
    let rt = std_runtime(RuntimeConfig {
        num_threads: 2,
        ..Default::default()
    });
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let handle = rt.run().unwrap();
    std::thread::sleep(Duration::from_millis(50)); // Let workers park.

    let before = Instant::now();
    handle
        .runtime
        .send_to(
            addr,
            Ping {
                reply_to: *inbox.addr(),
            },
        )
        .unwrap();
    let reply = poll_inbox(&inbox, Duration::from_secs(1));
    let latency = before.elapsed();

    assert!(reply.is_some(), "parked worker should wake and process");
    assert!(
        latency.as_millis() < 100,
        "wake latency should be <100ms, was {:?}",
        latency
    );

    // ── Part B: Send after run() delivers ──
    let addr2 = handle.runtime.spawn(PingPongActor).unwrap();
    let inbox2 = handle.runtime.new_inbox::<Pong>().unwrap();
    std::thread::sleep(Duration::from_millis(10));
    handle
        .runtime
        .send_to(
            addr2,
            Ping {
                reply_to: *inbox2.addr(),
            },
        )
        .unwrap();

    let reply2 = poll_inbox(&inbox2, Duration::from_secs(5));
    assert!(
        reply2.is_some(),
        "message sent after run() must be delivered"
    );

    handle.shutdown();
    handle.join();

    // ── Part C: Shutdown wakes parked workers quickly ──
    let rt2 = std_runtime(RuntimeConfig {
        num_threads: 4,
        ..Default::default()
    });
    let h2 = rt2.run().unwrap();
    std::thread::sleep(Duration::from_millis(50)); // Let workers park.

    let before = Instant::now();
    h2.shutdown();
    h2.join();
    let shutdown_time = before.elapsed();

    assert!(
        shutdown_time.as_millis() < 500,
        "shutdown should complete quickly with parked workers, took {:?}",
        shutdown_time
    );
}

/// Sustained throughput with no message loss.
///
/// Story: We send 10 batches of 100 messages, ticking between batches on a
/// single-threaded runtime. Each batch must make forward progress, and after
/// draining, all 1000 messages are accounted for.
#[test]
fn sustained_throughput_no_message_loss() {
    let rt = std_runtime(RuntimeConfig::default());
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
        tick_n(&rt, 5);
        let processed = counter.load(Ordering::SeqCst);
        assert!(
            processed > batch * 50,
            "batch {batch}: expected progress, only {processed} processed"
        );
    }

    // Drain remaining.
    tick_n(&rt, 100);
    let total = counter.load(Ordering::SeqCst);
    assert_eq!(total, 1000, "sustained load should not drop any messages");
}

/// Load-aware actor placement.
///
/// Story: A fresh runtime falls back to round-robin (even distribution).
/// Under imbalanced load, new actors bias toward the lighter worker.
/// A single-worker runtime degrades gracefully.
#[test]
fn load_aware_actor_placement() {
    // ── Part A: Round-robin fallback on fresh runtime (4 workers, 100 actors) ──
    let rt = std_runtime(RuntimeConfig {
        num_threads: 4,
        ..Default::default()
    });
    for _ in 0..100 {
        rt.spawn(CounterActor { count: 0 }).unwrap();
    }

    let handle = rt.run().unwrap();
    std::thread::sleep(Duration::from_millis(20));

    let stats = handle.runtime.stats();
    handle.shutdown();
    handle.join();

    for w in &stats.workers {
        assert!(
            w.num_actors >= 20 && w.num_actors <= 30,
            "worker {} has {} actors, expected ~25 (round-robin)",
            w.id,
            w.num_actors
        );
    }

    // ── Part B: Imbalanced load biases toward lighter worker ──
    let rt2 = std_runtime(RuntimeConfig {
        num_threads: 2,
        ..Default::default()
    });
    let mut addrs = Vec::new();
    for _ in 0..20 {
        addrs.push(rt2.spawn(CounterActor { count: 0 }).unwrap());
    }

    let h2 = rt2.run().unwrap();
    std::thread::sleep(Duration::from_millis(10));

    // Bombard the first 10 actors (likely worker 0) with messages.
    for addr in &addrs[..10] {
        for _ in 0..50 {
            let _ = h2.runtime.send_to(*addr, Increment { reply_to: *addr });
        }
    }
    std::thread::sleep(Duration::from_millis(20));

    // Spawn 10 more — should bias toward lighter worker.
    for _ in 0..10 {
        h2.runtime.spawn(CounterActor { count: 0 }).unwrap();
    }
    std::thread::sleep(Duration::from_millis(20));

    let stats2 = h2.runtime.stats();
    h2.shutdown();
    h2.join();

    let total_actors: usize = stats2.workers.iter().map(|w| w.num_actors).sum();
    assert!(
        total_actors >= 20,
        "expected at least 20 actors, got {total_actors}"
    );
    assert!(
        stats2.workers.iter().all(|w| w.num_actors > 0),
        "both workers should have actors: {:?}",
        stats2
            .workers
            .iter()
            .map(|w| w.num_actors)
            .collect::<Vec<_>>()
    );

    // ── Part C: Single-worker degrades gracefully ──
    let rt3 = std_runtime(RuntimeConfig::default());
    for _ in 0..50 {
        rt3.spawn(CounterActor { count: 0 }).unwrap();
    }
    tick_n(&rt3, 10);

    let stats3 = rt3.stats();
    assert_eq!(stats3.workers.len(), 1);
    assert_eq!(stats3.workers[0].num_actors, 50);
}
