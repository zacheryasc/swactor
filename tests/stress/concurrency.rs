//! Concurrency stress tests - hunt for race conditions.
//!
//! These tests target the shutdown races and concurrent access patterns
//! that are most likely to expose bugs.

use super::{BlackHole, Msg};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use swactor::runtime::{Runtime, RuntimeConfig};

/// Shutdown while messages are in flight.
/// Target: AtomicBool ordering bugs, use-after-shutdown.
#[test]
#[cfg(feature = "stress")]
fn shutdown_under_load() {
    println!("\n>>> STRESS: Shutdown Under Load");

    let mut panics = 0;
    let mut successes = 0;

    // Run many iterations to catch rare races
    for iteration in 0..100 {
        let result = std::panic::catch_unwind(|| {
            let config = RuntimeConfig {
                max_actors: 100,
                router_max_messages: 10_000,
                actor_max_messages: 1000,
                num_threads: 4,
            };
            let runtime = Runtime::new(config);

            // Spawn actors
            let mut actors = Vec::new();
            for _ in 0..50 {
                if let Ok(addr) = runtime.spawn(BlackHole) {
                    actors.push(addr);
                }
            }

            let handle = runtime.run().unwrap();
            let rt = handle.runtime.clone();

            // Sender thread - blast messages
            let actors_clone = actors.clone();
            let rt_send = rt.clone();
            let sender = thread::spawn(move || {
                for _ in 0..1000 {
                    for actor in &actors_clone {
                        let _ = rt_send.send_to::<Msg>(*actor, Msg);
                    }
                }
            });

            // Random delay before shutdown
            let delay = Duration::from_micros((iteration * 17) % 500);
            thread::sleep(delay);

            // Shutdown while sender is still going
            handle.shutdown();

            // Wait for sender (it should not panic)
            let _ = sender.join();

            // Join should complete (not hang)
            handle.join();
        });

        match result {
            Ok(_) => successes += 1,
            Err(_) => panics += 1,
        }
    }

    println!("  Iterations: 100");
    println!("  Successes:  {}", successes);
    println!("  Panics:     {}", panics);

    if panics > 0 {
        println!(">>> FAIL: {} panics detected during shutdown\n", panics);
    } else {
        println!(">>> PASS: No panics during shutdown under load\n");
    }

    assert_eq!(panics, 0, "Shutdown under load caused panics");
}

/// Send to actor immediately after spawn.
/// Target: Race between spawn registration and first message.
#[test]
#[cfg(feature = "stress")]
fn send_to_newborn() {
    println!("\n>>> STRESS: Send to Newborn Actor");

    let mut total_spawned = 0;
    let mut total_send_ok = 0;
    let mut total_send_fail = 0;

    for _ in 0..100 {
        let config = RuntimeConfig {
            max_actors: 1000,
            router_max_messages: 10_000,
            actor_max_messages: 100,
            num_threads: 4,
        };
        let runtime = Runtime::new(config);
        let handle = runtime.run().unwrap();

        // Immediately spawn and send
        for _ in 0..50 {
            if let Ok(addr) = handle.runtime.spawn(BlackHole) {
                total_spawned += 1;
                // Send immediately - actor may not be registered yet
                if handle.runtime.send_to::<Msg>(addr, Msg).is_ok() {
                    total_send_ok += 1;
                } else {
                    total_send_fail += 1;
                }
            }
        }

        handle.shutdown();
        handle.join();
    }

    println!("  Total spawned:    {}", total_spawned);
    println!("  Sends succeeded:  {}", total_send_ok);
    println!("  Sends failed:     {}", total_send_fail);

    if total_send_fail > 0 {
        println!(">>> FAIL: {} messages failed to send\n", total_send_fail);
    } else {
        println!(">>> PASS: All messages succeeded\n");
    }

    assert_eq!(total_send_fail, 0, "Race condition caused failed message delivery");

    println!(">>> Test complete\n");
}

/// FIXME: This test means nothing until we allow killing off actor processes
/// Rapid spawn/despawn cycles.
/// Target: Queue management under churn.
#[test]
#[cfg(feature = "stress")]
fn rapid_spawn_churn() {
    println!("\n>>> STRESS: Rapid Spawn Churn");

    let config = RuntimeConfig {
        max_actors: 100,
        router_max_messages: 10_000,
        actor_max_messages: 100,
        num_threads: 4,
    };
    let runtime = Runtime::new(config);
    let handle = runtime.run().unwrap();

    let spawn_count = Arc::new(AtomicUsize::new(0));
    let fail_count = Arc::new(AtomicUsize::new(0));

    // Multiple threads spawning actors
    let mut threads = Vec::new();
    for _ in 0..4 {
        let rt = handle.runtime.clone();
        let spawns = spawn_count.clone();
        let fails = fail_count.clone();

        threads.push(thread::spawn(move || {
            for _ in 0..500 {
                match rt.spawn(BlackHole) {
                    Ok(_) => {
                        spawns.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        fails.fetch_add(1, Ordering::Relaxed);
                    }
                }
                // Small yield to increase interleaving
                thread::yield_now();
            }
        }));
    }

    // Let it churn
    thread::sleep(Duration::from_millis(100));

    handle.shutdown();

    for t in threads {
        let _ = t.join();
    }
    handle.join();

    let total_spawns = spawn_count.load(Ordering::Relaxed);
    let total_fails = fail_count.load(Ordering::Relaxed);

    println!("  Spawn attempts: {}", total_spawns + total_fails);
    println!("  Successes:      {}", total_spawns);
    println!("  Failures:       {} (expected - queue fills)", total_fails);
    println!(">>> Test complete - no panics\n");
}

/// Multiple threads sending to same actor.
/// Target: Inbox contention, message ordering.
#[test]
#[cfg(feature = "stress")]
fn inbox_contention() {
    println!("\n>>> STRESS: Inbox Contention");

    let config = RuntimeConfig {
        max_actors: 10,
        router_max_messages: 100_000,
        actor_max_messages: 10_000,
        num_threads: 4,
    };
    let runtime = Runtime::new(config);
    let target = runtime.spawn(BlackHole).unwrap();
    let handle = runtime.run().unwrap();

    // Wait for registration
    thread::sleep(Duration::from_millis(10));

    let send_count = Arc::new(AtomicUsize::new(0));
    let fail_count = Arc::new(AtomicUsize::new(0));

    // 8 threads all sending to same actor
    let mut threads = Vec::new();
    for _ in 0..8 {
        let rt = handle.runtime.clone();
        let sends = send_count.clone();
        let fails = fail_count.clone();

        threads.push(thread::spawn(move || {
            for _ in 0..10_000 {
                if rt.send_to::<Msg>(target, Msg).is_ok() {
                    sends.fetch_add(1, Ordering::Relaxed);
                } else {
                    fails.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for t in threads {
        let _ = t.join();
    }

    // Let messages process
    thread::sleep(Duration::from_millis(50));

    handle.shutdown();
    handle.join();

    let total_sends = send_count.load(Ordering::Relaxed);
    let total_fails = fail_count.load(Ordering::Relaxed);

    println!("  Threads:         8");
    println!("  Msgs per thread: 10,000");
    println!("  Total sent:      {}", total_sends);
    println!("  Total failed:    {}", total_fails);
    println!(
        "  Success rate:    {:.1}%",
        (total_sends as f64 / (total_sends + total_fails) as f64) * 100.0
    );
    println!(">>> Test complete - no panics\n");
}

/// FIXME: Not sure this test is meaningful.
/// Shutdown timing fuzz - randomize when shutdown is called.
/// Target: Edge cases in shutdown state machine.
#[test]
#[cfg(feature = "stress")]
fn shutdown_timing_fuzz() {
    println!("\n>>> STRESS: Shutdown Timing Fuzz");

    let mut results = Vec::new();

    for delay_us in [0, 1, 10, 100, 1000, 5000] {
        let mut ok = 0;
        let mut fail = 0;

        for _ in 0..20 {
            let result = std::panic::catch_unwind(|| {
                let config = RuntimeConfig {
                    max_actors: 50,
                    router_max_messages: 1000,
                    actor_max_messages: 100,
                    num_threads: 4,
                };
                let runtime = Runtime::new(config);

                for _ in 0..20 {
                    let _ = runtime.spawn(BlackHole);
                }

                let handle = runtime.run().unwrap();

                // Specific delay
                if delay_us > 0 {
                    thread::sleep(Duration::from_micros(delay_us));
                }

                handle.shutdown();
                handle.join();
            });

            match result {
                Ok(_) => ok += 1,
                Err(_) => fail += 1,
            }
        }

        results.push((delay_us, ok, fail));
    }

    println!("  delay_us  ok  fail");
    println!("  --------  --  ----");
    for (delay, ok, fail) in &results {
        println!("  {:>8}  {:>2}  {:>4}", delay, ok, fail);
    }

    let total_fails: i32 = results.iter().map(|(_, _, f)| *f).sum();
    if total_fails > 0 {
        println!(
            "\n>>> FAIL: {} panics across timing variations",
            total_fails
        );
    } else {
        println!("\n>>> PASS: All timing variations succeeded");
    }
}
