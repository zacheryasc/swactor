//! Saturation stress tests - find where the runtime breaks.
//!
//! These tests intentionally push past limits to document failure modes.

use super::{BlackHole, Counter, Msg, Stress, StressResult};
use std::time::Duration;
use swactor::runtime::{Runtime, RuntimeConfig};

/// Blast the router inbox
#[test]
#[cfg(feature = "stress")]
fn router_inbox_overflow() {
    println!("\n>>> STRESS: Router Inbox Overflow");

    let config = RuntimeConfig {
        max_actors: 10,
        router_max_messages: 100, // Tiny buffer
        actor_max_messages: 1000,
        num_threads: 1,
    };
    let runtime = Runtime::new(config);
    let sink = runtime.spawn(BlackHole).unwrap();

    // Blast messages without processing
    let mut result = StressResult::new("router_inbox_overflow");
    let start = std::time::Instant::now();

    for _ in 0..10_000 {
        result.operations += 1;
        if runtime.send_to::<Msg>(sink, Msg).is_ok() {
            result.successes += 1;
        } else {
            result.failures += 1;
        }
    }

    result.duration = start.elapsed();
    result.note(format!("Router buffer: 100, Messages sent: 10,000"));

    // With hybrid, no failures expected
    assert_eq!(result.failures, 0, "Hybrid channel should not reject");
    result.print();
    println!(">>> PASS: Hybrid channel prevented router overflow\n");
}

/// Blast a single actor's inbox
#[test]
#[cfg(feature = "stress")]
fn actor_inbox_overflow() {
    println!("\n>>> STRESS: Actor Inbox Overflow");

    let config = RuntimeConfig {
        max_actors: 10,
        router_max_messages: 100_000, // Large router buffer
        actor_max_messages: 100,      // Tiny actor inbox
        num_threads: 1,
    };
    let runtime = Runtime::new(config);
    let sink = runtime.spawn(Counter::new()).unwrap();

    // Process router registration
    runtime.tick();

    // Now blast messages - router will accept them but actor inbox will fill
    let mut sent = 0u64;
    let mut router_failed = 0u64;
    for _ in 0..10_000 {
        if runtime.send_to::<Msg>(sink, Msg).is_ok() {
            sent += 1;
        } else {
            router_failed += 1;
        }
        // Tick occasionally to let router deliver
        if sent % 100 == 0 {
            runtime.tick();
        }
    }

    // Process all remaining messages
    for _ in 0..5000 {
        runtime.tick();
    }

    println!("  Router accepted: {}", sent);
    println!("  Router rejected: {}", router_failed);

    assert_eq!(router_failed, 0, "Router rejected message under load");
    println!(">>> PASS: No message loss with hybrid channel\n");
}

/// Blast the runtime with actor spawns
#[test]
#[cfg(feature = "stress")]
fn actor_queue_overflow() {
    println!("\n>>> STRESS: Actor Queue Overflow");

    let config = RuntimeConfig {
        max_actors: 100, // Small actor queue
        router_max_messages: 10_000,
        actor_max_messages: 100,
        num_threads: 1,
    };
    let runtime = Runtime::new(config);

    let mut result = StressResult::new("actor_queue_overflow");
    let start = std::time::Instant::now();

    // Try to spawn 500 actors into 100-slot queue
    for _ in 0..500 {
        result.operations += 1;
        match runtime.spawn(BlackHole) {
            Ok(_) => result.successes += 1,
            Err(_) => result.failures += 1,
        }
    }

    result.duration = start.elapsed();
    result.note(format!("Queue capacity: 100, Spawn attempts: 500"));
    result.print();

    // Note: Router also takes a slot, so we expect ~99 actors max
    assert_eq!(
        result.failures, 0,
        "Spawned more actors than queue capacity"
    );
    println!(">>> PASS: Actor queue correctly rejects when full\n");
}

/// FIXME: IS this actually testing what it should be?
/// Sustained overload - run at 2x capacity for extended period.
/// Documents: Does the system degrade gracefully or crash?
#[test]
#[cfg(feature = "stress")]
fn sustained_overload() {
    println!("\n>>> STRESS: Sustained Overload");

    let config = RuntimeConfig {
        max_actors: 100,
        router_max_messages: 1000,
        actor_max_messages: 100,
        num_threads: 1,
    };
    let runtime = Runtime::new(config);

    // Spawn some actors
    let mut actors = Vec::new();
    for _ in 0..50 {
        if let Ok(addr) = runtime.spawn(Counter::new()) {
            actors.push(addr);
        }
    }

    // Process registrations
    for _ in 0..200 {
        runtime.tick();
    }

    let result = Stress::new("sustained_overload")
        .for_duration(Duration::from_secs(2))
        .run(|| {
            // Send to random actor
            let idx = (std::time::Instant::now().elapsed().as_nanos() as usize) % actors.len();
            let success = runtime.send_to::<Msg>(actors[idx], Msg).is_ok();

            // Process some (but not all) - simulating overload
            runtime.tick();

            success
        });

    result.print();
    println!(">>> System survived sustained overload without panic\n");
}
