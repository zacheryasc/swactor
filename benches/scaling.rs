//! Scaling benchmarks for the swactor runtime.
//!
//! These benchmarks measure how performance scales with:
//! - Number of actors
//! - Number of worker threads
//! - Message payload size

use crate::harness::{black_box, Bench, BenchSuite};
use std::thread;
use swactor::{
    actor::ActorInterface,
    runtime::{Runtime, RuntimeConfig},
};

// ============================================================================
// Test Actors
// ============================================================================

/// A counter actor that just increments on each message
struct CounterActor {
    count: usize,
}

impl CounterActor {
    fn new() -> Self {
        Self { count: 0 }
    }
}

#[derive(Clone)]
struct Increment;

impl ActorInterface for CounterActor {
    type Incoming = Increment;
    type Response = ();

    fn handle(&mut self, _ctx: &Runtime, _msg: Increment) {
        self.count += 1;
    }
}

struct SharedCounter {
    count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ActorInterface for SharedCounter {
    type Incoming = Increment;
    type Response = ();

    fn handle(&mut self, _ctx: &Runtime, _msg: Increment) {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// An actor that handles variable-sized payloads
struct PayloadActor {
    bytes_received: usize,
}

impl PayloadActor {
    fn new() -> Self {
        Self { bytes_received: 0 }
    }
}

#[derive(Clone)]
struct Payload(Vec<u8>);

impl ActorInterface for PayloadActor {
    type Incoming = Payload;
    type Response = ();

    fn handle(&mut self, _ctx: &Runtime, msg: Payload) {
        self.bytes_received += msg.0.len();
        black_box(&msg.0);
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

/// Benchmark: How throughput scales with actor count
pub fn bench_actor_count_scaling(suite: &mut BenchSuite) {
    let messages_per_actor = 100u64;

    for actor_count in [10u64, 100, 500, 1000] {
        let name = format!("scaling_{}_actors", actor_count);
        let total_messages = actor_count * messages_per_actor;

        let result = Bench::new(&name)
            .warmup(2)
            .iters(10)
            .elements(total_messages)
            .run_with_setup(
                || {
                    let config = RuntimeConfig {
                        max_actors: (actor_count as usize) + 100,
                        router_max_messages: (total_messages as usize) * 3,
                        actor_max_messages: (messages_per_actor as usize) * 2,
                        num_threads: 1,
                    };
                    let runtime = Runtime::new(config);

                    // Spawn actors
                    let mut actors = Vec::with_capacity(actor_count as usize);
                    for _ in 0..actor_count {
                        let addr = runtime.spawn(CounterActor::new()).unwrap();
                        actors.push(addr);
                    }

                    // Process registrations
                    for _ in 0..(actor_count * 2) {
                        runtime.tick();
                    }

                    (runtime, actors, messages_per_actor)
                },
                |(runtime, actors, msgs_per)| {
                    // Distribute messages across all actors
                    for _ in 0..msgs_per {
                        for actor in &actors {
                            let _ = runtime.send_to::<Increment>(*actor, Increment);
                        }
                    }

                    // Process all
                    let total = actors.len() as u64 * msgs_per;
                    for _ in 0..(total * 3) {
                        runtime.tick();
                    }
                    black_box(());
                },
            );

        suite.add(result);
    }
}

/// Benchmark: How throughput scales with thread count (multithreaded runtime)
pub fn bench_thread_count_scaling(suite: &mut BenchSuite) {
    let actor_count = 100u64;
    let messages_per_actor = 500u64;
    let total_messages = actor_count * messages_per_actor;

    for thread_count in [2usize, 4, 8] {
        let name = format!("scaling_{}_threads", thread_count);

        let result = Bench::new(&name)
            .warmup(1)
            .iters(5)
            .elements(total_messages)
            .run_with_setup(
                || {
                    let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let config = RuntimeConfig {
                        max_actors: (actor_count as usize) + 100,
                        router_max_messages: (total_messages as usize) * 3,
                        actor_max_messages: (messages_per_actor as usize) * 2,
                        num_threads: thread_count,
                    };
                    let runtime = Runtime::new(config);

                    let mut actors = Vec::with_capacity(actor_count as usize);
                    for _ in 0..actor_count {
                        let addr = runtime
                            .spawn(SharedCounter {
                                count: counter.clone(),
                            })
                            .unwrap();
                        actors.push(addr);
                    }

                    let handle = runtime.run().unwrap();

                    for actor in &actors {
                        loop {
                            if handle
                                .runtime
                                .send_to::<Increment>(*actor, Increment)
                                .is_ok()
                            {
                                break;
                            }
                            thread::yield_now();
                        }
                    }

                    while counter.load(std::sync::atomic::Ordering::Relaxed) < actors.len() {
                        thread::yield_now();
                    }
                    counter.store(0, std::sync::atomic::Ordering::Relaxed);

                    (handle, actors, counter)
                },
                |(handle, actors, counter)| {
                    for _ in 0..messages_per_actor {
                        for actor in &actors {
                            let _ = handle.runtime.send_to::<Increment>(*actor, Increment);
                        }
                    }

                    while counter.load(std::sync::atomic::Ordering::Relaxed)
                        < total_messages as usize
                    {
                        thread::yield_now();
                    }

                    handle.shutdown();
                    handle.join();
                    black_box(());
                },
            );

        suite.add(result);
    }
}

/// Benchmark: How throughput scales with message payload size
pub fn bench_payload_size_scaling(suite: &mut BenchSuite) {
    let message_count = 1_000u64;

    for payload_size in [64usize, 1024, 16384, 65536] {
        let name = format!("payload_{}B", payload_size);
        let payload = vec![0u8; payload_size];

        let result = Bench::new(&name)
            .warmup(2)
            .iters(20)
            .elements(message_count)
            .run_with_setup(
                || {
                    let config = RuntimeConfig {
                        max_actors: 10,
                        router_max_messages: (message_count as usize) * 2,
                        actor_max_messages: (message_count as usize) * 2,
                        num_threads: 1,
                    };
                    let runtime = Runtime::new(config);
                    let sink = runtime.spawn(PayloadActor::new()).unwrap();

                    // Process registration
                    for _ in 0..10 {
                        runtime.tick();
                    }

                    (runtime, sink, payload.clone())
                },
                |(runtime, sink, payload)| {
                    for _ in 0..message_count {
                        let _ = runtime.send_to::<Payload>(sink, Payload(payload.clone()));
                    }

                    for _ in 0..(message_count * 3) {
                        runtime.tick();
                    }
                    black_box(());
                },
            );

        suite.add(result);
    }
}

/// Run all scaling benchmarks
pub fn run_all() -> BenchSuite {
    let mut suite = BenchSuite::new("Scaling Benchmarks");

    println!("\nRunning actor count scaling benchmarks...");
    bench_actor_count_scaling(&mut suite);

    println!("Running thread count scaling benchmarks...");
    bench_thread_count_scaling(&mut suite);

    println!("Running payload size scaling benchmarks...");
    bench_payload_size_scaling(&mut suite);

    suite
}
