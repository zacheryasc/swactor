use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use std::sync::Arc;
use swactor::{
    actor::{ActorAddress, ActorInterface},
    config::RuntimeConfig,
    runtime::{Ctx, Runtime},
};
use swactor_std::{RuntimeGroups, RuntimeNaming, StdExtension};

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn make_config(max_actors: usize, max_messages: usize) -> RuntimeConfig {
    RuntimeConfig {
        max_actors,
        channel_buffer_size: max_messages,
        num_threads: 1,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct NoopMessage;

#[derive(Clone)]
struct PingMessage {
    reply_to: ActorAddress,
}

#[derive(Clone)]
struct PongMessage;

#[derive(Clone)]
struct CountMessage(u64);

#[derive(Clone)]
struct RingMessage {
    hops: u64,
}

// ---------------------------------------------------------------------------
// Actor types
// ---------------------------------------------------------------------------

struct NoopActor;

impl ActorInterface for NoopActor {
    type Incoming = NoopMessage;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: NoopMessage) {}
}

struct EchoActor;

impl ActorInterface for EchoActor {
    type Incoming = PingMessage;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: PingMessage) {
        let _ = ctx.send(msg.reply_to, PongMessage);
    }
}

struct SinkActor;

impl ActorInterface for SinkActor {
    type Incoming = CountMessage;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: CountMessage) {}
}

struct RingActor {
    next: ActorAddress,
}

impl ActorInterface for RingActor {
    type Incoming = RingMessage;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: RingMessage) {
        let _ = ctx.send(self.next, RingMessage { hops: msg.hops + 1 });
    }
}

// ---------------------------------------------------------------------------
// Latency benchmarks
// ---------------------------------------------------------------------------

fn latency_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency");

    // A1 — Spawn latency
    group.bench_function("spawn", |b| {
        b.iter_batched(
            || Runtime::new(make_config(1_000, 1_000)),
            |rt| {
                rt.spawn(NoopActor).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    // A2 — Message round-trip
    group.bench_function("message_roundtrip", |b| {
        b.iter_batched(
            || {
                let rt = Runtime::new(make_config(1_000, 1_000));
                let addr = rt.spawn(EchoActor).unwrap();
                rt.tick(); // register actor
                let inbox = rt.new_inbox::<PongMessage>().unwrap();
                let inbox_addr = *inbox.addr();
                (rt, addr, inbox, inbox_addr)
            },
            |(rt, addr, inbox, inbox_addr)| {
                rt.send_to(addr, PingMessage { reply_to: inbox_addr }).unwrap();
                for _ in 0..20 {
                    rt.tick();
                    if inbox.try_recv().is_some() {
                        return;
                    }
                }
                panic!("PongMessage not received within 20 ticks");
            },
            BatchSize::SmallInput,
        );
    });

    // A3 — Fire-and-forget send
    group.bench_function("send_fire_and_forget", |b| {
        b.iter_batched(
            || {
                let rt = Runtime::new(make_config(1_000, 100_000));
                let addr = rt.spawn(NoopActor).unwrap();
                rt.tick(); // register actor
                (rt, addr)
            },
            |(rt, addr)| {
                rt.send_to(addr, NoopMessage).unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    // A4 — Inbox creation
    group.bench_function("inbox_creation", |b| {
        b.iter_batched(
            || Runtime::new(make_config(1_000, 1_000)),
            |rt| {
                rt.new_inbox::<NoopMessage>().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Throughput benchmarks
// ---------------------------------------------------------------------------

fn throughput_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");

    // B1 — Single-actor throughput
    for n in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("single_actor", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let rt = Runtime::new(make_config(100, n + 100));
                    let addr = rt.spawn(SinkActor).unwrap();
                    rt.tick(); // register actor
                    for i in 0..n {
                        rt.send_to(addr, CountMessage(i as u64)).unwrap();
                    }
                    rt
                },
                |rt| {
                    for _ in 0..50 {
                        rt.tick();
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    // B2 — Multi-actor throughput
    for (actors, msgs_per) in [(10, 100), (100, 100), (100, 1_000)] {
        let total = actors * msgs_per;
        group.throughput(Throughput::Elements(total as u64));
        let param = format!("{actors}x{msgs_per}");
        group.bench_with_input(BenchmarkId::new("multi_actor", &param), &(actors, msgs_per), |b, &(actors, msgs_per)| {
            b.iter_batched(
                || {
                    let rt = Runtime::new(make_config(actors + 100, msgs_per + 100));
                    let addrs: Vec<_> = (0..actors)
                        .map(|_| rt.spawn(SinkActor).unwrap())
                        .collect();
                    rt.tick(); // register actors
                    for &addr in &addrs {
                        for i in 0..msgs_per {
                            rt.send_to(addr, CountMessage(i as u64)).unwrap();
                        }
                    }
                    rt
                },
                |rt| {
                    for _ in 0..100 {
                        rt.tick();
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    // B3 — Ring throughput
    for ring_size in [10usize, 100, 500] {
        group.throughput(Throughput::Elements((ring_size + 1) as u64));
        group.bench_with_input(BenchmarkId::new("ring", ring_size), &ring_size, |b, &ring_size| {
            b.iter_batched(
                || {
                    let rt = Runtime::new(make_config(ring_size + 100, 100));
                    let inbox = rt.new_inbox::<RingMessage>().unwrap();
                    // Build the ring: last actor sends to inbox, each prior actor sends to the next
                    let mut next_addr = *inbox.addr();
                    let mut entry_addr = next_addr;
                    for _ in 0..ring_size {
                        let addr = rt.spawn(RingActor { next: next_addr }).unwrap();
                        entry_addr = addr;
                        next_addr = addr;
                    }
                    rt.tick(); // register all actors
                    (rt, entry_addr, inbox)
                },
                |(rt, entry_addr, inbox)| {
                    rt.send_to(entry_addr, RingMessage { hops: 0 }).unwrap();
                    for _ in 0..(ring_size + 10) {
                        rt.tick();
                        if inbox.try_recv().is_some() {
                            return;
                        }
                    }
                    panic!("RingMessage not received within tick budget");
                },
                BatchSize::LargeInput,
            );
        });
    }

    // B4 — Spawn throughput
    for n in [100, 1_000, 5_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("spawn", n), &n, |b, &n| {
            b.iter_batched(
                || Runtime::new(make_config(n + 100, 1_000)),
                |rt| {
                    for _ in 0..n {
                        rt.spawn(NoopActor).unwrap();
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Fairness benchmarks
// ---------------------------------------------------------------------------

fn fairness_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("fairness");

    // F1 — Cold-actor latency under hot-actor pressure
    // Measures how quickly a cold actor responds when a hot actor has a full mailbox
    for hot_msgs in [100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("cold_latency_under_pressure", hot_msgs),
            &hot_msgs,
            |b, &hot_msgs| {
                b.iter_batched(
                    || {
                        let rt = Runtime::new(RuntimeConfig {
                            max_actors: 100,
                            channel_buffer_size: hot_msgs + 100,
                            num_threads: 1,
                            ..Default::default()
                        });
                        let hot_addr = rt.spawn(SinkActor).unwrap();
                        let cold_addr = rt.spawn(EchoActor).unwrap();
                        rt.tick(); // register actors

                        // Load hot actor
                        for i in 0..hot_msgs {
                            rt.send_to(hot_addr, CountMessage(i as u64)).unwrap();
                        }

                        let inbox = rt.new_inbox::<PongMessage>().unwrap();
                        let inbox_addr = *inbox.addr();
                        (rt, cold_addr, inbox, inbox_addr)
                    },
                    |(rt, cold_addr, inbox, inbox_addr)| {
                        // Send to cold actor and measure ticks until reply
                        rt.send_to(cold_addr, PingMessage { reply_to: inbox_addr }).unwrap();
                        for _ in 0..200 {
                            rt.tick();
                            if inbox.try_recv().is_some() {
                                return;
                            }
                        }
                        panic!("Cold actor did not respond");
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    // F2 — Total throughput with budget vs without (ensures budget doesn't kill throughput)
    for budget in [0usize, 32, 64, 128] {
        let label = if budget == 0 { "unlimited".to_string() } else { format!("{budget}") };
        group.throughput(Throughput::Elements(10_000));
        group.bench_with_input(
            BenchmarkId::new("throughput_by_budget", &label),
            &budget,
            |b, &budget| {
                b.iter_batched(
                    || {
                        let rt = Runtime::new(RuntimeConfig {
                            max_actors: 200,
                            channel_buffer_size: 11_000,
                            num_threads: 1,
                            actor_message_budget: budget,
                            ..Default::default()
                        });
                        let mut addrs = Vec::new();
                        for _ in 0..10 {
                            addrs.push(rt.spawn(SinkActor).unwrap());
                        }
                        rt.tick();
                        for &addr in &addrs {
                            for i in 0..1_000 {
                                rt.send_to(addr, CountMessage(i as u64)).unwrap();
                            }
                        }
                        rt
                    },
                    |rt| {
                        for _ in 0..500 {
                            rt.tick();
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Message size sensitivity benchmarks
// ---------------------------------------------------------------------------

/// Payload message of configurable size
#[derive(Clone)]
struct SizedMessage {
    _payload: Vec<u8>,
}

struct SizedSinkActor;

impl ActorInterface for SizedSinkActor {
    type Incoming = SizedMessage;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: SizedMessage) {}
}

fn message_size_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("msg_size");

    // Throughput sensitivity to message size (8B, 64B, 256B, 1KB, 4KB)
    for size in [8usize, 64, 256, 1024, 4096] {
        let n = 10_000usize;
        group.throughput(Throughput::Bytes((n * size) as u64));
        group.bench_with_input(
            BenchmarkId::new("throughput", format!("{size}B")),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = Runtime::new(make_config(100, n + 100));
                        let addr = rt.spawn(SizedSinkActor).unwrap();
                        rt.tick();
                        let msg = SizedMessage { _payload: vec![0u8; size] };
                        for _ in 0..n {
                            rt.send_to(addr, msg.clone()).unwrap();
                        }
                        rt
                    },
                    |rt| {
                        for _ in 0..500 {
                            rt.tick();
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    // Send latency sensitivity to message size
    for size in [8usize, 64, 256, 1024, 4096] {
        group.bench_with_input(
            BenchmarkId::new("send_latency", format!("{size}B")),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = Runtime::new(make_config(100, 100_000));
                        let addr = rt.spawn(SizedSinkActor).unwrap();
                        rt.tick();
                        let msg = SizedMessage { _payload: vec![0u8; size] };
                        (rt, addr, msg)
                    },
                    |(rt, addr, msg)| {
                        rt.send_to(addr, msg).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Contention benchmarks (many-to-one fanin)
// ---------------------------------------------------------------------------

fn contention_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention");

    // Many actors sending to one sink (fanin pattern)
    for num_senders in [1usize, 10, 50, 100] {
        let msgs_per_sender = 100usize;
        let total = num_senders * msgs_per_sender;
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::new("fanin", format!("{num_senders}_senders")),
            &num_senders,
            |b, &num_senders| {
                b.iter_batched(
                    || {
                        let rt = Runtime::new(RuntimeConfig {
                            max_actors: num_senders + 100,
                            channel_buffer_size: total + 100,
                            num_threads: 1,
                            ..Default::default()
                        });
                        let sink = rt.spawn(SinkActor).unwrap();
                        // Create sender actors that forward to the sink
                        let senders: Vec<_> = (0..num_senders)
                            .map(|_| rt.spawn(NoopActor).unwrap())
                            .collect();
                        rt.tick(); // register all actors

                        // Each "sender" just contributes messages aimed at the sink
                        for _ in &senders {
                            for i in 0..msgs_per_sender {
                                rt.send_to(sink, CountMessage(i as u64)).unwrap();
                            }
                        }
                        rt
                    },
                    |rt| {
                        for _ in 0..200 {
                            rt.tick();
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    // Cross-worker vs same-worker delivery comparison
    for num_threads in [1usize, 2, 4] {
        let n = 10_000usize;
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("cross_worker", format!("{num_threads}t")),
            &num_threads,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let total = iters as usize * n;
                    let rt = Runtime::new(RuntimeConfig {
                        num_threads,
                        max_actors: 100,
                        channel_buffer_size: total + 1024,
                        ..Default::default()
                    });
                    let inbox = rt.new_inbox::<PongMessage>().unwrap();
                    let addr = rt.spawn(EchoActor).unwrap();
                    for _ in 0..total {
                        rt.send_to(addr, PingMessage { reply_to: *inbox.addr() }).unwrap();
                    }
                    if num_threads < 2 {
                        let start = std::time::Instant::now();
                        for _ in 0..(total * 2) {
                            rt.tick();
                        }
                        start.elapsed()
                    } else {
                        let start = std::time::Instant::now();
                        let handle = rt.run().unwrap();
                        let mut received = 0u64;
                        let deadline = std::time::Instant::now()
                            + std::time::Duration::from_secs(30);
                        while received < iters {
                            if inbox.try_recv().is_some() {
                                received += 1;
                            } else if std::time::Instant::now() > deadline {
                                panic!("Timed out");
                            } else {
                                std::hint::spin_loop();
                            }
                        }
                        let elapsed = start.elapsed();
                        handle.shutdown();
                        handle.join();
                        elapsed
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Placement benchmarks — measure spawn distribution quality under load
// ---------------------------------------------------------------------------

fn placement_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("placement");
    group.sample_size(20);

    // Measure spawn+process throughput under imbalanced load across workers
    for &num_threads in &[2, 4] {
        group.bench_with_input(
            BenchmarkId::new("spawn_under_load", num_threads),
            &num_threads,
            |b, &threads| {
                b.iter_custom(|iters| {
                    let rt = Runtime::new(RuntimeConfig {
                        num_threads: threads,
                        ..Default::default()
                    });

                    // Pre-spawn some actors and send them messages to create load
                    let mut addrs = Vec::new();
                    for _ in 0..20 {
                        addrs.push(rt.spawn(NoopActor).unwrap());
                    }
                    let handle = rt.run().unwrap();

                    // Create imbalanced load: flood first few actors
                    for addr in &addrs[..5] {
                        for _ in 0..200 {
                            let _ = handle.runtime.send_to(*addr, NoopMessage);
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(5));

                    // Now measure spawning new actors under this load
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let _ = handle.runtime.spawn(NoopActor);
                    }
                    let elapsed = start.elapsed();

                    handle.shutdown();
                    handle.join();
                    elapsed
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Registry benchmarks — named actors, groups, monitors, ask
// ---------------------------------------------------------------------------

fn registry_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("registry");

    // R1 — Named spawn + lookup roundtrip
    group.bench_function("named_spawn_lookup", |b| {
        let mut counter = 0u64;
        b.iter_batched(
            || {
                counter += 1;
                let rt = Runtime::new(make_config(1_000, 1_000))
                    .with_extension(Arc::new(StdExtension::new()));
                (rt, counter)
            },
            |(rt, i)| {
                let name = format!("actor-{i}");
                let addr = rt.spawn_named(&name, NoopActor).unwrap();
                let found = rt.where_is(&name);
                assert_eq!(found, Some(addr));
            },
            BatchSize::SmallInput,
        );
    });

    // R2 — where_is lookup latency (populated registry)
    group.bench_function("where_is_100_names", |b| {
        b.iter_batched(
            || {
                let rt = Runtime::new(make_config(1_000, 1_000))
                    .with_extension(Arc::new(StdExtension::new()));
                for i in 0..100 {
                    rt.spawn_named(format!("actor-{i}"), NoopActor).unwrap();
                }
                rt
            },
            |rt| {
                // Lookup a name in the middle
                rt.where_is("actor-50");
            },
            BatchSize::SmallInput,
        );
    });

    // R3 — Group join + publish broadcast
    for members in [10, 50, 100] {
        group.throughput(Throughput::Elements(members as u64));
        group.bench_with_input(
            BenchmarkId::new("group_publish", members),
            &members,
            |b, &members| {
                b.iter_batched(
                    || {
                        let rt = Runtime::new(make_config(members + 100, members * 10))
                            .with_extension(Arc::new(StdExtension::new()));
                        for _ in 0..members {
                            let addr = rt.spawn(SinkActor).unwrap();
                            rt.join_group(addr, "bench-group");
                        }
                        rt.tick();
                        rt
                    },
                    |rt| {
                        rt.publish_to("bench-group", CountMessage(42));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    // R4 — Monitor setup + teardown
    group.bench_function("monitor_setup", |b| {
        b.iter_batched(
            || {
                let rt = Runtime::new(make_config(1_000, 1_000));
                let target = rt.spawn(NoopActor).unwrap();
                rt.tick();
                (rt, target)
            },
            |(rt, target)| {
                use swactor::actor::Down;
                let inbox = rt.new_inbox::<Down>().unwrap();
                // We can't call ctx.monitor from outside, but we can benchmark
                // the registry operations indirectly via spawn+stop+tick
                let _ = inbox.addr();
                let _ = rt.stop_actor(target);
            },
            BatchSize::SmallInput,
        );
    });

    // R5 — Ask pattern roundtrip
    group.bench_function("ask_roundtrip", |b| {
        b.iter_batched(
            || {
                let rt = Runtime::new(make_config(1_000, 1_000));
                let addr = rt.spawn(EchoActor).unwrap();
                rt.tick();
                (rt, addr)
            },
            |(rt, addr)| {
                let resp = rt
                    .ask::<PingMessage, PongMessage>(addr, |reply_to| PingMessage { reply_to })
                    .unwrap()
                    .recv_ticking(&rt, 10)
                    .unwrap();
                std::hint::black_box(resp);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Allocation decomposition — where does send_to time go?
// ---------------------------------------------------------------------------

fn allocation_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocation");

    // D1 — Bare Box allocation + type erasure (no runtime, no channels)
    for size in [0usize, 64, 256, 1024, 4096] {
        let label = if size == 0 { "zero".to_string() } else { format!("{size}B") };
        group.bench_with_input(
            BenchmarkId::new("box_alloc_erase", &label),
            &size,
            |b, &size| {
                b.iter(|| {
                    let msg: Box<dyn std::any::Any + Send> = if size == 0 {
                        Box::new(NoopMessage)
                    } else {
                        Box::new(SizedMessage { _payload: vec![0u8; size] })
                    };
                    std::hint::black_box(msg);
                });
            },
        );
    }

    // D2 — Full send_to for comparison (same sizes as D1)
    for size in [0usize, 64, 256, 1024, 4096] {
        let label = if size == 0 { "zero".to_string() } else { format!("{size}B") };
        group.bench_with_input(
            BenchmarkId::new("full_send_to", &label),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let rt = Runtime::new(make_config(100, 100_000));
                        let addr = if size == 0 {
                            rt.spawn(NoopActor).unwrap()
                        } else {
                            rt.spawn(SizedSinkActor).unwrap()
                        };
                        rt.tick();
                        (rt, addr, size)
                    },
                    |(rt, addr, sz)| {
                        if sz == 0 {
                            rt.send_to(addr, NoopMessage).unwrap();
                        } else {
                            rt.send_to(addr, SizedMessage { _payload: vec![0u8; sz] }).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    latency_benchmarks,
    throughput_benchmarks,
    fairness_benchmarks,
    message_size_benchmarks,
    contention_benchmarks,
    placement_benchmarks,
    registry_benchmarks,
    allocation_benchmarks,
);
criterion_main!(benches);
