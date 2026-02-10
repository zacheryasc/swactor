use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use swactor::{
    actor::{ActorAddress, ActorInterface},
    config::RuntimeConfig,
    runtime::{Ctx, Runtime},
};

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

criterion_group!(benches, latency_benchmarks, throughput_benchmarks);
criterion_main!(benches);
