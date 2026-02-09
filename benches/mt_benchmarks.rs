use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use swactor::{
    actor::{ActorAddress, ActorInterface},
    config::{BackoffPolicy, RuntimeConfig},
    runtime::{Ctx, Runtime},
};

// ---------------------------------------------------------------------------
// Config helper
// ---------------------------------------------------------------------------

fn mt_config(threads: usize, max_actors: usize, max_messages: usize) -> RuntimeConfig {
    RuntimeConfig {
        num_threads: threads,
        max_actors,
        actor_max_messages: max_messages,
        backoff_policy: BackoffPolicy {
            spin_threshold: 32,
            yield_threshold: 64,
            sleep_increment_us: 10,
            sleep_max_us: 100,
        },
    }
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct WorkMsg;

#[derive(Clone)]
struct DoneSignal;

#[derive(Clone)]
struct RingMsg {
    hops: u64,
    max_hops: u64,
    reply_to: ActorAddress,
}

// ---------------------------------------------------------------------------
// Actor types
// ---------------------------------------------------------------------------

struct DoneActor {
    target: usize,
    count: usize,
    reply_to: ActorAddress,
}

impl ActorInterface for DoneActor {
    type Incoming = WorkMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: WorkMsg) {
        self.count += 1;
        if self.count % self.target == 0 {
            let _ = ctx.send(self.reply_to, DoneSignal);
        }
    }
}

struct MtRingActor {
    next: ActorAddress,
}

impl ActorInterface for MtRingActor {
    type Incoming = RingMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: RingMsg) {
        if msg.hops >= msg.max_hops {
            let _ = ctx.send(msg.reply_to, DoneSignal);
        } else {
            let _ = ctx.send(
                self.next,
                RingMsg {
                    hops: msg.hops + 1,
                    max_hops: msg.max_hops,
                    reply_to: msg.reply_to,
                },
            );
        }
    }
}

struct NoopActor;

impl ActorInterface for NoopActor {
    type Incoming = WorkMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: WorkMsg) {}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Wait for `n` DoneSignals on the inbox, with a timeout.
fn wait_for_n_done(inbox: &swactor::runtime::Inbox<DoneSignal>, n: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut received = 0u64;
    while received < n {
        if inbox.try_recv().is_some() {
            received += 1;
        } else if Instant::now() > deadline {
            panic!(
                "Timed out waiting for DoneSignal: got {received}/{n} in 30s"
            );
        } else {
            std::hint::spin_loop();
        }
    }
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn mt_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("mt");

    // -- single_actor: one DoneActor receiving all messages ----------------
    for &(threads, n) in &[(2, 10_000), (4, 10_000), (4, 100_000)] {
        let param = format!("{threads}t_{n}");
        group.bench_with_input(
            BenchmarkId::new("single_actor", &param),
            &(threads, n),
            |b, &(threads, n)| {
                b.iter_custom(|iters| {
                    let total_msgs = iters as usize * n;
                    let rt = Runtime::new(mt_config(threads, 64, total_msgs + 1024));
                    let inbox = rt.new_inbox::<DoneSignal>().unwrap();
                    let addr = rt
                        .spawn(DoneActor {
                            target: n,
                            count: 0,
                            reply_to: *inbox.addr(),
                        })
                        .unwrap();
                    for _ in 0..total_msgs {
                        rt.send_to(addr, WorkMsg).unwrap();
                    }
                    let start = Instant::now();
                    let handle = rt.run().unwrap();
                    wait_for_n_done(&inbox, iters);
                    let elapsed = start.elapsed();
                    handle.shutdown();
                    handle.join();
                    elapsed
                });
            },
        );
    }

    // -- multi_actor: fan-out across many DoneActors ----------------------
    for &(threads, actors, msgs_per) in &[(2, 10, 1_000), (4, 10, 1_000), (4, 100, 1_000)] {
        let param = format!("{threads}t_{actors}x{msgs_per}");
        group.bench_with_input(
            BenchmarkId::new("multi_actor", &param),
            &(threads, actors, msgs_per),
            |b, &(threads, actors, msgs_per)| {
                b.iter_custom(|iters| {
                    let total_per_actor = iters as usize * msgs_per;
                    let rt = Runtime::new(mt_config(
                        threads,
                        actors + 64,
                        total_per_actor + 1024,
                    ));
                    let inbox = rt.new_inbox::<DoneSignal>().unwrap();
                    let addrs: Vec<_> = (0..actors)
                        .map(|_| {
                            rt.spawn(DoneActor {
                                target: msgs_per,
                                count: 0,
                                reply_to: *inbox.addr(),
                            })
                            .unwrap()
                        })
                        .collect();
                    for &addr in &addrs {
                        for _ in 0..total_per_actor {
                            rt.send_to(addr, WorkMsg).unwrap();
                        }
                    }
                    let expected_signals = iters * actors as u64;
                    let start = Instant::now();
                    let handle = rt.run().unwrap();
                    wait_for_n_done(&inbox, expected_signals);
                    let elapsed = start.elapsed();
                    handle.shutdown();
                    handle.join();
                    elapsed
                });
            },
        );
    }

    // -- ring: message passes around a ring of actors ---------------------
    for &(threads, ring_size) in &[(2, 100), (4, 100), (4, 500)] {
        let param = format!("{threads}t_{ring_size}");
        group.bench_with_input(
            BenchmarkId::new("ring", &param),
            &(threads, ring_size),
            |b, &(threads, ring_size)| {
                b.iter_custom(|iters| {
                    let rt = Runtime::new(mt_config(threads, ring_size + 64, 1024));
                    let inbox = rt.new_inbox::<DoneSignal>().unwrap();

                    // Build chain backwards: first spawned actor forwards to inbox addr,
                    // each subsequent actor forwards to the previous. Entry = last spawned.
                    // When hops >= max_hops, the actor sends DoneSignal instead of forwarding.
                    let mut next_addr = *inbox.addr();
                    let mut entry_addr = next_addr;
                    for _ in 0..ring_size {
                        let addr = rt
                            .spawn(MtRingActor { next: next_addr })
                            .unwrap();
                        entry_addr = addr;
                        next_addr = addr;
                    }

                    let max_hops = (ring_size - 1) as u64;
                    // Pre-load ring messages
                    for _ in 0..iters {
                        rt.send_to(
                            entry_addr,
                            RingMsg {
                                hops: 0,
                                max_hops,
                                reply_to: *inbox.addr(),
                            },
                        )
                        .unwrap();
                    }

                    let start = Instant::now();
                    let handle = rt.run().unwrap();
                    wait_for_n_done(&inbox, iters);
                    let elapsed = start.elapsed();
                    handle.shutdown();
                    handle.join();
                    elapsed
                });
            },
        );
    }

    // -- spawn: spawn throughput on a running runtime ---------------------
    for &(threads, batch) in &[(2, 1_000), (4, 1_000), (4, 5_000)] {
        let param = format!("{threads}t_{batch}");
        group.bench_with_input(
            BenchmarkId::new("spawn", &param),
            &(threads, batch),
            |b, &(threads, batch)| {
                b.iter_custom(|iters| {
                    let total = iters as usize * batch;
                    let rt = Runtime::new(mt_config(threads, total + 64, 64));
                    let handle = rt.run().unwrap();
                    // Give workers a moment to start
                    std::thread::sleep(Duration::from_millis(1));

                    let start = Instant::now();
                    for _ in 0..total {
                        handle.runtime.spawn(NoopActor).unwrap();
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

criterion_group!(benches, mt_benchmarks);
criterion_main!(benches);
