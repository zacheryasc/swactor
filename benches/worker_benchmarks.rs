use std::collections::VecDeque;

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use swactor::worker::drain_count;

// ---------------------------------------------------------------------------
// drain_count O(1) verification
// ---------------------------------------------------------------------------

fn bench_drain_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("drain_count");

    // Below waterlevel
    group.bench_function("below", |b| {
        b.iter(|| std::hint::black_box(drain_count(50, 100)));
    });

    // At waterlevel
    group.bench_function("at", |b| {
        b.iter(|| std::hint::black_box(drain_count(100, 100)));
    });

    // Above waterlevel
    group.bench_function("above", |b| {
        b.iter(|| std::hint::black_box(drain_count(500, 100)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// VecDeque push throughput (mirrors old mailbox_push)
// ---------------------------------------------------------------------------

fn vecdeque_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("vecdeque_push");
    for n in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut q: VecDeque<u64> = VecDeque::new();
                for i in 0..n {
                    q.push_back(i as u64);
                }
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// VecDeque pop throughput (mirrors old mailbox_pop)
// ---------------------------------------------------------------------------

fn vecdeque_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("vecdeque_pop");
    for n in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut q: VecDeque<u64> = VecDeque::new();
                    for i in 0..n {
                        q.push_back(i as u64);
                    }
                    q
                },
                |mut q| {
                    for _ in 0..n {
                        std::hint::black_box(q.pop_front());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Simulated actor tick: drain_count + pop N from VecDeque
// ---------------------------------------------------------------------------

fn simulated_actor_tick(c: &mut Criterion) {
    let mut group = c.benchmark_group("simulated_actor_tick");

    for (wl, fill) in [(10, 5), (10, 10), (10, 50), (100, 200)] {
        let param = format!("wl={wl},fill={fill}");
        group.bench_function(BenchmarkId::from_parameter(&param), |b| {
            b.iter_batched(
                || {
                    let mut q: VecDeque<u64> = VecDeque::new();
                    for i in 0..fill {
                        q.push_back(i as u64);
                    }
                    q
                },
                |mut q| {
                    let n = drain_count(q.len(), wl);
                    for _ in 0..n {
                        std::hint::black_box(q.pop_front());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_drain_count,
    vecdeque_push,
    vecdeque_pop,
    simulated_actor_tick,
);
criterion_main!(benches);
