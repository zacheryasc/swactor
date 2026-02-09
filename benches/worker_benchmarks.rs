use std::collections::VecDeque;

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

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

criterion_group!(
    benches,
    vecdeque_push,
    vecdeque_pop,
);
criterion_main!(benches);
