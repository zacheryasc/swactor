use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use swactor::worker::Mailbox;

// ---------------------------------------------------------------------------
// Push throughput
// ---------------------------------------------------------------------------

fn mailbox_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_push");
    for n in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut mb: Mailbox<u64> = Mailbox::new(n);
                for i in 0..n {
                    mb.push(i as u64);
                }
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Pop throughput
// ---------------------------------------------------------------------------

fn mailbox_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_pop");
    for n in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let mut mb: Mailbox<u64> = Mailbox::new(n);
                    for i in 0..n {
                        mb.push(i as u64);
                    }
                    mb
                },
                |mut mb| {
                    for _ in 0..n {
                        std::hint::black_box(mb.pop());
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Interleaved push+pop
// ---------------------------------------------------------------------------

fn mailbox_interleaved(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_interleaved");
    for n in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64 * 2));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut mb: Mailbox<u64> = Mailbox::new(n);
                for i in 0..n {
                    mb.push(i as u64);
                    std::hint::black_box(mb.pop());
                }
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// drain_count O(1) verification
// ---------------------------------------------------------------------------

fn mailbox_drain_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_drain_count");

    // Below waterlevel
    group.bench_function("below", |b| {
        let mut mb: Mailbox<u64> = Mailbox::new(100);
        for i in 0..50 {
            mb.push(i);
        }
        b.iter(|| std::hint::black_box(mb.drain_count()));
    });

    // At waterlevel
    group.bench_function("at", |b| {
        let mut mb: Mailbox<u64> = Mailbox::new(100);
        for i in 0..100 {
            mb.push(i);
        }
        b.iter(|| std::hint::black_box(mb.drain_count()));
    });

    // Above waterlevel
    group.bench_function("above", |b| {
        let mut mb: Mailbox<u64> = Mailbox::new(100);
        for i in 0..500 {
            mb.push(i);
        }
        b.iter(|| std::hint::black_box(mb.drain_count()));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Simulated actor tick: drain_count + pop N
// ---------------------------------------------------------------------------

fn mailbox_actor_tick(c: &mut Criterion) {
    let mut group = c.benchmark_group("mailbox_actor_tick");

    for (wl, fill) in [(10, 5), (10, 10), (10, 50), (100, 200)] {
        let param = format!("wl={wl},fill={fill}");
        group.bench_function(BenchmarkId::from_parameter(&param), |b| {
            b.iter_batched(
                || {
                    let mut mb: Mailbox<u64> = Mailbox::new(wl);
                    for i in 0..fill {
                        mb.push(i as u64);
                    }
                    mb
                },
                |mut mb| {
                    let n = mb.drain_count();
                    for _ in 0..n {
                        std::hint::black_box(mb.pop());
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
    mailbox_push,
    mailbox_pop,
    mailbox_interleaved,
    mailbox_drain_count,
    mailbox_actor_tick,
);
criterion_main!(benches);
