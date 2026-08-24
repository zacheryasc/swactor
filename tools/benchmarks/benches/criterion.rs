use std::hint::black_box;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use swactor_benchmarks::{SetupPolicy, WorkUnits, Workload, codec, telemetry, validate};

fn register<W>(group: &mut BenchmarkGroup<'_, WallTime>, workload: &W)
where
    W: Workload,
{
    validate(workload);
    match workload.units() {
        WorkUnits::Operations(operations) | WorkUnits::Frames(operations) => {
            group.throughput(Throughput::Elements(operations));
        }
        WorkUnits::Bytes(bytes) => {
            group.throughput(Throughput::Bytes(bytes));
        }
    }

    let batch_size = match workload.setup_policy() {
        SetupPolicy::Batched => BatchSize::LargeInput,
        SetupPolicy::PerExecution => BatchSize::PerIteration,
    };
    group.bench_with_input(
        BenchmarkId::from_parameter(workload.name()),
        workload,
        |bencher, workload| {
            bencher.iter_batched(
                || workload.setup(),
                |mut state| black_box(workload.execute(&mut state)),
                batch_size,
            );
        },
    );
}

fn benchmark_workloads(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("swactor");
    for workload in codec::workloads() {
        register(&mut group, &workload);
    }
    for workload in telemetry::workloads() {
        register(&mut group, &workload);
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = benchmark_workloads
}
criterion_main!(benches);
