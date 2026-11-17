use std::hint::black_box;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_benchmarks::{SetupPolicy, WorkUnits, Workload, codec, telemetry, validate};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

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

fn main() {
    let mut criterion = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1))
        .configure_from_args();
    let engine = Engine::new(
        RuntimeParts::new(RuntimeConfig {
            worker_count: 4,
            ..RuntimeConfig::default()
        }),
        TokioBackend::new(TokioConfig {
            worker_threads: 4,
            ..TokioConfig::default()
        })
        .expect("benchmark Tokio backend"),
    )
    .expect("benchmark engine");
    let tasks = engine.task_spawner();
    let blocking_work = engine.handle().blocking_work_sender();
    let mut group = criterion.benchmark_group("swactor");
    for workload in codec::workloads() {
        register(&mut group, &workload);
    }
    for workload in telemetry::workloads(tasks, blocking_work) {
        register(&mut group, &workload);
    }
    group.finish();
    Criterion::default().configure_from_args().final_summary();
}
