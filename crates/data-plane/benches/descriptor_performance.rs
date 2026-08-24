#![cfg(target_os = "linux")]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::byte_ring::{ByteRingSpec, Endpoint, RecordKind, Role, attach, install};

const PAYLOAD_LEN: usize = 64 * 1024;

fn endpoints() -> (ArenaManager, Endpoint, Endpoint) {
    let mut arena = ArenaManager::boot(ArenaConfig {
        node_id: NodeId(91),
        reservation_ceiling: 2 << 20,
        base_alignment: 64,
    })
    .expect("benchmark arena");
    let handle = install(
        &mut arena,
        ByteRingSpec {
            capacity: (PAYLOAD_LEN + 5) as u64,
            generation: 1,
            alignment: 64,
            request_id: 1,
        },
    )
    .expect("benchmark ring");
    let producer = attach(&arena, handle, Role::Producer).expect("benchmark producer");
    let consumer = attach(&arena, handle, Role::Consumer).expect("benchmark consumer");
    (arena, producer, consumer)
}

fn descriptor_stream_throughput(criterion: &mut Criterion) {
    let payload = vec![0x5a_u8; PAYLOAD_LEN];
    let mut destination = vec![0_u8; PAYLOAD_LEN];
    let (_arena, mut producer, mut consumer) = endpoints();
    let mut group = criterion.benchmark_group("descriptor_stream_read");
    group.throughput(Throughput::Bytes(PAYLOAD_LEN as u64));

    group.bench_with_input(
        BenchmarkId::new("partial_allocation_free", PAYLOAD_LEN),
        &PAYLOAD_LEN,
        |bencher, _| {
            bencher.iter(|| {
                producer
                    .send_record(RecordKind::Data, &payload)
                    .expect("publish payload");
                let cursor = consumer
                    .record_cursor()
                    .expect("read cursor")
                    .expect("committed record");
                let count = consumer
                    .copy_record_range(cursor, 0, &mut destination)
                    .expect("copy payload");
                consumer
                    .release_record_cursor(cursor)
                    .expect("release payload");
                criterion::black_box(count)
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("legacy_allocating_record", PAYLOAD_LEN),
        &PAYLOAD_LEN,
        |bencher, _| {
            bencher.iter(|| {
                producer
                    .send_record(RecordKind::Data, &payload)
                    .expect("publish payload");
                let (_, bytes) = consumer
                    .recv_record()
                    .expect("receive payload")
                    .expect("committed record");
                criterion::black_box(bytes)
            });
        },
    );
    group.finish();
}

criterion_group!(benches, descriptor_stream_throughput);
criterion_main!(benches);
