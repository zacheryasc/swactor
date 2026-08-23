#![cfg(target_os = "linux")]

use std::sync::Arc;

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::byte_ring::{ByteRingSpec, RecordKind, Role, attach, install};
use data_plane::namespace::StreamIncarnation;
use data_plane::stream_transport::{
    LocalStreamTransport, StreamSinkRequest, StreamSourceRequest, StreamTransport,
    StreamTransportEvent, StreamTransportNotifier,
};
use parking_lot::Mutex;
use proptest::prelude::*;

#[derive(Default)]
struct Events(Mutex<Vec<StreamTransportEvent>>);

impl StreamTransportNotifier for Events {
    fn notify(&self, event: StreamTransportEvent) {
        self.0.lock().push(event);
    }
}

fn arena(node: u64) -> ArenaManager {
    ArenaManager::boot(ArenaConfig {
        node_id: NodeId(node),
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .expect("arena")
}

#[test]
fn local_transport_preserves_order_backpressure_and_eof() {
    let mut source_arena = arena(1);
    let source_handle = install(
        &mut source_arena,
        ByteRingSpec {
            capacity: 32,
            generation: 1,
            alignment: 64,
            request_id: 1,
        },
    )
    .expect("source ring");
    let mut destination_arena = arena(2);
    let destination_handle = install(
        &mut destination_arena,
        ByteRingSpec {
            capacity: 16,
            generation: 2,
            alignment: 64,
            request_id: 2,
        },
    )
    .expect("destination ring");

    let mut writer = attach(&source_arena, source_handle, Role::Producer).expect("writer");
    let source_pump = attach(&source_arena, source_handle, Role::Consumer).expect("source pump");
    let destination_pump =
        attach(&destination_arena, destination_handle, Role::Producer).expect("destination pump");
    let mut reader =
        attach(&destination_arena, destination_handle, Role::Consumer).expect("reader");

    let transport = LocalStreamTransport::new();
    let incarnation = StreamIncarnation {
        authority_epoch: 7,
        revision: 11,
    };
    let source_events = Arc::new(Events::default());
    let sink_events = Arc::new(Events::default());
    transport
        .install_sink(StreamSinkRequest {
            incarnation,
            endpoint: destination_pump,
            notifier: sink_events.clone(),
        })
        .expect("install sink");
    transport
        .install_source(StreamSourceRequest {
            incarnation,
            peer: transport.descriptor().expect("descriptor"),
            endpoint: source_pump,
            notifier: source_events.clone(),
        })
        .expect("install source");
    assert!(
        source_events
            .0
            .lock()
            .contains(&StreamTransportEvent::Ready)
    );
    assert!(sink_events.0.lock().contains(&StreamTransportEvent::Ready));

    writer
        .send_record(RecordKind::Data, b"first")
        .expect("write first");
    transport.source_progress(incarnation);
    writer
        .send_record(RecordKind::Data, b"next!")
        .expect("write second");
    transport.source_progress(incarnation);

    assert_eq!(
        reader.recv_record().expect("read first"),
        Some((RecordKind::Data, b"first".to_vec()))
    );
    assert_eq!(reader.recv_record().expect("second remains upstream"), None);
    transport.sink_progress(incarnation);
    assert_eq!(
        reader.recv_record().expect("read second"),
        Some((RecordKind::Data, b"next!".to_vec()))
    );

    writer.send_record(RecordKind::Eof, b"").expect("write eof");
    transport.source_progress(incarnation);
    assert_eq!(
        reader.recv_record().expect("read eof"),
        Some((RecordKind::Eof, Vec::new()))
    );
}

#[test]
fn local_transport_rejects_a_descriptor_from_another_backend() {
    let first = LocalStreamTransport::new();
    let second = LocalStreamTransport::new();
    assert_ne!(
        first.descriptor().expect("first descriptor"),
        second.descriptor().expect("second descriptor")
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn randomized_payloads_preserve_exact_bytes(
        payload in prop::collection::vec(any::<u8>(), 0..4096),
        raw_capacity in 16_u8..128,
        chunk_seeds in prop::collection::vec(1_u8..=255, 1..32),
    ) {
        let capacity = u64::from(raw_capacity);
        let mut source_arena = arena(21);
        let source_handle = install(
            &mut source_arena,
            ByteRingSpec {
                capacity,
                generation: 1,
                alignment: 64,
                request_id: 1,
            },
        ).expect("source ring");
        let mut destination_arena = arena(22);
        let destination_handle = install(
            &mut destination_arena,
            ByteRingSpec {
                capacity,
                generation: 2,
                alignment: 64,
                request_id: 2,
            },
        ).expect("destination ring");
        let mut writer = attach(&source_arena, source_handle, Role::Producer).expect("writer");
        let source_pump = attach(&source_arena, source_handle, Role::Consumer).expect("source pump");
        let destination_pump =
            attach(&destination_arena, destination_handle, Role::Producer).expect("destination pump");
        let mut reader =
            attach(&destination_arena, destination_handle, Role::Consumer).expect("reader");
        let transport = LocalStreamTransport::new();
        let incarnation = StreamIncarnation {
            authority_epoch: 3,
            revision: 9,
        };
        transport.install_sink(StreamSinkRequest {
            incarnation,
            endpoint: destination_pump,
            notifier: Arc::new(Events::default()),
        }).expect("sink");
        transport.install_source(StreamSourceRequest {
            incarnation,
            peer: transport.descriptor().expect("descriptor"),
            endpoint: source_pump,
            notifier: Arc::new(Events::default()),
        }).expect("source");

        let max_chunk = capacity as usize - 5;
        let mut offset = 0;
        let mut seed_index = 0;
        let mut observed = Vec::new();
        while offset < payload.len() {
            let chunk_len = usize::from(chunk_seeds[seed_index % chunk_seeds.len()])
                .min(max_chunk)
                .min(payload.len() - offset);
            seed_index += 1;
            loop {
                match writer.send_record(
                    RecordKind::Data,
                    &payload[offset..offset + chunk_len],
                ) {
                    Ok(()) => break,
                    Err(data_plane::byte_ring::FlowError::InsufficientSpace { .. }) => {
                        if let Some((RecordKind::Data, bytes)) =
                            reader.recv_record().expect("drain destination")
                        {
                            observed.extend(bytes);
                            transport.sink_progress(incarnation);
                        } else {
                            transport.source_progress(incarnation);
                        }
                    }
                    Err(error) => panic!("unexpected source error: {error:?}"),
                }
            }
            offset += chunk_len;
            transport.source_progress(incarnation);
        }

        loop {
            match writer.send_record(RecordKind::Eof, &[]) {
                Ok(()) => break,
                Err(data_plane::byte_ring::FlowError::InsufficientSpace { .. }) => {
                    if let Some((RecordKind::Data, bytes)) =
                        reader.recv_record().expect("drain for eof")
                    {
                        observed.extend(bytes);
                        transport.sink_progress(incarnation);
                    } else {
                        transport.source_progress(incarnation);
                    }
                }
                Err(error) => panic!("unexpected eof error: {error:?}"),
            }
        }
        transport.source_progress(incarnation);

        let mut steps = 0;
        loop {
            steps += 1;
            prop_assert!(steps < payload.len() + 1024, "transfer made no bounded progress");
            match reader.recv_record().expect("receive") {
                Some((RecordKind::Data, bytes)) => {
                    observed.extend(bytes);
                    transport.sink_progress(incarnation);
                }
                Some((RecordKind::Eof, _)) => break,
                Some((RecordKind::Fault, bytes)) => {
                    return Err(TestCaseError::fail(format!(
                        "unexpected fault record: {bytes:?}"
                    )));
                }
                None => transport.source_progress(incarnation),
            }
        }
        prop_assert_eq!(observed, payload);
    }
}
