pub mod common;

use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::byte_ring::{ByteRingSpec, RecordKind, Role, attach, install};
use data_plane::namespace::StreamIncarnation;
use data_plane::stream_transport::{
    StreamSinkRequest, StreamSourceRequest, StreamTransport, StreamTransportEvent,
    StreamTransportNotifier,
};

use common::iroh::make_driver;

struct ChannelNotifier(Sender<StreamTransportEvent>);

impl StreamTransportNotifier for ChannelNotifier {
    fn notify(&self, event: StreamTransportEvent) {
        let _ = self.0.send(event);
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

fn recv_until(receiver: &Receiver<StreamTransportEvent>, expected: StreamTransportEvent) {
    loop {
        let event = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("transport event deadline");
        if event == expected {
            return;
        }
        if let StreamTransportEvent::Fault(reason) = event {
            panic!("unexpected stream fault: {reason}");
        }
    }
}

#[test]
fn iroh_adapter_satisfies_ordering_and_terminal_contract() {
    let source_node = make_driver();
    let sink_node = make_driver();
    let source_transport = source_node.driver.stream_transport();
    let sink_transport = sink_node.driver.stream_transport();

    let mut source_arena = arena(1);
    let source_handle = install(
        &mut source_arena,
        ByteRingSpec {
            capacity: 128,
            generation: 1,
            alignment: 64,
            request_id: 1,
        },
    )
    .expect("source ring");
    let mut sink_arena = arena(2);
    let sink_handle = install(
        &mut sink_arena,
        ByteRingSpec {
            capacity: 128,
            generation: 2,
            alignment: 64,
            request_id: 2,
        },
    )
    .expect("sink ring");
    let mut writer = attach(&source_arena, source_handle, Role::Producer).expect("writer");
    let source_pump = attach(&source_arena, source_handle, Role::Consumer).expect("source pump");
    let sink_pump = attach(&sink_arena, sink_handle, Role::Producer).expect("sink pump");
    let mut reader = attach(&sink_arena, sink_handle, Role::Consumer).expect("reader");

    let incarnation = StreamIncarnation {
        authority_epoch: 17,
        revision: 23,
    };
    let (source_tx, source_rx) = mpsc::channel();
    let (sink_tx, sink_rx) = mpsc::channel();
    sink_transport
        .install_sink(StreamSinkRequest {
            incarnation,
            endpoint: sink_pump,
            notifier: Arc::new(ChannelNotifier(sink_tx)),
        })
        .expect("install sink");
    source_transport
        .install_source(StreamSourceRequest {
            incarnation,
            peer: sink_transport.descriptor().expect("sink descriptor"),
            endpoint: source_pump,
            notifier: Arc::new(ChannelNotifier(source_tx)),
        })
        .expect("install source");
    recv_until(&source_rx, StreamTransportEvent::Ready);
    recv_until(&sink_rx, StreamTransportEvent::Ready);

    writer
        .send_record(RecordKind::Data, b"one")
        .expect("write one");
    writer
        .send_record(RecordKind::Data, b"two")
        .expect("write two");
    source_transport.source_progress(incarnation);
    recv_until(&sink_rx, StreamTransportEvent::DataAvailable);
    recv_until(&sink_rx, StreamTransportEvent::DataAvailable);
    assert_eq!(
        reader.recv_record().expect("read one"),
        Some((RecordKind::Data, b"one".to_vec()))
    );
    assert_eq!(
        reader.recv_record().expect("read two"),
        Some((RecordKind::Data, b"two".to_vec()))
    );
    sink_transport.sink_progress(incarnation);

    writer.send_record(RecordKind::Eof, &[]).expect("write eof");
    source_transport.source_progress(incarnation);
    recv_until(&sink_rx, StreamTransportEvent::DataAvailable);
    assert_eq!(
        reader.recv_record().expect("read eof"),
        Some((RecordKind::Eof, Vec::new()))
    );
    recv_until(&source_rx, StreamTransportEvent::Quiesced);
    recv_until(&sink_rx, StreamTransportEvent::Quiesced);

    source_transport.terminate(incarnation);
    sink_transport.terminate(incarnation);
}
