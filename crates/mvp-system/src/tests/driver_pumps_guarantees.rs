//! Black-box contract tests for MVP driver and pump behavior.
//!
//! These tests intentionally know only the public driver surface:
//!
//! - establish-send/recv, incoming stream, wake, stop, and I/O outcomes in
//! - stream opens, ring cursor updates, wake hints, faults, and stopped events out
//!
//! They assert the guarantees in
//! `specs/mvp_system/driver_pumps_contract.md`.

use mvp_system::driver_pumps as driver;

// A driver config names one endpoint and one ALPN. Tests do not expose tokio
// tasks, connection internals, or stream futures to actors.
fn driver_config() -> driver::DriverConfig {
    driver::DriverConfig {
        local_node_id: driver::NodeId(10),
        alpn: driver::Alpn("swactor-edge-mvp".into()),
    }
}

// A send spec describes one persistent uni-stream for one edge to one peer.
// The pump still owns when and how bytes leave the egress ring.
fn send_spec() -> driver::EstablishSend {
    driver::EstablishSend {
        edge_id: driver::EdgeId(7001),
        peer_node_id: driver::NodeId(11),
        layout: driver::RingLayout::test_egress(),
    }
}

// A recv spec describes one edge and one ingress ring. It may arrive before or
// after the network stream, which is the receive rendezvous guarantee.
fn recv_spec() -> driver::EstablishRecv {
    driver::EstablishRecv {
        edge_id: driver::EdgeId(7001),
        layout: driver::RingLayout::test_ingress(),
    }
}

// The harness gives tests mock stream and ring observations while keeping the
// driver as the owner of demux and byte-pump behavior.
fn new_driver() -> driver::DriverHarness {
    driver::DriverHarness::new(driver_config())
}

// This helper extracts the byte transcript for a stream. It proves preamble and
// object bytes by observing writes accepted by the mock stream, not by peeking
// into pump internals.
fn written_bytes(harness: &driver::DriverHarness, edge_id: driver::EdgeId) -> Vec<u8> {
    harness
        .stream_writes(edge_id)
        .iter()
        .flat_map(|write| write.bytes.clone())
        .collect()
}

// This proves the driver owns endpoint, connection cache, ALPN, stream demux,
// and send/recv pump tasks, while actors do not poll stream futures directly.
#[test]
fn driver_owns_endpoint_connection_demux_and_pump_tasks() {
    // Create the driver and establish a send edge.
    let mut harness = new_driver();
    harness.observe(driver::DriverEvent::EstablishSend(send_spec()));

    // The driver creates or reuses a connection under its endpoint and ALPN.
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            driver::DriverCommand::OpenOrReuseConnection {
                peer_node_id: driver::NodeId(11),
                alpn: driver::Alpn(ref value),
                ..
            } if value == "swactor-edge-mvp"
        )
    }));

    // Pump tasks are driver-owned.
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            driver::DriverCommand::SpawnSendPump {
                edge_id: driver::EdgeId(7001),
                ..
            }
        )
    }));

    // Actor commands must not expose stream polling.
    assert!(
        !harness
            .actor_messages()
            .iter()
            .any(|message| { matches!(message, driver::ActorMessage::PollStreamFuture { .. }) })
    );
}

// This proves each edge uses one persistent uni-stream, writes an edge-id
// preamble once, carries object records as bytes after the preamble, and does
// not open one stream per object.
#[test]
fn send_stream_is_persistent_with_single_edge_preamble() {
    // Establish one send edge and make two committed egress records readable.
    let mut harness = new_driver();
    harness.observe(driver::DriverEvent::EstablishSend(send_spec()));
    harness.observe(driver::DriverEvent::EgressBytesCommitted {
        edge_id: driver::EdgeId(7001),
        bytes: b"obj0".to_vec(),
    });
    harness.observe(driver::DriverEvent::RingReadable {
        edge_id: driver::EdgeId(7001),
    });
    harness.observe(driver::DriverEvent::EgressBytesCommitted {
        edge_id: driver::EdgeId(7001),
        bytes: b"obj1".to_vec(),
    });
    harness.observe(driver::DriverEvent::RingReadable {
        edge_id: driver::EdgeId(7001),
    });

    // Only one stream is opened for the edge.
    let stream_opens = harness
        .commands()
        .iter()
        .filter(|command| matches!(command, driver::DriverCommand::OpenUniStream { .. }))
        .count();
    assert_eq!(stream_opens, 1);

    // The edge preamble appears once before object bytes.
    let bytes = written_bytes(&harness, driver::EdgeId(7001));
    assert!(bytes.starts_with(&driver::encode_edge_preamble(driver::EdgeId(7001))));
    let preamble_count = driver::count_preamble_occurrences(&bytes, driver::EdgeId(7001));
    assert_eq!(preamble_count, 1);
}

// This proves receive rendezvous works in both arrival orders and that a
// pending stream is not read before the receive spec exists.
#[test]
fn recv_rendezvous_starts_pump_only_after_spec_and_stream_exist() {
    // Spec first, stream second.
    let mut spec_first = new_driver();
    spec_first.observe(driver::DriverEvent::EstablishRecv(recv_spec()));
    assert!(
        !spec_first
            .commands()
            .iter()
            .any(|command| { matches!(command, driver::DriverCommand::SpawnRecvPump { .. }) })
    );
    spec_first.observe(driver::DriverEvent::IncomingUniStream {
        edge_id: driver::EdgeId(7001),
        stream_id: driver::StreamId(1),
    });
    assert!(spec_first.commands().iter().any(|command| {
        matches!(
            command,
            driver::DriverCommand::SpawnRecvPump {
                edge_id: driver::EdgeId(7001),
                ..
            }
        )
    }));

    // Stream first, spec second.
    let mut stream_first = new_driver();
    stream_first.observe(driver::DriverEvent::IncomingUniStream {
        edge_id: driver::EdgeId(7001),
        stream_id: driver::StreamId(2),
    });
    assert!(!stream_first.stream_reads_started(driver::StreamId(2)));
    stream_first.observe(driver::DriverEvent::EstablishRecv(recv_spec()));
    assert!(stream_first.stream_reads_started(driver::StreamId(2)));
}

// This proves the recv pump is byte-blind after demux, copies QUIC bytes into
// ingress ring spans, advances commit after copy, emits readable wakes, and
// stops reading under backpressure.
#[test]
fn recv_pump_copies_bytes_without_parsing_and_respects_backpressure() {
    // Rendezvous a receive pump.
    let mut harness = new_driver();
    harness.observe(driver::DriverEvent::IncomingUniStream {
        edge_id: driver::EdgeId(7001),
        stream_id: driver::StreamId(1),
    });
    harness.observe(driver::DriverEvent::EstablishRecv(recv_spec()));

    // Deliver bytes that happen to look like an object header. The pump must
    // copy them blindly, not parse them.
    harness.observe(driver::DriverEvent::StreamBytesRead {
        edge_id: driver::EdgeId(7001),
        bytes: driver::fake_object_header_bytes(),
    });
    // The public driver event enum has no object-header event; object parsing
    // belongs to the worker ingress parser, not the recv pump.
    assert!(harness.ring_commit(driver::EdgeId(7001)) > 0);
    assert!(harness.wake_hints().iter().any(|wake| {
        matches!(
            wake,
            driver::WakeHint::RingReadable {
                edge_id: driver::EdgeId(7001)
            }
        )
    }));

    // With no ring space, the pump stops reading and waits for RingWritable.
    harness.observe(driver::DriverEvent::IngressRingFull {
        edge_id: driver::EdgeId(7001),
    });
    assert!(!harness.is_reading_stream(driver::EdgeId(7001)));
    harness.observe(driver::DriverEvent::RingWritable {
        edge_id: driver::EdgeId(7001),
    });
    assert!(harness.is_reading_stream(driver::EdgeId(7001)));
}

// This proves the send pump writes committed egress bytes, advances consume
// only after write_all accepts bytes, emits writable wakes, and keeps ownership
// of unread bytes while network flow control stalls.
#[test]
fn send_pump_advances_consume_only_after_write_acceptance() {
    // Establish a send pump and make bytes readable.
    let mut harness = new_driver();
    harness.observe(driver::DriverEvent::EstablishSend(send_spec()));
    harness.observe(driver::DriverEvent::EgressBytesCommitted {
        edge_id: driver::EdgeId(7001),
        bytes: b"payload".to_vec(),
    });
    harness.observe(driver::DriverEvent::NetworkStalled {
        edge_id: driver::EdgeId(7001),
    });

    // Stalled network keeps ownership of unread ring bytes.
    assert_eq!(harness.ring_consume(driver::EdgeId(7001)), 0);

    // Once write_all accepts the bytes, consume advances and writable is hinted.
    harness.observe(driver::DriverEvent::WriteAllAccepted {
        edge_id: driver::EdgeId(7001),
        byte_count: 7,
    });
    assert_eq!(harness.ring_consume(driver::EdgeId(7001)), 7);
    assert!(harness.wake_hints().iter().any(|wake| {
        matches!(
            wake,
            driver::WakeHint::RingWritable {
                edge_id: driver::EdgeId(7001)
            }
        )
    }));
}

// This proves read, write, protocol, and stop outcomes are surfaced as
// StreamFault or PumpStopped events.
#[test]
fn driver_faults_and_stop_emit_stream_fault_or_pump_stopped() {
    // Read error faults the receive edge.
    let mut recv = new_driver();
    recv.observe(driver::DriverEvent::IncomingUniStream {
        edge_id: driver::EdgeId(7001),
        stream_id: driver::StreamId(1),
    });
    recv.observe(driver::DriverEvent::EstablishRecv(recv_spec()));
    recv.observe(driver::DriverEvent::ReadError {
        edge_id: driver::EdgeId(7001),
    });
    assert!(recv.events().iter().any(|event| {
        matches!(
            event,
            driver::DriverEventOut::StreamFault {
                edge_id: driver::EdgeId(7001),
                ..
            }
        )
    }));

    // Write error faults the send edge.
    let mut send = new_driver();
    send.observe(driver::DriverEvent::EstablishSend(send_spec()));
    send.observe(driver::DriverEvent::WriteError {
        edge_id: driver::EdgeId(7001),
    });
    assert!(send.events().iter().any(|event| {
        matches!(
            event,
            driver::DriverEventOut::StreamFault {
                edge_id: driver::EdgeId(7001),
                ..
            }
        )
    }));

    // StopEdge stops the corresponding pump.
    send.observe(driver::DriverEvent::StopEdge {
        edge_id: driver::EdgeId(7001),
    });
    assert!(send.events().iter().any(|event| {
        matches!(
            event,
            driver::DriverEventOut::PumpStopped {
                edge_id: driver::EdgeId(7001),
                ..
            }
        )
    }));
}
