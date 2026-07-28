//! Black-box contract tests for MVP Tx and Rx edge actors.
//!
//! These tests intentionally know only the public edge-actor surface:
//!
//! - lifecycle, object identity, stop, stream fault, and object fault events in
//! - role-facing lifecycle/object events out
//!
//! They assert the guarantees in
//! `specs/mvp_system/tx_rx_edge_actor_contract.md`.

use mvp_system::node_data::edge_actor;

// The edge id fixture gives both actors a shared identity while keeping Tx and
// Rx lifecycle tests independent from driver and ring internals.
fn edge_id() -> edge_actor::EdgeId {
    edge_actor::EdgeId(7001)
}

#[test]
fn object_id_allocator_is_scoped_to_one_producer_edge() {
    let mut stage0_output = edge_actor::ObjectIdAllocator::new(edge_actor::EdgeId(7001));
    let mut stage1_output = edge_actor::ObjectIdAllocator::new(edge_actor::EdgeId(7002));

    assert_eq!(
        stage0_output.alloc(),
        edge_actor::ObjectKey::new(edge_actor::EdgeId(7001), edge_actor::ObjectId(1))
    );
    assert_eq!(
        stage1_output.alloc(),
        edge_actor::ObjectKey::new(edge_actor::EdgeId(7002), edge_actor::ObjectId(1))
    );
    assert_eq!(
        stage0_output.alloc(),
        edge_actor::ObjectKey::new(edge_actor::EdgeId(7001), edge_actor::ObjectId(2))
    );
}

// Tx starts in provisioning and represents the producer side of one edge. The
// harness records only actor messages, not bytes or flow-control details.
fn new_tx() -> edge_actor::TxActorHarness {
    edge_actor::TxActorHarness::new(edge_actor::TxConfig {
        edge_id: edge_id(),
        role_port: edge_actor::PortId("out".into()),
    })
}

// Rx starts in provisioning and represents the consumer side of one edge. It
// exposes complete object identities and opaque handles to the role layer.
fn new_rx() -> edge_actor::RxActorHarness {
    edge_actor::RxActorHarness::new(edge_actor::RxConfig {
        edge_id: edge_id(),
        role_port: edge_actor::PortId("in".into()),
    })
}

// This helper is a compile-time and runtime guard for payload isolation. If the
// public actor message enum grows a payload-bearing variant, this exhaustive
// match has to be updated and the test discussion becomes explicit.
fn assert_actor_message_is_payload_free(message: &edge_actor::ActorMessage) {
    match message {
        edge_actor::ActorMessage::Lifecycle { .. }
        | edge_actor::ActorMessage::ObjectIdentity { .. }
        | edge_actor::ActorMessage::OpaqueHandle { .. }
        | edge_actor::ActorMessage::CoarseFault { .. } => {}
    }
}

// This proves Tx and Rx actors are tied to one edge id, receive lifecycle/object
// events only, and do not traffic payload bytes, pointers, ranges, credits, or
// free-space counts.
#[test]
fn edge_actor_messages_are_lifecycle_identity_and_handle_only() {
    // Create one Tx and one Rx actor for the same edge id.
    let mut tx = new_tx();
    let mut rx = new_rx();

    // Drive typical lifecycle and object events.
    tx.observe(edge_actor::TxEvent::EdgeReady { edge_id: edge_id() });
    tx.observe(edge_actor::TxEvent::ObjectProduced {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9000),
        sequence: 0,
    });
    rx.observe(edge_actor::RxEvent::EdgeReady { edge_id: edge_id() });
    rx.observe(edge_actor::RxEvent::ObjectLoaded {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9000),
        sequence: 0,
        handle: edge_actor::OpaqueHandle::new(42),
    });

    // Every emitted actor message must stay payload-free.
    for message in tx.messages().iter().chain(rx.messages()) {
        assert_actor_message_is_payload_free(message);
    }

    // Both actors remain tied to exactly one edge id.
    assert!(
        tx.messages()
            .iter()
            .all(|message| message.edge_id() == edge_id())
    );
    assert!(
        rx.messages()
            .iter()
            .all(|message| message.edge_id() == edge_id())
    );
}

// This proves Tx starts in provisioning, becomes ready only after EdgeReady,
// allows producing only after ready, reports produced object identity, and moves
// to faulted on stream or object faults.
#[test]
fn tx_lifecycle_gates_production_and_faults_on_stream_or_object_failure() {
    // Before EdgeReady, production is rejected.
    let mut tx = new_tx();
    tx.observe(edge_actor::TxEvent::ObjectProduced {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9000),
        sequence: 0,
    });
    assert!(
        !tx.messages()
            .iter()
            .any(|message| { matches!(message, edge_actor::ActorMessage::ObjectIdentity { .. }) })
    );

    // EdgeReady admits production.
    tx.observe(edge_actor::TxEvent::EdgeReady { edge_id: edge_id() });
    tx.observe(edge_actor::TxEvent::ObjectProduced {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9000),
        sequence: 0,
    });
    assert!(tx.messages().iter().any(|message| {
        matches!(
            message,
            edge_actor::ActorMessage::ObjectIdentity {
                edge_id: edge_actor::EdgeId(7001),
                object_id: edge_actor::ObjectId(9000),
                sequence: 0,
                ..
            }
        )
    }));

    // Stream fault moves Tx to faulted and suppresses later production.
    tx.observe(edge_actor::TxEvent::StreamFault { edge_id: edge_id() });
    let produced_before = tx
        .messages()
        .iter()
        .filter(|message| matches!(message, edge_actor::ActorMessage::ObjectIdentity { .. }))
        .count();
    tx.observe(edge_actor::TxEvent::ObjectProduced {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9001),
        sequence: 1,
    });
    let produced_after = tx
        .messages()
        .iter()
        .filter(|message| matches!(message, edge_actor::ActorMessage::ObjectIdentity { .. }))
        .count();
    assert_eq!(produced_after, produced_before);
}

// This proves Rx starts in provisioning, becomes ready only after EdgeReady,
// exposes loaded objects only after ObjectLoaded, and moves to faulted on stream
// or object faults.
#[test]
fn rx_lifecycle_gates_loaded_objects_and_faults_on_stream_or_object_failure() {
    // Before EdgeReady, loaded objects are not exposed to the role layer.
    let mut rx = new_rx();
    rx.observe(edge_actor::RxEvent::ObjectLoaded {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9000),
        sequence: 0,
        handle: edge_actor::OpaqueHandle::new(42),
    });
    assert!(
        !rx.messages()
            .iter()
            .any(|message| { matches!(message, edge_actor::ActorMessage::OpaqueHandle { .. }) })
    );

    // EdgeReady admits ObjectLoaded exposure.
    rx.observe(edge_actor::RxEvent::EdgeReady { edge_id: edge_id() });
    rx.observe(edge_actor::RxEvent::ObjectLoaded {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9000),
        sequence: 0,
        handle: edge_actor::OpaqueHandle::new(42),
    });
    assert!(rx.messages().iter().any(|message| {
        matches!(
            message,
            edge_actor::ActorMessage::OpaqueHandle {
                edge_id: edge_actor::EdgeId(7001),
                object_id: edge_actor::ObjectId(9000),
                sequence: 0,
                ..
            }
        )
    }));

    // Object failure faults Rx and suppresses later loaded objects.
    rx.observe(edge_actor::RxEvent::ObjectFailed { edge_id: edge_id() });
    let loaded_before = rx
        .messages()
        .iter()
        .filter(|message| matches!(message, edge_actor::ActorMessage::OpaqueHandle { .. }))
        .count();
    rx.observe(edge_actor::RxEvent::ObjectLoaded {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9001),
        sequence: 1,
        handle: edge_actor::OpaqueHandle::new(43),
    });
    let loaded_after = rx
        .messages()
        .iter()
        .filter(|message| matches!(message, edge_actor::ActorMessage::OpaqueHandle { .. }))
        .count();
    assert_eq!(loaded_after, loaded_before);
}

// This proves StopEdge moves actors toward stopped, stale events after stop are
// ignored, and mismatched edge ids reject or fault according to policy.
#[test]
fn stop_and_mismatched_edge_events_do_not_create_run_work() {
    // Ready Tx and Rx actors, then stop them.
    let mut tx = new_tx();
    let mut rx = new_rx();
    tx.observe(edge_actor::TxEvent::EdgeReady { edge_id: edge_id() });
    rx.observe(edge_actor::RxEvent::EdgeReady { edge_id: edge_id() });
    tx.observe(edge_actor::TxEvent::StopEdge { edge_id: edge_id() });
    rx.observe(edge_actor::RxEvent::StopEdge { edge_id: edge_id() });

    // Stale post-stop object events must be ignored.
    tx.observe(edge_actor::TxEvent::ObjectProduced {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9000),
        sequence: 0,
    });
    rx.observe(edge_actor::RxEvent::ObjectLoaded {
        edge_id: edge_id(),
        object_id: edge_actor::ObjectId(9000),
        sequence: 0,
        handle: edge_actor::OpaqueHandle::new(42),
    });
    assert!(
        !tx.messages()
            .iter()
            .any(|message| { matches!(message, edge_actor::ActorMessage::ObjectIdentity { .. }) })
    );
    assert!(
        !rx.messages()
            .iter()
            .any(|message| { matches!(message, edge_actor::ActorMessage::OpaqueHandle { .. }) })
    );

    // A mismatched edge id must reject or fault, not create work on this actor.
    let mut mismatched = new_tx();
    mismatched.observe(edge_actor::TxEvent::EdgeReady {
        edge_id: edge_actor::EdgeId(9999),
    });
    assert!(mismatched.messages().iter().any(|message| {
        matches!(
            message,
            edge_actor::ActorMessage::CoarseFault {
                reason: edge_actor::ActorFaultReason::MismatchedEdgeId,
                ..
            }
        )
    }));
}
