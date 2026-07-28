//! Black-box contract tests for data-plane edge establishment.
//!
//! These tests intentionally know only the public EdgeEstablisher surface:
//!
//! - `ProvisionTx`, `ProvisionRx`, lease, ring-install, driver, stop, and fault
//!   events in
//! - arena, worker/token, driver, ready, fault, and release commands out
//!
//! They assert the guarantees in
//! the reusable data-plane edge lifecycle contract.

use data_plane::edge_lifecycle as edge;

// A send provision carries the consumer node id because the driver must know
// where to send. It deliberately carries no remote actor address.
fn tx_provision() -> edge::ProvisionTx {
    edge::ProvisionTx {
        run_id: edge::RunId(7),
        edge_id: edge::EdgeId(7001),
        local_node_id: edge::NodeId(10),
        consumer_node_id: edge::NodeId(11),
        object_spec: edge::ObjectSpec::test_activation(),
        ring_spec: edge::RingSpec::test_activation(),
    }
}

// A receive provision needs the shared edge id and local layout information
// after leasing; it does not need producer actor addressing for data flow.
fn rx_provision() -> edge::ProvisionRx {
    edge::ProvisionRx {
        run_id: edge::RunId(7),
        edge_id: edge::EdgeId(7001),
        local_node_id: edge::NodeId(11),
        object_spec: edge::ObjectSpec::test_activation(),
        ring_spec: edge::RingSpec::test_activation(),
    }
}

// The harness records only public establishment outputs. Tests never inspect a
// private edge record; they infer it from commands and lifecycle events.
fn new_establisher() -> edge::EdgeEstablisherHarness {
    edge::EdgeEstablisherHarness::new(edge::NodeId(10))
}

// This helper drives a successful lease and ring install for the tx edge. It is
// used by driver and stop tests to stay on the public establishment path.
fn leased_and_installed_tx() -> edge::EdgeEstablisherHarness {
    let mut harness = new_establisher();
    harness.observe(edge::EdgeEvent::ProvisionTx(tx_provision()));
    harness.observe(edge::EdgeEvent::RingLeased {
        request_id: edge::LeaseRequestId(1),
        ring_id: edge::RingId(8001),
        layout: edge::RingLayout::test_layout(0),
    });
    harness.observe(edge::EdgeEvent::RingInstalled {
        edge_id: edge::EdgeId(7001),
        ring_id: edge::RingId(8001),
    });
    harness
}

// This proves ProvisionTx and ProvisionRx create local edge records with the
// required addressing facts and without remote actor addresses.
#[test]
fn provisioning_creates_local_edge_records_without_remote_actor_addresses() {
    // Provision both sides through public messages.
    let mut tx = new_establisher();
    tx.observe(edge::EdgeEvent::ProvisionTx(tx_provision()));
    let mut rx = edge::EdgeEstablisherHarness::new(edge::NodeId(11));
    rx.observe(edge::EdgeEvent::ProvisionRx(rx_provision()));

    // Tx must request a local lease and remember the consumer node id for the
    // later driver command.
    assert!(tx.commands().iter().any(|command| {
        matches!(
            command,
            edge::EdgeCommand::LeaseRing {
                edge_id: edge::EdgeId(7001),
                ..
            }
        )
    }));
    assert_eq!(
        tx.local_record(edge::EdgeId(7001)).unwrap().peer_node_id,
        Some(edge::NodeId(11))
    );

    // Rx must create a receive record keyed by edge id.
    assert!(rx.local_record(edge::EdgeId(7001)).is_some());

    // Data-flow provisioning must not require remote actor addresses.
    for record in [
        tx.local_record(edge::EdgeId(7001)).unwrap(),
        rx.local_record(edge::EdgeId(7001)).unwrap(),
    ] {
        assert!(record.remote_actor_address.is_none());
    }
}

// This proves lease events advance or fail only matching records, stale lease
// events for stopped records do not install state, and unused fresh leases after
// cancellation are released.
#[test]
fn lease_flow_matches_records_and_suppresses_stale_or_cancelled_leases() {
    // Start one tx record and send a mismatched lease event.
    let mut harness = new_establisher();
    harness.observe(edge::EdgeEvent::ProvisionTx(tx_provision()));
    harness.observe(edge::EdgeEvent::RingLeased {
        request_id: edge::LeaseRequestId(99),
        ring_id: edge::RingId(8999),
        layout: edge::RingLayout::test_layout(0),
    });

    // Mismatched lease must not install worker or pump state.
    assert!(
        !harness
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::InstallWorkerRing { .. }) })
    );

    // A matching rejection faults the record.
    harness.observe(edge::EdgeEvent::RingLeaseRejected {
        request_id: edge::LeaseRequestId(1),
        reason: edge::RingLeaseRejection::CannotFit,
    });
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            edge::EdgeLifecycleEvent::EdgeFaulted {
                edge_id: edge::EdgeId(7001),
                ..
            }
        )
    }));

    // Stop and then deliver a fresh lease; it must be released, not installed.
    harness.observe(edge::EdgeEvent::StopEdge {
        edge_id: edge::EdgeId(7001),
    });
    harness.observe(edge::EdgeEvent::RingLeased {
        request_id: edge::LeaseRequestId(1),
        ring_id: edge::RingId(8001),
        layout: edge::RingLayout::test_layout(0),
    });
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            edge::EdgeCommand::ReleaseArenaLease {
                ring_id: edge::RingId(8001),
                ..
            }
        )
    }));
}

// This proves driver state is established only after worker or token endpoint
// ring installation succeeds, using the ObjectSpec and RingSpec from
// provisioning.
#[test]
fn worker_ring_install_precedes_driver_establishment_and_uses_provision_specs() {
    // Provision and lease a tx edge.
    let mut harness = new_establisher();
    let provision = tx_provision();
    harness.observe(edge::EdgeEvent::ProvisionTx(provision.clone()));
    harness.observe(edge::EdgeEvent::RingLeased {
        request_id: edge::LeaseRequestId(1),
        ring_id: edge::RingId(8001),
        layout: edge::RingLayout::test_layout(0),
    });

    // Before ring installation, no driver command may be issued.
    assert!(
        !harness
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::EstablishSend { .. }) })
    );

    // Matching ring installation advances establishment.
    harness.observe(edge::EdgeEvent::RingInstalled {
        edge_id: edge::EdgeId(7001),
        ring_id: edge::RingId(8001),
    });

    // The worker install command must carry the provisioned object/ring specs.
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            edge::EdgeCommand::InstallWorkerRing {
                object_spec,
                ring_spec,
                ..
            } if *object_spec == provision.object_spec && *ring_spec == provision.ring_spec
        )
    }));

    // Now the driver may be established.
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            edge::EdgeCommand::EstablishSend {
                edge_id: edge::EdgeId(7001),
                ..
            }
        )
    }));
}

// This proves send and receive driver establishment uses the correct public
// arguments, and DriverEdgeReady is the readiness boundary for the local actor.
#[test]
fn driver_ready_marks_local_edge_actor_ready() {
    // Drive tx through lease and ring install.
    let mut tx = leased_and_installed_tx();

    // EstablishSend must include edge id, consumer node id, and local layout.
    assert!(tx.commands().iter().any(|command| {
        matches!(
            command,
            edge::EdgeCommand::EstablishSend {
                edge_id: edge::EdgeId(7001),
                consumer_node_id: edge::NodeId(11),
                layout,
            } if *layout == edge::RingLayout::test_layout(0)
        )
    }));

    // The edge is not ready until DriverEdgeReady arrives.
    assert!(
        !tx.events()
            .iter()
            .any(|event| { matches!(event, edge::EdgeLifecycleEvent::EdgeReady { .. }) })
    );
    tx.observe(edge::EdgeEvent::DriverEdgeReady {
        edge_id: edge::EdgeId(7001),
    });
    assert!(tx.events().iter().any(|event| {
        matches!(
            event,
            edge::EdgeLifecycleEvent::EdgeReady {
                edge_id: edge::EdgeId(7001),
                ..
            }
        )
    }));

    // After readiness, stream and pump behavior belongs to the driver; the
    // public command enum has no hot-path byte variant.
}

// This proves StopEdge cancels queued leases, stops pumps, uninstalls worker
// rings, releases arena lease only after pump stop, worker-ring quiescence, and
// quiescence proof, and makes Stopped terminal.
#[test]
fn stop_edge_tears_down_local_state_and_terminal_stopped_ignores_late_events() {
    // Drive an edge to ready.
    let mut harness = leased_and_installed_tx();
    harness.observe(edge::EdgeEvent::DriverEdgeReady {
        edge_id: edge::EdgeId(7001),
    });

    // Pre-stop proof-like events are stale for teardown and must not satisfy
    // the later release gate.
    harness.observe(edge::EdgeEvent::QuiescenceProven {
        ring_id: edge::RingId(8001),
    });
    harness.observe(edge::EdgeEvent::RingQuiesced {
        ring_id: edge::RingId(8001),
    });
    harness.observe(edge::EdgeEvent::PumpStopped {
        edge_id: edge::EdgeId(7001),
        ring_id: edge::RingId(8001),
    });

    // Stop the edge.
    harness.observe(edge::EdgeEvent::StopEdge {
        edge_id: edge::EdgeId(7001),
    });

    // Stop commands must cover queued lease, pump, and worker ring cleanup.
    assert!(
        harness
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::CancelQueuedLease { .. }) })
    );
    assert!(
        harness
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::StopPump { .. }) })
    );
    assert!(
        harness
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::UninstallWorkerRing { .. }) })
    );

    // Quiescence proof alone must not release the arena while the pump may
    // still write and the worker still owns the ring.
    assert!(
        !harness
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::ReleaseArenaLease { .. }) })
    );
    harness.observe(edge::EdgeEvent::QuiescenceProven {
        ring_id: edge::RingId(8001),
    });
    assert!(
        !harness
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::ReleaseArenaLease { .. }) })
    );

    // Pump stop plus proof is still insufficient until the worker ring is
    // quiesced.
    harness.observe(edge::EdgeEvent::PumpStopped {
        edge_id: edge::EdgeId(7001),
        ring_id: edge::RingId(8001),
    });
    assert!(
        !harness
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::ReleaseArenaLease { .. }) })
    );
    harness.observe(edge::EdgeEvent::RingQuiesced {
        ring_id: edge::RingId(8001),
    });
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            edge::EdgeCommand::ReleaseArenaLease {
                ring_id: edge::RingId(8001),
                ..
            }
        )
    }));

    // Ring quiescence must not independently release either, and pump stop plus
    // ring quiescence is still insufficient until the aggregate proof arrives.
    let mut ring_first = leased_and_installed_tx();
    ring_first.observe(edge::EdgeEvent::DriverEdgeReady {
        edge_id: edge::EdgeId(7001),
    });
    ring_first.observe(edge::EdgeEvent::StopEdge {
        edge_id: edge::EdgeId(7001),
    });
    ring_first.observe(edge::EdgeEvent::RingQuiesced {
        ring_id: edge::RingId(8001),
    });
    assert!(
        !ring_first
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::ReleaseArenaLease { .. }) })
    );
    ring_first.observe(edge::EdgeEvent::PumpStopped {
        edge_id: edge::EdgeId(7001),
        ring_id: edge::RingId(8001),
    });
    assert!(
        !ring_first
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::ReleaseArenaLease { .. }) })
    );
    ring_first.observe(edge::EdgeEvent::QuiescenceProven {
        ring_id: edge::RingId(8001),
    });
    assert!(ring_first.commands().iter().any(|command| {
        matches!(
            command,
            edge::EdgeCommand::ReleaseArenaLease {
                ring_id: edge::RingId(8001),
                ..
            }
        )
    }));

    // If StopEdge races with worker installation before RingInstalled, the
    // lease is still withheld until worker cleanup and proof complete.
    let mut install_in_flight = new_establisher();
    install_in_flight.observe(edge::EdgeEvent::ProvisionTx(tx_provision()));
    install_in_flight.observe(edge::EdgeEvent::RingLeased {
        request_id: edge::LeaseRequestId(1),
        ring_id: edge::RingId(8001),
        layout: edge::RingLayout::test_layout(0),
    });
    install_in_flight.observe(edge::EdgeEvent::StopEdge {
        edge_id: edge::EdgeId(7001),
    });
    assert!(
        install_in_flight
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::UninstallWorkerRing { .. }) })
    );
    assert!(
        !install_in_flight
            .commands()
            .iter()
            .any(|command| { matches!(command, edge::EdgeCommand::ReleaseArenaLease { .. }) })
    );
    install_in_flight.observe(edge::EdgeEvent::RingQuiesced {
        ring_id: edge::RingId(8001),
    });
    install_in_flight.observe(edge::EdgeEvent::QuiescenceProven {
        ring_id: edge::RingId(8001),
    });
    assert!(install_in_flight.commands().iter().any(|command| {
        matches!(
            command,
            edge::EdgeCommand::ReleaseArenaLease {
                ring_id: edge::RingId(8001),
                ..
            }
        )
    }));
    // Stopped is terminal: later stale events cannot revive readiness.
    harness.observe(edge::EdgeEvent::Stopped {
        edge_id: edge::EdgeId(7001),
    });
    let ready_before = harness
        .events()
        .iter()
        .filter(|event| matches!(event, edge::EdgeLifecycleEvent::EdgeReady { .. }))
        .count();
    harness.observe(edge::EdgeEvent::DriverEdgeReady {
        edge_id: edge::EdgeId(7001),
    });
    let ready_after = harness
        .events()
        .iter()
        .filter(|event| matches!(event, edge::EdgeLifecycleEvent::EdgeReady { .. }))
        .count();
    assert_eq!(ready_after, ready_before);
}
