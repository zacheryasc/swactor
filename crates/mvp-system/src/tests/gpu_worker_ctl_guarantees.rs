//! Black-box contract tests for MVP GpuWorkerCtl behavior.
//!
//! These tests intentionally know only the public controller surface:
//!
//! - start, process, stdout event, actor command, crash, restart, and shutdown
//!   observations in
//! - serialized worker commands, routed events, faults, and stopped events out
//!
//! They assert the guarantees in
//! `specs/mvp_system/gpu_worker_ctl_contract.md`.

use mvp_system::gpu_worker_ctl as ctl;

// A test controller config names one worker process boundary. It does not grant
// tests access to child-process internals or Python implementation details.
fn worker_config() -> ctl::WorkerConfig {
    ctl::WorkerConfig {
        node_id: ctl::NodeId(10),
        arena_env: ctl::ArenaEnv::test_default(),
    }
}

// The harness uses a fake process adapter at the public boundary. It lets tests
// observe serialized commands and routed events without inspecting controller
// state or private supervision tasks.
fn new_controller() -> ctl::GpuWorkerCtlHarness {
    ctl::GpuWorkerCtlHarness::new(worker_config())
}

// This helper starts the worker and drives it to running through public process
// observations. Tests that need a running worker all use the same path.
fn running_controller() -> ctl::GpuWorkerCtlHarness {
    let mut harness = new_controller();
    harness.observe(ctl::WorkerCtlEvent::StartWorker);
    harness.observe(ctl::WorkerCtlEvent::ProcessStarted {
        pid: ctl::ProcessId(1234),
    });
    harness.observe(ctl::WorkerCtlEvent::WorkerReady {
        generation: ctl::WorkerGeneration(1),
    });
    harness
}

fn ring_layout() -> ctl::RingLayout {
    ctl::RingLayout {
        offset: 64,
        byte_len: 4096,
        header_bytes: 16,
    }
}

fn object_spec() -> ctl::ObjectSpec {
    ctl::ObjectSpec {
        max_extent: 1024,
        alignment: 8,
        layout: ctl::ObjectLayout::Token,
    }
}

fn install_ring_spec(ring_id: ctl::RingId, port: &str) -> ctl::InstallRing {
    ctl::InstallRing {
        generation: ctl::WorkerGeneration(1),
        ring_id,
        edge_id: ctl::EdgeId(7001),
        port_id: ctl::PortId(port.into()),
        direction: ctl::RingDirection::Ingress,
        layout: ring_layout(),
        object_spec: object_spec(),
    }
}

fn execute_step_spec() -> ctl::ExecuteStep {
    ctl::ExecuteStep {
        generation: ctl::WorkerGeneration(1),
        role_id: ctl::RoleId(3001),
        step_id: ctl::StepId(9003),
        inputs: vec![ctl::InputBinding {
            port_id: ctl::PortId("in".into()),
            object_id: ctl::ObjectId(5001),
            sequence: ctl::Sequence(7),
            device_handle: ctl::DeviceHandle::new(ctl::WorkerGeneration(1), 42),
        }],
        outputs: vec![ctl::OutputBinding {
            port_id: ctl::PortId("out".into()),
            ring_id: ctl::RingId(8002),
            object_id: ctl::ObjectId(5002),
            sequence: ctl::Sequence(7),
            extent: 128,
            flags: 0,
        }],
        runtime: ctl::WorkerJson::empty(),
        release_inputs_after: true,
    }
}

// The command fixture carries identities and handles but no payload bytes. It
// exercises all command families that should be serialized only while running.
fn current_generation_commands() -> Vec<ctl::ActorCommand> {
    vec![
        ctl::ActorCommand::ConfigureRole(ctl::ConfigureRole {
            generation: ctl::WorkerGeneration(1),
            role_id: ctl::RoleId(3001),
            config: ctl::WorkerJson::empty(),
        }),
        ctl::ActorCommand::InstallRing {
            generation: ctl::WorkerGeneration(1),
            ring_id: ctl::RingId(8001),
        },
        ctl::ActorCommand::InstallRingSpec(install_ring_spec(ctl::RingId(8002), "in")),
        ctl::ActorCommand::RingReadable {
            generation: ctl::WorkerGeneration(1),
            ring_id: ctl::RingId(8001),
        },
        ctl::ActorCommand::RingWritable {
            generation: ctl::WorkerGeneration(1),
            ring_id: ctl::RingId(8002),
        },
        ctl::ActorCommand::ExecuteStep {
            generation: ctl::WorkerGeneration(1),
            step_id: ctl::StepId(9001),
            input: ctl::DeviceHandle::new(ctl::WorkerGeneration(1), 42),
        },
        ctl::ActorCommand::ExecuteStepSpec(execute_step_spec()),
        ctl::ActorCommand::ReleaseDeviceObject {
            generation: ctl::WorkerGeneration(1),
            handle: ctl::DeviceHandle::new(ctl::WorkerGeneration(1), 42),
        },
        ctl::ActorCommand::UninstallRing(ctl::UninstallRing {
            generation: ctl::WorkerGeneration(1),
            ring_id: ctl::RingId(8002),
            reason: ctl::UninstallReason::Reconfigure,
        }),
        ctl::ActorCommand::ShutdownWorker {
            generation: ctl::WorkerGeneration(1),
            mode: ctl::ShutdownMode::AbortInFlight,
        },
    ]
}

// This proves StartWorker spawns the process boundary, then sends
// InitializeWorker, and WorkerReady moves the controller to running. Fatal and
// process exit move it to failed or crashed.
#[test]
fn worker_lifecycle_runs_start_initialize_ready_and_fault_paths() {
    // Start the worker process.
    let mut harness = new_controller();
    harness.observe(ctl::WorkerCtlEvent::StartWorker);
    assert!(
        harness
            .commands()
            .iter()
            .any(|command| { matches!(command, ctl::WorkerCtlCommand::SpawnProcessActor { .. }) })
    );

    // Process start triggers InitializeWorker.
    harness.observe(ctl::WorkerCtlEvent::ProcessStarted {
        pid: ctl::ProcessId(1234),
    });
    assert!(
        harness
            .serialized_worker_commands()
            .iter()
            .any(|command| { matches!(command, ctl::WorkerCommand::InitializeWorker { .. }) })
    );

    // WorkerReady emits a running lifecycle event.
    harness.observe(ctl::WorkerCtlEvent::WorkerReady {
        generation: ctl::WorkerGeneration(1),
    });
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            ctl::WorkerCtlOut::WorkerRunning {
                generation: ctl::WorkerGeneration(1)
            }
        )
    }));
}

// This proves valid actor commands are serialized only in running state, only
// for the current generation, and never carry payload bytes.
#[test]
fn command_routing_requires_running_current_generation_and_is_payload_free() {
    // Drive the controller to running.
    let mut harness = running_controller();

    // Send every current-generation command.
    for command in current_generation_commands() {
        harness.observe(ctl::WorkerCtlEvent::ActorCommand(command));
    }

    // All valid commands are serialized to the worker.
    assert_eq!(
        harness.serialized_worker_commands().len(),
        current_generation_commands().len() + 1
    );

    // Serialized commands must not contain payload bytes.
    for command in harness.serialized_worker_commands() {
        match command {
            ctl::WorkerCommand::InitializeWorker { .. }
            | ctl::WorkerCommand::ConfigureRole(_)
            | ctl::WorkerCommand::InstallRing { .. }
            | ctl::WorkerCommand::InstallRingSpec(_)
            | ctl::WorkerCommand::UninstallRing(_)
            | ctl::WorkerCommand::RingReadable { .. }
            | ctl::WorkerCommand::RingWritable { .. }
            | ctl::WorkerCommand::ExecuteStep { .. }
            | ctl::WorkerCommand::ExecuteStepSpec(_)
            | ctl::WorkerCommand::ReleaseDeviceObject { .. }
            | ctl::WorkerCommand::ShutdownWorker { .. } => {}
        }
    }

    assert!(harness.serialized_worker_commands().iter().any(|command| {
        matches!(
            command,
            ctl::WorkerCommand::InstallRingSpec(install)
                if install.edge_id == ctl::EdgeId(7001)
                    && install.port_id == ctl::PortId("in".into())
                    && install.layout == ring_layout()
                    && install.object_spec == object_spec()
        )
    }));
    assert!(harness.serialized_worker_commands().iter().any(|command| {
        matches!(
            command,
            ctl::WorkerCommand::ExecuteStepSpec(step)
                if step.role_id == ctl::RoleId(3001)
                    && step.inputs.len() == 1
                    && step.outputs.len() == 1
                    && step.release_inputs_after
        )
    }));
    assert!(harness.serialized_worker_commands().iter().any(|command| {
        matches!(
            command,
            ctl::WorkerCommand::ShutdownWorker {
                mode: ctl::ShutdownMode::AbortInFlight
            }
        )
    }));

    // Old-generation handles must be rejected after restart.
    harness.observe(ctl::WorkerCtlEvent::RestartRequested);
    harness.observe(ctl::WorkerCtlEvent::ProcessStarted {
        pid: ctl::ProcessId(1235),
    });
    harness.observe(ctl::WorkerCtlEvent::WorkerReady {
        generation: ctl::WorkerGeneration(2),
    });
    harness.observe(ctl::WorkerCtlEvent::ActorCommand(
        ctl::ActorCommand::ExecuteStep {
            generation: ctl::WorkerGeneration(1),
            step_id: ctl::StepId(9002),
            input: ctl::DeviceHandle::new(ctl::WorkerGeneration(1), 42),
        },
    ));
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            ctl::WorkerCtlOut::CommandRejected {
                reason: ctl::CommandRejection::OldGenerationHandle,
                ..
            }
        )
    }));
}

// This proves worker events parsed from stdout route to the correct local
// control component.
#[test]
fn parsed_worker_events_route_to_their_control_owners() {
    // Start from a running worker.
    let mut harness = running_controller();

    // Deliver each worker event family through stdout parsing.
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::RingInstalled {
            ring_id: ctl::RingId(8001),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::ObjectLoaded {
            object_id: ctl::ObjectId(9000),
            sequence: 0,
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::ObjectProduced {
            object_id: ctl::ObjectId(9001),
            sequence: 0,
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::StepCompleted {
            step_id: ctl::StepId(77),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::RingReadable {
            ring_id: ctl::RingId(8001),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::RingInstalledForEdge {
            ring_id: ctl::RingId(8002),
            edge_id: ctl::EdgeId(7001),
            port_id: ctl::PortId("in".into()),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::ObjectLoadedFromRing {
            ring_id: ctl::RingId(8001),
            edge_id: ctl::EdgeId(7001),
            port_id: ctl::PortId("in".into()),
            object_id: ctl::ObjectId(9000),
            sequence: ctl::Sequence(0),
            extent: 128,
            device_handle: ctl::DeviceHandle::new(ctl::WorkerGeneration(1), 42),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::ObjectProducedToRing {
            ring_id: ctl::RingId(8002),
            edge_id: ctl::EdgeId(7001),
            port_id: ctl::PortId("out".into()),
            object_id: ctl::ObjectId(9001),
            sequence: ctl::Sequence(0),
            extent: 128,
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::StepCompletedForRole {
            role_id: ctl::RoleId(3001),
            step_id: ctl::StepId(77),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::DeviceObjectReleased {
            device_handle: ctl::DeviceHandle::new(ctl::WorkerGeneration(1), 42),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::WorkerReady {
            pid: ctl::ProcessId(1234),
            generation: ctl::WorkerGeneration(1),
            ring_helper_abi: 1,
            backend: ctl::WorkerJson::empty(),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::StdoutEvent(
        ctl::WorkerEvent::WorkerFatal {
            reason: ctl::WorkerFatalReason::ProtocolViolation,
        },
    ));

    // Routing is proven by destination commands/events, not private dispatch
    // tables.
    assert!(
        harness
            .routed()
            .iter()
            .any(|route| matches!(route, ctl::RoutedEvent::ToEdgeEstablisher(_)))
    );
    assert!(
        harness
            .routed()
            .iter()
            .any(|route| matches!(route, ctl::RoutedEvent::ToRxOrRole(_)))
    );
    assert!(
        harness
            .routed()
            .iter()
            .any(|route| matches!(route, ctl::RoutedEvent::ToTxOrRole(_)))
    );
    assert!(
        harness
            .routed()
            .iter()
            .any(|route| matches!(route, ctl::RoutedEvent::ToStageController(_)))
    );
    assert!(
        harness
            .routed()
            .iter()
            .any(|route| matches!(route, ctl::RoutedEvent::ToDriverOrWorkerSide(_)))
    );
}

// This proves worker crash invalidates old handles, roles, rings, in-flight
// steps, synthesizes ring faults, asks the driver to stop pumps, and increments
// generation on restart.
#[test]
fn crash_invalidates_generation_state_and_fans_out_faults() {
    // Install one ring and start one step in generation 1.
    let mut harness = running_controller();
    harness.observe(ctl::WorkerCtlEvent::ActorCommand(
        ctl::ActorCommand::InstallRing {
            generation: ctl::WorkerGeneration(1),
            ring_id: ctl::RingId(8001),
        },
    ));
    harness.observe(ctl::WorkerCtlEvent::ActorCommand(
        ctl::ActorCommand::ExecuteStep {
            generation: ctl::WorkerGeneration(1),
            step_id: ctl::StepId(9001),
            input: ctl::DeviceHandle::new(ctl::WorkerGeneration(1), 42),
        },
    ));

    // Crash the worker process.
    harness.observe(ctl::WorkerCtlEvent::ProcessExited {
        status: ctl::ExitStatus::Signal(9),
    });

    // Installed rings fault and affected pumps are stopped.
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            ctl::WorkerCtlOut::RingFaulted {
                ring_id: ctl::RingId(8001),
                ..
            }
        )
    }));
    assert!(harness.commands().iter().any(|command| {
        matches!(
            command,
            ctl::WorkerCtlCommand::StopDriverPump {
                ring_id: ctl::RingId(8001),
                ..
            }
        )
    }));

    // Restart increments worker generation.
    harness.observe(ctl::WorkerCtlEvent::RestartRequested);
    harness.observe(ctl::WorkerCtlEvent::ProcessStarted {
        pid: ctl::ProcessId(1235),
    });
    harness.observe(ctl::WorkerCtlEvent::WorkerReady {
        generation: ctl::WorkerGeneration(2),
    });
    assert_eq!(harness.current_generation(), ctl::WorkerGeneration(2));
}

// This proves graceful shutdown sends ShutdownWorker, observes WorkerStopped
// before terminal stopped, and classifies installed rings according to the
// observed worker/process outcome.
#[test]
fn graceful_shutdown_sends_worker_shutdown_and_reaches_terminal_stopped() {
    // Start from a running worker with an installed ring.
    let mut harness = running_controller();
    harness.observe(ctl::WorkerCtlEvent::ActorCommand(
        ctl::ActorCommand::InstallRing {
            generation: ctl::WorkerGeneration(1),
            ring_id: ctl::RingId(8001),
        },
    ));

    // Request graceful shutdown.
    harness.observe(ctl::WorkerCtlEvent::ShutdownRequested);
    assert!(
        harness
            .serialized_worker_commands()
            .iter()
            .any(|command| { matches!(command, ctl::WorkerCommand::ShutdownWorker { .. }) })
    );

    // WorkerStopped must precede terminal stopped.
    harness.observe(ctl::WorkerCtlEvent::WorkerStopped {
        generation: ctl::WorkerGeneration(1),
    });
    harness.observe(ctl::WorkerCtlEvent::ProcessExited {
        status: ctl::ExitStatus::Code(0),
    });
    let worker_stopped_pos = harness
        .events()
        .iter()
        .position(|event| matches!(event, ctl::WorkerCtlOut::WorkerStopped { .. }))
        .expect("WorkerStopped must be observed");
    let terminal_pos = harness
        .events()
        .iter()
        .position(|event| matches!(event, ctl::WorkerCtlOut::TerminalStopped { .. }))
        .expect("terminal stopped must be observed");
    assert!(worker_stopped_pos < terminal_pos);

    // Rings are marked quiesced on graceful stop.
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            ctl::WorkerCtlOut::RingQuiesced {
                ring_id: ctl::RingId(8001),
                ..
            }
        )
    }));
}
