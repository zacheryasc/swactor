use std::collections::HashMap;
use std::sync::Arc;

use data_plane::host::HostRouteRegistrar;
use data_plane::path::SessionAccess;
use distribution::directory_actor::{DirectoryIn, Located};
use distribution::types::NodeId;
use myelin_control_contract::{ContextualProcessEventKind, ContextualProcessSpec};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Inbox, Runtime};
use swactor_engine::{Engine, TokioBackend, TokioConfig};
use swactor_process::{ProcessOutput, ProcessSpec};
use swactor_process_context::{
    ContextualProcessOutput, ContextualProcessOutputConfig, ContextualProcessSpawner,
    ContextualProcessSpec as RuntimeContextualProcessSpec,
};

use crate::contextual_process::{
    ContextualNodeCommand, ContextualProcessController, ContextualProcessControllerIn,
    MyelinChildRouteRegistrar, MyelinContextualProcessConfig, build_contextual_process_spawner,
};
use crate::orchestration::actor::OrchestratorMsg;
use crate::tests::harness::prepare_iroh_composition;

fn contextual_event(
    runtime: &Runtime,
    events: &Inbox<OrchestratorMsg>,
) -> myelin_control_contract::ContextualProcessEvent {
    let OrchestratorMsg::ContextualEvent {
        observation,
        delivery_owner,
        sequence,
    } = futures_lite::future::block_on(events.recv())
    else {
        panic!("unexpected contextual message");
    };
    if let Some(owner) = delivery_owner {
        runtime
            .send_to(
                owner,
                crate::contextual_process::ContextualEventDeliveryIn::Acknowledge {
                    reply_to: *events.addr(),
                    sequence,
                },
            )
            .unwrap();
    }
    observation
}

struct Launch {
    spawner: Arc<ContextualProcessSpawner>,
    sender: ExternalSender,
    spec: Option<RuntimeContextualProcessSpec>,
    output: Option<ContextualProcessOutputConfig>,
}

impl ActorInterface for Launch {
    type Incoming = ();
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.spawner
            .spawn(
                ctx,
                &self.sender,
                self.spec.take().expect("contextual spec"),
                self.output.take().expect("contextual output"),
            )
            .expect("spawn contextual process");
        ctx.stop_self();
    }

    fn handle(&mut self, _ctx: &Ctx<'_>, _message: ()) {}
}

#[test]
fn shared_source_route_survives_one_owner_release_and_cleans_up_after_last() {
    let (parts, compose) = prepare_iroh_composition();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let (driver, stack) = compose(engine.task_spawner(), engine.handle());
    let host_reader: Arc<dyn HostRouteRegistrar> = Arc::new(MyelinChildRouteRegistrar::new(
        stack.route_view.clone(),
        stack.pinned_routes.clone(),
        stack.route_binder.clone(),
        stack.runtime.clone(),
        stack.actors.directory,
        driver.connection_observer(),
    ));
    let program_transfer = Arc::clone(&host_reader);
    let source = ActorAddress([23; 32]);
    let source_node = NodeId([17; 32]);
    let replies = stack.runtime.new_inbox::<Located>().unwrap();
    let resolve_after_republish = || {
        // Rebuild from the directory's own claims and live pins. Merely reading
        // the old route view would hide an incorrectly removed shared pin.
        stack
            .runtime
            .send_to(stack.actors.directory, DirectoryIn::Resync)
            .unwrap();
        stack
            .runtime
            .send_to(
                stack.actors.directory,
                DirectoryIn::Resolve {
                    actor: source,
                    reply: *replies.addr(),
                },
            )
            .unwrap();
        let located = futures_lite::future::block_on(replies.recv());
        assert_eq!(located.actor, source);
        located.host
    };

    host_reader.retain_source(source, source_node.0).unwrap();
    program_transfer
        .retain_source(source, source_node.0)
        .unwrap();
    assert_eq!(resolve_after_republish(), Some(source_node));

    host_reader.release_source(source);
    drop(host_reader);
    assert_eq!(resolve_after_republish(), Some(source_node));
    assert!(program_transfer.is_routable(source));

    program_transfer.release_source(source);
    assert_eq!(resolve_after_republish(), None);
    assert!(!program_transfer.is_routable(source));
    driver.shutdown();
}

#[test]
fn unaware_native_child_fails_bootstrap_before_terminal_output() {
    let (parts, compose) = prepare_iroh_composition();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let (driver, stack) = compose(engine.task_spawner(), engine.handle());
    let spawner = Arc::new(
        build_contextual_process_spawner(MyelinContextualProcessConfig {
            runtime: stack.runtime.clone(),
            engine: engine.handle(),
            arena_bytes: 1 << 20,
            arena_alignment: 64,
            namespace: None,
            transfer_receiver: None,
            source_sender: None,
            source_publisher: None,
            route_registrar: Arc::new(MyelinChildRouteRegistrar::new(
                stack.route_view.clone(),
                stack.pinned_routes.clone(),
                stack.route_binder.clone(),
                stack.runtime.clone(),
                stack.actors.directory,
                driver.connection_observer(),
            )),
            stream_transport: Some(driver.stream_transport()),
            host_endpoint: driver.endpoint_addr(),
        })
        .expect("Myelin contextual process services"),
    );
    let output = stack
        .runtime
        .new_inbox::<ContextualProcessOutput>()
        .expect("contextual output inbox");
    stack
        .runtime
        .spawn(Launch {
            spawner,
            sender: stack.runtime.create_sender(),
            spec: Some(RuntimeContextualProcessSpec::event_driven(
                ProcessSpec {
                    command: "/bin/true".to_owned(),
                    args: Vec::new(),
                    env: HashMap::new(),
                    working_dir: None,
                    label: Some("myelin-context-unaware".to_owned()),
                },
                SessionAccess {
                    execution_id: "myelin-context-test".to_owned(),
                    read_prefixes: Vec::new(),
                    write_prefixes: Vec::new(),
                },
            )),
            output: Some(ContextualProcessOutputConfig::disabled(*output.addr())),
        })
        .expect("launch contextual process");

    let mut observed = Vec::new();
    loop {
        let event = futures_lite::future::block_on(output.recv());
        let terminal = matches!(
            event,
            ContextualProcessOutput::Process(
                ProcessOutput::Exited { .. } | ProcessOutput::Error { .. }
            )
        );
        observed.push(event);
        if terminal {
            break;
        }
    }
    let failed = observed
        .iter()
        .position(|event| matches!(event, ContextualProcessOutput::BootstrapFailed { .. }))
        .expect("bootstrap failure");
    assert!(failed < observed.len() - 1, "outputs: {observed:?}");
    driver.shutdown();
}

#[test]
fn worker_contextual_controller_spawns_queries_stops_and_reclaims_processes() {
    let (parts, compose) = prepare_iroh_composition();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let (driver, stack) = compose(engine.task_spawner(), engine.handle());
    check_controller_stop(StopAction::Graceful, engine.handle(), driver, stack);
}

#[test]
fn explicit_cancellation_interrupts_graceful_stop_and_reclaims_processes() {
    let (parts, compose) = prepare_iroh_composition();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let (driver, stack) = compose(engine.task_spawner(), engine.handle());
    check_controller_stop(StopAction::Force, engine.handle(), driver, stack);
}

#[test]
fn contextual_output_observer_loss_cancels_native_before_reclaiming_execution() {
    let (parts, compose) = prepare_iroh_composition();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let (driver, stack) = compose(engine.task_spawner(), engine.handle());
    check_controller_stop(StopAction::ObserverLoss, engine.handle(), driver, stack);
}

enum StopAction {
    Graceful,
    Force,
    ObserverLoss,
}

fn check_controller_stop(
    action: StopAction,
    engine: swactor_engine::EngineHandle,
    driver: iroh_driver::IrohDriver,
    stack: crate::orchestration::distribution_stack::DistributionRuntimeStack,
) {
    let force_cancel = matches!(action, StopAction::Force);
    let observer_loss = matches!(action, StopAction::ObserverLoss);
    let spawner = Arc::new(
        build_contextual_process_spawner(MyelinContextualProcessConfig {
            runtime: stack.runtime.clone(),
            engine,
            arena_bytes: 1 << 20,
            arena_alignment: 64,
            namespace: None,
            transfer_receiver: None,
            source_sender: None,
            source_publisher: None,
            route_registrar: Arc::new(MyelinChildRouteRegistrar::new(
                stack.route_view.clone(),
                stack.pinned_routes.clone(),
                stack.route_binder.clone(),
                stack.runtime.clone(),
                stack.actors.directory,
                driver.connection_observer(),
            )),
            stream_transport: Some(driver.stream_transport()),
            host_endpoint: driver.endpoint_addr(),
        })
        .unwrap(),
    );
    let events = stack.runtime.new_inbox::<OrchestratorMsg>().unwrap();
    let controller = stack
        .runtime
        .spawn(
            ContextualProcessController::new(
                stack.runtime.clone(),
                7,
                spawner,
                stack.runtime.create_sender(),
                Arc::new(MyelinChildRouteRegistrar::new(
                    stack.route_view.clone(),
                    stack.pinned_routes.clone(),
                    stack.route_binder.clone(),
                    stack.runtime.clone(),
                    stack.actors.directory,
                    driver.connection_observer(),
                )),
            )
            .unwrap(),
        )
        .unwrap();
    let waiting = tempfile::tempdir().unwrap();
    let fifo = waiting.path().join("child-input");
    let fifo_c = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
    // A FIFO read is an interruptible child-owned readiness barrier, not a clock.
    assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
    let _fifo_owner = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fifo)
        .unwrap();
    let program = if force_cancel || observer_loss {
        r#"trap 'printf "stopping\n"; read ignored < "$1"' TERM; printf 'ready\n'; read value < "$1""#
    } else {
        r#"trap 'exit 17' TERM; printf 'ready\n'; read value < "$1""#
    };
    stack
        .runtime
        .send_to(
            controller,
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Spawn {
                request_id: "execution".to_owned(),
                spec: ContextualProcessSpec {
                    command: "/bin/sh".to_owned(),
                    args: vec![
                        "-c".to_owned(),
                        program.to_owned(),
                        "contextual-child".to_owned(),
                        fifo.display().to_string(),
                    ],
                    env: Default::default(),
                    working_dir: None,
                    label: Some("controller-stop".to_owned()),
                    execution_id: "controller-stop".to_owned(),
                    read_prefixes: vec!["/models".to_owned(), "/runs".to_owned()],
                    write_prefixes: vec!["/runs".to_owned()],
                    staged_program: None,
                },
                reply_to: *events.addr(),
            }),
        )
        .unwrap();

    let mut process = None;
    let mut stdout = Vec::new();
    loop {
        let event = contextual_event(&stack.runtime, &events);
        if event.request_id != "execution" {
            continue;
        }
        match event.event {
            ContextualProcessEventKind::Spawned {
                process: spawned, ..
            } => {
                process = Some(spawned);
            }
            ContextualProcessEventKind::Stdout { bytes } => {
                stdout.extend(bytes);
                if stdout
                    .windows(b"ready\n".len())
                    .any(|window| window == b"ready\n")
                {
                    break;
                }
            }
            ContextualProcessEventKind::SpawnRejected { error } => {
                panic!("contextual spawn rejected: {error}")
            }
            event if event.is_terminal() => panic!("child terminated before stop: {event:?}"),
            _ => {}
        }
    }
    let process = process.expect("spawned process identity");
    stack
        .runtime
        .send_to(
            controller,
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                request_id: "query-live".to_owned(),
                reply_to: *events.addr(),
            }),
        )
        .unwrap();
    loop {
        let event = contextual_event(&stack.runtime, &events);
        if event.request_id == "query-live"
            && let ContextualProcessEventKind::LiveExecutions { executions, .. } = event.event
        {
            assert_eq!(executions.len(), 1);
            assert_eq!(executions[0].process, process);
            break;
        }
    }

    if observer_loss {
        let census = stack.runtime.admin().list_actors().unwrap();
        let census = futures_lite::future::block_on(census.recv()).unwrap();
        let relay = census
            .actors
            .iter()
            .find(|actor| actor.actor_type.ends_with("::ContextualOutputRelay"))
            .expect("live contextual output relay")
            .address;
        stack.runtime.stop_actor(relay).unwrap();
    } else {
        stack
            .runtime
            .send_to(
                controller,
                ContextualProcessControllerIn::Command(ContextualNodeCommand::Stop {
                    request_id: "stop".to_owned(),
                    target_request_id: "execution".to_owned(),
                    force: false,
                    reply_to: *events.addr(),
                }),
            )
            .unwrap();
    }
    let mut stop_accepted = false;
    let mut stop_events = Vec::new();
    let mut terminal = false;
    let mut cancelled = false;
    let mut cancellation_accepted = false;
    let mut stopping_output = Vec::new();
    while !terminal {
        let event = contextual_event(&stack.runtime, &events);
        stop_events.push((event.request_id.clone(), format!("{:?}", event.event)));
        if event.request_id == "stop" {
            stop_accepted |= matches!(event.event, ContextualProcessEventKind::StopAccepted { .. });
        } else if event.request_id == "force-stop" {
            cancellation_accepted |=
                matches!(event.event, ContextualProcessEventKind::StopAccepted { .. });
        } else if event.request_id == "execution" {
            terminal = event.event.is_terminal();
            match event.event {
                ContextualProcessEventKind::Stdout { bytes } if force_cancel && !cancelled => {
                    stopping_output.extend(bytes);
                    if stopping_output
                        .windows(b"stopping\n".len())
                        .any(|window| window == b"stopping\n")
                    {
                        stack
                            .runtime
                            .send_to(
                                controller,
                                ContextualProcessControllerIn::Command(
                                    ContextualNodeCommand::Stop {
                                        request_id: "force-stop".to_owned(),
                                        target_request_id: "execution".to_owned(),
                                        force: true,
                                        reply_to: *events.addr(),
                                    },
                                ),
                            )
                            .unwrap();
                        cancelled = true;
                    }
                }
                ContextualProcessEventKind::Exited { status } => {
                    let expected = if force_cancel || observer_loss {
                        myelin_control_contract::ContextualExitStatus::Signal(libc::SIGKILL)
                    } else {
                        myelin_control_contract::ContextualExitStatus::Code(17)
                    };
                    assert_eq!(status, expected, "stop events: {stop_events:?}");
                }
                outcome if terminal => panic!("unexpected terminal outcome: {outcome:?}"),
                _ => {}
            }
        }
    }
    assert_eq!(
        stop_accepted, !observer_loss,
        "stop events: {stop_events:?}"
    );
    assert_eq!(cancelled, force_cancel);
    assert_eq!(cancellation_accepted, force_cancel);

    stack
        .runtime
        .send_to(
            controller,
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                request_id: "query-empty".to_owned(),
                reply_to: *events.addr(),
            }),
        )
        .unwrap();
    loop {
        let event = contextual_event(&stack.runtime, &events);
        if event.request_id == "query-empty"
            && let ContextualProcessEventKind::LiveExecutions { executions, .. } = event.event
        {
            assert!(executions.is_empty());
            break;
        }
    }
    driver.shutdown();
}

#[test]
fn bootstrap_failure_reports_native_exit_and_reclaims_execution() {
    let (parts, compose) = prepare_iroh_composition();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let (driver, stack) = compose(engine.task_spawner(), engine.handle());
    let spawner = Arc::new(
        build_contextual_process_spawner(MyelinContextualProcessConfig {
            runtime: stack.runtime.clone(),
            engine: engine.handle(),
            arena_bytes: 1 << 20,
            arena_alignment: 64,
            namespace: None,
            transfer_receiver: None,
            source_sender: None,
            source_publisher: None,
            route_registrar: Arc::new(MyelinChildRouteRegistrar::new(
                stack.route_view.clone(),
                stack.pinned_routes.clone(),
                stack.route_binder.clone(),
                stack.runtime.clone(),
                stack.actors.directory,
                driver.connection_observer(),
            )),
            stream_transport: Some(driver.stream_transport()),
            host_endpoint: driver.endpoint_addr(),
        })
        .unwrap(),
    );
    let events = stack.runtime.new_inbox::<OrchestratorMsg>().unwrap();
    let controller = stack
        .runtime
        .spawn(
            ContextualProcessController::new(
                stack.runtime.clone(),
                7,
                spawner,
                stack.runtime.create_sender(),
                Arc::new(MyelinChildRouteRegistrar::new(
                    stack.route_view.clone(),
                    stack.pinned_routes.clone(),
                    stack.route_binder.clone(),
                    stack.runtime.clone(),
                    stack.actors.directory,
                    driver.connection_observer(),
                )),
            )
            .unwrap(),
        )
        .unwrap();
    // A child that exits before claiming its bootstrap context fails the
    // bootstrap: this must be a terminal outcome that reclaims the execution.
    stack
        .runtime
        .send_to(
            controller,
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Spawn {
                request_id: "bootstrap-failure".to_owned(),
                spec: ContextualProcessSpec {
                    command: "/bin/sh".to_owned(),
                    args: vec!["-c".to_owned(), "exit 0".to_owned()],
                    env: Default::default(),
                    working_dir: None,
                    label: Some("controller-bootstrap-failure".to_owned()),
                    execution_id: "controller-bootstrap-failure".to_owned(),
                    read_prefixes: vec!["/models".to_owned()],
                    write_prefixes: vec!["/runs".to_owned()],
                    staged_program: None,
                },
                reply_to: *events.addr(),
            }),
        )
        .unwrap();

    let mut saw_bootstrap_failure = false;
    let mut reclaimed = false;
    while !reclaimed {
        let event = contextual_event(&stack.runtime, &events);
        if event.request_id == "bootstrap-failure" {
            match event.event {
                ContextualProcessEventKind::BootstrapFailed { .. } => {
                    assert!(
                        !event.event.is_terminal(),
                        "BootstrapFailed must stay non-terminal until the native process is reaped, saw {:?}",
                        event.event
                    );
                    saw_bootstrap_failure = true;
                }
                ContextualProcessEventKind::Exited { .. } => {
                    assert!(
                        saw_bootstrap_failure,
                        "bootstrap failure exited without reporting why: {:?}",
                        event.event
                    );
                    assert!(
                        event.event.is_terminal(),
                        "Exited must be terminal, saw {:?}",
                        event.event
                    );
                    reclaimed = true;
                }
                other if other.is_terminal() => {
                    panic!(
                        "unexpected terminal outcome before bootstrap failure: {:?}",
                        other
                    );
                }
                _ => {}
            }
        }
    }

    stack
        .runtime
        .send_to(
            controller,
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Query {
                request_id: "query-after-failure".to_owned(),
                reply_to: *events.addr(),
            }),
        )
        .unwrap();
    loop {
        let event = contextual_event(&stack.runtime, &events);
        if event.request_id == "query-after-failure"
            && let ContextualProcessEventKind::LiveExecutions { executions, .. } = event.event
        {
            assert!(
                executions.is_empty(),
                "failed bootstrap must reclaim its execution slot"
            );
            break;
        }
    }
    driver.shutdown();
}
