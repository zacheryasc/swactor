use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use data_plane::host::HostRouteRegistrar;
use data_plane::path::SessionAccess;
use distribution::directory_actor::{DirectoryIn, Located};
use distribution::types::NodeId;
use myelin_control_contract::{ContextualProcessEventKind, ContextualProcessSpec};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
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
use crate::tests::harness::build_iroh_composition;

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
    let (_engine, driver, stack) = build_iroh_composition(Duration::from_millis(5));
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
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(located) = replies.try_recv() {
                assert_eq!(located.actor, source);
                break located.host;
            }
            assert!(
                Instant::now() < deadline,
                "directory did not resolve source"
            );
            std::thread::yield_now();
        }
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
    let (engine, driver, stack) = build_iroh_composition(Duration::from_millis(5));
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
            spec: Some(RuntimeContextualProcessSpec {
                process: ProcessSpec {
                    command: "/bin/true".to_owned(),
                    args: Vec::new(),
                    env: HashMap::new(),
                    working_dir: None,
                    label: Some("myelin-context-unaware".to_owned()),
                },
                access: SessionAccess {
                    execution_id: "myelin-context-test".to_owned(),
                    read_prefixes: Vec::new(),
                    write_prefixes: Vec::new(),
                },
                attach_deadline: Duration::from_secs(1),
            }),
            output: Some(ContextualProcessOutputConfig::disabled(*output.addr())),
        })
        .expect("launch contextual process");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observed = Vec::new();
    while Instant::now() < deadline {
        while let Some(event) = output.try_recv() {
            let terminal = matches!(
                event,
                ContextualProcessOutput::Process(
                    ProcessOutput::Exited { .. } | ProcessOutput::Error { .. }
                )
            );
            observed.push(event);
            if terminal {
                let failed = observed
                    .iter()
                    .position(|event| {
                        matches!(event, ContextualProcessOutput::BootstrapFailed { .. })
                    })
                    .expect("bootstrap failure");
                let terminal = observed
                    .iter()
                    .position(|event| {
                        matches!(
                            event,
                            ContextualProcessOutput::Process(
                                ProcessOutput::Exited { .. } | ProcessOutput::Error { .. }
                            )
                        )
                    })
                    .expect("native terminal output");
                assert!(failed < terminal, "outputs: {observed:?}");
                driver.shutdown();
                return;
            }
        }
        std::thread::yield_now();
    }
    driver.shutdown();
    panic!("contextual process did not terminate: {observed:?}");
}

#[test]
fn worker_contextual_controller_spawns_queries_stops_and_reclaims_processes() {
    let (engine, driver, stack) = build_iroh_composition(Duration::from_millis(5));
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
        .spawn(ContextualProcessController::new(
            7,
            spawner,
            stack.runtime.create_sender(),
        ))
        .unwrap();
    stack
        .runtime
        .send_to(
            controller,
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Spawn {
                request_id: "execution".to_owned(),
                spec: ContextualProcessSpec {
                    command: "/bin/sh".to_owned(),
                    args: vec!["-c".to_owned(), "sleep 30".to_owned()],
                    env: Default::default(),
                    working_dir: None,
                    label: Some("controller-stop".to_owned()),
                    execution_id: "controller-stop".to_owned(),
                    read_prefixes: vec!["/models".to_owned(), "/runs".to_owned()],
                    write_prefixes: vec!["/runs".to_owned()],
                    attach_timeout_ms: 10_000,
                    staged_program: None,
                },
                reply_to: *events.addr(),
            }),
        )
        .unwrap();

    let spawn_deadline = Instant::now() + Duration::from_secs(10);
    let process = loop {
        assert!(
            Instant::now() < spawn_deadline,
            "spawn event timed out; runtime={:?}",
            stack.runtime.stats()
        );
        if let Some(OrchestratorMsg::ContextualEvent(event)) = events.try_recv()
            && event.request_id == "execution"
        {
            match event.event {
                ContextualProcessEventKind::Spawned { process, .. } => break process,
                ContextualProcessEventKind::SpawnRejected { error } => {
                    panic!("contextual spawn rejected: {error}")
                }
                _ => {}
            }
        }
        std::thread::yield_now();
    };
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
        assert!(Instant::now() < spawn_deadline, "live query timed out");
        if let Some(OrchestratorMsg::ContextualEvent(event)) = events.try_recv()
            && event.request_id == "query-live"
            && let ContextualProcessEventKind::LiveExecutions { executions, .. } = event.event
        {
            assert_eq!(executions.len(), 1);
            assert_eq!(executions[0].process, process);
            break;
        }
        std::thread::yield_now();
    }

    stack
        .runtime
        .send_to(
            controller,
            ContextualProcessControllerIn::Command(ContextualNodeCommand::Stop {
                request_id: "stop".to_owned(),
                target_request_id: "execution".to_owned(),
                kill_after_ms: Some(100),
                reply_to: *events.addr(),
            }),
        )
        .unwrap();
    let mut stop_accepted = false;
    let mut stop_events = Vec::new();
    let stop_deadline = Instant::now() + Duration::from_secs(20);
    let mut terminal = false;
    while !terminal {
        assert!(
            Instant::now() < stop_deadline,
            "stopped process did not terminate; stop_accepted={stop_accepted}; events={stop_events:?}"
        );
        if let Some(OrchestratorMsg::ContextualEvent(event)) = events.try_recv() {
            stop_events.push((event.request_id.clone(), format!("{:?}", event.event)));
            if event.request_id == "stop" {
                stop_accepted |=
                    matches!(event.event, ContextualProcessEventKind::StopAccepted { .. });
            } else if event.request_id == "execution" {
                terminal = event.event.is_terminal();
            }
        }
        std::thread::yield_now();
    }
    assert!(stop_accepted);

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
    let query_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < query_deadline, "empty query timed out");
        if let Some(OrchestratorMsg::ContextualEvent(event)) = events.try_recv()
            && event.request_id == "query-empty"
            && let ContextualProcessEventKind::LiveExecutions { executions, .. } = event.event
        {
            assert!(executions.is_empty());
            break;
        }
        std::thread::yield_now();
    }
    driver.shutdown();
}

#[test]
fn bootstrap_failure_reports_native_exit_and_reclaims_execution() {
    let (engine, driver, stack) = build_iroh_composition(Duration::from_millis(5));
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
        .spawn(ContextualProcessController::new(
            7,
            spawner,
            stack.runtime.create_sender(),
        ))
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
                    attach_timeout_ms: 10_000,
                    staged_program: None,
                },
                reply_to: *events.addr(),
            }),
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut saw_bootstrap_failure = false;
    let mut reclaimed = false;
    while !reclaimed {
        assert!(
            Instant::now() < deadline,
            "bootstrap failure did not terminate; runtime={:?}",
            stack.runtime.stats()
        );
        if let Some(OrchestratorMsg::ContextualEvent(event)) = events.try_recv()
            && event.request_id == "bootstrap-failure"
        {
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
        std::thread::yield_now();
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
    let query_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            Instant::now() < query_deadline,
            "post-failure query timed out"
        );
        if let Some(OrchestratorMsg::ContextualEvent(event)) = events.try_recv()
            && event.request_id == "query-after-failure"
            && let ContextualProcessEventKind::LiveExecutions { executions, .. } = event.event
        {
            assert!(
                executions.is_empty(),
                "failed bootstrap must reclaim its execution slot"
            );
            break;
        }
        std::thread::yield_now();
    }
    driver.shutdown();
}
