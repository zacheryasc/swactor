//! In-process contract tests for the MVP node provisioning specification.
//!
//! These tests deliberately use a mock provider and a deterministic bootstrap
//! session. They prove the node-local FSM, bootstrap handoff, datastream log
//! routing, and known-lease teardown without Vast.ai, Docker, or real SSH.

use mvp_system::orchestration::node_provisioning as provision;
use mvp_system::orchestration::node_provisioning::ProviderPlugin;

fn group_spec(count: u32) -> provision::RunNodeGroupSpec {
    provision::RunNodeGroupSpec {
        run_id: provision::RunId(42),
        group_id: provision::NodeGroupId("workers".into()),
        role: provision::RoleId("worker".into()),
        count,
        provider: provision::provider_kind::mock(),
        shape: provision::DesiredNodeShape {
            image: "ghcr.io/acme/mvp-worker:test".into(),
            disk_gb: 80,
            gpu_name: Some("RTX 4090".into()),
            min_gpu_ram_mb: Some(20_000),
            min_down_mbps: Some(100.0),
            min_up_mbps: Some(20.0),
            min_reliability: Some(0.95),
            require_verified: false,
            provider_labels: [("system".into(), "mvp".into())].into_iter().collect(),
        },
        boot: provision::BootSpec {
            ssh_user: "root".into(),
            verify_commands: vec!["test -x /opt/mvp/swactor".into()],
            start_swactor_command: "/opt/mvp/swactor-node --join ${ORCH_ADDR}".into(),
            stdout_sources: vec!["/var/log/mvp/stdout.log".into()],
            stderr_sources: vec!["/var/log/mvp/stderr.log".into()],
        },
        swarm_join: provision::SwarmJoinTemplate {
            orch_swactor_addr: "quic://orch.example:9443".into(),
            join_token_ref: "secret://run-42-token".into(),
        },
    }
}

fn one_logical_node() -> provision::LogicalNodeSpec {
    provision::expand_node_group(&group_spec(1))
        .into_iter()
        .next()
        .expect("fixture expands to one node")
}

fn start_manager(
    spec: provision::LogicalNodeSpec,
) -> (provision::NodeManager, provision::CreateLeaseRequest) {
    let mut manager = provision::NodeManager::new();
    let commands = manager
        .handle(provision::NodeManagerMsg::Start(spec))
        .expect("start succeeds");
    assert_eq!(commands.len(), 1);
    let provision::NodeManagerCommand::CreateLease(request) = commands[0].clone() else {
        panic!("start must emit CreateLease, got {:?}", commands[0]);
    };
    (manager, request)
}

fn start_bootstrap_command(
    commands: Vec<provision::NodeManagerCommand>,
) -> provision::BootstrapSessionSpec {
    assert_eq!(commands.len(), 1);
    let provision::NodeManagerCommand::StartBootstrap(spec) = commands[0].clone() else {
        panic!("expected StartBootstrap, got {:?}", commands[0]);
    };
    spec
}

#[test]
fn runplan_group_expands_to_stable_logical_node_specs() {
    let nodes = provision::expand_node_group(&group_spec(3));

    let ids: Vec<_> = nodes
        .iter()
        .map(|node| node.logical_node_id.0.as_str())
        .collect();
    assert_eq!(ids, vec!["workers-0", "workers-1", "workers-2"]);

    for node in nodes {
        assert_eq!(node.run_id, provision::RunId(42));
        assert_eq!(node.group_id, provision::NodeGroupId("workers".into()));
        assert_eq!(node.role, provision::RoleId("worker".into()));
        assert_eq!(
            node.swarm_join.expected_logical_node_id, node.logical_node_id,
            "join spec must carry the stable logical node id"
        );
    }
}

#[test]
fn node_manager_start_records_desired_state_and_requests_lease() {
    let spec = one_logical_node();
    let (manager, request) = start_manager(spec.clone());

    let record = manager.record().expect("record exists after start");
    assert_eq!(
        record.logical_node_id,
        provision::LogicalNodeId("workers-0".into())
    );
    assert_eq!(record.stage, provision::NodeStage::LeaseRequested);
    assert!(!record.ready);
    assert_eq!(record.desired, spec);
    assert_eq!(
        request.spec.logical_node_id,
        provision::LogicalNodeId("workers-0".into())
    );
}

#[test]
fn node_manager_handoff_sets_ready_only_after_bootstrap_closed() {
    let spec = one_logical_node();
    let logical_node_id = spec.logical_node_id.clone();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let lease = provider.create_lease(request).expect("mock lease succeeds");

    let commands = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease))
        .expect("lease accepted");
    let _bootstrap = start_bootstrap_command(commands);

    let commands = manager
        .handle(provision::NodeManagerMsg::SwactorJoined {
            logical_node_id,
            swactor_id: provision::SwactorId("swactor-a".into()),
        })
        .expect("swactor join accepted");
    assert!(matches!(
        commands.as_slice(),
        [provision::NodeManagerCommand::BootstrapConvergenceObserved { .. }]
    ));
    assert_eq!(
        manager.record().expect("record exists").stage,
        provision::NodeStage::SwactorJoined
    );
    assert!(!manager.is_ready(), "join alone must not mark readiness");

    manager
        .handle(provision::NodeManagerMsg::BootstrapClosed)
        .expect("bootstrap closes after convergence");
    let record = manager.record().expect("record exists");
    assert_eq!(record.stage, provision::NodeStage::Dormant);
    assert!(record.ready);
    assert!(
        record
            .swactor
            .as_ref()
            .expect("swactor facts recorded")
            .handed_off_at
            .is_some()
    );
    assert_eq!(manager.active_bootstrap(), None);
}

#[test]
fn node_manager_stores_compact_bootstrap_facts_not_log_bodies() {
    let spec = one_logical_node();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let lease = provider.create_lease(request).expect("mock lease succeeds");
    let commands = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease))
        .expect("lease accepted");
    let _bootstrap = start_bootstrap_command(commands);

    manager
        .handle(provision::NodeManagerMsg::BootstrapObserved(
            provision::BootstrapObservation {
                stage: provision::BootstrapStage::StdoutStreaming,
                last_stdout_seq: Some(7),
                last_stderr_seq: Some(3),
                marker: Some("this full line belongs in datastream".into()),
            },
        ))
        .expect("observation accepted");

    let facts = manager
        .record()
        .expect("record exists")
        .bootstrap
        .as_ref()
        .expect("bootstrap facts exist");
    assert_eq!(facts.last_stage, provision::BootstrapStage::StdoutStreaming);
    assert_eq!(facts.last_stdout_seq, Some(7));
    assert_eq!(facts.last_stderr_seq, Some(3));
}

#[test]
fn delayed_provider_endpoint_starts_bootstrap_after_endpoint_known() {
    let spec = one_logical_node();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let endpoint = provision::SshEndpoint {
        host: "203.0.113.10".into(),
        port: 22001,
        user: "root".into(),
        auth_ref: "mock-key".into(),
    };
    provider.queue_create_result(Ok(provision::MockProviderPlugin::result_with_endpoint(
        77, None,
    )));
    provider.queue_endpoint_result(Ok(Some(endpoint.clone())));

    let lease = provider
        .create_lease(request)
        .expect("queued lease succeeds");
    let commands = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease.clone()))
        .expect("lease accepted");
    assert_eq!(
        commands,
        vec![provision::NodeManagerCommand::LookupEndpoint(lease.lease)]
    );

    let endpoint_result = provider
        .lookup_endpoint(manager.record().unwrap().lease.as_ref().unwrap())
        .expect("endpoint lookup succeeds")
        .expect("endpoint appears");
    let commands = manager
        .handle(provision::NodeManagerMsg::EndpointKnown(endpoint_result))
        .expect("endpoint accepted");
    let bootstrap = start_bootstrap_command(commands);
    assert_eq!(bootstrap.ssh, endpoint);
    assert_eq!(
        manager.record().expect("record exists").stage,
        provision::NodeStage::BootstrapRunning
    );
    assert_eq!(
        provider.lookup_requests(),
        &[provision::ProviderLeaseId("mock:77".into())]
    );
}

#[test]
fn pre_handoff_failures_are_terminal_and_do_not_replace() {
    let spec = one_logical_node();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let lease = provider.create_lease(request).expect("mock lease succeeds");
    let _ = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease.clone()))
        .expect("lease accepted");

    manager
        .handle(provision::NodeManagerMsg::BootstrapFailed(
            "boot check failed".into(),
        ))
        .expect("failure accepted");

    let record = manager.record().expect("record exists");
    assert_eq!(record.stage, provision::NodeStage::Failed);
    assert!(!record.ready);
    assert_eq!(record.failed_reason.as_deref(), Some("boot check failed"));
    assert_eq!(record.lease.as_ref().expect("lease retained"), &lease.lease);
}

#[test]
fn swactor_join_for_wrong_logical_node_is_rejected() {
    let spec = one_logical_node();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let lease = provider.create_lease(request).expect("mock lease succeeds");
    let _ = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease))
        .expect("lease accepted");

    let result = manager.handle(provision::NodeManagerMsg::SwactorJoined {
        logical_node_id: provision::LogicalNodeId("workers-99".into()),
        swactor_id: provision::SwactorId("swactor-wrong".into()),
    });

    assert!(result.is_err());
    assert_eq!(
        manager.record().expect("record exists").stage,
        provision::NodeStage::BootstrapRunning
    );
    assert!(!manager.is_ready());
}

#[test]
fn bootstrap_session_streams_logs_flushes_and_closes_on_convergence() {
    let spec = one_logical_node();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let lease = provider.create_lease(request).expect("mock lease succeeds");
    let commands = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease))
        .expect("lease accepted");
    let bootstrap_spec = start_bootstrap_command(commands);
    let mut session = provision::BootstrapSession::new(bootstrap_spec);
    let mut datastream = provision::InMemoryBootstrapDatastream::default();
    let script = provision::MockBootstrapScript::successful(vec![
        (provision::BootstrapLogStream::Stdout, "boot entered".into()),
        (provision::BootstrapLogStream::Stderr, "warning".into()),
        (
            provision::BootstrapLogStream::Stdout,
            "swactor starting".into(),
        ),
    ]);

    let events = session.start(&script, &mut datastream);

    assert_eq!(
        session.stage(),
        provision::BootstrapStage::WaitingForSwactorJoin
    );
    assert_eq!(datastream.records().len(), 3);
    assert_eq!(datastream.records()[0].seq, 1);
    assert_eq!(
        datastream.records()[0].stream,
        provision::BootstrapLogStream::Stdout
    );
    assert_eq!(datastream.records()[1].seq, 2);
    assert_eq!(
        datastream.records()[1].stream,
        provision::BootstrapLogStream::Stderr
    );
    assert_eq!(datastream.records()[2].seq, 3);
    assert_eq!(datastream.records()[2].line, "swactor starting");
    assert!(events.iter().any(|event| matches!(
        event,
        provision::BootstrapSessionEvent::Observed(obs)
            if obs.stage == provision::BootstrapStage::SshReady
    )));

    let events =
        session.convergence_observed(provision::SwactorId("swactor-a".into()), &mut datastream);
    assert!(session.is_closed());
    assert_eq!(datastream.flush_count(), 1);
    assert!(matches!(
        events.as_slice(),
        [_, provision::BootstrapSessionEvent::Closed]
    ));
}

#[test]
fn node_manager_closes_bootstrap_only_after_expected_swactor_convergence() {
    let spec = one_logical_node();
    let logical_node_id = spec.logical_node_id.clone();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let lease = provider.create_lease(request).expect("mock lease succeeds");
    let commands = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease))
        .expect("lease accepted");
    let bootstrap_spec = start_bootstrap_command(commands);
    let mut session = provision::BootstrapSession::new(bootstrap_spec);
    let mut datastream = provision::InMemoryBootstrapDatastream::default();
    let events = session.start(
        &provision::MockBootstrapScript::successful(vec![(
            provision::BootstrapLogStream::Stdout,
            "boot entered".into(),
        )]),
        &mut datastream,
    );
    for event in events {
        if let provision::BootstrapSessionEvent::Observed(observation) = event {
            manager
                .handle(provision::NodeManagerMsg::BootstrapObserved(observation))
                .expect("bootstrap observation accepted");
        }
    }

    let wrong_join = manager.handle(provision::NodeManagerMsg::SwactorJoined {
        logical_node_id: provision::LogicalNodeId("workers-99".into()),
        swactor_id: provision::SwactorId("swactor-wrong".into()),
    });
    assert!(wrong_join.is_err());
    assert_eq!(
        manager.active_bootstrap(),
        Some(provision::BootstrapSessionId(1))
    );
    assert_eq!(
        manager.record().expect("record exists").stage,
        provision::NodeStage::BootstrapRunning
    );
    assert!(!manager.is_ready());

    let commands = manager
        .handle(provision::NodeManagerMsg::SwactorJoined {
            logical_node_id,
            swactor_id: provision::SwactorId("swactor-a".into()),
        })
        .expect("expected swactor join accepted");
    assert!(matches!(
        commands.as_slice(),
        [provision::NodeManagerCommand::BootstrapConvergenceObserved {
            session_id: provision::BootstrapSessionId(1),
            swactor_id
        }] if swactor_id == &provision::SwactorId("swactor-a".into())
    ));
    assert!(!manager.is_ready(), "join alone must not close bootstrap");

    let events =
        session.convergence_observed(provision::SwactorId("swactor-a".into()), &mut datastream);
    assert_eq!(datastream.flush_count(), 1);
    for event in events {
        match event {
            provision::BootstrapSessionEvent::Observed(observation) => {
                manager
                    .handle(provision::NodeManagerMsg::BootstrapObserved(observation))
                    .expect("convergence observation accepted");
            }
            provision::BootstrapSessionEvent::Closed => {
                manager
                    .handle(provision::NodeManagerMsg::BootstrapClosed)
                    .expect("bootstrap close accepted");
            }
            provision::BootstrapSessionEvent::Failed(reason) => {
                panic!("convergence must not fail: {reason}");
            }
        }
    }

    let record = manager.record().expect("record exists");
    assert_eq!(record.stage, provision::NodeStage::Dormant);
    assert!(record.ready);
    assert_eq!(manager.active_bootstrap(), None);
    assert!(session.is_closed());
}

#[test]
fn bootstrap_session_reports_boot_check_failure_without_handoff() {
    let spec = one_logical_node();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let lease = provider.create_lease(request).expect("mock lease succeeds");
    let commands = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease))
        .expect("lease accepted");
    let bootstrap_spec = start_bootstrap_command(commands);
    let mut session = provision::BootstrapSession::new(bootstrap_spec);
    let mut datastream = provision::InMemoryBootstrapDatastream::default();
    let script = provision::MockBootstrapScript {
        ssh_ok: true,
        verify_ok: false,
        start_ok: true,
        records: vec![],
    };

    let events = session.start(&script, &mut datastream);

    assert_eq!(session.stage(), provision::BootstrapStage::BootCheckFailed);
    assert!(events.iter().any(|event| matches!(
        event,
        provision::BootstrapSessionEvent::Failed(reason) if reason == "boot check failed"
    )));
    assert!(!session.is_closed());
}

#[test]
fn teardown_cancels_active_bootstrap_and_destroys_known_lease_only_once() {
    let spec = one_logical_node();
    let (mut manager, request) = start_manager(spec);
    let mut provider = provision::MockProviderPlugin::new();
    let lease = provider.create_lease(request).expect("mock lease succeeds");
    let commands = manager
        .handle(provision::NodeManagerMsg::LeaseCreated(lease.clone()))
        .expect("lease accepted");
    let _bootstrap = start_bootstrap_command(commands);

    let commands = manager
        .handle(provision::NodeManagerMsg::Destroy)
        .expect("destroy accepted");
    assert!(matches!(
        commands.as_slice(),
        [
            provision::NodeManagerCommand::CancelBootstrap { .. },
            provision::NodeManagerCommand::DestroyLease(_)
        ]
    ));
    for command in commands {
        if let provision::NodeManagerCommand::DestroyLease(handle) = command {
            provider.destroy_lease(&handle).expect("destroy succeeds");
            manager
                .handle(provision::NodeManagerMsg::LeaseDestroyed)
                .expect("destroy recorded");
        }
    }

    assert_eq!(provider.destroyed_handles().len(), 1);
    assert_eq!(provider.destroyed_handles()[0], lease.lease.destroy_handle);
    assert_eq!(
        manager.record().expect("record exists").stage,
        provision::NodeStage::Destroyed
    );
    let commands = manager
        .handle(provision::NodeManagerMsg::Destroy)
        .expect("duplicate destroy accepted");
    assert!(
        commands.is_empty(),
        "destroy must be idempotent after lease destruction"
    );
    assert_eq!(provider.destroyed_handles().len(), 1);
}

#[test]
fn in_process_mock_provision_bootstrap_handoff_and_teardown() {
    let specs = provision::expand_node_group(&group_spec(2));
    let mut provider = provision::MockProviderPlugin::new();
    let mut managers = Vec::new();
    let mut datastream = provision::InMemoryBootstrapDatastream::default();

    for spec in specs {
        let logical_node_id = spec.logical_node_id.clone();
        let swactor_id = provision::SwactorId(format!("swactor-{}", logical_node_id.0));
        let (mut manager, request) = start_manager(spec);

        let lease = provider.create_lease(request).expect("mock lease succeeds");
        let commands = manager
            .handle(provision::NodeManagerMsg::LeaseCreated(lease))
            .expect("lease accepted");
        let bootstrap_spec = start_bootstrap_command(commands);
        let mut session = provision::BootstrapSession::new(bootstrap_spec);
        let script = provision::MockBootstrapScript::successful(vec![
            (
                provision::BootstrapLogStream::Stdout,
                format!("{} boot entered", logical_node_id.0),
            ),
            (
                provision::BootstrapLogStream::Stdout,
                format!("{} swactor starting", logical_node_id.0),
            ),
        ]);

        for event in session.start(&script, &mut datastream) {
            match event {
                provision::BootstrapSessionEvent::Observed(observation) => {
                    manager
                        .handle(provision::NodeManagerMsg::BootstrapObserved(observation))
                        .expect("bootstrap observation accepted");
                }
                provision::BootstrapSessionEvent::Failed(reason) => {
                    manager
                        .handle(provision::NodeManagerMsg::BootstrapFailed(reason))
                        .expect("bootstrap failure recorded");
                }
                provision::BootstrapSessionEvent::Closed => {
                    manager
                        .handle(provision::NodeManagerMsg::BootstrapClosed)
                        .expect("bootstrap closed recorded");
                }
            }
        }

        let commands = manager
            .handle(provision::NodeManagerMsg::SwactorJoined {
                logical_node_id: logical_node_id.clone(),
                swactor_id: swactor_id.clone(),
            })
            .expect("swactor join accepted");
        assert!(matches!(
            commands.as_slice(),
            [provision::NodeManagerCommand::BootstrapConvergenceObserved {
                swactor_id: observed,
                ..
            }] if *observed == swactor_id
        ));

        for event in session.convergence_observed(swactor_id, &mut datastream) {
            match event {
                provision::BootstrapSessionEvent::Observed(observation) => {
                    manager
                        .handle(provision::NodeManagerMsg::BootstrapObserved(observation))
                        .expect("convergence observation accepted");
                }
                provision::BootstrapSessionEvent::Failed(reason) => {
                    manager
                        .handle(provision::NodeManagerMsg::BootstrapFailed(reason))
                        .expect("bootstrap failure recorded");
                }
                provision::BootstrapSessionEvent::Closed => {
                    manager
                        .handle(provision::NodeManagerMsg::BootstrapClosed)
                        .expect("bootstrap closed recorded");
                }
            }
        }

        assert!(manager.is_ready());
        assert_eq!(
            manager.record().expect("record exists").stage,
            provision::NodeStage::Dormant
        );
        managers.push(manager);
    }

    assert_eq!(datastream.records().len(), 4);
    assert_eq!(datastream.flush_count(), 2);
    assert!(managers.iter().all(provision::NodeManager::is_ready));

    for manager in &mut managers {
        let commands = manager
            .handle(provision::NodeManagerMsg::Destroy)
            .expect("destroy accepted");
        for command in commands {
            if let provision::NodeManagerCommand::DestroyLease(handle) = command {
                provider.destroy_lease(&handle).expect("destroy succeeds");
                manager
                    .handle(provision::NodeManagerMsg::LeaseDestroyed)
                    .expect("destroy recorded");
            }
        }
        assert_eq!(
            manager.record().expect("record exists").stage,
            provision::NodeStage::Destroyed
        );
    }
    assert_eq!(provider.destroyed_handles().len(), 2);
}
