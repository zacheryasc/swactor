//! Behavior guarantees for the `orchestration` module.

mod run_fsm {
    //! Black-box contract tests for the Myelin orchestrator run FSM.
    //!
    //! These tests intentionally know only the public orchestrator surface:
    //!
    //! - pool, plan, stage, endpoint, token, fault, and stop events in
    //! - commands, lifecycle events, and terminal outcome out
    //!
    //! They assert the guarantees in
    //! `specs/BEHAVIOR_GUARANTEES.md`.

    use crate::run_fsm as fsm;
    use crate::tests::harness::OrchestratorHarness;

    // A three-stage plan proves multi-stage provisioning and readiness without
    // making tests depend on any placement heuristic. The plan is already valid;
    // these tests are about how the orchestrator consumes it.
    fn committed_plan() -> fsm::RunPlan {
        fsm::RunPlan::test_linear(
            fsm::RunId(7),
            vec![
                fsm::StageRef {
                    stage_index: 0,
                    node_id: fsm::NodeId(10),
                },
                fsm::StageRef {
                    stage_index: 1,
                    node_id: fsm::NodeId(11),
                },
                fsm::StageRef {
                    stage_index: 2,
                    node_id: fsm::NodeId(12),
                },
            ],
        )
    }

    // The harness is the black-box public boundary for the run FSM. It accepts
    // observable events and records emitted commands/events; tests never inspect an
    // internal FSM enum or private readiness counter.
    fn new_run() -> OrchestratorHarness {
        OrchestratorHarness::new(fsm::RunConfig {
            run_id: fsm::RunId(7),
            max_tokens: 4,
            prompt: vec![101, 102, 103],
        })
    }

    // Stage readiness events are generated from the committed plan so the tests
    // prove readiness by stage identity instead of relying on command ordering.
    fn stage_ready_events(plan: &fsm::RunPlan) -> Vec<fsm::RunEvent> {
        plan.stages
            .iter()
            .map(|stage| fsm::RunEvent::StageReady {
                run_id: plan.run_id,
                stage_index: stage.stage_index,
            })
            .collect()
    }

    // Transcript positions turn ordering claims into proofs over observable output.
    // If an event is missing, the test fails at the boundary where users and other
    // components would also lose the guarantee.
    fn position_of(events: &[fsm::LifecycleEvent], needle: &fsm::LifecycleEvent) -> usize {
        events
            .iter()
            .position(|event| event == needle)
            .expect("expected lifecycle event missing")
    }

    // This proves planning and provisioning are gated by PoolReady, and that the
    // orchestrator provisions exactly the committed stages and local token endpoints
    // from a valid RunPlan.
    #[test]
    fn planning_and_provisioning_start_only_after_pool_ready() {
        // Start the run and give it a valid plan, but no PoolReady event.
        let plan = committed_plan();
        let mut harness = new_run();
        harness.observe(fsm::RunEvent::PlanAvailable(plan.clone()));

        // Without PoolReady, provisioning must not begin.
        assert!(
            !harness
                .commands()
                .iter()
                .any(|command| { matches!(command, fsm::RunCommand::ProvisionStage { .. }) })
        );

        // Once PoolReady is observed, the committed plan may be provisioned.
        harness.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });

        // Every planned stage gets exactly one provision command.
        let provisioned = harness
            .commands()
            .iter()
            .filter_map(|command| match command {
                fsm::RunCommand::ProvisionStage { provision } => Some(provision.stage_index),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        let expected = plan
            .stages
            .iter()
            .map(|stage| stage.stage_index)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(provisioned, expected);

        // Provisioning must not mention nodes outside the committed plan.
        let plan_nodes = plan
            .stage_nodes()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        for command in harness.commands() {
            if let fsm::RunCommand::ProvisionStage { provision } = command {
                assert!(plan_nodes.contains(&provision.node_id));
            }
        }

        // Token endpoints are created locally from the same committed plan.
        assert!(harness.commands().iter().any(|command| {
            matches!(command, fsm::RunCommand::CreateTokenInEndpoint { run_id } if *run_id == fsm::RunId(7))
        }));
        assert!(harness.commands().iter().any(|command| {
            matches!(command, fsm::RunCommand::CreateTokenOutEndpoint { run_id } if *run_id == fsm::RunId(7))
        }));
    }

    // This proves prompt injection is blocked until every planned stage and both
    // local token endpoints are ready. Duplicate readiness must not count as a
    // missing stage, and foreign readiness must fault or reject.
    #[test]
    fn readiness_barrier_controls_prompt_injection() {
        // Provision a valid plan after PoolReady.
        let plan = committed_plan();
        let mut harness = new_run();
        harness.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });
        harness.observe(fsm::RunEvent::PlanAvailable(plan.clone()));

        // A duplicate StageReady for stage 0 cannot satisfy stage 1 or 2.
        harness.observe(fsm::RunEvent::StageReady {
            run_id: fsm::RunId(7),
            stage_index: 0,
        });
        harness.observe(fsm::RunEvent::StageReady {
            run_id: fsm::RunId(7),
            stage_index: 0,
        });
        harness.observe(fsm::RunEvent::TokenInEndpointReady);
        harness.observe(fsm::RunEvent::TokenOutEndpointReady);
        assert!(
            !harness
                .commands()
                .iter()
                .any(|command| { matches!(command, fsm::RunCommand::InjectTokenObject { .. }) })
        );

        // Complete the remaining stage readiness facts.
        for event in stage_ready_events(&plan).into_iter().skip(1) {
            harness.observe(event);
        }

        // Prompt injection is the public start signal after the full barrier.
        assert!(harness.commands().iter().any(|command| {
            matches!(
                command,
                fsm::RunCommand::InjectTokenObject {
                    run_id: fsm::RunId(7),
                    object: fsm::TokenObjectInjection {
                        sequence: 0,
                        payload: fsm::TokenObjectPayload::Prompt { tokens },
                    },
                } if tokens.as_slice() == [101, 102, 103]
            )
        }));

        // Unknown stage readiness must not silently advance another run.
        let mut invalid = new_run();
        invalid.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });
        invalid.observe(fsm::RunEvent::PlanAvailable(plan));
        invalid.observe(fsm::RunEvent::StageReady {
            run_id: fsm::RunId(7),
            stage_index: 99,
        });
        assert!(
            invalid
                .events()
                .iter()
                .any(|event| { matches!(event, fsm::LifecycleEvent::RunFaulted { .. }) })
        );
    }

    // This proves execution has one start signal and advances by the token feedback
    // rule: inject sequence 0 first, then inject k + 1 only after consuming k.
    #[test]
    fn execution_injects_next_sequence_only_after_consuming_previous_token() {
        // Drive a run through the complete readiness barrier.
        let plan = committed_plan();
        let mut harness = new_run();
        harness.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });
        harness.observe(fsm::RunEvent::PlanAvailable(plan.clone()));
        harness.observe(fsm::RunEvent::TokenInEndpointReady);
        harness.observe(fsm::RunEvent::TokenOutEndpointReady);
        for event in stage_ready_events(&plan) {
            harness.observe(event);
        }

        // Sequence 0 must be injected first as a prompt token object.
        let initial_objects = harness
            .commands()
            .iter()
            .filter_map(|command| match command {
                fsm::RunCommand::InjectTokenObject { object, .. } => Some(object),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(initial_objects.len(), 1);
        assert_eq!(
            *initial_objects[0],
            fsm::TokenObjectInjection {
                sequence: 0,
                payload: fsm::TokenObjectPayload::Prompt {
                    tokens: vec![101, 102, 103],
                },
            }
        );
        assert_eq!(harness.injected_sequences(), vec![0]);

        // Consuming token 0 permits injecting sequence 1.
        harness.observe(fsm::RunEvent::TokenReceived {
            sequence: 0,
            token_id: 201,
            eos: false,
        });
        assert_eq!(harness.injected_sequences(), vec![0, 1]);
        let decode_object = harness
            .commands()
            .iter()
            .filter_map(|command| match command {
                fsm::RunCommand::InjectTokenObject { object, .. } => Some(object),
                _ => None,
            })
            .last()
            .expect("decode injection must be recorded");
        assert_eq!(
            *decode_object,
            fsm::TokenObjectInjection {
                sequence: 1,
                payload: fsm::TokenObjectPayload::Decode {
                    token_id: 201,
                    sampling: fsm::SamplingData { source_sequence: 0 },
                },
            }
        );

        // No additional injection may happen without consuming sequence 1.
        harness.advance_time_ms(10);
        assert_eq!(harness.injected_sequences(), vec![0, 1]);

        // EOS stops further injection after the consumed sequence.
        harness.observe(fsm::RunEvent::TokenReceived {
            sequence: 1,
            token_id: 2,
            eos: true,
        });
        assert_eq!(harness.injected_sequences(), vec![0, 1]);
    }

    // This proves every run-level fault source records one terminal fault, and the
    // first failure reason is retained if later failures arrive.
    #[test]
    fn first_run_fault_reason_is_terminal_and_sticky() {
        // Prepare an executing run so both setup and execution-time faults would be
        // meaningful if observed.
        let plan = committed_plan();
        let mut harness = new_run();
        harness.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });
        harness.observe(fsm::RunEvent::PlanAvailable(plan.clone()));
        harness.observe(fsm::RunEvent::TokenInEndpointReady);
        harness.observe(fsm::RunEvent::TokenOutEndpointReady);
        for event in stage_ready_events(&plan) {
            harness.observe(event);
        }

        // Inject the first failure source.
        harness.observe(fsm::RunEvent::StageFault {
            run_id: fsm::RunId(7),
            stage_index: 1,
            reason: fsm::StageFaultReason::WorkerCrashed,
        });

        // Inject later failures that must not replace the terminal reason.
        harness.observe(fsm::RunEvent::EndpointFault {
            run_id: fsm::RunId(7),
            endpoint: fsm::EndpointKind::TokenOut,
        });
        harness.observe(fsm::RunEvent::MembershipLost {
            run_id: fsm::RunId(7),
            node_id: fsm::NodeId(11),
        });

        // Exactly one terminal fault is recorded.
        let faults = harness
            .events()
            .iter()
            .filter_map(|event| match event {
                fsm::LifecycleEvent::RunFaulted { reason, .. } => Some(reason),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(faults.len(), 1);
        assert_eq!(
            *faults[0],
            fsm::RunFaultReason::StageFault {
                stage_index: 1,
                reason: fsm::StageFaultReason::WorkerCrashed,
            }
        );
    }

    #[test]
    fn membership_loss_faults_run() {
        let plan = committed_plan();
        let mut membership_lost = new_run();
        membership_lost.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });
        membership_lost.observe(fsm::RunEvent::PlanAvailable(plan.clone()));
        membership_lost.observe(fsm::RunEvent::MembershipLost {
            run_id: fsm::RunId(7),
            node_id: fsm::NodeId(12),
        });
        assert!(membership_lost.events().iter().any(|event| {
            matches!(
                event,
                fsm::LifecycleEvent::RunFaulted {
                    reason: fsm::RunFaultReason::MembershipLost {
                        node_id: fsm::NodeId(12)
                    },
                    ..
                }
            )
        }));
    }

    // This proves terminal outcomes are mutually exclusive, reject new work, and
    // always lead into teardown for success, fault, and operator stop.
    #[test]
    fn terminal_outcome_is_single_and_requires_teardown() {
        // Complete a run by reaching EOS.
        let plan = committed_plan();
        let mut harness = new_run();
        harness.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });
        harness.observe(fsm::RunEvent::PlanAvailable(plan.clone()));
        harness.observe(fsm::RunEvent::TokenInEndpointReady);
        harness.observe(fsm::RunEvent::TokenOutEndpointReady);
        for event in stage_ready_events(&plan) {
            harness.observe(event);
        }
        harness.observe(fsm::RunEvent::TokenReceived {
            sequence: 0,
            token_id: 2,
            eos: true,
        });

        // Completed and Faulted are mutually exclusive public outcomes.
        let completed = harness
            .events()
            .iter()
            .filter(|event| matches!(event, fsm::LifecycleEvent::RunCompleted { .. }))
            .count();
        let faulted = harness
            .events()
            .iter()
            .filter(|event| matches!(event, fsm::LifecycleEvent::RunFaulted { .. }))
            .count();
        assert_eq!(completed, 1);
        assert_eq!(faulted, 0);

        // New token work after terminal outcome begins must be rejected.
        let before = harness.injected_sequences();
        harness.observe(fsm::RunEvent::TokenReceived {
            sequence: 99,
            token_id: 333,
            eos: false,
        });
        assert_eq!(harness.injected_sequences(), before);

        // Teardown commands must be emitted for every provisioned stage and local
        // endpoint after the terminal outcome.
        let stopped_stages = harness
            .commands()
            .iter()
            .filter_map(|command| match command {
                fsm::RunCommand::StopRun { stage_index, .. } => Some(*stage_index),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        let expected_stages = plan
            .stages
            .iter()
            .map(|stage| stage.stage_index)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(stopped_stages, expected_stages);
        assert!(
            harness.commands().iter().any(|command| {
                matches!(command, fsm::RunCommand::TearDownTokenEndpoints { .. })
            })
        );

        let mut stopped = new_run();
        stopped.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });
        stopped.observe(fsm::RunEvent::PlanAvailable(plan));
        stopped.observe(fsm::RunEvent::OperatorStop {
            run_id: fsm::RunId(7),
        });
        let stopped_count = stopped
            .events()
            .iter()
            .filter(|event| matches!(event, fsm::LifecycleEvent::RunOperatorStopped { .. }))
            .count();
        let stopped_faults = stopped
            .events()
            .iter()
            .filter(|event| matches!(event, fsm::LifecycleEvent::RunFaulted { .. }))
            .count();
        assert_eq!(stopped_count, 1);
        assert_eq!(stopped_faults, 0);
        assert!(
            stopped.commands().iter().any(|command| {
                matches!(command, fsm::RunCommand::TearDownTokenEndpoints { .. })
            })
        );
    }

    // This proves run_torn_down is emitted exactly once and only after teardown
    // observes every planned stage stop and local endpoint stop.
    #[test]
    fn run_torn_down_is_emitted_once_after_teardown_terminal_state() {
        // Fault a provisioned run so teardown is required.
        let plan = committed_plan();
        let mut harness = new_run();
        harness.observe(fsm::RunEvent::PoolReady {
            nodes: plan.stage_nodes(),
        });
        harness.observe(fsm::RunEvent::PlanAvailable(plan.clone()));
        harness.observe(fsm::RunEvent::StageFault {
            run_id: fsm::RunId(7),
            stage_index: 0,
            reason: fsm::StageFaultReason::WorkerCrashed,
        });

        // StageStopped from only a prefix of stages is not enough to finish
        // teardown.
        harness.observe(fsm::RunEvent::StageStopped {
            run_id: fsm::RunId(7),
            stage_index: 0,
        });
        assert!(
            !harness
                .events()
                .iter()
                .any(|event| { matches!(event, fsm::LifecycleEvent::RunTornDown { .. }) })
        );

        // StageStopped for every stage still is not enough until local endpoints stop.
        harness.observe(fsm::RunEvent::StageStopped {
            run_id: fsm::RunId(7),
            stage_index: 1,
        });
        harness.observe(fsm::RunEvent::StageStopped {
            run_id: fsm::RunId(7),
            stage_index: 2,
        });
        assert!(
            !harness
                .events()
                .iter()
                .any(|event| { matches!(event, fsm::LifecycleEvent::RunTornDown { .. }) })
        );
        harness.observe(fsm::RunEvent::TokenEndpointsStopped);

        // The final event may now appear, exactly once.
        let torn_down_count = harness
            .events()
            .iter()
            .filter(|event| matches!(event, fsm::LifecycleEvent::RunTornDown { .. }))
            .count();
        assert_eq!(torn_down_count, 1);

        // Ordering is proven over the lifecycle transcript.
        let fault_pos = position_of(
            harness.events(),
            &fsm::LifecycleEvent::RunFaulted {
                run_id: fsm::RunId(7),
                reason: fsm::RunFaultReason::StageFault {
                    stage_index: 0,
                    reason: fsm::StageFaultReason::WorkerCrashed,
                },
            },
        );
        let torn_down_pos = position_of(
            harness.events(),
            &fsm::LifecycleEvent::RunTornDown {
                run_id: fsm::RunId(7),
            },
        );
        assert!(fault_pos < torn_down_pos);
    }
}
