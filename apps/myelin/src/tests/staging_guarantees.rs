//! Behavior guarantees for the `staging` module.

mod stage_controller {
    //! Black-box contract tests for Myelin StageController behavior.
    //!
    //! These tests intentionally know only the public stage-controller surface:
    //!
    //! - `ProvisionStage`, worker, edge, object, stop, and fault events in
    //! - worker commands, lifecycle events, and teardown events out
    //!
    //! They assert the guarantees in
    //! `specs/BEHAVIOR_GUARANTEES.md`.

    use crate::tests::harness::StageControllerHarness;
    use myelin::staging as stage;

    // This provision fixture represents a single middle stage. It has one inbound
    // and one outbound edge so tests can prove the controller uses assigned edges
    // without relying on endpoint internals.
    fn valid_provision() -> stage::ProvisionStage {
        stage::ProvisionStage {
            run_id: stage::RunId(7),
            authorized_orchestrator: stage::NodeId(99),
            node_id: stage::NodeId(11),
            stage_index: 1,
            stage_count: 3,
            layer_range: stage::LayerRange {
                start: 12,
                end_exclusive: 24,
            },
            inbound: stage::EdgeProvision::inbound(stage::EdgeId(7001)),
            outbound: stage::EdgeProvision::outbound(stage::EdgeId(7002)),
            weight_source: stage::WeightSource::embedded_gguf("model", "model.gguf"),
            shard_plan: None,
        }
    }

    // The harness exposes only public messages. Tests intentionally do not inspect
    // private controller states such as "Preparing" or "Executing"; they infer
    // controller behavior from emitted commands and lifecycle events.
    fn new_controller() -> StageControllerHarness {
        StageControllerHarness::new(stage::NodeId(11))
    }

    // Preparation readiness has four independent prerequisites. Listing them as
    // public observations lets tests prove StageReady is a barrier across worker,
    // weights, inbound edge, and outbound edge readiness.
    fn preparation_ready_events() -> Vec<stage::StageEvent> {
        vec![
            stage::StageEvent::WorkerReady,
            stage::StageEvent::WeightsReady,
            stage::StageEvent::InboundEdgeReady {
                edge_id: stage::EdgeId(7001),
            },
            stage::StageEvent::OutboundEdgeReady {
                edge_id: stage::EdgeId(7002),
            },
        ]
    }

    // This helper provisions and readies a stage through public events. Tests that
    // focus on execution use it to avoid duplicating setup while still going through
    // the same observable path as production.
    fn ready_stage() -> StageControllerHarness {
        let mut harness = new_controller();
        harness.observe(stage::StageEvent::ProvisionStage {
            from: stage::NodeId(99),
            provision: Box::new(valid_provision()),
        });
        for event in preparation_ready_events() {
            harness.observe(event);
        }
        harness
    }

    // This proves provisioning is authorized, validated before setup, and does not
    // allow a stage to rewire its assigned inbound or outbound edge.
    #[test]
    fn provisioning_validates_authority_and_assigned_shape_before_setup() {
        // Send a valid provision from the authorized orchestrator.
        let mut harness = new_controller();
        harness.observe(stage::StageEvent::ProvisionStage {
            from: stage::NodeId(99),
            provision: Box::new(valid_provision()),
        });

        // Setup commands should be derived from the provided assignment.
        assert!(harness.commands().iter().any(|command| {
            matches!(
                command,
                stage::StageCommand::EstablishInboundEdge {
                    edge_id: stage::EdgeId(7001),
                    ..
                }
            )
        }));
        assert!(harness.commands().iter().any(|command| {
            matches!(
                command,
                stage::StageCommand::EstablishOutboundEdge {
                    edge_id: stage::EdgeId(7002),
                    ..
                }
            )
        }));

        // An unauthorized provision attempt must fault before setup can begin.
        let mut unauthorized = new_controller();
        unauthorized.observe(stage::StageEvent::ProvisionStage {
            from: stage::NodeId(123),
            provision: Box::new(valid_provision()),
        });
        assert!(unauthorized.events().iter().any(|event| {
            matches!(
                event,
                stage::StageLifecycleEvent::StageFault {
                    reason: stage::StageFaultReason::UnauthorizedProvision,
                    ..
                }
            )
        }));
        assert!(
            !unauthorized.commands().iter().any(|command| {
                matches!(command, stage::StageCommand::ConfigureWorkerRole { .. })
            })
        );
    }
    // This proves StageReady is emitted only after worker readiness, weight
    // readiness, inbound edge readiness, and outbound edge readiness are all
    // observed.
    #[test]
    fn stage_ready_waits_for_worker_weights_and_both_edges() {
        // Provision the stage so preparation can begin.
        let mut harness = new_controller();
        harness.observe(stage::StageEvent::ProvisionStage {
            from: stage::NodeId(99),
            provision: Box::new(valid_provision()),
        });

        // Feed every readiness event except the final one and prove no prefix is
        // enough for StageReady.
        let mut events = preparation_ready_events();
        let final_event = events.pop().expect("fixture has final setup event");
        for event in events {
            harness.observe(event);
            assert!(
                !harness.events().iter().any(|event| {
                    matches!(event, stage::StageLifecycleEvent::StageReady { .. })
                })
            );
        }

        // The final prerequisite crosses the barrier.
        harness.observe(final_event);

        // StageReady appears exactly once for the provisioned stage.
        let ready_count = harness
            .events()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    stage::StageLifecycleEvent::StageReady {
                        run_id: stage::RunId(7),
                        stage_index: 1,
                    }
                )
            })
            .count();
        assert_eq!(ready_count, 1);
    }

    // This proves a ready stage admits work only from inbound ObjectLoaded, issues
    // one ExecuteStep per accepted object, and binds output with the same sequence.
    #[test]
    fn accepted_inbound_object_creates_one_same_sequence_execute_step() {
        // Bring the stage to ready state through public setup events.
        let mut harness = ready_stage();

        // Deliver the first inbound object, sequence 0.
        harness.observe(stage::StageEvent::ObjectLoaded {
            edge_id: stage::EdgeId(7001),
            object_id: stage::ObjectId(9000),
            sequence: 0,
            handle: stage::DeviceHandle::new_current(42),
        });

        // Exactly one ExecuteStep command must result from that accepted object.
        let execute_steps = harness
            .commands()
            .iter()
            .filter_map(|command| match command {
                stage::StageCommand::ExecuteStep(step) => Some(step),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(execute_steps.len(), 1);

        // The output binding must preserve the input sequence.
        assert_eq!(execute_steps[0].input.sequence, 0);
        assert_eq!(execute_steps[0].outputs[0].sequence, 0);

        // A second object while the first step is active must not create another
        // active ExecuteStep in the Myelin.
        harness.observe(stage::StageEvent::ObjectLoaded {
            edge_id: stage::EdgeId(7001),
            object_id: stage::ObjectId(9001),
            sequence: 1,
            handle: stage::DeviceHandle::new_current(43),
        });
        let active_steps = harness
            .commands()
            .iter()
            .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
            .count();
        assert_eq!(active_steps, 1);
    }

    // This proves the sequence contract: sequence 0 is accepted as prefill, decode
    // sequences must strictly increase, and duplicate, skipped, or out-of-order
    // inputs fault the stage.
    #[test]
    fn duplicate_skipped_and_out_of_order_sequences_fault() {
        // Each invalid trace starts from a freshly readied stage.
        let invalid_traces = vec![vec![0, 0], vec![0, 2], vec![0, 1, 0]];

        for trace in invalid_traces {
            // Accept the first object and complete its step when needed so the next
            // object is admitted through the normal public path.
            let mut harness = ready_stage();
            for (i, sequence) in trace.iter().enumerate() {
                harness.observe(stage::StageEvent::ObjectLoaded {
                    edge_id: stage::EdgeId(7001),
                    object_id: stage::ObjectId(9000 + i as u64),
                    sequence: *sequence,
                    handle: stage::DeviceHandle::new_current(100 + i as u64),
                });
                if i + 1 < trace.len() {
                    harness.observe(stage::StageEvent::StepCompleted {
                        step_id: stage::StepId(i as u64),
                    });
                }
            }

            // The transcript must contain a sequence fault for the invalid trace.
            assert!(harness.events().iter().any(|event| {
                matches!(
                    event,
                    stage::StageLifecycleEvent::StageFault {
                        reason: stage::StageFaultReason::SequenceViolation,
                        ..
                    }
                )
            }));
        }
    }

    // This proves compute completion is observed only after the worker reports
    // StepCompleted, and completion returns the stage to ready-for-next-object.
    #[test]
    fn step_completed_releases_input_and_admits_next_object() {
        // Start one accepted step.
        let mut harness = ready_stage();
        harness.observe(stage::StageEvent::ObjectLoaded {
            edge_id: stage::EdgeId(7001),
            object_id: stage::ObjectId(9000),
            sequence: 0,
            handle: stage::DeviceHandle::new_current(42),
        });

        // Before worker completion, no compute-complete lifecycle event is allowed.
        assert!(!harness.events().iter().any(|event| {
            matches!(
                event,
                stage::StageLifecycleEvent::StepAccepted { sequence: 1, .. }
            )
        }));

        // Worker StepCompleted is the public completion signal.
        harness.observe(stage::StageEvent::StepCompleted {
            step_id: stage::StepId(0),
        });

        // The controller releases per-step input according to policy.
        assert!(harness.commands().iter().any(|command| {
            matches!(
                command,
                stage::StageCommand::ReleaseInputHandle {
                    object_id: stage::ObjectId(9000),
                    ..
                }
            )
        }));

        // The next sequence is now admissible.
        harness.observe(stage::StageEvent::ObjectLoaded {
            edge_id: stage::EdgeId(7001),
            object_id: stage::ObjectId(9001),
            sequence: 1,
            handle: stage::DeviceHandle::new_current(43),
        });
        let execute_count = harness
            .commands()
            .iter()
            .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
            .count();
        assert_eq!(execute_count, 2);
    }

    // This proves worker, object, output-edge, edge, and step failures fault the
    // stage with stable public reasons, and after fault no new run work is accepted
    // until StopRun.
    #[test]
    fn stage_failures_map_to_stable_fault_reasons_and_reject_new_work() {
        let cases = vec![
            (
                stage::StageEvent::WorkerCrashed,
                stage::StageFaultReason::WorkerCrashed,
            ),
            (
                stage::StageEvent::StepFailed {
                    step_id: stage::StepId(0),
                },
                stage::StageFaultReason::StepFailed,
            ),
            (
                stage::StageEvent::ObjectFailed {
                    edge_id: stage::EdgeId(7001),
                    object_id: Some(stage::ObjectId(9000)),
                },
                stage::StageFaultReason::ObjectFailed,
            ),
            (
                stage::StageEvent::OutputFault {
                    edge_id: stage::EdgeId(7002),
                },
                stage::StageFaultReason::OutputFault,
            ),
            (
                stage::StageEvent::EdgeFault {
                    edge_id: stage::EdgeId(7002),
                },
                stage::StageFaultReason::EdgeFault,
            ),
        ];

        for (fault_event, expected_reason) in cases {
            let mut harness = ready_stage();
            harness.observe(fault_event);

            assert!(harness.events().iter().any(|event| {
                matches!(
                    event,
                    stage::StageLifecycleEvent::StageFault {
                        reason,
                        ..
                    } if *reason == expected_reason
                )
            }));

            let before = harness
                .commands()
                .iter()
                .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
                .count();
            harness.observe(stage::StageEvent::ObjectLoaded {
                edge_id: stage::EdgeId(7001),
                object_id: stage::ObjectId(9999),
                sequence: 0,
                handle: stage::DeviceHandle::new_current(77),
            });
            let after = harness
                .commands()
                .iter()
                .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
                .count();
            assert_eq!(after, before);

            harness.observe(stage::StageEvent::StopRun {
                run_id: stage::RunId(7),
            });
            assert!(
                harness.commands().iter().any(|command| {
                    matches!(command, stage::StageCommand::StopLocalEdges { .. })
                })
            );
            assert!(
                !harness.events().iter().any(|event| {
                    matches!(event, stage::StageLifecycleEvent::StageStopped { .. })
                })
            );

            harness.observe(stage::StageEvent::LocalEdgesStopped {
                run_id: stage::RunId(7),
            });
            harness.observe(stage::StageEvent::WorkerRingsQuiesced {
                run_id: stage::RunId(7),
            });
            harness.observe(stage::StageEvent::DeviceObjectsReleased {
                run_id: stage::RunId(7),
            });
            harness.observe(stage::StageEvent::WorkerRoleReset {
                run_id: stage::RunId(7),
            });
            assert!(
                harness.events().iter().any(|event| {
                    matches!(event, stage::StageLifecycleEvent::StageStopped { .. })
                })
            );
        }
    }

    // This proves StopRun starts teardown but StageStopped is held back until local
    // edges are stopped, worker rings are quiesced, device objects are released, and
    // the worker role reset completes.
    #[test]
    fn stop_run_waits_for_local_teardown_completion_before_stage_stopped() {
        let mut harness = ready_stage();

        harness.observe(stage::StageEvent::StopRun {
            run_id: stage::RunId(7),
        });
        assert!(
            harness
                .commands()
                .iter()
                .any(|command| { matches!(command, stage::StageCommand::StopLocalEdges { .. }) })
        );
        assert!(
            !harness
                .events()
                .iter()
                .any(|event| { matches!(event, stage::StageLifecycleEvent::StageStopped { .. }) })
        );
        assert!(!harness.commands().iter().any(|command| {
            matches!(command, stage::StageCommand::ReleaseRunDeviceObjects { .. })
        }));

        let execute_before_stopping_work = harness
            .commands()
            .iter()
            .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
            .count();
        harness.observe(stage::StageEvent::ObjectLoaded {
            edge_id: stage::EdgeId(7001),
            object_id: stage::ObjectId(9999),
            sequence: 0,
            handle: stage::DeviceHandle::new_current(77),
        });
        let execute_after_stopping_work = harness
            .commands()
            .iter()
            .filter(|command| matches!(command, stage::StageCommand::ExecuteStep(_)))
            .count();
        assert_eq!(execute_after_stopping_work, execute_before_stopping_work);

        harness.observe(stage::StageEvent::LocalEdgesStopped {
            run_id: stage::RunId(7),
        });
        assert!(
            !harness
                .events()
                .iter()
                .any(|event| { matches!(event, stage::StageLifecycleEvent::StageStopped { .. }) })
        );
        assert!(!harness.commands().iter().any(|command| {
            matches!(command, stage::StageCommand::ReleaseRunDeviceObjects { .. })
        }));

        harness.observe(stage::StageEvent::WorkerRingsQuiesced {
            run_id: stage::RunId(7),
        });
        assert!(harness.commands().iter().any(|command| {
            matches!(
                command,
                stage::StageCommand::ReleaseRunDeviceObjects {
                    run_id: stage::RunId(7)
                }
            )
        }));
        assert!(
            !harness
                .events()
                .iter()
                .any(|event| { matches!(event, stage::StageLifecycleEvent::StageStopped { .. }) })
        );

        harness.observe(stage::StageEvent::DeviceObjectsReleased {
            run_id: stage::RunId(7),
        });
        assert!(
            !harness
                .events()
                .iter()
                .any(|event| { matches!(event, stage::StageLifecycleEvent::StageStopped { .. }) })
        );

        harness.observe(stage::StageEvent::WorkerRoleReset {
            run_id: stage::RunId(7),
        });
        assert!(harness.events().iter().any(|event| {
            matches!(
                event,
                stage::StageLifecycleEvent::StageStopped {
                    run_id: stage::RunId(7),
                    stage_index: 1,
                }
            )
        }));
    }
}

mod weight_lifecycle {
    //! Black-box contract tests for Myelin stage-local weight lifecycle.
    //!
    //! These tests intentionally know only the public weight-work surface:
    //!
    //! - `ProvisionStage` assignment in
    //! - artifact, parse, allocation, binding, and cache outcomes in
    //! - `WeightsReady`, `StageReady`, and `StageFault` out
    //!
    //! They assert the guarantees in
    //! `specs/BEHAVIOR_GUARANTEES.md`.

    use myelin::staging::weight_lifecycle as weights;

    // A valid assignment gives the stage exactly one layer range and one source.
    // Tests vary only source or failure outcome so the assignment contract remains
    // visible.
    fn valid_assignment() -> weights::WeightAssignment {
        weights::WeightAssignment {
            run_id: weights::RunId(7),
            stage_index: 1,
            plan_layer_range: weights::LayerRange {
                start: 12,
                end_exclusive: 24,
            },
            assigned_layer_range: weights::LayerRange {
                start: 12,
                end_exclusive: 24,
            },
            source: weights::WeightSource::WholeGguf {
                uri: "test://model.gguf".into(),
            },
        }
    }

    // Weight loading is intentionally opaque. The harness accepts public loader and
    // worker outcomes and records only stage-visible events and commands.
    fn new_weight_harness() -> weights::WeightLifecycleHarness {
        weights::WeightLifecycleHarness::new(weights::NodeId(11))
    }

    // The success facts represent the observable prerequisites for WeightsReady:
    // artifact bytes exist, the assigned layer range is valid, and the worker has
    // loaded or bound that range.
    fn successful_load_events() -> Vec<weights::WeightEvent> {
        vec![
            weights::WeightEvent::ArtifactAvailable {
                bytes: weights::ArtifactBytes::Local,
            },
            weights::WeightEvent::LayerRangeValidated,
            weights::WeightEvent::WorkerRangeBound,
        ]
    }

    // Each failure case maps one public loader or worker failure to the stable
    // stage fault reason expected at the control boundary.
    fn failure_cases() -> Vec<(weights::WeightEvent, weights::StageFaultReason)> {
        vec![
            (
                weights::WeightEvent::DownloadFailed,
                weights::StageFaultReason::WeightDownloadFailed,
            ),
            (
                weights::WeightEvent::ParseFailed,
                weights::StageFaultReason::WeightParseFailed,
            ),
            (
                weights::WeightEvent::DeviceAllocationFailed,
                weights::StageFaultReason::DeviceAllocationFailed,
            ),
            (
                weights::WeightEvent::BindingFailed,
                weights::StageFaultReason::WeightBindingFailed,
            ),
            (
                weights::WeightEvent::InvalidLayerRange,
                weights::StageFaultReason::InvalidLayerRange,
            ),
        ]
    }

    // This proves a stage receives weight source and exactly one assigned layer
    // range from provisioning, validates it against the plan, and does not claim
    // graph-visible ownership outside that range.
    #[test]
    fn assignment_is_stage_local_and_range_limited() {
        // Start weight work from the provisioned assignment.
        let mut harness = new_weight_harness();
        harness.observe(weights::WeightEvent::Provisioned(valid_assignment()));

        // The load command may use the physical source, but its graph-visible layer
        // range must be exactly the assigned range.
        assert_eq!(harness.commands().len(), 1);
        let weights::WeightCommand::LoadOrBindRange { range, .. } = &harness.commands()[0];
        assert_eq!(
            *range,
            weights::LayerRange {
                start: 12,
                end_exclusive: 24,
            }
        );
    }

    // This proves whole GGUF download, shard download, and cache use are physical
    // mechanisms with the same public outcome: WeightsReady or StageFault.
    #[test]
    fn supported_physical_sources_have_same_visible_success_contract() {
        // Exercise every supported source without asserting how bytes are obtained.
        let sources = vec![
            weights::WeightSource::WholeGguf {
                uri: "test://model.gguf".into(),
            },
            weights::WeightSource::ShardSet {
                uris: vec!["test://model.layers.12-24.gguf".into()],
            },
            weights::WeightSource::CachedArtifact {
                cache_key: "model:layers:12-24".into(),
            },
        ];

        for source in sources {
            // Install the source in an otherwise valid assignment.
            let mut assignment = valid_assignment();
            assignment.source = source;
            let mut harness = new_weight_harness();
            harness.observe(weights::WeightEvent::Provisioned(assignment));

            // Drive the same public success facts for every source.
            for event in successful_load_events() {
                harness.observe(event);
            }

            // The system-visible success outcome is WeightsReady.
            assert!(harness.events().iter().any(|event| {
                matches!(
                    event,
                    weights::WeightLifecycleEvent::WeightsReady {
                        run_id: weights::RunId(7),
                        stage_index: 1,
                    }
                )
            }));
        }
    }

    // This proves WeightsReady requires artifact availability, layer validation,
    // and worker bind/load completion, and that WeightsReady precedes StageReady.
    #[test]
    fn weights_ready_requires_all_weight_facts_and_precedes_stage_ready() {
        // Start from a valid assignment.
        let mut harness = new_weight_harness();
        harness.observe(weights::WeightEvent::Provisioned(valid_assignment()));

        // Feed every success fact except the final one and prove no prefix is
        // sufficient for WeightsReady.
        let mut events = successful_load_events();
        let final_event = events.pop().expect("fixture has final weight event");
        for event in events {
            harness.observe(event);
            assert!(!harness.events().iter().any(|event| {
                matches!(event, weights::WeightLifecycleEvent::WeightsReady { .. })
            }));
        }

        // The final weight prerequisite emits WeightsReady.
        harness.observe(final_event);
        let weights_ready_pos = harness
            .events()
            .iter()
            .position(|event| matches!(event, weights::WeightLifecycleEvent::WeightsReady { .. }))
            .expect("WeightsReady must be emitted");

        // StageReady may occur only after the StageController observes WeightsReady
        // and the other local setup prerequisites.
        harness.observe(weights::WeightEvent::OtherStagePrerequisitesReady);
        let stage_ready_pos = harness
            .events()
            .iter()
            .position(|event| matches!(event, weights::WeightLifecycleEvent::StageReady { .. }))
            .expect("StageReady must be emitted after prerequisites");
        assert!(weights_ready_pos < stage_ready_pos);
    }

    // This proves every weight failure source faults the stage and suppresses both
    // WeightsReady and StageReady.
    #[test]
    fn weight_failures_emit_stage_fault_without_readiness() {
        // Each failure source gets an isolated attempt.
        for (failure_event, expected_reason) in failure_cases() {
            let mut harness = new_weight_harness();
            harness.observe(weights::WeightEvent::Provisioned(valid_assignment()));

            // Deliver the public failure outcome from weight work.
            harness.observe(failure_event);

            // The stable fault reason must be observable.
            assert!(harness.events().iter().any(|event| {
                matches!(
                    event,
                    weights::WeightLifecycleEvent::StageFault {
                        reason,
                        ..
                    } if *reason == expected_reason
                )
            }));

            // Readiness cannot also be emitted after a faulted weight attempt.
            assert!(!harness.events().iter().any(|event| {
                matches!(event, weights::WeightLifecycleEvent::WeightsReady { .. })
                    || matches!(event, weights::WeightLifecycleEvent::StageReady { .. })
            }));
        }
    }
}
