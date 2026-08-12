//! Behavior guarantees for the `orchestration` module.

mod run_plan {
    //! Black-box contract tests for Myelin RunPlan formation.
    //!
    //! These tests intentionally know only the public planning surface:
    //!
    //! - `plan_run(input) -> Result<RunPlan, PlanRejection>`
    //! - `derive_stage_provision(&plan, stage_index) -> Result<ProvisionStage, ProjectionRejection>`
    //!
    //! They assert the guarantees in `specs/BEHAVIOR_GUARANTEES.md`.
    //! The planner implementation, placement heuristic, helper APIs, internal graph
    //! representation, and allocation strategy are not observable here.

    use crate::run_plan as plan;

    // Local aliases keep the test prose readable while importing only the public
    // planning module. The aliases do not grant access to planner internals.
    type DTypeFamily = plan::DTypeFamily;
    type EdgeEndpoint = plan::EdgeEndpoint;
    type EdgeKind = plan::EdgeKind;
    type GgufSource = plan::GgufSource;
    type ModelFacts = plan::ModelFacts;
    use plan::NodeId;
    type PlacementInput = plan::PlacementInput;
    type PlanRejectionKind = plan::PlanRejectionKind;
    type PlannerInput = plan::PlannerInput;
    type RingSpec = plan::RingSpec;
    type RuntimeConfig = plan::RuntimeConfig;
    type SamplingPolicy = plan::SamplingPolicy;
    type StagePlacement = plan::StagePlacement;
    type TokenizerSource = plan::TokenizerSource;

    // Keep test node ids small and readable. The concrete identity mechanism is
    // outside this contract; these ids exist only so assertions can name topology
    // facts without depending on any address or discovery machinery.
    fn node(id: u64) -> NodeId {
        NodeId(id)
    }

    // The candidate pool is deliberately larger than some test placements. That
    // lets the tests distinguish "known to the orchestrator" from "assigned to a
    // stage", which is one of the planner authority boundaries.
    fn valid_nodes() -> Vec<NodeId> {
        vec![node(10), node(11), node(12), node(13)]
    }

    // Fixed linear placement is the smallest placement input that still exercises
    // the contract. It supplies stage-to-node intent, while the planner remains
    // responsible for validating it and minting the full RunPlan topology.
    fn linear_placement(stage_count: u32) -> PlacementInput {
        PlacementInput::FixedLinear(
            (0..stage_count)
                .map(|stage_index| StagePlacement {
                    stage_index,
                    node_id: node(10 + u64::from(stage_index)),
                })
                .collect(),
        )
    }

    // This is the canonical valid fixture for RunPlan guarantees. Each test tweaks
    // only the fact it is trying to prove, so a failure points at the violated
    // contract instead of at accidental fixture drift.
    fn valid_input(stage_count: u32, num_layers: u32) -> PlannerInput {
        PlannerInput {
            run_id: 7.into(),
            orchestrator_node_id: node(99),
            model: ModelFacts {
                model_id: "test-gguf".into(),
                gguf_source: GgufSource::LocalPath("/models/test-gguf.gguf".into()),
                num_layers,
                hidden_dim: 4096,
                dtype_family: DTypeFamily::BFloat,
                dtype_width_bytes: 2,
                max_seq_len: 2048,
                eos_token_id: 2,
                tokenizer: TokenizerSource::LocalPath("/tokenizers/test-gguf.json".into()),
            },
            runtime: RuntimeConfig {
                max_tokens: 4,
                sampling: SamplingPolicy {
                    temperature_millis: 125,
                    top_k: 7,
                },
            },
            candidate_pool: valid_nodes(),
            stage_count,
            placement: linear_placement(stage_count),
            activation_ring: RingSpec {
                data_capacity: 1 << 20,
                alignment: 64,
                direction: plan::RingDirection::Egress,
                host_pinning: plan::HostPinning::Pageable,
                wake_coalescing: plan::WakeCoalescing::PendingBit,
            },
            token_ring: RingSpec {
                data_capacity: 4096,
                alignment: 8,
                direction: plan::RingDirection::Egress,
                host_pinning: plan::HostPinning::Pageable,
                wake_coalescing: plan::WakeCoalescing::PendingBit,
            },
        }
    }

    // Edge endpoints can be orchestrator or stage endpoints. Tests use this helper
    // when they care only about stage adjacency and want orchestrator endpoints to
    // remain visibly outside the stage index space.
    fn edge_stage_index(endpoint: &EdgeEndpoint) -> Option<u32> {
        match endpoint {
            EdgeEndpoint::Orchestrator { .. } => None,
            EdgeEndpoint::Stage { stage_index, .. } => Some(*stage_index),
        }
    }

    // This proves RunPlan formation is a total public boundary for valid input:
    // the caller observes one complete plan, not hidden follow-up topology work or
    // a partially initialized result.
    #[test]
    fn valid_input_emits_one_complete_plan() {
        // Build one ordinary valid planning request.
        let input = valid_input(3, 36);

        // Planning valid input must produce a usable plan, not a deferred partial.
        let plan = plan::plan_run(input).expect("valid input must emit a plan");

        // The plan-level identifiers and counts must be complete immediately.
        assert_eq!(plan.run_id, 7.into());
        assert_eq!(plan.stages.len(), 3);
        assert_eq!(plan.edges.len(), 4);
        assert_eq!(plan.max_tokens, 4);
        assert_eq!(plan.model.model_id, "test-gguf");
        assert_eq!(
            plan.model.gguf_source,
            GgufSource::LocalPath("/models/test-gguf.gguf".into())
        );
        assert_eq!(plan.model.num_layers, 36);
        assert_eq!(plan.model.hidden_dim, 4096);
        assert_eq!(plan.model.dtype_family, DTypeFamily::BFloat);
        assert_eq!(plan.model.dtype_width_bytes, 2);
        assert_eq!(plan.model.max_seq_len, 2048);
        assert_eq!(plan.model.eos_token_id, 2);
        assert_eq!(
            plan.model.tokenizer,
            TokenizerSource::LocalPath("/tokenizers/test-gguf.json".into())
        );
        assert_eq!(
            plan.sampling,
            SamplingPolicy {
                temperature_millis: 125,
                top_k: 7,
            }
        );

        // Every stage must be bound to this run and know the run's stage count.
        for stage in &plan.stages {
            assert_eq!(stage.run_id, plan.run_id);
            assert_eq!(stage.stage_count, 3);
            assert_eq!(stage.gguf_source, plan.model.gguf_source);
        }

        // Every edge must also be bound to this run; no edge can be a loose fact.
        for edge in &plan.edges {
            assert_eq!(edge.run_id, plan.run_id);
        }
    }
    // This proves layer assignment is a contiguous, non-overlapping partition of
    // the intended GGUF block range, with one non-empty range per stage.
    #[test]
    fn stage_ranges_partition_the_model_layers() {
        // Exercise several deterministic sizes so the check covers one-stage and
        // multi-stage partitioning without relying on random generation.
        for (stage_count, num_layers) in [(1, 12), (2, 24), (3, 36), (4, 40)] {
            // Produce the plan from public inputs.
            let plan = plan::plan_run(valid_input(stage_count, num_layers)).unwrap();

            // Read only the public stage assignments and sort by stage index.
            let mut ranges = plan
                .stages
                .iter()
                .map(|stage| {
                    (
                        stage.stage_index,
                        stage.layer_start,
                        stage.layer_end_exclusive,
                    )
                })
                .collect::<Vec<_>>();
            ranges.sort_by_key(|(stage_index, _, _)| *stage_index);

            // Walk the sorted ranges as a proof of contiguity. The next start must
            // equal the previous end, and every range must consume at least one
            // layer inside the model range.
            let mut expected_start = 0;
            for (_, start, end) in ranges {
                assert_eq!(start, expected_start, "range gap or overlap");
                assert!(end > start, "stage range must be non-empty");
                assert!(end <= num_layers, "stage range exceeds model layer range");
                expected_start = end;
            }

            // The final end must cover the whole intended block range.
            assert_eq!(expected_start, num_layers);
        }
    }
    // This proves every edge has one public producer and one public consumer, and
    // that the returned edge graph is exactly the Myelin linear pipeline.
    #[test]
    fn edge_graph_is_exactly_the_linear_pipeline() {
        // Use four stages so the activation chain has multiple interior edges.
        let stage_count = 4;
        let plan = plan::plan_run(valid_input(stage_count, 40)).unwrap();

        // Token-in must be unique and must enter stage 0 from the orchestrator.
        let token_in = plan
            .edges
            .iter()
            .filter(|edge| edge.kind == EdgeKind::TokenIn)
            .collect::<Vec<_>>();
        assert_eq!(token_in.len(), 1);
        assert!(matches!(
            token_in[0].producer,
            EdgeEndpoint::Orchestrator { node_id } if node_id == node(99)
        ));
        assert_eq!(
            token_in[0].consumer,
            EdgeEndpoint::Stage {
                node_id: node(10),
                stage_index: 0,
            }
        );

        // Activation edges must be the only stage-to-stage edges, one per adjacent
        // stage pair.
        let activation_edges = plan
            .edges
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Activation)
            .collect::<Vec<_>>();
        assert_eq!(activation_edges.len(), (stage_count - 1) as usize);

        // Each activation edge produced by stage i must be consumed by stage i+1.
        for stage_index in 0..stage_count - 1 {
            let edge = activation_edges
                .iter()
                .find(|edge| edge_stage_index(&edge.producer) == Some(stage_index))
                .expect("activation edge produced by stage");

            assert_eq!(
                edge.consumer,
                EdgeEndpoint::Stage {
                    node_id: node(11 + u64::from(stage_index)),
                    stage_index: stage_index + 1,
                }
            );
            assert_ne!(edge.producer, edge.consumer, "self-edge is forbidden");
        }

        // Token-out must be unique and must leave the final stage for the
        // orchestrator.
        let token_out = plan
            .edges
            .iter()
            .filter(|edge| edge.kind == EdgeKind::TokenOut)
            .collect::<Vec<_>>();
        assert_eq!(token_out.len(), 1);
        assert_eq!(
            edge_stage_index(&token_out[0].producer),
            Some(stage_count - 1)
        );
        assert!(matches!(
            token_out[0].consumer,
            EdgeEndpoint::Orchestrator { node_id } if node_id == node(99)
        ));
    }

    // This proves edge ids are run-unique and that stage plans refer only to edge
    // ids present in the returned RunPlan, so stages receive assigned ids rather
    // than deriving data-flow identity themselves.
    #[test]
    fn edge_ids_are_unique_and_stage_references_resolve_to_plan_edges() {
        // Produce a plan with enough edges to make duplicate ids observable.
        let plan = plan::plan_run(valid_input(4, 40)).unwrap();

        // Insert every public edge id into a set; a duplicate shrinks the set.
        let edge_ids = plan
            .edges
            .iter()
            .map(|edge| edge.edge_id)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(edge_ids.len(), plan.edges.len(), "duplicate edge id");

        // Stage plans may reference only ids that the RunPlan itself assigned.
        for stage in &plan.stages {
            assert!(
                edge_ids.contains(&stage.inbound_edge),
                "stage inbound edge id must come from RunPlan edges"
            );
            assert!(
                edge_ids.contains(&stage.outbound_edge),
                "stage outbound edge id must come from RunPlan edges"
            );
        }
    }

    // This proves deriving ProvisionStage is deterministic and stage-local:
    // repeated projection returns the same value, and the projected layer range is
    // exactly the range assigned to that stage in the RunPlan.
    #[test]
    fn provision_stage_projection_is_deterministic_and_stage_local() {
        // Start from one committed plan; projection is a pure public view of it.
        let plan = plan::plan_run(valid_input(3, 36)).unwrap();

        // Check every stage projection, not only one representative stage.
        for stage_index in 0..3 {
            // Derive twice to prove projection does not depend on hidden mutable
            // state or call order.
            let first = plan::derive_stage_provision(&plan, stage_index).unwrap();
            let second = plan::derive_stage_provision(&plan, stage_index).unwrap();

            // Find the corresponding public stage assignment in the plan.
            let stage = plan
                .stages
                .iter()
                .find(|stage| stage.stage_index == stage_index)
                .unwrap();

            // The projected message must be stable and expose only that stage's
            // assigned run position and layer range.
            assert_eq!(first, second);
            assert_eq!(first.stage_index, stage_index);
            assert_eq!(first.stage_count, 3);
            assert_eq!(first.layer_start, stage.layer_start);
            assert_eq!(first.layer_end_exclusive, stage.layer_end_exclusive);
            assert_eq!(first.gguf_source, stage.gguf_source);
            assert_eq!(first.tokenizer, plan.model.tokenizer);
            assert_eq!(first.model.model_id, plan.model.model_id);
            assert_eq!(first.model.hidden_dim, plan.model.hidden_dim);
            assert_eq!(first.model.dtype_family, plan.model.dtype_family);
            assert_eq!(first.model.dtype_width_bytes, plan.model.dtype_width_bytes);
            assert_eq!(first.model.max_seq_len, plan.model.max_seq_len);
            assert_eq!(first.runtime.role_id, plan::RoleId(u64::from(stage_index)));
            assert_eq!(first.runtime.input_port, plan::PortId("input".into()));
            assert_eq!(first.runtime.output_port, plan::PortId("output".into()));
            let expected_sampling = if stage_index + 1 == stage.stage_count {
                Some(plan.sampling)
            } else {
                None
            };
            assert_eq!(first.runtime.sampling, expected_sampling);
        }
    }

    // This proves each stage receives exactly one inbound and one outbound edge
    // provision, and that the provisioned ids are the ids assigned to that stage by
    // the RunPlan.
    #[test]
    fn provision_stage_contains_exactly_the_assigned_inbound_and_outbound_edges() {
        // Build a valid plan and test projection for every stage in it.
        let plan = plan::plan_run(valid_input(3, 36)).unwrap();

        for stage in &plan.stages {
            // Derive the public provisioning message for this stage.
            let provision = plan::derive_stage_provision(&plan, stage.stage_index).unwrap();

            // The message exposes exactly the inbound and outbound ids assigned in
            // that stage's StagePlan.
            assert_eq!(provision.inbound.edge_id, stage.inbound_edge);
            assert_eq!(provision.outbound.edge_id, stage.outbound_edge);
        }
    }
    // This proves typed rejection is the public behavior for invalid authority and
    // topology inputs. No invalid case is allowed to emit a partial plan.
    #[test]
    fn invalid_authority_and_topology_inputs_reject_without_plan() {
        // Each case changes one authority/topology fact from the valid fixture and
        // names the typed rejection the planner must expose.
        let cases = [
            (
                invalid_unknown_node(),
                PlanRejectionKind::UnknownNode,
                "unknown node id",
            ),
            (
                invalid_duplicate_stage_assignment(),
                PlanRejectionKind::DuplicateStageAssignment,
                "duplicate stage assignment",
            ),
            (
                invalid_missing_stage_assignment(),
                PlanRejectionKind::MissingStage,
                "missing stage",
            ),
            (
                invalid_zero_stage_count(),
                PlanRejectionKind::InvalidStageCount,
                "invalid stage count",
            ),
            (
                invalid_model_stage_layout(),
                PlanRejectionKind::ModelStageLayoutMismatch,
                "model/stage layout mismatch",
            ),
        ];

        // Invalid inputs must not produce a partial plan. The observable result is
        // a typed rejection kind.
        for (input, expected, label) in cases {
            let err = plan::plan_run(input).expect_err(label);
            assert_eq!(err.kind(), expected, "{label}");
        }
    }
    // Unknown-node rejection needs the placement to name a node outside the
    // orchestrator's candidate pool. The rest of the input stays valid so the
    // expected rejection is isolated to authority over node identity.
    fn invalid_unknown_node() -> PlannerInput {
        let mut input = valid_input(3, 36);
        input.placement = PlacementInput::FixedLinear(vec![
            StagePlacement {
                stage_index: 0,
                node_id: node(10),
            },
            StagePlacement {
                stage_index: 1,
                node_id: node(404),
            },
            StagePlacement {
                stage_index: 2,
                node_id: node(12),
            },
        ]);
        input
    }

    // Duplicate-stage rejection is observable when two placement entries claim the
    // same stage index. This checks that the planner does not silently pick one and
    // continue with ambiguous authority.
    fn invalid_duplicate_stage_assignment() -> PlannerInput {
        let mut input = valid_input(3, 36);
        input.placement = PlacementInput::FixedLinear(vec![
            StagePlacement {
                stage_index: 0,
                node_id: node(10),
            },
            StagePlacement {
                stage_index: 1,
                node_id: node(11),
            },
            StagePlacement {
                stage_index: 1,
                node_id: node(12),
            },
        ]);
        input
    }

    // Missing-stage rejection is observable when placement skips an index inside
    // `0..stage_count`. This checks that the planner does not invent hidden stage
    // ownership to patch an incomplete placement.
    fn invalid_missing_stage_assignment() -> PlannerInput {
        let mut input = valid_input(3, 36);
        input.placement = PlacementInput::FixedLinear(vec![
            StagePlacement {
                stage_index: 0,
                node_id: node(10),
            },
            StagePlacement {
                stage_index: 2,
                node_id: node(12),
            },
        ]);
        input
    }

    // Zero stages cannot form the Myelin pipeline. This fixture isolates the invalid
    // stage-count path without adding any other contradictory facts.
    fn invalid_zero_stage_count() -> PlannerInput {
        valid_input(0, 36)
    }

    // The current contract requires one non-empty layer range per stage. Fewer
    // layers than stages forces an empty range unless explicitly allowed, so this
    // fixture should reject at the model/stage-layout boundary.
    fn invalid_model_stage_layout() -> PlannerInput {
        let mut input = valid_input(4, 3);
        input.placement = linear_placement(4);
        input
    }
}

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
