//! Black-box contract tests for MVP RunPlan formation.
//!
//! These tests intentionally know only the public planning surface:
//!
//! - `plan_run(input) -> Result<RunPlan, PlanRejection>`
//! - `derive_stage_provision(&plan, stage_index) -> Result<ProvisionStage, ProjectionRejection>`
//!
//! They assert the guarantees in `specs/mvp_system/run_plan_contract.md`.
//! The planner implementation, placement heuristic, helper APIs, internal graph
//! representation, and allocation strategy are not observable here.

use mvp_system::run_plan as plan;

// Local aliases keep the test prose readable while the file imports only the
// public planning module. The aliases do not grant access to planner internals.
type DTypeFamily = plan::DTypeFamily;
type EdgeEndpoint = plan::EdgeEndpoint;
type EdgeId = plan::EdgeId;
type EdgeKind = plan::EdgeKind;
type EdgePlan = plan::EdgePlan;
type GgufSource = plan::GgufSource;
type HostPinning = plan::HostPinning;
type InboundEdgeProvision = plan::InboundEdgeProvision;
type ModelFacts = plan::ModelFacts;
use plan::NodeId;
type LayoutRule = plan::LayoutRule;
type ObjectKind = plan::ObjectKind;
type OutboundEdgeProvision = plan::OutboundEdgeProvision;
type PromptSource = plan::PromptSource;
type PlacementInput = plan::PlacementInput;
type PlanRejectionKind = plan::PlanRejectionKind;
type PlannerInput = plan::PlannerInput;
type RingSpec = plan::RingSpec;
type RingDirection = plan::RingDirection;
type RunPlan = plan::RunPlan;
type RuntimeConfig = plan::RuntimeConfig;
type SamplingPolicy = plan::SamplingPolicy;
type SequencePolicy = plan::SequencePolicy;
type ShapeRule = plan::ShapeRule;
type StagePlacement = plan::StagePlacement;
type TokenOutputPolicy = plan::TokenOutputPolicy;
type TokenizerSource = plan::TokenizerSource;
type WakeCoalescing = plan::WakeCoalescing;

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
            prompt: PromptSource::Inline("hello from planner input".into()),
            sampling: SamplingPolicy {
                temperature_millis: 125,
                top_k: 7,
            },
            token_output_policy: TokenOutputPolicy::EmitAll,
        },
        candidate_pool: valid_nodes(),
        stage_count,
        placement: linear_placement(stage_count),
        activation_ring: RingSpec::test_default_activation(),
        token_ring: RingSpec::test_default_token(),
    }
}

// Tests frequently need to compare a provisioned edge id back to the canonical
// edge record in the RunPlan. This helper makes that lookup explicit without
// giving tests access to any planner-private index.
fn plan_edges_by_id(plan: &RunPlan) -> std::collections::BTreeMap<EdgeId, &EdgePlan> {
    plan.edges
        .iter()
        .map(|edge| (edge.edge_id, edge))
        .collect::<std::collections::BTreeMap<_, _>>()
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

// Provisioning sends concrete node ids across the data-flow boundary. This
// helper extracts the observable node id from either endpoint shape so tests
// can compare projection output to plan topology.
fn edge_node_id(endpoint: &EdgeEndpoint) -> NodeId {
    match endpoint {
        EdgeEndpoint::Orchestrator { node_id } => *node_id,
        EdgeEndpoint::Stage { node_id, .. } => *node_id,
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
        plan.runtime.sampling,
        SamplingPolicy {
            temperature_millis: 125,
            top_k: 7,
        }
    );
    assert_eq!(
        plan.runtime.prompt,
        PromptSource::Inline("hello from planner input".into())
    );
    assert_eq!(plan.runtime.token_output_policy, TokenOutputPolicy::EmitAll);

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

// This proves planning does not mutate the candidate pool supplied by the
// caller. The only topology facts the caller can use after planning are the
// facts emitted in the RunPlan itself.
#[test]
fn planner_does_not_mutate_candidate_pool() {
    // Keep a copy of the caller-owned pool before the planner sees it.
    let input = valid_input(3, 36);
    let original_pool = input.candidate_pool.clone();

    // Run planning through the public API only.
    let _ = plan::plan_run(input.clone()).expect("valid input must emit a plan");

    // The input pool remains the caller's fact; topology facts must be in the
    // returned plan, not back-written into the input.
    assert_eq!(input.candidate_pool, original_pool);
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

// This proves stage indices are exactly the dense range required by the
// contract. Missing, duplicate, or out-of-range stage indices are observable in
// the returned RunPlan and fail this check.
#[test]
fn stage_indices_are_exactly_zero_to_stage_count_minus_one() {
    // Check several stage counts so the dense-index guarantee is not tied to
    // the canonical three-stage fixture.
    for stage_count in 1..=4 {
        // Produce a valid plan and observe only its public stage indices.
        let plan = plan::plan_run(valid_input(stage_count, stage_count * 8)).unwrap();
        let observed = plan
            .stages
            .iter()
            .map(|stage| stage.stage_index)
            .collect::<std::collections::BTreeSet<_>>();

        // Compare against the contract's exact dense index set.
        let expected = (0..stage_count).collect::<std::collections::BTreeSet<_>>();

        assert_eq!(observed, expected);
    }
}

// This proves every edge has one public producer and one public consumer, and
// that the returned edge graph is exactly the MVP linear pipeline.
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
        assert_eq!(first.model.model_id, plan.model.model_id);
        assert_eq!(first.model.hidden_dim, plan.model.hidden_dim);
        assert_eq!(first.model.dtype_family, plan.model.dtype_family);
        assert_eq!(first.model.dtype_width_bytes, plan.model.dtype_width_bytes);
        assert_eq!(first.model.max_seq_len, plan.model.max_seq_len);
        assert_eq!(first.runtime.role_id, plan::RoleId(u64::from(stage_index)));
        assert_eq!(first.runtime.input_port, plan::PortId("input".into()));
        assert_eq!(first.runtime.output_port, plan::PortId("output".into()));
        let expected_sampling = if stage_index + 1 == stage.stage_count {
            Some(plan.runtime.sampling)
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

// This proves the data-flow addressing contract of provisioning. The outbound
// side carries the consumer node id; exhaustive struct destructuring also makes
// remote actor-address fields a compile-time contract violation.
#[test]
fn provision_stage_uses_node_id_addressing_not_remote_actor_addresses() {
    // Build one plan and an edge lookup using only public edge records.
    let plan = plan::plan_run(valid_input(3, 36)).unwrap();
    let edges = plan_edges_by_id(&plan);

    for stage in &plan.stages {
        // Project the stage-local provisioning message.
        let provision = plan::derive_stage_provision(&plan, stage.stage_index).unwrap();

        // Destructure the inbound provision exhaustively. If a remote actor
        // address becomes part of this public type, this test must be updated
        // consciously instead of silently accepting it.
        let InboundEdgeProvision {
            edge_id: inbound_edge_id,
            kind: _,
            object_spec: _,
            ring_spec: _,
        } = provision.inbound.clone();

        // Destructure the outbound provision exhaustively. The only remote
        // routing fact it may expose is the consumer node id.
        let OutboundEdgeProvision {
            edge_id: outbound_edge_id,
            kind: _,
            consumer_node_id,
            object_spec: _,
            ring_spec: _,
        } = provision.outbound.clone();

        assert_eq!(inbound_edge_id, stage.inbound_edge);
        assert_eq!(outbound_edge_id, stage.outbound_edge);

        // The provisioned consumer node id must match the consumer endpoint of
        // the canonical RunPlan edge.
        let outbound_edge = edges.get(&outbound_edge_id).unwrap();
        assert_eq!(consumer_node_id, edge_node_id(&outbound_edge.consumer));
    }
}

// This proves every edge carries object and ring specs, activation capacity is
// derived from model facts, and edge kind selects the correct object kind.
#[test]
fn object_and_ring_specs_are_present_and_match_edge_kind() {
    // Use the canonical model facts so the expected activation capacity is
    // known directly from the public input.
    let plan = plan::plan_run(valid_input(3, 36)).unwrap();
    let expected_activation_extent = 2048 * 4096 * 2;

    // Every edge must carry complete movement specs; no worker or transport may
    // invent these later.
    for edge in &plan.edges {
        assert!(edge.object_spec.max_extent > 0);
        assert!(edge.ring_spec.data_capacity > 0);
        assert!(edge.object_spec.alignment > 0);
        assert_eq!(edge.object_spec.layout, LayoutRule::Contiguous);
        assert_eq!(edge.object_spec.sequence_policy, SequencePolicy::Ordered);
        assert!(edge.ring_spec.alignment > 0);
        assert_eq!(edge.ring_spec.direction, RingDirection::Egress);
        assert_eq!(edge.ring_spec.host_pinning, HostPinning::Pageable);
        assert_eq!(edge.ring_spec.wake_coalescing, WakeCoalescing::PendingBit);

        // Edge kind selects the object kind, and activation capacity is derived
        // from model facts.
        match edge.kind {
            EdgeKind::Activation => {
                assert_eq!(edge.object_spec.kind, ObjectKind::Activation);
                assert_eq!(edge.object_spec.max_extent, expected_activation_extent);
                assert_eq!(edge.object_spec.dtype_family, DTypeFamily::BFloat);
                assert_eq!(edge.object_spec.dtype_width_bytes, 2);
                assert_eq!(
                    edge.object_spec.shape,
                    ShapeRule::ActivationRows {
                        max_seq_len: 2048,
                        hidden_dim: 4096,
                    }
                );
            }
            EdgeKind::TokenIn | EdgeKind::TokenOut => {
                assert_eq!(edge.object_spec.kind, ObjectKind::Token);
                assert_eq!(edge.object_spec.dtype_width_bytes, 4);
                assert_eq!(edge.object_spec.shape, ShapeRule::TokenIds);
            }
        }
    }
}

// This proves object and ring specs are projected consistently into every stage
// provision. A stage provision narrows the ring direction to the local role
// while preserving the edge's capacity, alignment, pinning, and wake policy.
#[test]
fn object_and_ring_specs_are_copied_consistently_into_stage_provisions() {
    // Build a canonical plan and index its public edge records.
    let plan = plan::plan_run(valid_input(3, 36)).unwrap();
    let edges = plan_edges_by_id(&plan);

    for stage in &plan.stages {
        // Project a stage-local provisioning message.
        let provision = plan::derive_stage_provision(&plan, stage.stage_index).unwrap();

        // The inbound edge spec must preserve the plan edge facts and mark the
        // local ring as ingress.
        let inbound_edge = edges.get(&provision.inbound.edge_id).unwrap();
        let mut expected_inbound_ring = inbound_edge.ring_spec;
        expected_inbound_ring.direction = RingDirection::Ingress;
        assert_eq!(provision.inbound.kind, inbound_edge.kind);
        assert_eq!(provision.inbound.object_spec, inbound_edge.object_spec);
        assert_eq!(provision.inbound.ring_spec, expected_inbound_ring);

        // The outbound edge spec must preserve the plan edge facts and mark the
        // local ring as egress.
        let outbound_edge = edges.get(&provision.outbound.edge_id).unwrap();
        let mut expected_outbound_ring = outbound_edge.ring_spec;
        expected_outbound_ring.direction = RingDirection::Egress;
        assert_eq!(provision.outbound.kind, outbound_edge.kind);
        assert_eq!(provision.outbound.object_spec, outbound_edge.object_spec);
        assert_eq!(provision.outbound.ring_spec, expected_outbound_ring);
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
            invalid_edge_endpoint_mismatch(),
            PlanRejectionKind::EdgeEndpointMismatch,
            "edge endpoint mismatch",
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

// This proves invalid object and ring spec facts reject before provisioning.
// The planner may choose the exact diagnostic payload, but the rejection kind
// must be typed and no RunPlan may be emitted.
#[test]
fn invalid_object_or_ring_specs_reject_without_plan() {
    // Each case changes one object/ring fact from the valid fixture and names
    // the typed rejection expected at the planning boundary.
    let cases = [
        (
            invalid_zero_activation_extent(),
            PlanRejectionKind::InvalidObjectSpec,
            "zero activation extent",
        ),
        (
            invalid_dtype_width(),
            PlanRejectionKind::InvalidObjectSpec,
            "invalid dtype width",
        ),
        (
            invalid_unsupported_shape_or_layout(),
            PlanRejectionKind::UnsupportedShapeOrLayout,
            "unsupported shape/layout",
        ),
        (
            invalid_ring_alignment(),
            PlanRejectionKind::InvalidRingSpec,
            "invalid ring alignment",
        ),
    ];

    // Rejection happens before provisioning: the only public output is the
    // typed error, never a RunPlan with invalid specs.
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

// Zero stages cannot form the MVP pipeline. This fixture isolates the invalid
// stage-count path without adding any other contradictory facts.
fn invalid_zero_stage_count() -> PlannerInput {
    valid_input(0, 36)
}

// Endpoint mismatch rejection needs an input that tries to override the linear
// edge contract. The planner must reject a skipped-stage activation edge rather
// than accepting a non-MVP topology.
fn invalid_edge_endpoint_mismatch() -> PlannerInput {
    let mut input = valid_input(3, 36);
    input.placement = PlacementInput::FixedLinearWithEdgeOverride {
        stages: vec![
            StagePlacement {
                stage_index: 0,
                node_id: node(10),
            },
            StagePlacement {
                stage_index: 1,
                node_id: node(11),
            },
            StagePlacement {
                stage_index: 2,
                node_id: node(12),
            },
        ],
        forced_activation_edges: vec![(0, 2)],
    };
    input
}

// The current contract requires one non-empty layer range per stage. Fewer
// layers than stages forces an empty range unless explicitly allowed, so this
// fixture should reject at the model/stage-layout boundary.
fn invalid_model_stage_layout() -> PlannerInput {
    let mut input = valid_input(4, 3);
    input.placement = linear_placement(4);
    input
}

// Zero sequence length makes activation capacity zero. The planner must reject
// before creating edges whose object specs cannot carry an activation.
fn invalid_zero_activation_extent() -> PlannerInput {
    let mut input = valid_input(3, 36);
    input.model.max_seq_len = 0;
    input
}

// Dtype width participates directly in activation extent and object layout.
// A zero width is not a valid dtype fact and must reject before planning.
fn invalid_dtype_width() -> PlannerInput {
    let mut input = valid_input(3, 36);
    input.model.dtype_width_bytes = 0;
    input
}

// Hidden dimension participates directly in activation shape. A zero hidden
// dimension represents an unsupported shape/layout fact for the MVP contract.
fn invalid_unsupported_shape_or_layout() -> PlannerInput {
    let mut input = valid_input(3, 36);
    input.model.hidden_dim = 0;
    input
}

// Ring alignment must be a usable alignment contract for shared memory and
// device copy boundaries. A non-power-of-two alignment makes the ring spec
// invalid before any edge can be provisioned.
fn invalid_ring_alignment() -> PlannerInput {
    let mut input = valid_input(3, 36);
    input.activation_ring.alignment = 3;
    input
}
