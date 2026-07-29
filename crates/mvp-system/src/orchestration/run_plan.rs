#![allow(dead_code)]

pub const MO01_HEADER_BYTES: u64 = 40;
const TOKEN_ID_WIDTH_BYTES: u32 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub u64);

impl From<u64> for RunId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeAllocator {
    next: u64,
}

impl EdgeAllocator {
    pub fn new() -> Self {
        Self { next: 1 }
    }

    pub fn alloc(&mut self) -> EdgeId {
        let edge_id = EdgeId(self.next);
        self.next += 1;
        edge_id
    }
}

impl Default for EdgeAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DTypeFamily {
    BFloat,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelFacts {
    pub model_id: String,
    pub gguf_source: GgufSource,
    pub num_layers: u32,
    pub hidden_dim: u64,
    pub dtype_family: DTypeFamily,
    pub dtype_width_bytes: u64,
    pub max_seq_len: u64,
    pub eos_token_id: u32,
    pub tokenizer: TokenizerSource,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GgufSource {
    LocalPath(String),
    HuggingFaceGguf {
        repo: String,
        file: String,
        revision: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TokenizerSource {
    EmbeddedGguf,
    LocalPath(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PromptSource {
    Inline(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SamplingPolicy {
    pub temperature_millis: u32,
    pub top_k: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenOutputPolicy {
    EmitAll,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoleId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortId(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GgufModelPlan {
    pub model_id: String,
    pub gguf_source: GgufSource,
    pub num_layers: u32,
    pub hidden_dim: u32,
    pub dtype_family: DTypeFamily,
    pub dtype_width_bytes: u32,
    pub max_seq_len: u32,
    pub eos_token_id: u32,
    pub tokenizer: TokenizerSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimePlan {
    pub prompt: PromptSource,
    pub sampling: SamplingPolicy,
    pub token_output_policy: TokenOutputPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub max_tokens: u32,
    pub prompt: PromptSource,
    pub sampling: SamplingPolicy,
    pub token_output_policy: TokenOutputPolicy,
}

impl RuntimeConfig {
    pub fn test_default() -> Self {
        Self {
            max_tokens: 4,
            prompt: PromptSource::Inline("test prompt".to_owned()),
            sampling: SamplingPolicy {
                temperature_millis: 0,
                top_k: 1,
            },
            token_output_policy: TokenOutputPolicy::EmitAll,
        }
    }

    fn plan(&self) -> RuntimePlan {
        RuntimePlan {
            prompt: self.prompt.clone(),
            sampling: self.sampling,
            token_output_policy: self.token_output_policy,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagePlacement {
    pub stage_index: u32,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementInput {
    FixedLinear(Vec<StagePlacement>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingDirection {
    Ingress,
    Egress,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostPinning {
    Pageable,
    PinnedRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeCoalescing {
    PendingBit,
    ReadySet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingSpec {
    pub data_capacity: u64,
    pub alignment: u32,
    pub direction: RingDirection,
    pub host_pinning: HostPinning,
    pub wake_coalescing: WakeCoalescing,
}

impl RingSpec {
    pub fn test_default_activation() -> Self {
        Self {
            data_capacity: 1 << 20,
            alignment: 64,
            direction: RingDirection::Egress,
            host_pinning: HostPinning::Pageable,
            wake_coalescing: WakeCoalescing::PendingBit,
        }
    }

    pub fn test_default_token() -> Self {
        Self {
            data_capacity: 4096,
            alignment: 8,
            direction: RingDirection::Egress,
            host_pinning: HostPinning::Pageable,
            wake_coalescing: WakeCoalescing::PendingBit,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerInput {
    pub run_id: RunId,
    pub orchestrator_node_id: NodeId,
    pub model: ModelFacts,
    pub runtime: RuntimeConfig,
    pub candidate_pool: Vec<NodeId>,
    pub stage_count: u32,
    pub placement: PlacementInput,
    pub activation_ring: RingSpec,
    pub token_ring: RingSpec,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EdgeKind {
    TokenIn,
    Activation,
    TokenOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectKind {
    Token,
    Activation,
    Weight,
    ModelShard,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShapeRule {
    TokenIds,
    ActivationRows { max_seq_len: u32, hidden_dim: u32 },
    WeightTensor,
    ModelShardBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayoutRule {
    Contiguous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SequencePolicy {
    Ordered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectSpec {
    pub kind: ObjectKind,
    pub max_extent: u64,
    pub dtype_family: DTypeFamily,
    pub dtype_width_bytes: u32,
    pub shape: ShapeRule,
    pub layout: LayoutRule,
    pub alignment: u32,
    pub sequence_policy: SequencePolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EdgeEndpoint {
    Orchestrator { node_id: NodeId },
    Stage { node_id: NodeId, stage_index: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgePlan {
    pub run_id: RunId,
    pub edge_id: EdgeId,
    pub kind: EdgeKind,
    pub producer: EdgeEndpoint,
    pub consumer: EdgeEndpoint,
    pub object_spec: ObjectSpec,
    pub ring_spec: RingSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagePlan {
    pub run_id: RunId,
    pub stage_index: u32,
    pub stage_count: u32,
    pub node_id: NodeId,
    pub gguf_source: GgufSource,
    pub layer_start: u32,
    pub layer_end_exclusive: u32,
    pub inbound_edge: EdgeId,
    pub outbound_edge: EdgeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunPlan {
    pub run_id: RunId,
    pub model: GgufModelPlan,
    pub runtime: RuntimePlan,
    pub stages: Vec<StagePlan>,
    pub edges: Vec<EdgePlan>,
    pub max_tokens: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundEdgeProvision {
    pub edge_id: EdgeId,
    pub kind: EdgeKind,
    pub object_spec: ObjectSpec,
    pub ring_spec: RingSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundEdgeProvision {
    pub edge_id: EdgeId,
    pub kind: EdgeKind,
    pub consumer_node_id: NodeId,
    pub object_spec: ObjectSpec,
    pub ring_spec: RingSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageModelFacts {
    pub model_id: String,
    pub hidden_dim: u32,
    pub dtype_family: DTypeFamily,
    pub dtype_width_bytes: u32,
    pub max_seq_len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageRuntimeFacts {
    pub role_id: RoleId,
    pub input_port: PortId,
    pub output_port: PortId,
    pub sampling: Option<SamplingPolicy>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionStage {
    pub run_id: RunId,
    pub node_id: NodeId,
    pub stage_index: u32,
    pub stage_count: u32,
    pub gguf_source: GgufSource,
    pub tokenizer: TokenizerSource,
    pub layer_start: u32,
    pub layer_end_exclusive: u32,
    pub inbound: InboundEdgeProvision,
    pub outbound: OutboundEdgeProvision,
    pub model: StageModelFacts,
    pub runtime: StageRuntimeFacts,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanRejectionKind {
    UnknownNode,
    DuplicateStageAssignment,
    MissingStage,
    InvalidStageCount,
    EdgeEndpointMismatch,
    ModelStageLayoutMismatch,
    InvalidObjectSpec,
    UnsupportedShapeOrLayout,
    InvalidRingSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanRejection {
    kind: PlanRejectionKind,
}

impl PlanRejection {
    pub fn kind(&self) -> PlanRejectionKind {
        self.kind
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionRejection {
    UnknownStage,
    MissingEdge,
}

pub fn plan_run(input: PlannerInput) -> Result<RunPlan, PlanRejection> {
    validate_global_input(&input)?;
    let placements = validated_placements(&input)?;
    let model = model_plan(&input.model)?;
    let runtime = input.runtime.plan();
    let max_tokens = input.runtime.max_tokens;
    let gguf_source = model.gguf_source.clone();
    let hidden_dim = model.hidden_dim;
    let dtype_width_bytes = model.dtype_width_bytes;
    let max_seq_len = model.max_seq_len;

    let activation_extent = input
        .model
        .max_seq_len
        .checked_mul(input.model.hidden_dim)
        .and_then(|value| value.checked_mul(input.model.dtype_width_bytes))
        .ok_or_else(|| reject(PlanRejectionKind::InvalidObjectSpec))?;
    if activation_extent == 0 {
        return Err(reject(PlanRejectionKind::InvalidObjectSpec));
    }

    let token_extent = input
        .model
        .max_seq_len
        .checked_mul(u64::from(TOKEN_ID_WIDTH_BYTES))
        .ok_or_else(|| reject(PlanRejectionKind::InvalidObjectSpec))?;
    if token_extent == 0 {
        return Err(reject(PlanRejectionKind::InvalidObjectSpec));
    }
    let token_spec = ObjectSpec {
        kind: ObjectKind::Token,
        max_extent: token_extent,
        dtype_family: input.model.dtype_family,
        dtype_width_bytes: TOKEN_ID_WIDTH_BYTES,
        shape: ShapeRule::TokenIds,
        layout: LayoutRule::Contiguous,
        alignment: TOKEN_ID_WIDTH_BYTES,
        sequence_policy: SequencePolicy::Ordered,
    };
    let activation_spec = ObjectSpec {
        kind: ObjectKind::Activation,
        max_extent: activation_extent,
        dtype_family: input.model.dtype_family,
        dtype_width_bytes,
        shape: ShapeRule::ActivationRows {
            max_seq_len,
            hidden_dim,
        },
        layout: LayoutRule::Contiguous,
        alignment: dtype_width_bytes,
        sequence_policy: SequencePolicy::Ordered,
    };
    let token_data_capacity = MO01_HEADER_BYTES
        .checked_add(token_extent)
        .ok_or_else(|| reject(PlanRejectionKind::InvalidObjectSpec))?;
    let mut token_ring = input.token_ring;
    token_ring.data_capacity = token_ring.data_capacity.max(token_data_capacity);

    let mut edge_allocator = EdgeAllocator::new();
    let token_in_edge = edge_allocator.alloc();
    let mut activation_edges = Vec::with_capacity(input.stage_count.saturating_sub(1) as usize);
    for _ in 0..input.stage_count.saturating_sub(1) {
        activation_edges.push(edge_allocator.alloc());
    }
    let token_out_edge = edge_allocator.alloc();

    let mut edges = Vec::with_capacity(input.stage_count as usize + 1);
    edges.push(EdgePlan {
        run_id: input.run_id,
        edge_id: token_in_edge,
        kind: EdgeKind::TokenIn,
        producer: EdgeEndpoint::Orchestrator {
            node_id: input.orchestrator_node_id,
        },
        consumer: EdgeEndpoint::Stage {
            node_id: placements[0].node_id,
            stage_index: 0,
        },
        object_spec: token_spec,
        ring_spec: token_ring,
    });

    for stage_index in 0..input.stage_count.saturating_sub(1) {
        edges.push(EdgePlan {
            run_id: input.run_id,
            edge_id: activation_edges[stage_index as usize],
            kind: EdgeKind::Activation,
            producer: EdgeEndpoint::Stage {
                node_id: placements[stage_index as usize].node_id,
                stage_index,
            },
            consumer: EdgeEndpoint::Stage {
                node_id: placements[stage_index as usize + 1].node_id,
                stage_index: stage_index + 1,
            },
            object_spec: activation_spec,
            ring_spec: input.activation_ring,
        });
    }

    edges.push(EdgePlan {
        run_id: input.run_id,
        edge_id: token_out_edge,
        kind: EdgeKind::TokenOut,
        producer: EdgeEndpoint::Stage {
            node_id: placements[input.stage_count as usize - 1].node_id,
            stage_index: input.stage_count - 1,
        },
        consumer: EdgeEndpoint::Orchestrator {
            node_id: input.orchestrator_node_id,
        },
        object_spec: token_spec,
        ring_spec: token_ring,
    });

    let mut stages = Vec::with_capacity(input.stage_count as usize);
    for placement in &placements {
        let stage_index = placement.stage_index;
        let (start, end) = layer_range(input.model.num_layers, input.stage_count, stage_index);
        let inbound_edge = if stage_index == 0 {
            token_in_edge
        } else {
            activation_edges[stage_index as usize - 1]
        };
        let outbound_edge = if stage_index + 1 == input.stage_count {
            token_out_edge
        } else {
            activation_edges[stage_index as usize]
        };
        stages.push(StagePlan {
            run_id: input.run_id,
            stage_index,
            stage_count: input.stage_count,
            node_id: placement.node_id,
            gguf_source: gguf_source.clone(),
            layer_start: start,
            layer_end_exclusive: end,
            inbound_edge,
            outbound_edge,
        });
    }

    Ok(RunPlan {
        run_id: input.run_id,
        model,
        runtime,
        stages,
        edges,
        max_tokens,
    })
}

pub fn derive_stage_provision(
    plan: &RunPlan,
    stage_index: u32,
) -> Result<ProvisionStage, ProjectionRejection> {
    let stage = plan
        .stages
        .iter()
        .find(|stage| stage.stage_index == stage_index)
        .ok_or(ProjectionRejection::UnknownStage)?;
    let inbound = plan
        .edges
        .iter()
        .find(|edge| edge.edge_id == stage.inbound_edge)
        .ok_or(ProjectionRejection::MissingEdge)?;
    let outbound = plan
        .edges
        .iter()
        .find(|edge| edge.edge_id == stage.outbound_edge)
        .ok_or(ProjectionRejection::MissingEdge)?;
    Ok(ProvisionStage {
        run_id: plan.run_id,
        node_id: stage.node_id,
        stage_index,
        stage_count: stage.stage_count,
        gguf_source: stage.gguf_source.clone(),
        tokenizer: plan.model.tokenizer.clone(),
        layer_start: stage.layer_start,
        layer_end_exclusive: stage.layer_end_exclusive,
        inbound: InboundEdgeProvision {
            edge_id: inbound.edge_id,
            kind: inbound.kind,
            object_spec: inbound.object_spec,
            ring_spec: ring_spec_for_direction(inbound.ring_spec, RingDirection::Ingress),
        },
        outbound: OutboundEdgeProvision {
            edge_id: outbound.edge_id,
            kind: outbound.kind,
            consumer_node_id: endpoint_node_id(&outbound.consumer),
            object_spec: outbound.object_spec,
            ring_spec: ring_spec_for_direction(outbound.ring_spec, RingDirection::Egress),
        },
        model: StageModelFacts {
            model_id: plan.model.model_id.clone(),
            hidden_dim: plan.model.hidden_dim,
            dtype_family: plan.model.dtype_family,
            dtype_width_bytes: plan.model.dtype_width_bytes,
            max_seq_len: plan.model.max_seq_len,
        },
        runtime: StageRuntimeFacts {
            role_id: RoleId(u64::from(stage.stage_index)),
            input_port: PortId("input".to_owned()),
            output_port: PortId("output".to_owned()),
            sampling: if stage.stage_index + 1 == stage.stage_count {
                Some(plan.runtime.sampling)
            } else {
                None
            },
        },
    })
}

fn validate_global_input(input: &PlannerInput) -> Result<(), PlanRejection> {
    if input.stage_count == 0 {
        return Err(reject(PlanRejectionKind::InvalidStageCount));
    }
    if input.model.num_layers < input.stage_count {
        return Err(reject(PlanRejectionKind::ModelStageLayoutMismatch));
    }
    if input.model.max_seq_len == 0 || input.model.dtype_width_bytes == 0 {
        return Err(reject(PlanRejectionKind::InvalidObjectSpec));
    }
    if input.model.hidden_dim == 0 {
        return Err(reject(PlanRejectionKind::UnsupportedShapeOrLayout));
    }
    if !valid_ring(input.activation_ring) || !valid_ring(input.token_ring) {
        return Err(reject(PlanRejectionKind::InvalidRingSpec));
    }
    Ok(())
}

fn validated_placements(input: &PlannerInput) -> Result<Vec<StagePlacement>, PlanRejection> {
    let PlacementInput::FixedLinear(stages) = &input.placement;

    let candidate_nodes = input
        .candidate_pool
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut by_stage = std::collections::BTreeMap::new();
    for placement in stages {
        if !candidate_nodes.contains(&placement.node_id) {
            return Err(reject(PlanRejectionKind::UnknownNode));
        }
        if by_stage.insert(placement.stage_index, *placement).is_some() {
            return Err(reject(PlanRejectionKind::DuplicateStageAssignment));
        }
    }

    let mut dense = Vec::with_capacity(input.stage_count as usize);
    for stage_index in 0..input.stage_count {
        let placement = by_stage
            .remove(&stage_index)
            .ok_or_else(|| reject(PlanRejectionKind::MissingStage))?;
        dense.push(placement);
    }
    Ok(dense)
}

fn model_plan(model: &ModelFacts) -> Result<GgufModelPlan, PlanRejection> {
    let hidden_dim = u32::try_from(model.hidden_dim)
        .map_err(|_| reject(PlanRejectionKind::UnsupportedShapeOrLayout))?;
    let dtype_width_bytes = u32::try_from(model.dtype_width_bytes)
        .map_err(|_| reject(PlanRejectionKind::InvalidObjectSpec))?;
    let max_seq_len = u32::try_from(model.max_seq_len)
        .map_err(|_| reject(PlanRejectionKind::InvalidObjectSpec))?;

    Ok(GgufModelPlan {
        model_id: model.model_id.clone(),
        gguf_source: model.gguf_source.clone(),
        num_layers: model.num_layers,
        hidden_dim,
        dtype_family: model.dtype_family,
        dtype_width_bytes,
        max_seq_len,
        eos_token_id: model.eos_token_id,
        tokenizer: model.tokenizer.clone(),
    })
}

fn ring_spec_for_direction(mut spec: RingSpec, direction: RingDirection) -> RingSpec {
    spec.direction = direction;
    spec
}

fn layer_range(num_layers: u32, stage_count: u32, stage_index: u32) -> (u32, u32) {
    let start = (u64::from(num_layers) * u64::from(stage_index) / u64::from(stage_count)) as u32;
    let end = (u64::from(num_layers) * u64::from(stage_index + 1) / u64::from(stage_count)) as u32;
    (start, end)
}

fn endpoint_node_id(endpoint: &EdgeEndpoint) -> NodeId {
    match endpoint {
        EdgeEndpoint::Orchestrator { node_id } | EdgeEndpoint::Stage { node_id, .. } => *node_id,
    }
}

fn valid_ring(spec: RingSpec) -> bool {
    spec.data_capacity > 0 && spec.alignment > 0 && spec.alignment.is_power_of_two()
}

fn reject(kind: PlanRejectionKind) -> PlanRejection {
    PlanRejection { kind }
}
