//! Planned and completed coverage ledgers for fixed campaign requirements.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ir::{
    ActionClass, ActionObservation, ActionOp, BarrierObservation, BehaviorCase, CaseObservation,
    CoverageScenario, DataKind, DataRoute, DescriptorFinish, DescriptorObservation,
    DescriptorReadMethod, DescriptorTerminalResult, DescriptorWriteMethod, ExecutionObservation,
    ExpectedOutcome, FailureInjection, ProcessProgram, TopologyFamily,
};

pub const COVERAGE_SCHEMA_VERSION: u32 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Source,
    Sink,
    Relay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadClass {
    Empty,
    Small,
    FramingBoundary,
    ChunkBoundary,
    MultiChunk,
    Randomized,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoverageKey {
    Topology {
        family: TopologyFamily,
    },
    NodeRole {
        node: u64,
        kind: DataKind,
        role: NodeRole,
    },
    OrderedEdge {
        source: u64,
        destination: u64,
        kind: DataKind,
    },
    ActionAdjacency {
        before: ActionClass,
        after: ActionClass,
    },
    Payload {
        class: PayloadClass,
    },
    DescriptorMethod {
        direction: String,
        method: String,
    },
    DescriptorTerminal {
        direction: String,
        finish: String,
    },
    Outcome {
        class: String,
    },
    ConcurrentStart {
        family: TopologyFamily,
    },
    Scenario {
        scenario: CoverageScenario,
    },
}

/// Immutable environment that produced coverage. Changing any component
/// requires fresh evidence, even when the campaign seed and node set agree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceIdentity {
    pub plan_digest: String,
    pub artifacts_digest: String,
    pub deployment_generation: String,
    pub fixture_mapping_digest: String,
}

impl EvidenceIdentity {
    fn validate(&self) -> Result<(), String> {
        if !valid_digest(&self.plan_digest)
            || !valid_digest(&self.artifacts_digest)
            || !valid_digest(&self.fixture_mapping_digest)
            || self.deployment_generation.trim().is_empty()
        {
            return Err("coverage evidence identity is incomplete".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PlannedCaseEvidence {
    case_digest: String,
    keys: BTreeSet<CoverageKey>,
    execution_ids: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CompletedCaseEvidence {
    planned_case_digest: String,
    identity_digest: String,
    executed_case_digest: String,
    observation_digest: String,
    attempt: Option<(u64, bool)>,
    execution_ids: BTreeMap<String, String>,
    request_ids: BTreeMap<String, String>,
    keys: BTreeSet<CoverageKey>,
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn evidence_digest(value: &impl Serialize) -> Result<String, String> {
    struct DigestWriter(Sha256);
    impl std::io::Write for DigestWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = DigestWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value)
        .map_err(|error| format!("hash coverage evidence: {error}"))?;
    Ok(format!("{:x}", writer.0.finalize()))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageLedger {
    pub schema_version: u32,
    pub live_nodes: BTreeSet<u64>,
    #[serde(with = "coverage_map")]
    pub required: BTreeMap<CoverageKey, u32>,
    #[serde(with = "coverage_map")]
    pub planned: BTreeMap<CoverageKey, u32>,
    #[serde(with = "coverage_map")]
    pub observed: BTreeMap<CoverageKey, u32>,
    pub completed_cases: BTreeSet<String>,
    identity: Option<EvidenceIdentity>,
    planned_cases: BTreeMap<String, PlannedCaseEvidence>,
    completed_evidence: BTreeMap<String, CompletedCaseEvidence>,
    /// Deserialization never authorizes reuse; the live consumer must compare
    /// the persisted identity and complete evidence closure with its own plan.
    #[serde(skip)]
    identity_validated: bool,
}
mod coverage_map {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::CoverageKey;

    pub fn serialize<S>(map: &BTreeMap<CoverageKey, u32>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<CoverageKey, u32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(CoverageKey, u32)>::deserialize(deserializer)?;
        let mut map = BTreeMap::new();
        for (key, count) in entries {
            if map.insert(key, count).is_some() {
                return Err(serde::de::Error::custom("duplicate coverage key"));
            }
        }
        Ok(map)
    }
}

impl CoverageLedger {
    pub fn five_node(live_nodes: BTreeSet<u64>) -> Result<Self, String> {
        if live_nodes.len() != 5 {
            return Err("five-node coverage ledger requires exactly five nodes".to_owned());
        }
        let mut ledger = Self::survivor(live_nodes)?;
        for family in [
            TopologyFamily::Chain,
            TopologyFamily::RingWalk,
            TopologyFamily::FanOut,
            TopologyFamily::FanIn,
            TopologyFamily::Diamond,
            TopologyFamily::RandomDag,
        ] {
            ledger.require(CoverageKey::Topology { family }, 8);
        }
        let classes = [
            ActionClass::PublishBlob,
            ActionClass::ReadBlob,
            ActionClass::StreamWrite,
            ActionClass::StreamRead,
            ActionClass::Lookup,
            ActionClass::Rename,
            ActionClass::Unlink,
            ActionClass::Descriptor,
        ];
        for before in classes {
            for after in classes {
                ledger.require(CoverageKey::ActionAdjacency { before, after }, 1);
            }
        }
        for class in [
            PayloadClass::Empty,
            PayloadClass::Small,
            PayloadClass::FramingBoundary,
            PayloadClass::ChunkBoundary,
            PayloadClass::MultiChunk,
            PayloadClass::Randomized,
        ] {
            ledger.require(CoverageKey::Payload { class }, 1);
        }
        for (direction, methods) in [
            ("write", ["write", "write_from", "mapping"]),
            ("read", ["read", "read_into", "mapping"]),
        ] {
            for method in methods {
                ledger.require(
                    CoverageKey::DescriptorMethod {
                        direction: direction.to_owned(),
                        method: method.to_owned(),
                    },
                    1,
                );
            }
            for finish in ["close", "abort", "drop", "close_twice", "abort_twice"] {
                ledger.require(
                    CoverageKey::DescriptorTerminal {
                        direction: direction.to_owned(),
                        finish: finish.to_owned(),
                    },
                    1,
                );
            }
        }
        for class in ["success", "modeled_error", "contextual_failure"] {
            ledger.require(
                CoverageKey::Outcome {
                    class: class.to_owned(),
                },
                1,
            );
        }
        for family in [
            TopologyFamily::Chain,
            TopologyFamily::RingWalk,
            TopologyFamily::FanOut,
            TopologyFamily::FanIn,
            TopologyFamily::Diamond,
        ] {
            ledger.require(CoverageKey::ConcurrentStart { family }, 1);
        }
        for scenario in [
            CoverageScenario::ConcurrentStartup,
            CoverageScenario::RingCompletion,
            CoverageScenario::HotColdFairness,
            CoverageScenario::ProcessChurn,
            CoverageScenario::ActiveStreamWriterAbort,
            CoverageScenario::ActiveStreamReaderStop,
            CoverageScenario::ActiveMutation,
            CoverageScenario::QuiescentMutation,
            CoverageScenario::FailureIsolation,
            CoverageScenario::Authorization,
        ] {
            ledger.require(CoverageKey::Scenario { scenario }, 1);
        }
        Ok(ledger)
    }

    pub fn survivor(live_nodes: BTreeSet<u64>) -> Result<Self, String> {
        if live_nodes.len() < 3 || live_nodes.len() > 5 || live_nodes.contains(&0) {
            return Err(
                "survivor coverage ledger requires three to five valid exact nodes".to_owned(),
            );
        }
        let mut ledger = Self {
            schema_version: COVERAGE_SCHEMA_VERSION,
            live_nodes: live_nodes.clone(),
            required: BTreeMap::new(),
            planned: BTreeMap::new(),
            observed: BTreeMap::new(),
            completed_cases: BTreeSet::new(),
            identity: None,
            planned_cases: BTreeMap::new(),
            completed_evidence: BTreeMap::new(),
            identity_validated: false,
        };
        for kind in [DataKind::Blob, DataKind::Stream] {
            for &node in &live_nodes {
                for role in [NodeRole::Source, NodeRole::Sink, NodeRole::Relay] {
                    ledger.require(CoverageKey::NodeRole { node, kind, role }, 1);
                }
            }
            for &source in &live_nodes {
                for &destination in &live_nodes {
                    if source != destination {
                        ledger.require(
                            CoverageKey::OrderedEdge {
                                source,
                                destination,
                                kind,
                            },
                            1,
                        );
                    }
                }
            }
        }
        Ok(ledger)
    }

    fn require(&mut self, key: CoverageKey, count: u32) {
        self.required
            .entry(key)
            .and_modify(|required| *required = (*required).max(count))
            .or_insert(count);
    }

    pub fn identity(&self) -> Option<&EvidenceIdentity> {
        self.identity.as_ref()
    }

    pub fn plan_case(&mut self, case: &BehaviorCase) -> Result<(), String> {
        case.validate()?;
        if case.live_nodes != self.live_nodes {
            return Err(format!(
                "coverage ledger live set {:?} differs from case {} live set {:?}",
                self.live_nodes, case.id, case.live_nodes
            ));
        }
        if self.identity.is_some() || self.planned_cases.contains_key(&case.id) {
            return Err(format!(
                "case {} is duplicate or the coverage plan is sealed",
                case.id
            ));
        }
        let keys = case_coverage(case);
        let evidence = PlannedCaseEvidence {
            case_digest: evidence_digest(case)?,
            execution_ids: case
                .processes
                .iter()
                .map(|process| (process.id.clone(), process.access.execution_id.clone()))
                .collect(),
            keys,
        };
        for key in &evidence.keys {
            *self.planned.entry(key.clone()).or_default() += 1;
        }
        self.planned_cases.insert(case.id.clone(), evidence);
        Ok(())
    }

    pub fn bind_identity(&mut self, identity: EvidenceIdentity) -> Result<(), String> {
        identity.validate()?;
        if !self.completed_cases.is_empty()
            || !self.completed_evidence.is_empty()
            || !self.observed.is_empty()
        {
            return Err(
                "completed coverage must be validated against the live plan, not rebound"
                    .to_owned(),
            );
        }
        if self
            .identity
            .as_ref()
            .is_some_and(|bound| bound != &identity)
        {
            return Err("coverage is already bound to a different deployment".to_owned());
        }
        self.validate_evidence_closure()?;
        self.identity = Some(identity);
        self.identity_validated = true;
        Ok(())
    }

    pub(crate) fn validate_plan(&self, expected: &Self) -> Result<(), String> {
        self.validate_evidence_closure()?;
        expected.validate_evidence_closure()?;
        if self.schema_version != expected.schema_version
            || self.live_nodes != expected.live_nodes
            || self.required != expected.required
            || self.planned != expected.planned
            || self.planned_cases != expected.planned_cases
        {
            return Err("coverage differs from the immutable campaign cases".to_owned());
        }
        Ok(())
    }

    pub fn validate_resume(&mut self, expected: &Self) -> Result<(), String> {
        self.identity_validated = false;
        expected.require_validated_identity()?;
        self.validate_plan(expected)?;
        if self.identity != expected.identity {
            return Err(
                "persisted coverage differs from the immutable plan or deployment identity"
                    .to_owned(),
            );
        }
        self.identity_validated = true;
        Ok(())
    }

    fn require_validated_identity(&self) -> Result<(), String> {
        if !self.identity_validated {
            return Err("coverage deployment identity has not been validated".to_owned());
        }
        self.identity
            .as_ref()
            .ok_or_else(|| "coverage has no deployment identity".to_owned())?
            .validate()
    }

    fn validate_evidence_closure(&self) -> Result<(), String> {
        if self.schema_version != COVERAGE_SCHEMA_VERSION {
            return Err("coverage schema is incompatible".to_owned());
        }
        let mut planned = BTreeMap::new();
        for evidence in self.planned_cases.values() {
            if !valid_digest(&evidence.case_digest) {
                return Err("coverage has an invalid planned case digest".to_owned());
            }
            for key in &evidence.keys {
                let count = planned.entry(key.clone()).or_insert(0_u32);
                *count = count
                    .checked_add(1)
                    .ok_or("planned coverage count overflow")?;
            }
        }
        if planned != self.planned
            || self.completed_cases.len() != self.completed_evidence.len()
            || self
                .completed_cases
                .iter()
                .any(|id| !self.completed_evidence.contains_key(id))
        {
            return Err("coverage planned/completed evidence closure is corrupt".to_owned());
        }
        let mut observed = BTreeMap::new();
        let mut requests = BTreeSet::new();
        let mut executions = BTreeSet::new();
        let identity_digest = self.identity.as_ref().map(evidence_digest).transpose()?;
        for (id, evidence) in &self.completed_evidence {
            let plan = self
                .planned_cases
                .get(id)
                .ok_or_else(|| format!("coverage completed unplanned case {id}"))?;
            if evidence.planned_case_digest != plan.case_digest
                || identity_digest.as_ref() != Some(&evidence.identity_digest)
                || !plan.execution_ids.keys().eq(evidence.execution_ids.keys())
                || plan.execution_ids.iter().any(|(process, execution_id)| {
                    let expected = match evidence.attempt {
                        Some((attempt, _)) => format!("{execution_id}-attempt-{attempt}"),
                        None => execution_id.clone(),
                    };
                    evidence.execution_ids.get(process) != Some(&expected)
                })
                || !valid_digest(&evidence.executed_case_digest)
                || !valid_digest(&evidence.observation_digest)
                || evidence.request_ids.is_empty()
                || !evidence
                    .request_ids
                    .keys()
                    .eq(evidence.execution_ids.keys())
                || evidence
                    .request_ids
                    .values()
                    .any(|id| id.is_empty() || !requests.insert(id))
                || evidence
                    .execution_ids
                    .values()
                    .any(|id| id.is_empty() || !executions.insert(id))
            {
                return Err(format!(
                    "coverage attempt evidence for {id} is corrupt or reused"
                ));
            }
            for key in &evidence.keys {
                let count = observed.entry(key.clone()).or_insert(0_u32);
                *count = count
                    .checked_add(1)
                    .ok_or("observed coverage count overflow")?;
            }
        }
        if observed != self.observed {
            return Err(
                "coverage observed counts differ from completed attempt evidence".to_owned(),
            );
        }
        Ok(())
    }

    pub fn observe_case(
        &mut self,
        case: &BehaviorCase,
        observation: &CaseObservation,
    ) -> Result<(), String> {
        self.observe_case_inner(case, case, observation, None, None)
    }

    pub fn observe_case_with_budget(
        &mut self,
        case: &BehaviorCase,
        observation: &CaseObservation,
        budget: &crate::budget::Budget,
    ) -> Result<(), String> {
        self.observe_case_inner(case, case, observation, None, Some(budget))
    }

    /// Record only an execution derived from the exact immutable planned case.
    pub fn observe_attempt(
        &mut self,
        template: &BehaviorCase,
        executed: &BehaviorCase,
        observation: &CaseObservation,
        attempt: u64,
        isolate_paths: bool,
        budget: &crate::budget::Budget,
    ) -> Result<(), String> {
        budget.check("coverage attempt provenance")?;
        if &template.for_attempt(attempt, isolate_paths) != executed {
            return Err(
                "coverage execution differs from its planned attempt transformation".to_owned(),
            );
        }
        self.observe_case_inner(
            template,
            executed,
            observation,
            Some((attempt, isolate_paths)),
            Some(budget),
        )
    }

    fn observe_case_inner(
        &mut self,
        template: &BehaviorCase,
        case: &BehaviorCase,
        observation: &CaseObservation,
        attempt: Option<(u64, bool)>,
        budget: Option<&crate::budget::Budget>,
    ) -> Result<(), String> {
        if let Some(budget) = budget {
            budget.check("observed coverage validation")?;
        }
        self.require_validated_identity()?;
        let planned = self
            .planned_cases
            .get(&case.id)
            .ok_or_else(|| format!("coverage cannot complete unplanned case {}", case.id))?;
        if template.id != case.id || evidence_digest(template)? != planned.case_digest {
            return Err(format!(
                "case {} differs from its immutable coverage plan",
                case.id
            ));
        }
        case.validate()?;
        if case.live_nodes != self.live_nodes {
            return Err(format!(
                "case {} differs from the coverage live set",
                case.id
            ));
        }
        if self.completed_cases.contains(&case.id) {
            return Err(format!("case {} was counted twice", case.id));
        }
        let keys = observed_case_coverage(case, observation, budget)?;
        let evidence = CompletedCaseEvidence {
            planned_case_digest: planned.case_digest.clone(),
            identity_digest: evidence_digest(self.identity.as_ref().expect("validated identity"))?,
            attempt,
            executed_case_digest: evidence_digest(case)?,
            observation_digest: evidence_digest(observation)?,
            execution_ids: case
                .processes
                .iter()
                .map(|process| (process.id.clone(), process.access.execution_id.clone()))
                .collect(),
            request_ids: observation
                .executions
                .iter()
                .map(|execution| (execution.process.clone(), execution.request_id.clone()))
                .collect(),
            keys,
        };
        if evidence.request_ids.values().any(|id| {
            self.completed_evidence
                .values()
                .any(|previous| previous.request_ids.values().any(|previous| previous == id))
        }) || evidence.execution_ids.values().any(|id| {
            self.completed_evidence.values().any(|previous| {
                previous
                    .execution_ids
                    .values()
                    .any(|previous| previous == id)
            })
        }) {
            return Err("coverage attempt reuses a completed execution identity".to_owned());
        }
        if let Some(budget) = budget {
            budget.check("observed coverage commit")?;
        }
        self.completed_cases.insert(case.id.clone());
        for key in &evidence.keys {
            *self.observed.entry(key.clone()).or_default() += 1;
        }
        self.completed_evidence.insert(case.id.clone(), evidence);
        Ok(())
    }

    pub fn assert_planned_closed(&self) -> Result<(), String> {
        self.validate_evidence_closure()?;
        self.assert_closed(&self.planned, "planned")
    }

    pub fn assert_observed_closed(&self) -> Result<(), String> {
        self.require_validated_identity()?;
        self.validate_evidence_closure()?;
        self.assert_closed(&self.observed, "observed")
    }

    fn assert_closed(
        &self,
        actual: &BTreeMap<CoverageKey, u32>,
        label: &str,
    ) -> Result<(), String> {
        if self.schema_version != COVERAGE_SCHEMA_VERSION {
            return Err("coverage schema is incompatible".to_owned());
        }
        let missing = self
            .required
            .iter()
            .filter_map(|(key, required)| {
                let actual = actual.get(key).copied().unwrap_or_default();
                (actual < *required).then_some(format!("{key:?}: {actual}/{required}"))
            })
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{label} coverage ledger is incomplete: {}",
                missing.join(", ")
            ))
        }
    }
}

struct ProcessRecords<'a> {
    program: &'a ProcessProgram,
    execution: &'a ExecutionObservation,
    terminals: Vec<&'a ActionObservation>,
    steps: Vec<Vec<&'a ActionObservation>>,
    barriers: Vec<&'a BarrierObservation>,
}

/// All values borrow original records. Per-step buckets retain multiplicity
/// and emission order; no sorting or last-write-wins lookup can hide ambiguity.
struct ObservationIndex<'a> {
    observation: &'a CaseObservation,
    processes: BTreeMap<&'a str, ProcessRecords<'a>>,
    endpoints: BTreeMap<(u64, &'a str, &'a str), Vec<&'a ActionObservation>>,
    paths: BTreeMap<&'a str, Vec<&'a ActionObservation>>,
}

impl<'a> ObservationIndex<'a> {
    fn new(case: &'a BehaviorCase, observation: &'a CaseObservation) -> Result<Self, String> {
        let programs = case
            .processes
            .iter()
            .map(|program| (program.id.as_str(), program))
            .collect::<BTreeMap<_, _>>();
        let mut index = Self {
            observation,
            processes: BTreeMap::new(),
            endpoints: BTreeMap::new(),
            paths: BTreeMap::new(),
        };
        for execution in &observation.executions {
            let program = *programs
                .get(execution.process.as_str())
                .ok_or_else(|| format!("unplanned execution {:?}", execution.process))?;
            let mut records = ProcessRecords {
                program,
                execution,
                terminals: Vec::with_capacity(program.actions.len()),
                steps: vec![Vec::new(); program.actions.len()],
                barriers: Vec::new(),
            };
            for result in &execution.results {
                let action = program.actions.get(result.step).ok_or_else(|| {
                    format!("{} has an unplanned step {}", program.id, result.step)
                })?;
                if result.process != execution.process
                    || !crate::oracle::stream_record_action_matches(&action.operation, result)
                    || result.path != action.operation.path()
                {
                    return Err(format!(
                        "{} step {} has a mismatched record identity",
                        program.id, result.step
                    ));
                }
                let step = &mut records.steps[result.step];
                if result.outcome == "barrier" {
                    let barrier = result.barrier.as_ref().ok_or_else(|| {
                        format!(
                            "{} step {} has an untyped milestone",
                            program.id, result.step
                        )
                    })?;
                    let stream_milestone = matches!(
                        barrier,
                        BarrierObservation::StreamOpened { .. }
                            | BarrierObservation::StreamFirstFrame { .. }
                            | BarrierObservation::StreamFrame { .. }
                            | BarrierObservation::StreamEof { .. }
                            | BarrierObservation::ReleaseObserved { .. }
                    );
                    if stream_milestone
                        && !matches!(
                            action.operation,
                            ActionOp::StreamWrite { .. }
                                | ActionOp::GatedStreamWrite { .. }
                                | ActionOp::StreamRead { .. }
                                | ActionOp::StreamReadWithRetry { .. }
                                | ActionOp::GatedStreamRead { .. }
                                | ActionOp::StreamRoundTrip { .. }
                                | ActionOp::StreamReadInto { .. }
                        )
                    {
                        return Err(format!(
                            "{} step {} has stream evidence for a non-stream action",
                            program.id, result.step
                        ));
                    }
                    let precedes_terminal = stream_milestone
                        || matches!(barrier, BarrierObservation::MutationApplied { .. });
                    let terminal_seen = step.iter().any(|record| record.outcome != "barrier");
                    if precedes_terminal == terminal_seen {
                        return Err(format!(
                            "{} step {} has a reordered milestone",
                            program.id, result.step
                        ));
                    }
                    if !matches!(barrier, BarrierObservation::StreamFrame { .. })
                        && step.iter().any(|previous| {
                            previous.barrier.as_ref().is_some_and(|previous| {
                                std::mem::discriminant(previous) == std::mem::discriminant(barrier)
                            })
                        })
                    {
                        return Err(format!(
                            "{} step {} has an ambiguous duplicate milestone",
                            program.id, result.step
                        ));
                    }
                    records.barriers.push(barrier);
                } else {
                    if result.barrier.is_some() {
                        return Err(format!(
                            "{} step {} mixes a terminal outcome and milestone",
                            program.id, result.step
                        ));
                    }
                    records.terminals.push(result);
                    if execution.exit_success && result.outcome == "ok" {
                        index
                            .endpoints
                            .entry((
                                execution.logical_node_id,
                                result.path.as_str(),
                                result.action.as_str(),
                            ))
                            .or_default()
                            .push(result);
                    }
                }
                step.push(result);
                index
                    .paths
                    .entry(result.path.as_str())
                    .or_default()
                    .push(result);
            }
            if index
                .processes
                .insert(execution.process.as_str(), records)
                .is_some()
            {
                return Err(format!("duplicate execution {}", execution.process));
            }
        }
        Ok(index)
    }
}

/// Coverage is committed only after the complete observation has been checked.
/// The planned ledger is deliberately not an input to this derivation.
fn observed_case_coverage(
    case: &BehaviorCase,
    observation: &CaseObservation,
    budget: Option<&crate::budget::Budget>,
) -> Result<BTreeSet<CoverageKey>, String> {
    let index = ObservationIndex::new(case, observation)?;
    let mut requests = BTreeSet::new();
    let stream_route_paths = case
        .routes
        .iter()
        .filter(|route| route.kind == DataKind::Stream)
        .flat_map(|route| route.edges.iter().map(|edge| edge.path.as_str()))
        .collect::<BTreeSet<_>>();
    let route_paths = case
        .routes
        .iter()
        .flat_map(|route| route.edges.iter().map(|edge| edge.path.as_str()))
        .collect::<BTreeSet<_>>();
    for execution in &observation.executions {
        let program = index.processes[execution.process.as_str()].program;
        if execution.request_id.is_empty() || !requests.insert(execution.request_id.as_str()) {
            return Err(format!(
                "duplicate or unidentified execution {}",
                execution.process
            ));
        }
        if execution.logical_node_id != program.logical_node_id {
            return Err(format!(
                "{} ran on node {}, expected {}",
                program.id, execution.logical_node_id, program.logical_node_id
            ));
        }
        if crate::oracle::process_failure_is_expected(case, program) {
            // A causally stopped transfer can have an authentic partial
            // prefix. The complete independent oracle below must validate
            // that prefix and its terminal lifecycle before any credit.
            if execution.exit_success {
                return Err(format!(
                    "{} must prove its expected process failure",
                    program.id
                ));
            }
            if program
                .actions
                .iter()
                .any(|action| matches!(action.expected, ExpectedOutcome::Linearized { .. }))
            {
                return Err(format!(
                    "{} has an unobserved linearized operation in a failed execution",
                    program.id
                ));
            }
        } else if !execution.exit_success {
            return Err(format!("{} did not execute successfully", program.id));
        }
        // Results are emitted in program order. Sorting by the claimed step would
        // conceal reordered or duplicated records and invent adjacency evidence.
        // Action results are emitted in program order; barrier milestone
        // records ride along at their owning action's step (stream milestones
        // precede the action record, hint barriers follow it).
        let first_route_step = program.actions.iter().position(|action| {
            route_paths.contains(action.operation.path())
                && (action_payload_length(&action.operation).is_some()
                    || matches!(&action.operation, ActionOp::AwaitEntry { path, .. }
                        if case.routes.iter().any(|route| route.edges.iter().any(|edge|
                            edge.destination_role == program.id && edge.path == *path))))
        });
        let mut next_action_step = 0_usize;
        let mut last_action_step = None::<usize>;
        for result in &execution.results {
            if result.outcome == "barrier" {
                if stream_route_paths.contains(result.path.as_str())
                    && matches!(
                        result.barrier,
                        Some(BarrierObservation::StreamOpened { .. })
                    )
                {
                    if first_route_step != Some(next_action_step) {
                        return Err(format!(
                            "{} prepared a route endpoint outside startup",
                            program.id
                        ));
                    }
                    continue;
                }
                if result.step != next_action_step && Some(result.step) != last_action_step {
                    return Err(format!(
                        "{} has a barrier record detached from its action step {}",
                        program.id, result.step
                    ));
                }
                continue;
            }
            if result.step != next_action_step {
                return Err(format!(
                    "{} has a missing, duplicate, or reordered step {next_action_step}",
                    program.id
                ));
            }
            if result.outcome == "ok"
                && (result.errno.is_some() || result.error_type.is_some() || result.error.is_some())
            {
                return Err(format!(
                    "{} step {next_action_step} claims both success and failure",
                    program.id
                ));
            }
            last_action_step = Some(next_action_step);
            next_action_step += 1;
        }
    }
    // Reuse the behavioral checks for exact process/step/class/path, typed
    // outcomes, independently computed payload digests, and lifecycle completion.
    match budget {
        Some(budget) => {
            crate::oracle::BehaviorOracle::verify_with_budget(case, observation, budget)
        }
        None => crate::oracle::BehaviorOracle::verify(case, observation),
    }
    .map_err(|error| error.to_string())?;

    let mut keys = BTreeSet::new();
    let mut unsupported = BTreeSet::new();
    for records in index.processes.values() {
        let program = records.program;
        let execution = records.execution;
        if !execution.exit_success {
            keys.insert(CoverageKey::Outcome {
                class: "contextual_failure".to_owned(),
            });
            continue;
        }
        for pair in records.terminals.windows(2) {
            keys.insert(CoverageKey::ActionAdjacency {
                before: program.actions[pair[0].step].operation.class(),
                after: program.actions[pair[1].step].operation.class(),
            });
        }
        for result in &records.terminals {
            let operation = &program.actions[result.step].operation;
            keys.insert(CoverageKey::Outcome {
                class: if result.outcome == "ok" {
                    "success"
                } else {
                    "modeled_error"
                }
                .to_owned(),
            });
            if result.outcome == "ok"
                && (action_payload_length(operation).is_some()
                    || matches!(operation, ActionOp::StreamRoundTrip { .. }))
            {
                // The oracle checked this actual length and digest against the
                // independent expected bytes, not just the number of records.
                for class in payload_classes(result.length.expect("payload verified above")) {
                    keys.insert(CoverageKey::Payload { class });
                }
            }
            observe_descriptor(operation, result, &mut keys)?;
        }
    }
    for scenario in &case.scenarios {
        if !observe_scenario(case, *scenario, &index, &mut keys)? {
            unsupported.insert(format!(
                "{scenario:?} lacks its required typed milestone evidence"
            ));
        }
    }
    let mut topology_proven = case.topology == TopologyFamily::Fixed;
    for route in &case.routes {
        observe_route(route, &index, &mut keys)?;
        topology_proven |= route_has_topology(route, case.topology);
    }
    if !topology_proven {
        return Err(format!(
            "case {} has no completely observed route proving topology {:?}",
            case.id, case.topology
        ));
    }
    if !unsupported.is_empty() {
        return Err(format!(
            "case {} has unprovable coverage; observation schema extension required: {}",
            case.id,
            unsupported.into_iter().collect::<Vec<_>>().join("; ")
        ));
    }
    if case.topology != TopologyFamily::Fixed {
        keys.insert(CoverageKey::Topology {
            family: case.topology,
        });
    }
    Ok(keys)
}

/// Proves one declared scenario from typed milestone records actually
/// emitted by the workload. Returns false when the required evidence is
/// absent; construction-level guarantees (dependency edges, failures) are
/// cross-checked against the case, never against poll order.
fn observe_scenario(
    case: &BehaviorCase,
    scenario: CoverageScenario,
    index: &ObservationIndex<'_>,
    keys: &mut BTreeSet<CoverageKey>,
) -> Result<bool, String> {
    let observation = index.observation;
    let execution_of = |process: &str| {
        index
            .processes
            .get(process)
            .map(|records| records.execution)
    };
    let barriers = |process: &str| {
        index
            .processes
            .get(process)
            .map(|records| records.barriers.as_slice())
            .unwrap_or(&[])
    };
    match scenario {
        CoverageScenario::ConcurrentStartup => {
            // Startup participants are the case's mutually independent route
            // processes; every stream participant opened its endpoint and
            // every participant completed, proving progress that never
            // depended on endpoint creation order.
            let route_paths = case
                .routes
                .iter()
                .flat_map(|route| route.edges.iter().map(|edge| edge.path.as_str()))
                .collect::<BTreeSet<_>>();
            let participants = case
                .processes
                .iter()
                .filter(|program| {
                    program
                        .actions
                        .iter()
                        .any(|action| route_paths.contains(action.operation.path()))
                })
                .collect::<Vec<_>>();
            if participants.len() < 2
                || participants
                    .iter()
                    .any(|program| !program.depends_on.is_empty())
            {
                return Ok(false);
            }
            let stream_paths = case
                .routes
                .iter()
                .filter(|route| route.kind == DataKind::Stream)
                .flat_map(|route| route.edges.iter().map(|edge| edge.path.as_str()))
                .collect::<BTreeSet<_>>();
            if stream_paths.is_empty() {
                return Ok(false);
            }
            for program in &participants {
                let Some(execution) = execution_of(&program.id) else {
                    return Ok(false);
                };
                if !execution.exit_success {
                    return Ok(false);
                }
                let required_steps = program
                    .actions
                    .iter()
                    .enumerate()
                    .filter(|(_, action)| stream_paths.contains(action.operation.path()))
                    .map(|(step, _)| step)
                    .collect::<BTreeSet<_>>();
                let mut opened = BTreeSet::new();
                for result in &execution.results {
                    if !required_steps.contains(&result.step) {
                        continue;
                    }
                    if matches!(
                        result.barrier,
                        Some(BarrierObservation::StreamOpened { .. })
                    ) {
                        opened.insert(result.step);
                    } else if (result.outcome != "barrier"
                        || matches!(
                            result.barrier,
                            Some(
                                BarrierObservation::StreamFirstFrame { .. }
                                    | BarrierObservation::StreamFrame { .. }
                                    | BarrierObservation::StreamEof { .. }
                            )
                        ))
                        && opened != required_steps
                    {
                        return Ok(false);
                    }
                }
                if opened != required_steps {
                    return Ok(false);
                }
            }
            keys.insert(CoverageKey::ConcurrentStart {
                family: case.topology,
            });
            keys.insert(CoverageKey::Scenario {
                scenario: CoverageScenario::ConcurrentStartup,
            });
            Ok(true)
        }
        CoverageScenario::RingCompletion => {
            // A token label cannot manufacture a ring. Derive lap and edge
            // identities from a closed, ordered, repeated physical route, then
            // bind every milestone to that edge's successful endpoint record.
            let mut proven = false;
            for route in &case.routes {
                let Some(hops) = returning_ring_hops(route) else {
                    continue;
                };
                let mut token = None;
                let mut previous_sink: Option<&ActionObservation> = None;
                for (edge_index, edge) in route.edges.iter().enumerate() {
                    if edge_index % hops == 0 {
                        token = None;
                    }
                    let source = route_endpoint(
                        index,
                        route.kind,
                        NodeRole::Source,
                        edge.source,
                        &edge.path,
                        &edge.source_role,
                    )?;
                    let sink = route_endpoint(
                        index,
                        route.kind,
                        NodeRole::Sink,
                        edge.destination,
                        &edge.path,
                        &edge.destination_role,
                    )?;
                    let Some(digest) = source.digest.as_deref() else {
                        return Ok(false);
                    };
                    if source.length.is_none()
                        || source.length != sink.length
                        || sink.digest.as_deref() != Some(digest)
                        || token.is_some_and(|known| known != digest)
                        || previous_sink.is_some_and(|previous| {
                            previous.process != source.process || previous.step >= source.step
                        })
                    {
                        return Ok(false);
                    }
                    token = Some(digest);
                    let lap = (edge_index / hops) as u32;
                    let forwarded = BarrierObservation::TokenForwarded {
                        token: digest.to_owned(),
                        lap,
                        edge_index: edge_index as u32,
                    };
                    let received = if edge_index + 1 == route.edges.len() {
                        BarrierObservation::LapCompleted {
                            token: digest.to_owned(),
                            lap,
                        }
                    } else {
                        BarrierObservation::TokenReceived {
                            token: digest.to_owned(),
                            lap,
                            edge_index: edge_index as u32,
                        }
                    };
                    for (endpoint, expected) in [(source, forwarded), (sink, received)] {
                        let records = &index.processes[endpoint.process.as_str()];
                        if records.steps[endpoint.step]
                            .iter()
                            .filter(|result| result.barrier.as_ref() == Some(&expected))
                            .count()
                            != 1
                        {
                            return Ok(false);
                        }
                    }
                    previous_sink = Some(sink);
                }
                proven = true;
            }
            if !proven {
                return Ok(false);
            }
            keys.insert(CoverageKey::Scenario {
                scenario: CoverageScenario::RingCompletion,
            });
            Ok(true)
        }
        CoverageScenario::HotColdFairness => {
            // Causal chain: hot reader observed its first frame, every cold
            // flow completed, the release publisher (dependent on all colds)
            // published release, the hot writer observed that release, and
            // the hot pair completed.
            let Some((hot_writer, hot_reader)) =
                case.processes
                    .iter()
                    .find(|program| {
                        program.actions.iter().any(|action| {
                            matches!(action.operation, ActionOp::GatedStreamWrite { .. })
                        })
                    })
                    .zip(case.processes.iter().find(|program| {
                        program.actions.iter().any(|action| {
                            matches!(action.operation, ActionOp::GatedStreamRead { .. })
                        })
                    }))
            else {
                return Ok(false);
            };
            let (hot_path, release_path) = hot_writer
                .actions
                .iter()
                .find_map(|action| match &action.operation {
                    ActionOp::GatedStreamWrite {
                        path, release_path, ..
                    } => Some((path.as_str(), release_path.as_str())),
                    _ => None,
                })
                .expect("hot writer shape checked above");
            let Some(observed_path) =
                hot_reader
                    .actions
                    .iter()
                    .find_map(|action| match &action.operation {
                        ActionOp::GatedStreamRead {
                            path,
                            observed_path,
                            ..
                        } if path == hot_path => Some(observed_path.as_str()),
                        _ => None,
                    })
            else {
                return Ok(false);
            };
            let is_cold = |program: &crate::ProcessProgram| {
                program.id != hot_writer.id
                    && program.id != hot_reader.id
                    && program.actions.first().is_some_and(|action| {
                        action.expected == ExpectedOutcome::Ok
                            && matches!(&action.operation, ActionOp::AwaitEntry { path, expected_kind }
                                if path == observed_path && expected_kind == "blob")
                    })
                    && program.actions.iter().skip(1).any(|action| {
                        matches!(action.operation,
                            ActionOp::PublishBlob { .. } | ActionOp::ReadBlob { .. }
                                | ActionOp::StreamWrite { .. } | ActionOp::StreamRead { .. }
                                | ActionOp::StreamRoundTrip { .. } | ActionOp::StreamReadInto { .. }
                                | ActionOp::DescriptorWrite { .. } | ActionOp::DescriptorRead { .. })
                    })
                    && !program.actions.iter().any(|action| {
                        matches!(action.operation, ActionOp::GatedStreamWrite { .. })
                            || matches!(
                                &action.operation,
                                ActionOp::PublishBlob { path, .. } if path == release_path
                            )
                    })
            };
            // The release publisher declares its cold set exactly; unrelated
            // route participants that merely await entries are not colds.
            let publisher = case.processes.iter().find(|program| {
                !program.depends_on.is_empty()
                    && program.depends_on.iter().all(|dependency| {
                        index.processes.get(dependency.as_str()).is_some_and(|records| is_cold(records.program))
                    })
                    && program.actions.iter().any(|action| {
                        matches!(&action.operation, ActionOp::PublishBlob { path, .. } if path == release_path)
                    })
            });
            let Some(publisher) = publisher else {
                return Ok(false);
            };
            let colds = case
                .processes
                .iter()
                .filter(|program| publisher.depends_on.contains(&program.id))
                .filter(|program| is_cold(program))
                .collect::<Vec<_>>();
            if colds.len() < 2 {
                return Ok(false);
            }
            for cold in &colds {
                let Some(records) = index.processes.get(cold.id.as_str()) else {
                    return Ok(false);
                };
                // Expected errors can make an execution successful without
                // moving data. Credit only a verified payload operation after
                // this cold's first-frame gate, not its planned action class.
                let transferred = records.terminals.iter().any(|result| {
                    result.step > 0
                        && successful_data_observation(&cold.actions[result.step].operation, result)
                });
                if !records.execution.exit_success || !transferred {
                    return Ok(false);
                }
            }
            if !execution_of(&publisher.id).is_some_and(|e| e.exit_success) {
                return Ok(false);
            }
            let reader_first_frame = barriers(&hot_reader.id)
                .iter()
                .any(|barrier| matches!(barrier, BarrierObservation::StreamFirstFrame { .. }));
            let writer_release = barriers(&hot_writer.id)
                .iter()
                .any(|barrier| {
                    matches!(barrier, BarrierObservation::ReleaseObserved { path } if path == release_path)
                });
            let hot_completed = execution_of(&hot_writer.id).is_some_and(|e| e.exit_success)
                && execution_of(&hot_reader.id).is_some_and(|e| e.exit_success);
            if !reader_first_frame || !writer_release || !hot_completed {
                return Ok(false);
            }
            keys.insert(CoverageKey::Scenario {
                scenario: CoverageScenario::HotColdFairness,
            });
            Ok(true)
        }
        CoverageScenario::ProcessChurn => {
            // The paired route is already transferring when the target stops.
            // Only its terminal stop unlocks the release publisher, and the
            // gated writer subsequently observes release and completes.
            let Some((pair, publisher)) = churn_stream_pair(case) else {
                return Ok(false);
            };
            let FailureInjection::StopProcess { process, .. } = &case.failure else {
                return Ok(false);
            };
            let Some(target) = execution_of(process) else {
                return Ok(false);
            };
            let ActionOp::GatedStreamWrite { release_path, .. } =
                &pair.writer.actions[pair.write_step].operation
            else {
                return Ok(false);
            };
            if target.exit_success
                || !target.terminal
                || observed_stream_attachment(&pair, index).is_none()
                || observation
                    .executions
                    .iter()
                    .any(|execution| execution.process != *process && !execution.exit_success)
            {
                return Ok(false);
            }
            let writer = &index.processes[pair.writer.id.as_str()];
            let reader = &index.processes[pair.reader.id.as_str()];
            let release = &index.processes[publisher.id.as_str()];
            if !writer.steps[pair.write_step].iter().any(|result| {
                successful_data_observation(&pair.writer.actions[pair.write_step].operation, result)
            }) || !reader.steps[pair.read_step].iter().any(|result| {
                successful_data_observation(&pair.reader.actions[pair.read_step].operation, result)
            }) || !writer.steps[pair.write_step].iter().any(|result| {
                matches!(&result.barrier, Some(BarrierObservation::ReleaseObserved { path }) if path == release_path)
            }) || !release.terminals.iter().any(|result| {
                result.outcome == "ok"
                    && matches!(&publisher.actions[result.step].operation,
                        ActionOp::PublishBlob { path, .. } if path == release_path)
            }) {
                return Ok(false);
            }
            keys.insert(CoverageKey::Scenario {
                scenario: CoverageScenario::ProcessChurn,
            });
            Ok(true)
        }
        CoverageScenario::ActiveStreamWriterAbort | CoverageScenario::ActiveStreamReaderStop => {
            let Some(pair) = active_stream_fault_pair(case, scenario) else {
                return Ok(false);
            };
            let Some(incarnation) = observed_stream_attachment(&pair, index) else {
                return Ok(false);
            };
            let writer_abort = scenario == CoverageScenario::ActiveStreamWriterAbort;
            let (target, survivor, survivor_step) = if writer_abort {
                (pair.writer, pair.reader, pair.read_step)
            } else {
                (pair.reader, pair.writer, pair.write_step)
            };
            let target_records = &index.processes[target.id.as_str()];
            let survivor_records = &index.processes[survivor.id.as_str()];
            let target_step = if writer_abort {
                pair.write_step
            } else {
                pair.read_step
            };
            if target_records.execution.exit_success
                || !target_records.execution.terminal
                || target_records.steps[target_step]
                    .iter()
                    .any(|result| result.outcome != "barrier")
                || !survivor_records.execution.exit_success
                || !survivor_records.steps[survivor_step].iter().any(|result| {
                    result.outcome == "expected_error"
                        && result.incarnation == Some(incarnation)
                        && matches!(result.errno, None | Some(libc::EPIPE | libc::ECONNRESET))
                        && result.error_type.as_deref() == Some("StreamError")
                })
            {
                return Ok(false);
            }
            let path = pair.writer.actions[pair.write_step].operation.path();
            // Peer loss must release the same attached incarnation. A new
            // replacement's inactivity cannot stand in for fault cleanup.
            let cleaned = survivor_records.terminals.iter().any(|result| {
                result.step > survivor_step
                    && result.outcome == "ok"
                    && result.kind.as_deref() == Some("stream")
                    && result.active == Some(false)
                    && result.revision == Some(incarnation)
                    && matches!(
                        &survivor.actions[result.step].operation,
                        ActionOp::WaitForQuiescent { path: cleanup_path } if cleanup_path == path
                    )
            });
            if cleaned {
                keys.insert(CoverageKey::Scenario { scenario });
            }
            Ok(cleaned)
        }
        CoverageScenario::ActiveMutation | CoverageScenario::QuiescentMutation => {
            // Classify the displaced incarnation, not a fresh replacement's
            // eventual EOF. Inactive namespace evidence retains the same
            // revision; active displacement produces ESTALE on old handles.
            let active = scenario == CoverageScenario::ActiveMutation;
            let mut proven = false;
            for program in &case.processes {
                let Some(process_records) = index.processes.get(program.id.as_str()) else {
                    continue;
                };
                for (step, action) in program.actions.iter().enumerate() {
                    let is_mutation = match &action.operation {
                        ActionOp::StreamWrite { replace, .. }
                        | ActionOp::GatedStreamWrite { replace, .. } => *replace,
                        ActionOp::Rename { .. } | ActionOp::Unlink { .. } => true,
                        _ => false,
                    };
                    if !is_mutation {
                        continue;
                    }
                    let path = action.operation.path();
                    let records = &process_records.steps[step];
                    let transition = records
                        .iter()
                        .find_map(|result| match &result.barrier {
                            Some(BarrierObservation::MutationApplied {
                                from_revision: Some(from),
                                to_revision: Some(to),
                            }) if to > from => Some((*from, *to)),
                            _ => None,
                        })
                        .or_else(|| {
                            if active
                                || !matches!(
                                    action.operation,
                                    ActionOp::StreamWrite { replace: true, .. }
                                        | ActionOp::GatedStreamWrite { replace: true, .. }
                                )
                            {
                                return None;
                            }
                            // A reader can create B before the writer's origin
                            // lookup, so its receipt legitimately reports B/B.
                            // That lookup is not an atomic predecessor receipt:
                            // prove A -> B from an endpoint's ordered evidence.
                            records.iter().find_map(|result| match &result.barrier {
                                Some(BarrierObservation::MutationApplied {
                                    from_revision: Some(_),
                                    to_revision: Some(replacement),
                                }) => quiescent_stream_origin(path, *replacement, index)
                                    .map(|displaced| (displaced, *replacement)),
                                _ => None,
                            })
                        });
                    let Some((displaced, replacement)) = transition else {
                        continue;
                    };
                    if !records.iter().any(|result| {
                        result.outcome == "ok" && result.incarnation == Some(replacement)
                    }) {
                        continue;
                    }
                    let displaced_records = index.paths.get(path).map(Vec::as_slice).unwrap_or(&[]);
                    let quiescence_observed = displaced_records.iter().any(|result| {
                        result.outcome == "ok"
                            && result.kind.as_deref() == Some("stream")
                            && result.revision == Some(displaced)
                            && result.active == Some(false)
                    });
                    let active_displacement = displaced_records.iter().any(|result| {
                        result.outcome == "expected_error"
                            && result.errno == Some(libc::ESTALE)
                            && result.incarnation == Some(displaced)
                    });
                    if (active && active_displacement && !quiescence_observed)
                        || (!active && quiescence_observed)
                    {
                        proven = true;
                    }
                }
            }
            if proven {
                keys.insert(CoverageKey::Scenario { scenario });
            }
            Ok(proven)
        }
        CoverageScenario::FailureIsolation => {
            // A failing branch is isolated, or an ordering-only slow branch
            // parks until every healthy sibling completes. Case validation and
            // the namespace oracle prove the latter from public marker actions.
            let target = match &case.failure {
                FailureInjection::StopProcess { process, .. }
                | FailureInjection::LaunchFailure { process, .. }
                | FailureInjection::SlowProcess { process, .. } => process.clone(),
                FailureInjection::None => return Ok(false),
            };
            let Some(failed) = execution_of(&target) else {
                return Ok(false);
            };
            let ordering_only = matches!(case.failure, FailureInjection::SlowProcess { .. });
            if failed.exit_success != ordering_only {
                return Ok(false);
            }
            let siblings_ok = observation
                .executions
                .iter()
                .filter(|execution| execution.process != target)
                .all(|execution| execution.exit_success);
            if !siblings_ok {
                return Ok(false);
            }
            keys.insert(CoverageKey::Scenario {
                scenario: CoverageScenario::FailureIsolation,
            });
            Ok(true)
        }
        CoverageScenario::Authorization => {
            // A restricted session observed both a denied operation (typed
            // EACCES on a path outside its grants) and a successful
            // operation inside its grants.
            for program in &case.processes {
                if program.access.read_prefixes.is_empty()
                    && program.access.write_prefixes.is_empty()
                {
                    continue;
                }
                let Some(execution) = execution_of(&program.id) else {
                    continue;
                };
                let mut denied = false;
                let mut authorized = false;
                for result in &execution.results {
                    if result.outcome == "expected_error" && result.errno == Some(libc::EACCES) {
                        denied = true;
                    }
                    if result.outcome == "ok" {
                        authorized = true;
                    }
                }
                if denied && authorized {
                    keys.insert(CoverageKey::Scenario {
                        scenario: CoverageScenario::Authorization,
                    });
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn observe_descriptor(
    operation: &ActionOp,
    result: &ActionObservation,
    keys: &mut BTreeSet<CoverageKey>,
) -> Result<(), String> {
    if result.outcome != "ok" && result.descriptor.is_none() {
        return if result.length.is_none() && result.digest.is_none() {
            Ok(())
        } else {
            Err(format!(
                "{} step {} reports payload without an observed completed transfer",
                result.process, result.step
            ))
        };
    }
    let (direction, method, finish, terminals, bytes) = match (
        operation,
        result.descriptor.as_ref(),
    ) {
        (
            ActionOp::DescriptorWrite {
                method,
                finish,
                bytes,
                ..
            },
            Some(DescriptorObservation::Write {
                method: observed_method,
                finish: observed_finish,
                terminal_results,
                ..
            }),
        ) if method == observed_method && finish == observed_finish => (
            "write",
            descriptor_write_method(*observed_method),
            *observed_finish,
            terminal_results,
            bytes,
        ),
        (
            ActionOp::DescriptorRead {
                method,
                finish,
                expected,
                ..
            },
            Some(DescriptorObservation::Read {
                method: observed_method,
                finish: observed_finish,
                terminal_results,
            }),
        ) if method == observed_method && finish == observed_finish => (
            "read",
            descriptor_read_method(*observed_method),
            *observed_finish,
            terminal_results,
            expected,
        ),
        (ActionOp::DescriptorWrite { .. } | ActionOp::DescriptorRead { .. }, _) => {
            return Err(format!(
                "{} step {} lacks matching observed descriptor direction, method, and completed terminal operations",
                result.process, result.step
            ));
        }
        (_, Some(_)) => {
            return Err(format!(
                "{} step {} reports descriptor evidence for another action",
                result.process, result.step
            ));
        }
        (_, None) => return Ok(()),
    };
    let dropped = finish == DescriptorFinish::Drop;
    if dropped && (!terminals.is_empty() || result.outcome != "ok") {
        return Err("descriptor drop must not claim an uncalled terminal API result".to_owned());
    }
    let writer_dropped = direction == "write" && dropped;
    if writer_dropped
        && !matches!(&result.descriptor,
        Some(DescriptorObservation::Write {
            dropped: true, reservation_released: true, terminal_results, ..
        }) if terminal_results.is_empty() && result.outcome == "ok")
    {
        return Err("writer drop lacks observed release and nonpublication evidence".to_owned());
    }
    let terminal_count = if matches!(
        finish,
        DescriptorFinish::CloseTwice | DescriptorFinish::AbortTwice
    ) {
        2
    } else {
        1
    };
    if !dropped
        && (terminals.len() != terminal_count
            || terminals[..terminal_count - 1]
                .iter()
                .any(|terminal| !matches!(terminal, DescriptorTerminalResult::Ok))
            || !match terminals.last() {
                Some(DescriptorTerminalResult::Ok) => result.outcome == "ok",
                Some(DescriptorTerminalResult::Error { errno, error_type }) => {
                    result.outcome == "expected_error"
                        && *errno == result.errno
                        && Some(error_type.as_str()) == result.error_type.as_deref()
                }
                None => false,
            })
    {
        return Err(format!(
            "{} step {} has missing, reordered, or inconsistent descriptor terminal outcomes",
            result.process, result.step
        ));
    }
    if let Some(transfer) = &result.transfer {
        if !transfer.complete
            || transfer.length != bytes.len()
            || transfer.digest != crate::codegen::digest(bytes)
        {
            return Err(format!(
                "{} step {} changed transferred descriptor bytes",
                result.process, result.step
            ));
        }
        for class in payload_classes(transfer.length) {
            keys.insert(CoverageKey::Payload { class });
        }
    } else if result.outcome != "ok" && (result.length.is_some() || result.digest.is_some()) {
        return Err(
            "failed descriptor outcome lacks distinct completed transfer evidence".to_owned(),
        );
    }
    keys.insert(CoverageKey::DescriptorMethod {
        direction: direction.to_owned(),
        method: method.to_owned(),
    });
    keys.insert(CoverageKey::DescriptorTerminal {
        direction: direction.to_owned(),
        finish: descriptor_finish(finish).to_owned(),
    });
    Ok(())
}

fn route_endpoint<'a>(
    index: &ObservationIndex<'a>,
    kind: DataKind,
    role: NodeRole,
    node: u64,
    path: &str,
    process: &str,
) -> Result<&'a ActionObservation, String> {
    let action = match (kind, role) {
        (DataKind::Blob, NodeRole::Source) => "publish_blob",
        (DataKind::Blob, NodeRole::Sink) => "read_blob",
        (DataKind::Stream, NodeRole::Source) => "stream_write",
        (DataKind::Stream, NodeRole::Sink) => "stream_read",
        (_, NodeRole::Relay) => unreachable!("relay requires both endpoints"),
    };
    let matches = index
        .endpoints
        .get(&(node, path, action))
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let result = matches.first().ok_or_else(|| {
        format!("route has no successful {kind:?} {role:?} on node {node} at {path:?}")
    })?;
    if matches.len() != 1 || result.process != process {
        return Err(format!(
            "route has ambiguous {kind:?} {role:?} on node {node} at {path:?}; publication/stream incarnation identity is required"
        ));
    }
    Ok(result)
}

fn observe_route(
    route: &DataRoute,
    index: &ObservationIndex<'_>,
    keys: &mut BTreeSet<CoverageKey>,
) -> Result<(), String> {
    let mut incoming = BTreeMap::<&str, Vec<&ActionObservation>>::new();
    let mut outgoing = BTreeMap::<&str, Vec<&ActionObservation>>::new();
    let mut paths = BTreeSet::new();
    for edge in &route.edges {
        if !paths.insert(edge.path.as_str()) {
            return Err(format!(
                "route {} reuses edge path {:?}",
                route.id, edge.path
            ));
        }
        let source = route_endpoint(
            index,
            route.kind,
            NodeRole::Source,
            edge.source,
            &edge.path,
            &edge.source_role,
        )?;
        let sink = route_endpoint(
            index,
            route.kind,
            NodeRole::Sink,
            edge.destination,
            &edge.path,
            &edge.destination_role,
        )?;
        if source.length.is_none()
            || source.digest.is_none()
            || source.length != sink.length
            || source.digest != sink.digest
        {
            return Err(format!(
                "route {} changed payload on {:?}",
                route.id, edge.path
            ));
        }
        outgoing.entry(&edge.source_role).or_default().push(source);
        incoming
            .entry(&edge.destination_role)
            .or_default()
            .push(sink);
        keys.insert(CoverageKey::OrderedEdge {
            source: edge.source,
            destination: edge.destination,
            kind: route.kind,
        });
        keys.insert(CoverageKey::NodeRole {
            node: edge.source,
            kind: route.kind,
            role: NodeRole::Source,
        });
        keys.insert(CoverageKey::NodeRole {
            node: edge.destination,
            kind: route.kind,
            role: NodeRole::Sink,
        });
    }
    for path in &route.join_inputs {
        if !paths.contains(path.as_str()) {
            return Err(format!(
                "route {} has an unobserved join input {path:?}",
                route.id
            ));
        }
    }
    for (role, inputs) in &incoming {
        let distinct_sources = route
            .edges
            .iter()
            .filter(|edge| edge.destination_role == *role)
            .map(|edge| edge.source_role.as_str())
            .collect::<BTreeSet<_>>();
        // A fan-in join from several distinct upstream nodes must be consumed
        // by one observed execution. Repeated edges from the same upstream
        // node are sequential ring laps through this node, not a join: each
        // lap may close in a different consumer of that node.
        if distinct_sources.len() > 1
            && inputs
                .iter()
                .any(|input| input.process != inputs[0].process)
        {
            return Err(format!(
                "route {} joins across executions at role {role}; observed cross-process input/join causal links are required",
                route.id
            ));
        }
        let Some(outputs) = outgoing.get(role) else {
            continue;
        };
        // Every consumed input is either relayed onward by the same
        // execution at a later step, or ends its execution's walk as a
        // terminal consumer. Per-edge payload integrity is checked above;
        // graph-derived relay transformations can change the outgoing digest.
        let mut witnessed_relay = false;
        for input in inputs {
            let relayed = outputs
                .iter()
                .any(|output| input.process == output.process && input.step < output.step);
            witnessed_relay |= relayed;
            let terminal = !outputs.iter().any(|output| input.process == output.process);
            if !relayed && !terminal {
                return Err(format!(
                    "route {} has no observed receive-before-forward relay at role {role}; cross-process causal links and payload provenance are required",
                    route.id
                ));
            }
        }
        if witnessed_relay {
            keys.insert(CoverageKey::NodeRole {
                node: index.processes[*role].program.logical_node_id,
                kind: route.kind,
                role: NodeRole::Relay,
            });
        }
    }
    Ok(())
}

fn process_depends_on(case: &BehaviorCase, process: &str, dependency: &str) -> bool {
    let mut pending = vec![process];
    let mut visited = BTreeSet::new();
    while let Some(process) = pending.pop() {
        if !visited.insert(process) {
            continue;
        }
        let Some(program) = case.processes.iter().find(|program| program.id == process) else {
            continue;
        };
        for predecessor in &program.depends_on {
            if predecessor == dependency {
                return true;
            }
            pending.push(predecessor);
        }
    }
    false
}

fn churn_stream_pair(case: &BehaviorCase) -> Option<(StreamPair<'_>, &ProcessProgram)> {
    let FailureInjection::StopProcess {
        process,
        phase: crate::ir::ProcessStopPhase::AfterSiblingStreamFirstFrame,
        ..
    } = &case.failure
    else {
        return None;
    };
    let target = case
        .processes
        .iter()
        .find(|program| program.id == *process)?;
    for route in &case.routes {
        for edge in &route.edges {
            if [&edge.source_role, &edge.destination_role]
                .into_iter()
                .any(|role| {
                    role == process
                        || process_depends_on(case, process, role)
                        || process_depends_on(case, role, process)
                })
                || target
                    .actions
                    .iter()
                    .any(|action| action.operation.path() == edge.path)
            {
                return None;
            }
        }
    }
    for route in case
        .routes
        .iter()
        .filter(|route| route.kind == DataKind::Stream)
    {
        for edge in &route.edges {
            let Some(pair) = stream_pair(case, &edge.path) else {
                continue;
            };
            let ActionOp::GatedStreamWrite { release_path, .. } =
                &pair.writer.actions[pair.write_step].operation
            else {
                continue;
            };
            if !matches!(
                pair.reader.actions[pair.read_step].operation,
                ActionOp::GatedStreamRead { .. }
            ) || pair.writer.id != edge.source_role
                || pair.reader.id != edge.destination_role
            {
                continue;
            }
            if let Some(publisher) = case.processes.iter().find(|program| {
                program.depends_on.contains(process)
                    && program.actions.iter().any(|action| {
                        action.expected == ExpectedOutcome::Ok
                            && matches!(&action.operation, ActionOp::PublishBlob { path, .. } if path == release_path)
                    })
            }) {
                return Some((pair, publisher));
            }
        }
    }
    None
}

struct StreamPair<'a> {
    writer: &'a ProcessProgram,
    write_step: usize,
    reader: &'a ProcessProgram,
    read_step: usize,
}

fn stream_pair<'a>(case: &'a BehaviorCase, path: &str) -> Option<StreamPair<'a>> {
    let mut writer = None;
    let mut reader = None;
    for program in &case.processes {
        for (step, action) in program.actions.iter().enumerate() {
            if action.operation.path() != path {
                continue;
            }
            let endpoint = match action.operation {
                ActionOp::StreamWrite { .. } | ActionOp::GatedStreamWrite { .. } => &mut writer,
                ActionOp::StreamRead { .. }
                | ActionOp::StreamReadWithRetry { .. }
                | ActionOp::GatedStreamRead { .. }
                | ActionOp::StreamReadInto { .. } => &mut reader,
                _ => continue,
            };
            if endpoint.replace((program, step)).is_some() {
                return None;
            }
        }
    }
    let (writer, write_step) = writer?;
    let (reader, read_step) = reader?;
    (writer.id != reader.id).then_some(StreamPair {
        writer,
        write_step,
        reader,
        read_step,
    })
}

fn active_stream_fault_pair(
    case: &BehaviorCase,
    scenario: CoverageScenario,
) -> Option<StreamPair<'_>> {
    let FailureInjection::StopProcess {
        process,
        phase: crate::ir::ProcessStopPhase::AfterStreamFirstFrame,
        ..
    } = &case.failure
    else {
        return None;
    };
    let writer_abort = scenario == CoverageScenario::ActiveStreamWriterAbort;
    let target = case
        .processes
        .iter()
        .find(|program| program.id == *process)?;
    for action in &target.actions {
        let Some(pair) = stream_pair(case, action.operation.path()) else {
            continue;
        };
        let (faulted, survivor, survivor_step) = if writer_abort {
            (pair.writer, pair.reader, pair.read_step)
        } else {
            (pair.reader, pair.writer, pair.write_step)
        };
        let faulted_step = if writer_abort {
            pair.write_step
        } else {
            pair.read_step
        };
        let first_frame = match &pair.writer.actions[pair.write_step].operation {
            ActionOp::StreamWrite { chunks, .. } => chunks.first(),
            ActionOp::GatedStreamWrite { frames, .. } => frames.first(),
            _ => None,
        };
        if faulted.id != *process
            || faulted.actions[faulted_step].expected != ExpectedOutcome::Ok
            || first_frame.is_none_or(Vec::is_empty)
            || survivor.actions[survivor_step].expected
                != ExpectedOutcome::Exception(crate::ir::PythonException::StreamError)
        {
            continue;
        }
        let path = action.operation.path();
        if survivor.actions.iter().skip(survivor_step + 1).any(|action| {
            action.expected == ExpectedOutcome::Ok
                && matches!(&action.operation, ActionOp::WaitForQuiescent { path: cleanup } if cleanup == path)
        }) {
            return Some(pair);
        }
    }
    None
}

/// Public quiescence followed by a later same-process open proves an old-to-new
/// binding even when the opposite endpoint joined the new pending incarnation.
/// The oracle has already checked both identities against a legal history.
fn quiescent_stream_origin(
    path: &str,
    replacement: u64,
    index: &ObservationIndex<'_>,
) -> Option<u64> {
    for records in index.processes.values() {
        for (position, result) in records.execution.results.iter().enumerate() {
            let Some(displaced) = result.revision else {
                continue;
            };
            if result.path != path
                || result.outcome != "ok"
                || result.kind.as_deref() != Some("stream")
                || result.active != Some(false)
                || displaced >= replacement
                || !matches!(
                    records.program.actions[result.step].operation,
                    ActionOp::WaitForQuiescent { .. }
                )
            {
                continue;
            }
            // Check emission order too: route endpoints may be prepared before
            // earlier-numbered actions, so step numbers alone are insufficient.
            if records.execution.results[position + 1..]
                .iter()
                .any(|opened| {
                    opened.step > result.step
                        && opened.path == path
                        && opened.barrier
                            == Some(BarrierObservation::StreamOpened {
                                incarnation: replacement,
                            })
                        && records.steps[opened.step].iter().any(|terminal| {
                            terminal.outcome == "ok" && terminal.incarnation == Some(replacement)
                        })
                })
            {
                return Some(displaced);
            }
        }
    }
    None
}

fn observed_stream_attachment(pair: &StreamPair<'_>, index: &ObservationIndex<'_>) -> Option<u64> {
    let writer = index.processes.get(pair.writer.id.as_str())?;
    let reader = index.processes.get(pair.reader.id.as_str())?;
    let writer_records = &writer.steps[pair.write_step];
    let reader_records = &reader.steps[pair.read_step];
    let first = writer_records
        .iter()
        .find_map(|result| match &result.barrier {
            Some(BarrierObservation::StreamFrame {
                incarnation,
                index: 0,
                length,
                digest,
            }) if *incarnation != 0 && *length > 0 => {
                Some((*incarnation, *length, digest.as_str()))
            }
            _ => None,
        })?;
    for records in [writer_records, reader_records] {
        if !records.iter().any(|result| {
            result.barrier
                == Some(BarrierObservation::StreamOpened {
                    incarnation: first.0,
                })
        }) {
            return None;
        }
    }
    if !reader_records.iter().any(|result| {
        matches!(&result.barrier, Some(BarrierObservation::StreamFrame {
            incarnation, index: 0, length, digest,
        }) if *incarnation == first.0 && *length == first.1 && digest == first.2)
    }) || !reader_records.iter().any(|result| {
        result.barrier
            == Some(BarrierObservation::StreamFirstFrame {
                incarnation: first.0,
            })
    }) {
        return None;
    }
    Some(first.0)
}

fn successful_data_observation(operation: &ActionOp, result: &ActionObservation) -> bool {
    result.outcome == "ok"
        && result.length.is_some()
        && result.digest.is_some()
        && (action_payload_length(operation).is_some()
            || matches!(operation, ActionOp::StreamRoundTrip { .. }))
}

/// Fresh edge paths can revisit physical nodes and end in a separate reader
/// on the origin node, but every intermediate receive must feed the next
/// outgoing action in the same process. Hints never define this structure.
fn returning_ring_hops(route: &DataRoute) -> Option<usize> {
    if !route_has_topology(route, TopologyFamily::RingWalk) || !route.join_inputs.is_empty() {
        return None;
    }
    let hops = route
        .edges
        .iter()
        .map(|edge| edge.source)
        .collect::<BTreeSet<_>>()
        .len();
    if route.edges.len() < 2 * hops || route.edges.len() % hops != 0 {
        return None;
    }
    let origin = route.edges[0].source;
    if route.edges.last()?.destination != origin
        || route.edges.windows(2).any(|pair| {
            pair[0].destination != pair[1].source || pair[0].destination_role != pair[1].source_role
        })
        || route.edges.iter().enumerate().any(|(index, edge)| {
            let first_lap = &route.edges[index % hops];
            edge.source != first_lap.source
                || edge.destination != first_lap.destination
                || ((index + 1) % hops == 0 && edge.destination != origin)
        })
    {
        return None;
    }
    Some(hops)
}

/// Shared structural admission; observations must independently prove transfers.
pub(crate) fn route_has_topology(route: &DataRoute, family: TopologyFamily) -> bool {
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum Vertex<'a> {
        Node(u64),
        Role(&'a str),
    }
    let mut degrees = BTreeMap::<Vertex<'_>, (usize, usize)>::new();
    let mut edges = BTreeSet::new();
    for edge in &route.edges {
        // Ring walks revisit physical nodes over fresh per-lap roles. Every
        // other topology, especially a DAG, is a graph of endpoint roles.
        let (source, destination) = if family == TopologyFamily::RingWalk {
            (Vertex::Node(edge.source), Vertex::Node(edge.destination))
        } else {
            (
                Vertex::Role(&edge.source_role),
                Vertex::Role(&edge.destination_role),
            )
        };
        if edges.insert((source, destination)) {
            degrees.entry(source).or_default().1 += 1;
            degrees.entry(destination).or_default().0 += 1;
        }
    }
    let Some(&first) = degrees.keys().next() else {
        return false;
    };
    let mut connected = BTreeSet::from([first]);
    loop {
        let previous = connected.len();
        for &(source, destination) in &edges {
            if connected.contains(&source) || connected.contains(&destination) {
                connected.insert(source);
                connected.insert(destination);
            }
        }
        if previous == connected.len() {
            break;
        }
    }
    if connected.len() != degrees.len() {
        return false;
    }
    let count = |degree| degrees.values().filter(|&&actual| actual == degree).count();
    let nodes = degrees.len();
    match family {
        TopologyFamily::Fixed => true,
        TopologyFamily::Chain => {
            let physical_nodes = route
                .edges
                .iter()
                .flat_map(|edge| [edge.source, edge.destination])
                .collect::<BTreeSet<_>>();
            (3..=5).contains(&nodes)
                && physical_nodes.len() == nodes
                && count((0, 1)) == 1
                && count((1, 0)) == 1
                && count((1, 1)) == nodes - 2
        }
        TopologyFamily::RingWalk => nodes >= 3 && count((1, 1)) == nodes,
        TopologyFamily::FanOut => {
            (3..=5).contains(&nodes) && count((0, nodes - 1)) == 1 && count((1, 0)) == nodes - 1
        }
        TopologyFamily::FanIn => {
            (3..=5).contains(&nodes) && count((nodes - 1, 0)) == 1 && count((0, 1)) == nodes - 1
        }
        TopologyFamily::Diamond => {
            nodes == 4 && count((0, 2)) == 1 && count((1, 1)) == 2 && count((2, 0)) == 1
        }
        TopologyFamily::RandomDag => {
            if degrees
                .values()
                .any(|&(incoming, outgoing)| incoming > 4 || outgoing > 4)
            {
                return false;
            }
            let mut ready = degrees
                .iter()
                .filter_map(|(&node, &(incoming, _))| (incoming == 0).then_some(node))
                .collect::<Vec<_>>();
            let mut visited = 0;
            while let Some(node) = ready.pop() {
                visited += 1;
                for &(_, destination) in edges.iter().filter(|&&(source, _)| source == node) {
                    let incoming = &mut degrees.get_mut(&destination).expect("edge node").0;
                    *incoming -= 1;
                    if *incoming == 0 {
                        ready.push(destination);
                    }
                }
            }
            (3..=20).contains(&nodes) && visited == nodes
        }
    }
}

pub fn case_coverage(case: &BehaviorCase) -> BTreeSet<CoverageKey> {
    let mut keys = BTreeSet::new();
    if case.topology != TopologyFamily::Fixed {
        keys.insert(CoverageKey::Topology {
            family: case.topology,
        });
    }
    for scenario in &case.scenarios {
        if matches!(
            scenario,
            CoverageScenario::ActiveStreamWriterAbort | CoverageScenario::ActiveStreamReaderStop
        ) && active_stream_fault_pair(case, *scenario).is_none()
        {
            continue;
        }
        keys.insert(CoverageKey::Scenario {
            scenario: *scenario,
        });
    }
    if case
        .scenarios
        .contains(&CoverageScenario::ConcurrentStartup)
    {
        keys.insert(CoverageKey::ConcurrentStart {
            family: case.topology,
        });
    }
    for route in &case.routes {
        let mut incoming = BTreeMap::<&str, u64>::new();
        let mut outgoing = BTreeSet::<&str>::new();
        for edge in &route.edges {
            outgoing.insert(&edge.source_role);
            incoming.insert(&edge.destination_role, edge.destination);
            keys.insert(CoverageKey::OrderedEdge {
                source: edge.source,
                destination: edge.destination,
                kind: route.kind,
            });
            keys.insert(CoverageKey::NodeRole {
                node: edge.source,
                kind: route.kind,
                role: NodeRole::Source,
            });
            keys.insert(CoverageKey::NodeRole {
                node: edge.destination,
                kind: route.kind,
                role: NodeRole::Sink,
            });
        }
        for (_, node) in incoming
            .iter()
            .filter(|(role, _)| outgoing.contains(**role))
        {
            keys.insert(CoverageKey::NodeRole {
                node: *node,
                kind: route.kind,
                role: NodeRole::Relay,
            });
        }
    }
    for process in &case.processes {
        for pair in process.actions.windows(2) {
            keys.insert(CoverageKey::ActionAdjacency {
                before: pair[0].operation.class(),
                after: pair[1].operation.class(),
            });
        }
        for action in &process.actions {
            if let Some(length) = action_payload_length(&action.operation) {
                for class in payload_classes(length) {
                    keys.insert(CoverageKey::Payload { class });
                }
            }
            match &action.operation {
                ActionOp::DescriptorWrite { method, finish, .. } => {
                    keys.insert(CoverageKey::DescriptorMethod {
                        direction: "write".to_owned(),
                        method: descriptor_write_method(*method).to_owned(),
                    });
                    keys.insert(CoverageKey::DescriptorTerminal {
                        direction: "write".to_owned(),
                        finish: descriptor_finish(*finish).to_owned(),
                    });
                }
                ActionOp::DescriptorRead { method, finish, .. } => {
                    keys.insert(CoverageKey::DescriptorMethod {
                        direction: "read".to_owned(),
                        method: descriptor_read_method(*method).to_owned(),
                    });
                    keys.insert(CoverageKey::DescriptorTerminal {
                        direction: "read".to_owned(),
                        finish: descriptor_finish(*finish).to_owned(),
                    });
                }
                _ => {}
            }
            let outcome = match action.expected {
                ExpectedOutcome::Ok => "success",
                ExpectedOutcome::Error(_)
                | ExpectedOutcome::Exception(_)
                | ExpectedOutcome::Linearized { .. } => "modeled_error",
            };
            keys.insert(CoverageKey::Outcome {
                class: outcome.to_owned(),
            });
        }
    }
    if !matches!(
        case.failure,
        crate::ir::FailureInjection::None | crate::ir::FailureInjection::SlowProcess { .. }
    ) {
        keys.insert(CoverageKey::Outcome {
            class: "contextual_failure".to_owned(),
        });
    }
    keys
}

fn action_payload_length(operation: &ActionOp) -> Option<usize> {
    match operation {
        ActionOp::PublishBlob { bytes, .. } | ActionOp::DescriptorWrite { bytes, .. } => {
            Some(bytes.len())
        }
        ActionOp::ReadBlob { expected, .. }
        | ActionOp::StreamRead { expected, .. }
        | ActionOp::StreamReadWithRetry { expected, .. }
        | ActionOp::GatedStreamRead { expected, .. }
        | ActionOp::StreamReadInto { expected, .. }
        | ActionOp::DescriptorRead { expected, .. } => Some(expected.len()),
        ActionOp::StreamWrite { chunks, .. } | ActionOp::StreamRoundTrip { chunks, .. } => {
            Some(chunks.iter().map(Vec::len).sum())
        }
        ActionOp::GatedStreamWrite { frames, .. } => Some(frames.iter().map(Vec::len).sum()),
        _ => None,
    }
}

fn payload_classes(length: usize) -> BTreeSet<PayloadClass> {
    let mut classes = BTreeSet::new();
    match length {
        0 => {
            classes.insert(PayloadClass::Empty);
        }
        1..=64 => {
            classes.insert(PayloadClass::Small);
        }
        255..=257 => {
            classes.insert(PayloadClass::FramingBoundary);
        }
        4_095..=4_097 | 65_535..=65_537 => {
            classes.insert(PayloadClass::ChunkBoundary);
        }
        65_538.. => {
            classes.insert(PayloadClass::MultiChunk);
        }
        _ => {
            classes.insert(PayloadClass::Randomized);
        }
    }
    classes
}

const fn descriptor_write_method(method: DescriptorWriteMethod) -> &'static str {
    match method {
        DescriptorWriteMethod::Write => "write",
        DescriptorWriteMethod::WriteFrom => "write_from",
        DescriptorWriteMethod::Mapping => "mapping",
    }
}

const fn descriptor_read_method(method: DescriptorReadMethod) -> &'static str {
    match method {
        DescriptorReadMethod::Read => "read",
        DescriptorReadMethod::ReadInto => "read_into",
        DescriptorReadMethod::Mapping => "mapping",
    }
}

const fn descriptor_finish(finish: DescriptorFinish) -> &'static str {
    match finish {
        DescriptorFinish::Close => "close",
        DescriptorFinish::Abort => "abort",
        DescriptorFinish::Drop => "drop",
        DescriptorFinish::CloseTwice => "close_twice",
        DescriptorFinish::AbortTwice => "abort_twice",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus::ordered_pair_corpus;

    fn evidence_identity(generation: &str) -> EvidenceIdentity {
        EvidenceIdentity {
            plan_digest: crate::codegen::digest(b"test immutable plan"),
            artifacts_digest: crate::codegen::digest(b"test artifact contents"),
            deployment_generation: generation.to_owned(),
            fixture_mapping_digest: crate::codegen::digest(b"test exact fixture mapping"),
        }
    }

    #[test]
    fn survivor_ledger_requires_every_exact_pair_and_role() {
        let nodes = BTreeSet::from([2, 4, 7]);
        let ledger = CoverageLedger::survivor(nodes.clone()).unwrap();
        for kind in [DataKind::Blob, DataKind::Stream] {
            for source in &nodes {
                for destination in &nodes {
                    if source != destination {
                        assert!(ledger.required.contains_key(&CoverageKey::OrderedEdge {
                            source: *source,
                            destination: *destination,
                            kind,
                        }));
                    }
                }
            }
        }
    }

    #[test]
    fn incomplete_planned_coverage_is_a_failure() {
        let mut ledger = CoverageLedger::five_node(BTreeSet::from([1, 2, 3, 4, 5])).unwrap();
        for case in ordered_pair_corpus(5, 11) {
            ledger.plan_case(&case).unwrap();
        }
        assert!(
            ledger
                .assert_planned_closed()
                .unwrap_err()
                .contains("incomplete")
        );
    }

    #[test]
    fn expected_launch_failure_counts_without_inventing_actions() {
        use crate::ir::{FailureInjection, LaunchFailureKind};

        let mut case = ordered_pair_corpus(5, 11).remove(0);
        case.id = "expected-launch-failure".to_owned();
        case.processes.truncate(1);
        case.failure = FailureInjection::LaunchFailure {
            process: case.processes[0].id.clone(),
            kind: LaunchFailureKind::MissingExecutable,
        };
        let mut ledger = CoverageLedger::five_node(BTreeSet::from([1, 2, 3, 4, 5])).unwrap();
        ledger.plan_case(&case).unwrap();
        ledger
            .bind_identity(evidence_identity("generation-a"))
            .unwrap();
        let observation = CaseObservation {
            case_id: case.id.clone(),
            executions: case
                .processes
                .iter()
                .map(|process| crate::ir::ExecutionObservation {
                    process: process.id.clone(),
                    request_id: case.execution_request_id(process),
                    logical_node_id: process.logical_node_id,
                    lifecycle: vec!["spawned".to_owned(), "spawn_failed".to_owned()],
                    results: Vec::new(),
                    terminal: true,
                    exit_success: false,
                    exit_status: None,
                    stdout: String::new(),
                    stderr: String::new(),
                })
                .collect(),
        };

        ledger.observe_case(&case, &observation).unwrap();
        assert!(ledger.completed_cases.contains("expected-launch-failure"));
        assert_eq!(
            ledger.observed,
            BTreeMap::from([(
                CoverageKey::Outcome {
                    class: "contextual_failure".to_owned(),
                },
                1
            )])
        );
        assert!(ledger.assert_observed_closed().is_err());
    }

    fn observed_blob_chain() -> (BehaviorCase, CaseObservation) {
        use crate::ir::{AccessSpec, Action, DataEdge, ExecutionObservation, ProcessProgram};

        let mut case = ordered_pair_corpus(3, 11).remove(0);
        case.id = "observed-chain".to_owned();
        case.topology = TopologyFamily::Chain;
        let payload = b"route-payload".to_vec();
        let mut relayed = payload.clone();
        let rotation = 2 % relayed.len();
        relayed.rotate_left(rotation);
        let left = "/cases/observed-chain/left".to_owned();
        let right = "/cases/observed-chain/right".to_owned();
        case.routes = vec![DataRoute {
            id: "chain".to_owned(),
            kind: DataKind::Blob,
            edges: vec![
                DataEdge {
                    source: 1,
                    destination: 2,
                    source_role: "source".to_owned(),
                    destination_role: "relay".to_owned(),
                    path: left.clone(),
                },
                DataEdge {
                    source: 2,
                    destination: 3,
                    source_role: "relay".to_owned(),
                    destination_role: "sink".to_owned(),
                    path: right.clone(),
                },
            ],
            join_inputs: Vec::new(),
        }];
        case.processes = vec![
            ProcessProgram {
                id: "source".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("source"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::PublishBlob {
                    path: left.clone(),
                    bytes: payload.clone(),
                })],
            },
            ProcessProgram {
                id: "relay".to_owned(),
                logical_node_id: 2,
                access: AccessSpec::unrestricted("relay"),
                depends_on: Vec::new(),
                actions: vec![
                    Action::ok(ActionOp::ReadBlob {
                        path: left,
                        expected: payload.clone(),
                    }),
                    Action::ok(ActionOp::PublishBlob {
                        path: right.clone(),
                        bytes: relayed.clone(),
                    }),
                ],
            },
            ProcessProgram {
                id: "sink".to_owned(),
                logical_node_id: 3,
                access: AccessSpec::unrestricted("sink"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::ReadBlob {
                    path: right,
                    expected: relayed,
                })],
            },
        ];
        let observation = CaseObservation {
            case_id: case.id.clone(),
            executions: case
                .processes
                .iter()
                .map(|process| ExecutionObservation {
                    process: process.id.clone(),
                    request_id: case.execution_request_id(process),
                    logical_node_id: process.logical_node_id,
                    lifecycle: ["process_started", "context_ready", "user_result", "exited"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                    results: process
                        .actions
                        .iter()
                        .enumerate()
                        .map(|(step, action)| {
                            let payload = match &action.operation {
                                ActionOp::PublishBlob { bytes, .. } => bytes,
                                ActionOp::ReadBlob { expected, .. } => expected,
                                operation => panic!(
                                    "unexpected observed blob-chain operation: {operation:?}"
                                ),
                            };
                            ActionObservation {
                                process: process.id.clone(),
                                step,
                                action: action.operation.class().as_str().to_owned(),
                                path: action.operation.path().to_owned(),
                                outcome: "ok".to_owned(),
                                length: Some(payload.len()),
                                digest: Some(crate::codegen::digest(payload)),
                                kind: None,
                                revision: None,
                                active: None,
                                errno: None,
                                error_type: None,
                                error: None,
                                descriptor: None,
                                transfer: None,
                                barrier: None,
                                incarnation: None,
                                token: None,
                                lap: None,
                            }
                        })
                        .collect(),
                    terminal: true,
                    exit_success: true,
                    exit_status: Some("success".to_owned()),
                    stdout: String::new(),
                    stderr: String::new(),
                })
                .collect(),
        };
        (case, observation)
    }

    fn observed_fairness() -> (BehaviorCase, CaseObservation) {
        use crate::ir::{AccessSpec, Action};

        let (mut case, template) = observed_blob_chain();
        case.topology = TopologyFamily::Fixed;
        case.routes.clear();
        case.scenarios = BTreeSet::from([CoverageScenario::HotColdFairness]);
        let hot = "/cases/observed-chain/hot";
        let observed = "/cases/observed-chain/first-frame";
        let release = "/cases/observed-chain/release";
        let frames = vec![b"first".to_vec(), b"after-release".to_vec()];
        let program = |id: &str, node, actions, depends_on| ProcessProgram {
            id: id.to_owned(),
            logical_node_id: node,
            access: AccessSpec::unrestricted(id),
            actions,
            depends_on,
        };
        case.processes = vec![
            program(
                "hot-writer",
                1,
                vec![Action::ok(ActionOp::GatedStreamWrite {
                    path: hot.to_owned(),
                    replace: false,
                    frames: frames.clone(),
                    release_path: release.to_owned(),
                })],
                Vec::new(),
            ),
            program(
                "hot-reader",
                2,
                vec![Action::ok(ActionOp::GatedStreamRead {
                    path: hot.to_owned(),
                    expected: frames.concat(),
                    observed_path: observed.to_owned(),
                    retry_attach: false,
                    park_after_first_frame: false,
                })],
                Vec::new(),
            ),
        ];
        for id in ["cold-a", "cold-b"] {
            let path = format!("/cases/observed-chain/{id}");
            case.processes.push(program(
                id,
                3,
                vec![
                    Action::ok(ActionOp::AwaitEntry {
                        path: observed.to_owned(),
                        expected_kind: "blob".to_owned(),
                    }),
                    Action::ok(ActionOp::PublishBlob {
                        path: path.clone(),
                        bytes: b"cold".to_vec(),
                    }),
                    Action::ok(ActionOp::ReadBlob {
                        path,
                        expected: b"cold".to_vec(),
                    }),
                ],
                Vec::new(),
            ));
        }
        case.processes.push(program(
            "release",
            3,
            vec![Action::ok(ActionOp::PublishBlob {
                path: release.to_owned(),
                bytes: Vec::new(),
            })],
            vec!["cold-a".to_owned(), "cold-b".to_owned()],
        ));
        let observation = CaseObservation {
            case_id: case.id.clone(),
            executions: case
                .processes
                .iter()
                .map(|program| {
                    let mut execution = template.executions[0].clone();
                    execution.process = program.id.clone();
                    execution.request_id = case.execution_request_id(program);
                    execution.logical_node_id = program.logical_node_id;
                    execution.results.clear();
                    for (step, action) in program.actions.iter().enumerate() {
                        let mut terminal = template.executions[0].results[0].clone();
                        terminal.process = program.id.clone();
                        terminal.step = step;
                        terminal.action = action.operation.class().as_str().to_owned();
                        terminal.path = action.operation.path().to_owned();
                        let payload = match &action.operation {
                            ActionOp::PublishBlob { bytes, .. } => Some(bytes.clone()),
                            ActionOp::ReadBlob { expected, .. }
                            | ActionOp::GatedStreamRead { expected, .. } => Some(expected.clone()),
                            ActionOp::GatedStreamWrite { frames, .. } => Some(frames.concat()),
                            ActionOp::AwaitEntry { .. } => {
                                terminal.kind = Some("blob".to_owned());
                                terminal.revision = Some(2);
                                None
                            }
                            _ => unreachable!(),
                        };
                        terminal.length = payload.as_ref().map(Vec::len);
                        terminal.digest =
                            payload.as_ref().map(|bytes| crate::codegen::digest(bytes));
                        let write = matches!(action.operation, ActionOp::GatedStreamWrite { .. });
                        let read = matches!(action.operation, ActionOp::GatedStreamRead { .. });
                        if write || read {
                            terminal.incarnation = Some(1);
                            let mut emit = |barrier| {
                                let mut record = terminal.clone();
                                record.outcome = "barrier".to_owned();
                                record.length = None;
                                record.digest = None;
                                record.barrier = Some(barrier);
                                execution.results.push(record);
                            };
                            emit(BarrierObservation::StreamOpened { incarnation: 1 });
                            for (index, frame) in frames.iter().enumerate() {
                                emit(BarrierObservation::StreamFrame {
                                    incarnation: 1,
                                    index: index as u64,
                                    length: frame.len() as u64,
                                    digest: crate::codegen::digest(frame),
                                });
                                if index == 0 {
                                    emit(if write {
                                        BarrierObservation::ReleaseObserved {
                                            path: release.to_owned(),
                                        }
                                    } else {
                                        BarrierObservation::StreamFirstFrame { incarnation: 1 }
                                    });
                                }
                            }
                            if read {
                                emit(BarrierObservation::StreamEof { incarnation: 1 });
                            }
                        }
                        execution.results.push(terminal);
                    }
                    execution
                })
                .collect(),
        };
        (case, observation)
    }

    fn observed_active_stream_fault(writer_abort: bool) -> (BehaviorCase, CaseObservation) {
        let (mut case, mut observation) = observed_fairness();
        case.processes.truncate(2);
        observation.executions.truncate(2);
        let target = if writer_abort { 0 } else { 1 };
        let survivor = 1 - target;
        let scenario = if writer_abort {
            CoverageScenario::ActiveStreamWriterAbort
        } else {
            CoverageScenario::ActiveStreamReaderStop
        };
        case.scenarios = BTreeSet::from([scenario]);
        case.failure = FailureInjection::StopProcess {
            process: case.processes[target].id.clone(),
            phase: crate::ir::ProcessStopPhase::AfterStreamFirstFrame,
            kill_after_ms: Some(100),
        };
        if !writer_abort {
            let ActionOp::GatedStreamWrite { path, frames, .. } =
                case.processes[0].actions[0].operation.clone()
            else {
                unreachable!();
            };
            case.processes[0].actions[0].operation = ActionOp::StreamWrite {
                path,
                chunks: frames,
                replace: false,
            };
            let ActionOp::GatedStreamRead {
                park_after_first_frame,
                ..
            } = &mut case.processes[1].actions[0].operation
            else {
                unreachable!();
            };
            *park_after_first_frame = true;
        }
        case.processes[survivor].actions[0].expected =
            ExpectedOutcome::Exception(crate::ir::PythonException::StreamError);
        let path = case.processes[survivor].actions[0]
            .operation
            .path()
            .to_owned();
        case.processes[survivor]
            .actions
            .push(crate::ir::Action::ok(ActionOp::WaitForQuiescent { path }));
        for (position, execution) in observation.executions.iter_mut().enumerate() {
            let mut terminal = execution.results.last().unwrap().clone();
            execution.results.retain(|result| {
                matches!(
                    result.barrier,
                    Some(
                        BarrierObservation::StreamOpened { .. }
                            | BarrierObservation::StreamFrame { index: 0, .. }
                            | BarrierObservation::StreamFirstFrame { .. }
                    )
                )
            });
            if position == target {
                execution.exit_success = false;
                execution.exit_status = Some("signal=15".to_owned());
                execution.lifecycle = [
                    "process_started",
                    "context_ready",
                    "stop_accepted",
                    "exited",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect();
            } else {
                let mut cleanup = terminal.clone();
                cleanup.step = 1;
                cleanup.action = "lookup".to_owned();
                cleanup.length = None;
                cleanup.digest = None;
                cleanup.incarnation = None;
                cleanup.kind = Some("stream".to_owned());
                cleanup.revision = Some(1);
                cleanup.active = Some(false);
                terminal.outcome = "expected_error".to_owned();
                terminal.length = None;
                terminal.digest = None;
                terminal.error_type = Some("StreamError".to_owned());
                terminal.transfer = Some(crate::ir::TransferObservation {
                    length: b"first".len(),
                    digest: crate::codegen::digest(b"first"),
                    complete: false,
                });
                execution.results.extend([terminal, cleanup]);
            }
        }
        (case, observation)
    }

    #[test]
    fn active_stream_fault_credit_requires_attached_peer_loss_and_exact_cleanup() {
        for writer_abort in [true, false] {
            let (case, observation) = observed_active_stream_fault(writer_abort);
            let scenario = *case.scenarios.first().unwrap();
            let mut complete = ledger_for_case(&case);
            complete.observe_case(&case, &observation).unwrap();
            assert!(
                complete
                    .observed
                    .contains_key(&CoverageKey::Scenario { scenario })
            );
            let survivor = usize::from(writer_abort);
            for corruption in 0..3 {
                let mut incomplete = observation.clone();
                match corruption {
                    0 => incomplete.executions[1].results.retain(|result| {
                        !matches!(
                            result.barrier,
                            Some(BarrierObservation::StreamFirstFrame { .. })
                        )
                    }),
                    1 => {
                        let error = incomplete.executions[survivor]
                            .results
                            .iter_mut()
                            .find(|result| result.outcome == "expected_error")
                            .unwrap();
                        error.errno = Some(libc::ESTALE);
                        error.error_type = Some("OSError".to_owned());
                    }
                    _ => {
                        incomplete.executions[survivor]
                            .results
                            .last_mut()
                            .unwrap()
                            .revision = Some(2)
                    }
                }
                let mut rejected = ledger_for_case(&case);
                assert!(rejected.observe_case(&case, &incomplete).is_err());
                assert!(rejected.observed.is_empty());
                assert!(rejected.completed_cases.is_empty());
            }
        }
    }

    #[test]
    fn descriptor_aborts_unmatched_stops_and_replacements_leave_stream_fault_obligations_open() {
        let mut ledger = CoverageLedger::five_node(BTreeSet::from([1, 2, 3, 4, 5])).unwrap();
        let faults = [
            CoverageScenario::ActiveStreamWriterAbort,
            CoverageScenario::ActiveStreamReaderStop,
        ];
        for mut case in crate::corpus::stable_corpus(5, 11)
            .into_iter()
            .chain(crate::corpus::failure_corpus(5, 11))
            .filter(|case| {
                matches!(case.failure, FailureInjection::StopProcess { .. })
                    || case
                        .processes
                        .iter()
                        .flat_map(|program| &program.actions)
                        .any(|action| {
                            matches!(
                                action.operation,
                                ActionOp::DescriptorWrite {
                                    finish: DescriptorFinish::Abort | DescriptorFinish::AbortTwice,
                                    ..
                                } | ActionOp::DescriptorRead {
                                    finish: DescriptorFinish::Abort | DescriptorFinish::AbortTwice,
                                    ..
                                } | ActionOp::StreamWrite { replace: true, .. }
                            )
                        })
            })
        {
            // Even relabeling the old corpus cannot plan an attached fault.
            case.scenarios.extend(faults);
            ledger.plan_case(&case).unwrap();
        }
        for scenario in faults {
            let key = CoverageKey::Scenario { scenario };
            assert_eq!(ledger.required.get(&key), Some(&1));
            assert!(!ledger.planned.contains_key(&key));
            assert!(!ledger.observed.contains_key(&key));
        }
        assert!(ledger.assert_planned_closed().is_err());
    }

    #[test]
    fn error_only_cold_flows_cannot_close_fairness() {
        let (case, observation) = observed_fairness();
        let mut complete = ledger_for_case(&case);
        complete.observe_case(&case, &observation).unwrap();
        assert!(complete.observed.contains_key(&CoverageKey::Scenario {
            scenario: CoverageScenario::HotColdFairness,
        }));
        // Retain the first-frame gate, release dependency, and all hot-stream
        // evidence. Even one error-only cold must prevent fairness credit.
        for cold in ["cold-a", "cold-b"] {
            let mut denied_case = case.clone();
            let mut denied_observation = observation.clone();
            let program = denied_case
                .processes
                .iter_mut()
                .find(|p| p.id == cold)
                .unwrap();
            let gate_path = program.actions[0].operation.path().to_owned();
            program.access.read_prefixes = vec![gate_path];
            program.actions.truncate(1);
            program.actions.push(crate::ir::Action::error(
                ActionOp::ReadBlob {
                    path: format!("/cases/observed-chain/denied-{cold}"),
                    expected: b"cold".to_vec(),
                },
                libc::EACCES,
            ));
            let execution = denied_observation
                .executions
                .iter_mut()
                .find(|e| e.process == cold)
                .unwrap();
            let mut denied = execution.results.last().unwrap().clone();
            denied.step = 1;
            denied.path = program.actions[1].operation.path().to_owned();
            denied.outcome = "expected_error".to_owned();
            denied.errno = Some(libc::EACCES);
            denied.length = None;
            denied.digest = None;
            execution.results.truncate(1);
            execution.results.push(denied);
            crate::oracle::BehaviorOracle::verify(&denied_case, &denied_observation).unwrap();
            let mut rejected = ledger_for_case(&denied_case);
            assert!(
                rejected
                    .observe_case(&denied_case, &denied_observation)
                    .is_err()
            );
            assert!(rejected.observed.is_empty());
        }
    }

    #[test]
    fn churn_requires_route_continuation_released_by_the_stop() {
        let (mut case, mut observation) = observed_fairness();
        case.processes
            .retain(|program| !program.id.starts_with("cold-"));
        observation
            .executions
            .retain(|execution| !execution.process.starts_with("cold-"));
        let mut target = case.processes[0].clone();
        target.id = "churn-target".to_owned();
        target.access = crate::ir::AccessSpec::unrestricted("churn-target");
        target.actions = vec![crate::ir::Action::ok(ActionOp::StreamRead {
            path: "/cases/observed-chain/parked".to_owned(),
            expected: Vec::new(),
        })];
        case.processes[2].depends_on = vec![target.id.clone()];
        let mut stopped = observation.executions[0].clone();
        stopped.process = target.id.clone();
        stopped.request_id = case.execution_request_id(&target);
        stopped.results.clear();
        stopped.exit_success = false;
        stopped.exit_status = Some("signal=15".to_owned());
        stopped.lifecycle = [
            "process_started",
            "context_ready",
            "stop_accepted",
            "exited",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        case.failure = FailureInjection::StopProcess {
            process: target.id.clone(),
            phase: crate::ir::ProcessStopPhase::AfterSiblingStreamFirstFrame,
            kill_after_ms: Some(100),
        };
        case.processes.push(target);
        observation.executions.push(stopped);
        case.scenarios = BTreeSet::from([CoverageScenario::ProcessChurn]);
        case.routes = vec![DataRoute {
            id: "healthy-gated-route".to_owned(),
            kind: DataKind::Stream,
            edges: vec![crate::ir::DataEdge {
                source: 1,
                destination: 2,
                source_role: case.processes[0].id.clone(),
                destination_role: case.processes[1].id.clone(),
                path: case.processes[0].actions[0].operation.path().to_owned(),
            }],
            join_inputs: Vec::new(),
        }];
        let mut complete = ledger_for_case(&case);
        complete.observe_case(&case, &observation).unwrap();
        assert!(complete.observed.contains_key(&CoverageKey::Scenario {
            scenario: CoverageScenario::ProcessChurn,
        }));
        // Identical successful work is not proof that it crossed the stop
        // when release can publish independently of the target's terminal.
        case.processes[2].depends_on.clear();
        crate::oracle::BehaviorOracle::verify(&case, &observation).unwrap();
        let mut rejected = ledger_for_case(&case);
        assert!(rejected.observe_case(&case, &observation).is_err());
        assert!(rejected.observed.is_empty());
    }

    #[test]
    fn serialized_stop_after_healthy_routes_cannot_close_churn() {
        let (mut case, mut observation) = observed_blob_chain();
        let mut target = case.processes[0].clone();
        target.id = "churn-target".to_owned();
        target.access = crate::ir::AccessSpec::unrestricted("churn-target");
        target.depends_on = case
            .processes
            .iter()
            .map(|program| program.id.clone())
            .collect();
        target.actions = vec![crate::ir::Action::ok(ActionOp::StreamRead {
            path: "/cases/observed-chain/parked".to_owned(),
            expected: Vec::new(),
        })];
        let mut stopped = observation.executions[0].clone();
        stopped.process = target.id.clone();
        stopped.request_id = case.execution_request_id(&target);
        stopped.results.clear();
        stopped.exit_success = false;
        stopped.exit_status = Some("signal=15".to_owned());
        stopped.lifecycle = [
            "process_started",
            "context_ready",
            "stop_accepted",
            "exited",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        case.failure = FailureInjection::StopProcess {
            process: target.id.clone(),
            phase: crate::ir::ProcessStopPhase::AfterContextReady,
            kill_after_ms: Some(100),
        };
        case.processes.push(target);
        observation.executions.push(stopped);
        crate::oracle::BehaviorOracle::verify(&case, &observation).unwrap();
        case.scenarios.insert(CoverageScenario::ProcessChurn);
        let mut rejected = ledger_for_case(&case);
        assert!(rejected.observe_case(&case, &observation).is_err());
        assert!(rejected.observed.is_empty());
    }

    fn ledger_for_case(case: &BehaviorCase) -> CoverageLedger {
        let mut ledger = CoverageLedger::survivor(case.live_nodes.clone()).unwrap();
        ledger.required = case_coverage(case)
            .into_iter()
            .map(|key| (key, 1))
            .collect();
        ledger.plan_case(case).unwrap();
        ledger
            .bind_identity(evidence_identity("generation-a"))
            .unwrap();
        ledger
    }

    #[test]
    fn coverage_rejects_forged_missing_and_swapped_records_atomically() {
        let (case, observation) = observed_blob_chain();
        let mut valid = ledger_for_case(&case);
        valid.observe_case(&case, &observation).unwrap();
        valid.assert_observed_closed().unwrap();

        let corruptions: &[(&str, fn(&mut CaseObservation))] = &[
            ("mislocated execution", |seen| {
                seen.executions[0].logical_node_id = 3
            }),
            ("duplicate execution", |seen| {
                seen.executions[2] = seen.executions[0].clone()
            }),
            ("duplicate request", |seen| {
                seen.executions[2].request_id = seen.executions[0].request_id.clone()
            }),
            ("swapped process records", |seen| {
                let source = seen.executions[0].results[0].clone();
                seen.executions[0].results[0] = seen.executions[2].results[0].clone();
                seen.executions[2].results[0] = source;
            }),
            ("duplicate action replacing missing step", |seen| {
                seen.executions[1].results[1] = seen.executions[1].results[0].clone();
            }),
            ("reordered actions", |seen| {
                seen.executions[1].results.swap(0, 1)
            }),
            ("wrong action", |seen| {
                seen.executions[0].results[0].action = "stream_write".to_owned()
            }),
            ("wrong path", |seen| {
                seen.executions[0].results[0].path.push_str("-other")
            }),
            ("wrong payload length", |seen| {
                seen.executions[0].results[0].length = Some(0)
            }),
            ("missing payload digest", |seen| {
                seen.executions[0].results[0].digest = None
            }),
            ("wrong payload digest", |seen| {
                seen.executions[0].results[0].digest = Some(crate::codegen::digest(b"forged"))
            }),
            ("wrong outcome", |seen| {
                seen.executions[0].results[0].outcome = "expected_error".to_owned()
            }),
        ];
        for (label, corrupt) in corruptions {
            let mut ledger = ledger_for_case(&case);
            let mut forged = observation.clone();
            corrupt(&mut forged);
            assert!(ledger.observe_case(&case, &forged).is_err(), "{label}");
            assert!(ledger.assert_observed_closed().is_err(), "{label}");
            assert!(ledger.observed.is_empty(), "{label}");
            assert!(!ledger.completed_cases.contains(&case.id), "{label}");
            // A rejected attempt cannot consume the completion identity.
            ledger.observe_case(&case, &observation).unwrap();
            ledger.assert_observed_closed().unwrap();
        }
    }

    #[test]
    fn route_admission_requires_endpoint_roles_and_receive_before_forward() {
        let (case, _) = observed_blob_chain();
        let mut wrong_route = case.clone();
        wrong_route.routes[0].edges[0].source = 3;
        wrong_route.topology = TopologyFamily::Fixed;
        let mut planned = CoverageLedger::survivor(case.live_nodes.clone()).unwrap();
        assert!(planned.plan_case(&wrong_route).is_err());

        let mut wrong_topology = case.clone();
        wrong_topology.topology = TopologyFamily::Diamond;
        assert!(planned.plan_case(&wrong_topology).is_err());

        let mut no_relay = case;
        no_relay.processes[1].actions.swap(0, 1);
        assert!(planned.plan_case(&no_relay).is_err());
    }

    fn label_ring_observation(
        case: &mut BehaviorCase,
        observation: &mut CaseObservation,
        hops: usize,
    ) {
        for (program, execution) in case.processes.iter_mut().zip(&mut observation.executions) {
            let mut labeled = Vec::new();
            for mut result in execution.results.drain(..) {
                let edge_index = case.routes[0]
                    .edges
                    .iter()
                    .position(|edge| edge.path == result.path)
                    .unwrap();
                let lap = (edge_index / hops) as u32;
                let token = result.digest.clone().unwrap();
                let source = result.action == "publish_blob";
                let completed = !source && edge_index + 1 == case.routes[0].edges.len();
                let barrier_name = if source {
                    crate::ir::BARRIER_TOKEN_FORWARDED
                } else if completed {
                    crate::ir::BARRIER_LAP_COMPLETED
                } else {
                    crate::ir::BARRIER_TOKEN_RECEIVED
                };
                program.actions[result.step].evidence_hints = BTreeMap::from([
                    ("token".to_owned(), token.clone()),
                    ("lap".to_owned(), lap.to_string()),
                    ("edge_index".to_owned(), edge_index.to_string()),
                    ("barrier".to_owned(), barrier_name.to_owned()),
                ]);
                result.token = Some(token.clone());
                result.lap = Some(lap);
                let mut milestone = result.clone();
                milestone.outcome = "barrier".to_owned();
                milestone.length = None;
                milestone.digest = None;
                milestone.barrier = Some(if source {
                    BarrierObservation::TokenForwarded {
                        token,
                        lap,
                        edge_index: edge_index as u32,
                    }
                } else if completed {
                    BarrierObservation::LapCompleted { token, lap }
                } else {
                    BarrierObservation::TokenReceived {
                        token,
                        lap,
                        edge_index: edge_index as u32,
                    }
                });
                labeled.extend([result, milestone]);
            }
            execution.results = labeled;
        }
        case.scenarios.insert(CoverageScenario::RingCompletion);
    }

    #[test]
    fn labeled_open_chain_cannot_complete_a_ring() {
        let (mut case, mut observation) = observed_blob_chain();
        label_ring_observation(&mut case, &mut observation, 2);
        // All hinted transfers and their payloads really succeeded. Only the
        // alleged return/multi-lap traversal is missing.
        crate::oracle::BehaviorOracle::verify(&case, &observation).unwrap();
        let mut ledger = ledger_for_case(&case);
        assert!(ledger.observe_case(&case, &observation).is_err());
        assert!(ledger.observed.is_empty());
        assert!(ledger.completed_cases.is_empty());
    }

    #[test]
    fn ring_laps_through_one_node_close_coverage_across_consumers() {
        use crate::ir::{AccessSpec, Action, DataEdge, ExecutionObservation, ProcessProgram};

        let mut case = ordered_pair_corpus(3, 11).remove(0);
        case.id = "observed-ring".to_owned();
        case.live_nodes = BTreeSet::from([2, 3, 4]);
        case.topology = TopologyFamily::RingWalk;
        let lap0 = b"ring-lap-0-payload".to_vec();
        let lap1 = b"ring-lap-1-payload".to_vec();
        let p = |hop: &str| format!("/cases/observed-ring/{hop}");
        let edge = |source: u64,
                    destination: u64,
                    source_role: &str,
                    destination_role: &str,
                    hop: &str| DataEdge {
            source,
            destination,
            source_role: source_role.to_owned(),
            destination_role: destination_role.to_owned(),
            path: p(hop),
        };
        case.routes = vec![DataRoute {
            id: "ring".to_owned(),
            kind: DataKind::Blob,
            edges: vec![
                edge(2, 3, "origin-relay", "relay-3", "lap-0/2-3"),
                edge(3, 4, "relay-3", "relay-4", "lap-0/3-4"),
                edge(4, 2, "relay-4", "origin-relay", "lap-0/4-2"),
                edge(2, 3, "origin-relay", "relay-3", "lap-1/2-3"),
                edge(3, 4, "relay-3", "relay-4", "lap-1/3-4"),
                edge(4, 2, "relay-4", "reader", "lap-1/4-2"),
            ],
            join_inputs: Vec::new(),
        }];
        let publish = |hop: &str, payload: &Vec<u8>| {
            Action::ok(ActionOp::PublishBlob {
                path: p(hop),
                bytes: payload.clone(),
            })
        };
        let read = |hop: &str, payload: &Vec<u8>| {
            Action::ok(ActionOp::ReadBlob {
                path: p(hop),
                expected: payload.clone(),
            })
        };
        // The origin relay closes lap 0 and mints the lap-1 payload; a
        // dedicated reader terminates lap 1. Both lap closures are edges
        // from node 4 into node 2 but land in different executions.
        case.processes = vec![
            ProcessProgram {
                id: "origin-relay".to_owned(),
                logical_node_id: 2,
                access: AccessSpec::unrestricted("origin-relay"),
                depends_on: Vec::new(),
                actions: vec![
                    publish("lap-0/2-3", &lap0),
                    read("lap-0/4-2", &lap0),
                    publish("lap-1/2-3", &lap1),
                ],
            },
            ProcessProgram {
                id: "relay-3".to_owned(),
                logical_node_id: 3,
                access: AccessSpec::unrestricted("relay-3"),
                depends_on: Vec::new(),
                actions: vec![
                    read("lap-0/2-3", &lap0),
                    publish("lap-0/3-4", &lap0),
                    read("lap-1/2-3", &lap1),
                    publish("lap-1/3-4", &lap1),
                ],
            },
            ProcessProgram {
                id: "relay-4".to_owned(),
                logical_node_id: 4,
                access: AccessSpec::unrestricted("relay-4"),
                depends_on: Vec::new(),
                actions: vec![
                    read("lap-0/3-4", &lap0),
                    publish("lap-0/4-2", &lap0),
                    read("lap-1/3-4", &lap1),
                    publish("lap-1/4-2", &lap1),
                ],
            },
            ProcessProgram {
                id: "reader".to_owned(),
                logical_node_id: 2,
                access: AccessSpec::unrestricted("reader"),
                depends_on: Vec::new(),
                actions: vec![read("lap-1/4-2", &lap1)],
            },
        ];
        let payload_for = |hop: &str| match hop.starts_with("lap-0") {
            true => lap0.clone(),
            false => lap1.clone(),
        };
        let mut observation = CaseObservation {
            case_id: case.id.clone(),
            executions: case
                .processes
                .iter()
                .map(|process| ExecutionObservation {
                    process: process.id.clone(),
                    request_id: case.execution_request_id(process),
                    logical_node_id: process.logical_node_id,
                    lifecycle: ["process_started", "context_ready", "user_result", "exited"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                    results: process
                        .actions
                        .iter()
                        .enumerate()
                        .map(|(step, action)| {
                            let payload = payload_for(
                                action
                                    .operation
                                    .path()
                                    .trim_start_matches("/cases/observed-ring/"),
                            );
                            ActionObservation {
                                process: process.id.clone(),
                                step,
                                action: action.operation.class().as_str().to_owned(),
                                path: action.operation.path().to_owned(),
                                outcome: "ok".to_owned(),
                                length: Some(payload.len()),
                                digest: Some(crate::codegen::digest(&payload)),
                                kind: None,
                                revision: None,
                                active: None,
                                errno: None,
                                error_type: None,
                                error: None,
                                descriptor: None,
                                transfer: None,
                                barrier: None,
                                incarnation: None,
                                token: None,
                                lap: None,
                            }
                        })
                        .collect(),
                    terminal: true,
                    exit_success: true,
                    exit_status: Some("success".to_owned()),
                    stdout: String::new(),
                    stderr: String::new(),
                })
                .collect(),
        };
        label_ring_observation(&mut case, &mut observation, 3);
        let mut ledger = ledger_for_case(&case);
        ledger.observe_case(&case, &observation).unwrap();
        ledger.assert_observed_closed().unwrap();
        assert!(ledger.observed.contains_key(&CoverageKey::Scenario {
            scenario: CoverageScenario::RingCompletion,
        }));

        // A genuine single closed lap still does not prove repeated traversal.
        let mut single_lap = case.clone();
        single_lap.routes[0].edges.truncate(3);
        let index = ObservationIndex::new(&single_lap, &observation).unwrap();
        let mut keys = BTreeSet::new();
        assert!(
            !observe_scenario(
                &single_lap,
                CoverageScenario::RingCompletion,
                &index,
                &mut keys,
            )
            .unwrap()
        );
        assert!(!keys.contains(&CoverageKey::Scenario {
            scenario: CoverageScenario::RingCompletion,
        }));

        // A physical placement is not ownership: a claimed reader role must
        // execute that edge's read itself, not another process on its node.
        let mut fanin_case = case.clone();
        fanin_case.topology = TopologyFamily::Fixed;
        fanin_case.routes[0].edges.push(DataEdge {
            source: 3,
            destination: 2,
            source_role: "relay-3".to_owned(),
            destination_role: "reader".to_owned(),
            path: p("lap-1/3-2"),
        });
        fanin_case.processes[0]
            .actions
            .push(read("lap-1/3-2", &lap1));
        fanin_case.processes[1]
            .actions
            .push(publish("lap-1/3-2", &lap1));
        let mut fanin_ledger = CoverageLedger::survivor(fanin_case.live_nodes.clone()).unwrap();
        assert!(fanin_ledger.plan_case(&fanin_case).is_err());
    }

    #[test]
    fn labels_without_required_observation_metadata_do_not_close_coverage() {
        let (mut case, observation) = observed_blob_chain();
        case.scenarios.insert(CoverageScenario::ConcurrentStartup);
        let mut ledger = ledger_for_case(&case);
        assert!(ledger.observe_case(&case, &observation).is_err());
        assert!(ledger.assert_observed_closed().is_err());
        assert!(ledger.observed.is_empty());

        case.scenarios.clear();
        let ActionOp::PublishBlob { path, bytes } = case.processes[0].actions[0].operation.clone()
        else {
            unreachable!();
        };
        case.processes[0].actions[0].operation = ActionOp::DescriptorWrite {
            path,
            length: Some(bytes.len() as u64),
            bytes,
            flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
            method: DescriptorWriteMethod::WriteFrom,
            finish: DescriptorFinish::Abort,
        };
        case.routes.clear();
        case.topology = TopologyFamily::Fixed;
        case.processes.truncate(1);
        let mut descriptor_observation = observation;
        descriptor_observation.executions.truncate(1);
        descriptor_observation.executions[0].results[0].action = "descriptor".to_owned();
        let mut ledger = ledger_for_case(&case);
        assert!(ledger.observe_case(&case, &descriptor_observation).is_err());
        assert!(ledger.assert_observed_closed().is_err());
        assert!(ledger.observed.is_empty());
        descriptor_observation.executions[0].results[0].descriptor =
            Some(DescriptorObservation::Write {
                method: DescriptorWriteMethod::WriteFrom,
                finish: DescriptorFinish::Close,
                terminal_results: vec![DescriptorTerminalResult::Ok],
                dropped: false,
                reservation_released: false,
            });
        assert!(ledger.observe_case(&case, &descriptor_observation).is_err());
        descriptor_observation.executions[0].results[0].descriptor =
            Some(DescriptorObservation::Write {
                method: DescriptorWriteMethod::WriteFrom,
                finish: DescriptorFinish::Abort,
                terminal_results: vec![DescriptorTerminalResult::Ok],
                dropped: false,
                reservation_released: false,
            });
        ledger.observe_case(&case, &descriptor_observation).unwrap();
        ledger.assert_observed_closed().unwrap();
    }

    #[test]
    fn modeled_error_does_not_credit_payload_that_was_not_transferred() {
        let (mut case, mut observation) = observed_blob_chain();
        case.topology = TopologyFamily::Fixed;
        case.routes.clear();
        case.processes.truncate(1);
        // EACCES is only a legal modeled outcome when the session's write
        // prefixes genuinely deny the publication path.
        case.processes[0].access.write_prefixes = vec!["/runs".to_owned()];
        case.processes[0].actions[0].expected = ExpectedOutcome::Error(libc::EACCES);
        observation.executions.truncate(1);
        let result = &mut observation.executions[0].results[0];
        result.outcome = "expected_error".to_owned();
        result.errno = Some(libc::EACCES);
        result.length = None;
        result.digest = None;
        let mut ledger = ledger_for_case(&case);
        ledger.observe_case(&case, &observation).unwrap();
        assert_eq!(
            ledger.observed,
            BTreeMap::from([(
                CoverageKey::Outcome {
                    class: "modeled_error".to_owned()
                },
                1
            )])
        );
        assert!(ledger.assert_observed_closed().is_err());
    }

    #[test]
    fn repeated_descriptor_terminal_requires_both_observed_attempts() {
        let (mut case, mut observation) = observed_blob_chain();
        case.topology = TopologyFamily::Fixed;
        case.routes.clear();
        case.processes.truncate(1);
        observation.executions.truncate(1);
        let ActionOp::PublishBlob { path, bytes } = case.processes[0].actions[0].operation.clone()
        else {
            unreachable!();
        };
        case.processes[0].actions[0] = crate::ir::Action::error(
            ActionOp::DescriptorWrite {
                path,
                length: Some(bytes.len() as u64),
                bytes: bytes.clone(),
                flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                method: DescriptorWriteMethod::Write,
                finish: DescriptorFinish::CloseTwice,
            },
            libc::EBADF,
        );
        let terminal_error = DescriptorTerminalResult::Error {
            errno: Some(libc::EBADF),
            error_type: "OSError".to_owned(),
        };
        let result = &mut observation.executions[0].results[0];
        result.action = "descriptor".to_owned();
        result.outcome = "expected_error".to_owned();
        result.errno = Some(libc::EBADF);
        result.error_type = Some("OSError".to_owned());
        result.length = None;
        result.digest = None;
        result.descriptor = Some(DescriptorObservation::Write {
            method: DescriptorWriteMethod::Write,
            finish: DescriptorFinish::CloseTwice,
            terminal_results: vec![DescriptorTerminalResult::Ok, terminal_error.clone()],
            dropped: false,
            reservation_released: false,
        });
        let mut without_bytes = ledger_for_case(&case);
        assert!(without_bytes.observe_case(&case, &observation).is_err());
        assert!(without_bytes.observed.is_empty());

        observation.executions[0].results[0].transfer = Some(crate::ir::TransferObservation {
            length: bytes.len(),
            digest: crate::codegen::digest(&bytes),
            complete: true,
        });
        let mut complete = ledger_for_case(&case);
        complete.observe_case(&case, &observation).unwrap();
        complete.assert_observed_closed().unwrap();

        for terminal_results in [
            vec![terminal_error.clone()],
            vec![terminal_error, DescriptorTerminalResult::Ok],
            vec![
                DescriptorTerminalResult::Ok,
                DescriptorTerminalResult::Error {
                    errno: Some(libc::ENOENT),
                    error_type: "OSError".to_owned(),
                },
            ],
        ] {
            let mut forged = observation.clone();
            forged.executions[0].results[0].descriptor = Some(DescriptorObservation::Write {
                method: DescriptorWriteMethod::Write,
                finish: DescriptorFinish::CloseTwice,
                terminal_results,
                dropped: false,
                reservation_released: false,
            });
            let mut ledger = ledger_for_case(&case);
            assert!(ledger.observe_case(&case, &forged).is_err());
            assert!(ledger.observed.is_empty());
            assert!(ledger.assert_observed_closed().is_err());
        }
    }

    #[test]
    fn legacy_planned_recount_ledgers_cannot_claim_observed_closure() {
        let (case, observation) = observed_blob_chain();
        let mut ledger = ledger_for_case(&case);
        ledger.observe_case(&case, &observation).unwrap();
        ledger.assert_observed_closed().unwrap();
        ledger.schema_version = 1;
        assert!(ledger.assert_observed_closed().is_err());
    }

    fn observed_stream_replacement(reader_first: bool) -> (BehaviorCase, CaseObservation) {
        let mut case = crate::corpus::stable_corpus(3, 11)
            .into_iter()
            .find(|case| case.id == "stream-replacement-11")
            .unwrap();
        case.scenarios.insert(CoverageScenario::QuiescentMutation);
        let (_, template) = observed_blob_chain();
        let mut observation = CaseObservation {
            case_id: case.id.clone(),
            executions: Vec::new(),
        };
        // Public revisions: first stream=20, first-complete=21, quiescent=22,
        // second stream=23. Reader-first creates 23 before the writer's lookup;
        // writer-first observes 20 before creating 23. Neither uses poll order.
        for program in &case.processes {
            let mut execution = template.executions[0].clone();
            execution.process = program.id.clone();
            execution.request_id = case.execution_request_id(program);
            execution.logical_node_id = program.logical_node_id;
            execution.results.clear();
            for (step, action) in program.actions.iter().enumerate() {
                let mut terminal = template.executions[0].results[0].clone();
                terminal.process = program.id.clone();
                terminal.step = step;
                terminal.action = action.operation.class().as_str().to_owned();
                terminal.path = action.operation.path().to_owned();
                terminal.length = None;
                terminal.digest = None;
                let payload = match &action.operation {
                    ActionOp::StreamWrite { chunks, .. } => Some(chunks.concat()),
                    ActionOp::StreamRead { expected, .. } => Some(expected.clone()),
                    ActionOp::PublishBlob { bytes, .. } => Some(bytes.clone()),
                    ActionOp::AwaitEntry { path, .. } => {
                        terminal.kind = Some("blob".to_owned());
                        terminal.revision = Some(if path.ends_with("-first-complete") {
                            21
                        } else {
                            22
                        });
                        None
                    }
                    ActionOp::WaitForQuiescent { .. } => {
                        terminal.kind = Some("stream".to_owned());
                        terminal.revision = Some(20);
                        terminal.active = Some(false);
                        None
                    }
                    _ => unreachable!(),
                };
                if let Some(payload) = &payload {
                    terminal.length = Some(payload.len());
                    terminal.digest = Some(crate::codegen::digest(payload));
                }
                if matches!(
                    action.operation,
                    ActionOp::StreamWrite { .. } | ActionOp::StreamRead { .. }
                ) {
                    let payload = payload.as_ref().unwrap();
                    let incarnation = if payload == b"first" { 20 } else { 23 };
                    terminal.incarnation = Some(incarnation);
                    let mut emit = |barrier| {
                        let mut record = terminal.clone();
                        record.outcome = "barrier".to_owned();
                        record.length = None;
                        record.digest = None;
                        record.incarnation = None;
                        record.barrier = Some(barrier);
                        execution.results.push(record);
                    };
                    emit(BarrierObservation::StreamOpened { incarnation });
                    emit(BarrierObservation::StreamFrame {
                        incarnation,
                        index: 0,
                        length: payload.len() as u64,
                        digest: crate::codegen::digest(payload),
                    });
                    if matches!(action.operation, ActionOp::StreamRead { .. }) {
                        emit(BarrierObservation::StreamFirstFrame { incarnation });
                        emit(BarrierObservation::StreamEof { incarnation });
                    } else if matches!(
                        action.operation,
                        ActionOp::StreamWrite { replace: true, .. }
                    ) {
                        emit(BarrierObservation::MutationApplied {
                            from_revision: Some(if reader_first { 23 } else { 20 }),
                            to_revision: Some(23),
                        });
                    }
                }
                execution.results.push(terminal);
            }
            observation.executions.push(execution);
        }
        (case, observation)
    }

    #[test]
    fn quiescent_replacement_closes_for_reader_and_writer_created_incarnations() {
        for reader_first in [true, false] {
            let (case, mut observation) = observed_stream_replacement(reader_first);
            for _ in 0..2 {
                let mut ledger = ledger_for_case(&case);
                ledger.observe_case(&case, &observation).unwrap();
                assert_eq!(
                    ledger.observed.get(&CoverageKey::Scenario {
                        scenario: CoverageScenario::QuiescentMutation,
                    }),
                    Some(&1)
                );
                ledger.assert_observed_closed().unwrap();
                observation.executions.reverse();
            }
        }
    }

    #[test]
    fn quiescent_replacement_rejects_unchanged_and_stale_identity() {
        let (case, observation) = observed_stream_replacement(true);
        for unchanged in [true, false] {
            let mut forged = observation.clone();
            for execution in &mut forged.executions {
                for result in &mut execution.results {
                    if unchanged {
                        if result.incarnation == Some(23) {
                            result.incarnation = Some(20);
                        }
                        match &mut result.barrier {
                            Some(
                                BarrierObservation::StreamOpened { incarnation }
                                | BarrierObservation::StreamFrame { incarnation, .. }
                                | BarrierObservation::StreamFirstFrame { incarnation }
                                | BarrierObservation::StreamEof { incarnation },
                            ) if *incarnation == 23 => *incarnation = 20,
                            Some(BarrierObservation::MutationApplied {
                                from_revision,
                                to_revision,
                            }) => {
                                *from_revision = Some(20);
                                *to_revision = Some(20);
                            }
                            _ => {}
                        }
                    } else if result.kind.as_deref() == Some("stream") {
                        // A revision older than the stream actually drained
                        // cannot stand in for the observed quiescent binding.
                        result.revision = Some(19);
                    }
                }
            }
            if unchanged {
                let index = ObservationIndex::new(&case, &forged).unwrap();
                let mut keys = BTreeSet::new();
                assert!(
                    !observe_scenario(
                        &case,
                        CoverageScenario::QuiescentMutation,
                        &index,
                        &mut keys,
                    )
                    .unwrap()
                );
                assert!(keys.is_empty());
            }
            let mut ledger = ledger_for_case(&case);
            assert!(ledger.observe_case(&case, &forged).is_err());
            assert!(ledger.observed.is_empty());
            assert!(ledger.completed_cases.is_empty());
        }
    }

    #[test]
    fn quiescent_replacement_does_not_credit_the_new_incarnations_eventual_eof() {
        let (mut case, mut observation) = observed_stream_replacement(true);
        let reader = case
            .processes
            .iter_mut()
            .find(|process| process.id == "replacement-reader")
            .unwrap();
        let probe = reader.actions.remove(2);
        reader.actions.push(probe);
        let execution = observation
            .executions
            .iter_mut()
            .find(|execution| execution.process == reader.id)
            .unwrap();
        let position = execution
            .results
            .iter()
            .position(|result| result.step == 2)
            .unwrap();
        let mut probe = execution.results.remove(position);
        for result in &mut execution.results {
            if result.step > 2 {
                result.step -= 1;
            }
        }
        probe.step = 4;
        probe.revision = Some(23);
        execution.results.push(probe);
        // The revised program is legal, but only observes quiescence after B.
        // Successful replacement I/O plus B/B is not old-to-new evidence.
        crate::oracle::BehaviorOracle::verify(&case, &observation).unwrap();
        let mut ledger = ledger_for_case(&case);
        assert!(ledger.observe_case(&case, &observation).is_err());
        assert!(ledger.observed.is_empty());
        assert!(ledger.completed_cases.is_empty());
    }

    fn observed_roundtrip() -> (BehaviorCase, CaseObservation) {
        let (mut case, mut observation) = observed_blob_chain();
        case.topology = TopologyFamily::Fixed;
        case.routes.clear();
        case.processes.truncate(1);
        observation.executions.truncate(1);
        let path = case.processes[0].actions[0].operation.path().to_owned();
        let chunks = vec![Vec::new(), b"a".to_vec(), Vec::new()];
        case.processes[0].actions[0] = crate::ir::Action::ok(ActionOp::StreamRoundTrip {
            path,
            chunks: chunks.clone(),
        });
        let terminal = &mut observation.executions[0].results[0];
        terminal.action = ActionClass::StreamWrite.as_str().to_owned();
        terminal.length = Some(1);
        terminal.digest = Some(crate::codegen::digest(b"a"));
        terminal.incarnation = Some(20);
        let terminal = terminal.clone();
        let mut records = Vec::new();
        let mut emit = |barrier, action: &str| {
            let mut record = terminal.clone();
            record.outcome = "barrier".to_owned();
            record.length = None;
            record.digest = None;
            record.action = action.to_owned();
            record.barrier = Some(barrier);
            records.push(record);
        };
        emit(
            BarrierObservation::StreamOpened { incarnation: 20 },
            "stream_write",
        );
        for direction in ["stream_write", "stream_read"] {
            for (index, chunk) in chunks.iter().enumerate() {
                emit(
                    BarrierObservation::StreamFrame {
                        incarnation: 20,
                        index: index as u64,
                        length: chunk.len() as u64,
                        digest: crate::codegen::digest(chunk),
                    },
                    direction,
                );
                if direction == "stream_read" && index == 0 {
                    emit(
                        BarrierObservation::StreamFirstFrame { incarnation: 20 },
                        "stream_write",
                    );
                }
            }
        }
        emit(
            BarrierObservation::StreamEof { incarnation: 20 },
            "stream_write",
        );
        records.push(terminal);
        observation.executions[0].results = records;
        (case, observation)
    }

    #[test]
    fn stream_milestones_do_not_manufacture_errors_or_action_adjacency() {
        let (case, observation) = observed_roundtrip();
        let mut ledger = ledger_for_case(&case);
        ledger.observe_case(&case, &observation).unwrap();
        ledger.assert_observed_closed().unwrap();
        assert!(!ledger.observed.contains_key(&CoverageKey::Outcome {
            class: "modeled_error".to_owned(),
        }));
        assert!(
            !ledger.observed.contains_key(&CoverageKey::ActionAdjacency {
                before: ActionClass::StreamWrite,
                after: ActionClass::StreamWrite,
            })
        );

        for corruption in 0..3 {
            let mut forged = observation.clone();
            let records = &mut forged.executions[0].results;
            match corruption {
                0 => records.insert(0, records[0].clone()),
                1 => records.swap(0, 1),
                _ => records.swap(2, 3),
            }
            let mut ledger = ledger_for_case(&case);
            assert!(ledger.observe_case(&case, &forged).is_err());
            assert!(ledger.completed_cases.is_empty());
            assert!(ledger.observed.is_empty());
        }
    }

    #[test]
    fn framing_corruption_never_commits_aggregate_only_coverage() {
        let (case, observation) = observed_roundtrip();
        for direction in ["stream_write", "stream_read"] {
            for corruption in 0..3 {
                let mut forged = observation.clone();
                let records = &mut forged.executions[0].results;
                let last = records
                    .iter()
                    .rposition(|record| {
                        record.action == direction
                            && matches!(
                                record.barrier,
                                Some(BarrierObservation::StreamFrame { .. })
                            )
                    })
                    .unwrap();
                match corruption {
                    0 => {
                        records.remove(last);
                    }
                    1 => {
                        let mut duplicate = records[last].clone();
                        if let Some(BarrierObservation::StreamFrame { index, .. }) =
                            &mut duplicate.barrier
                        {
                            *index += 1;
                        }
                        records.insert(last + 1, duplicate);
                    }
                    _ => {
                        if let Some(BarrierObservation::StreamFrame { digest, .. }) =
                            &mut records[last].barrier
                        {
                            *digest = crate::codegen::digest(b"corrupt empty frame");
                        }
                    }
                }
                let mut rejected = ledger_for_case(&case);
                assert!(rejected.observe_case(&case, &forged).is_err());
                assert!(rejected.completed_cases.is_empty());
                assert!(rejected.observed.is_empty());
            }
        }
    }

    #[test]
    fn dag_topology_counts_roles_not_revisited_physical_nodes() {
        let mut route = DataRoute {
            id: "roles".to_owned(),
            kind: DataKind::Stream,
            join_inputs: Vec::new(),
            edges: vec![
                crate::ir::DataEdge {
                    source: 1,
                    destination: 2,
                    source_role: "a".to_owned(),
                    destination_role: "b".to_owned(),
                    path: "/cases/roles/a-b".to_owned(),
                },
                crate::ir::DataEdge {
                    source: 2,
                    destination: 1,
                    source_role: "b".to_owned(),
                    destination_role: "c".to_owned(),
                    path: "/cases/roles/b-c".to_owned(),
                },
                crate::ir::DataEdge {
                    source: 1,
                    destination: 3,
                    source_role: "c".to_owned(),
                    destination_role: "d".to_owned(),
                    path: "/cases/roles/c-d".to_owned(),
                },
            ],
        };
        assert!(route_has_topology(&route, TopologyFamily::RandomDag));
        route.edges[1].destination_role = "a".to_owned();
        assert!(!route_has_topology(&route, TopologyFamily::RandomDag));
    }

    #[test]
    fn actual_consecutive_stream_actions_receive_adjacency_credit() {
        let (mut case, mut observation) = observed_roundtrip();
        let mut second = case.processes[0].actions[0].clone();
        let second_path = format!("{}-second", second.operation.path());
        let ActionOp::StreamRoundTrip { path, .. } = &mut second.operation else {
            unreachable!();
        };
        *path = second_path.clone();
        case.processes[0].actions.push(second);
        let mut second_records = observation.executions[0].results.clone();
        for record in &mut second_records {
            record.step = 1;
            record.path = second_path.clone();
            record.incarnation = Some(21);
            match &mut record.barrier {
                Some(
                    BarrierObservation::StreamOpened { incarnation }
                    | BarrierObservation::StreamFirstFrame { incarnation }
                    | BarrierObservation::StreamFrame { incarnation, .. }
                    | BarrierObservation::StreamEof { incarnation },
                ) => *incarnation = 21,
                _ => {}
            }
        }
        observation.executions[0].results.extend(second_records);
        let mut ledger = ledger_for_case(&case);
        ledger.observe_case(&case, &observation).unwrap();
        assert_eq!(
            ledger.observed.get(&CoverageKey::ActionAdjacency {
                before: ActionClass::StreamWrite,
                after: ActionClass::StreamWrite,
            }),
            Some(&1)
        );
        assert!(!ledger.observed.contains_key(&CoverageKey::Outcome {
            class: "modeled_error".to_owned(),
        }));
        // One remaining EOF cannot cover another completed reader.
        for missing_step in 0..2 {
            let mut incomplete = observation.clone();
            incomplete.executions[0].results.retain(|record| {
                record.step != missing_step
                    || !matches!(record.barrier, Some(BarrierObservation::StreamEof { .. }))
            });
            let mut rejected = ledger_for_case(&case);
            assert!(rejected.observe_case(&case, &incomplete).is_err());
            assert!(rejected.completed_cases.is_empty());
            assert!(rejected.observed.is_empty());
        }
    }

    #[test]
    fn unrelated_terminal_reader_and_writer_do_not_receive_relay_credit() {
        let (mut case, mut observation) = observed_blob_chain();
        let relay = CoverageKey::NodeRole {
            node: 2,
            kind: DataKind::Blob,
            role: NodeRole::Relay,
        };
        let mut valid = ledger_for_case(&case);
        valid.observe_case(&case, &observation).unwrap();
        assert_eq!(valid.observed.get(&relay), Some(&1));

        let root_payload = match &case.processes[0].actions[0].operation {
            ActionOp::PublishBlob { bytes, .. } => bytes.clone(),
            _ => unreachable!(),
        };
        let mut writer = case.processes[1].clone();
        writer.id = "unrelated-writer".to_owned();
        writer.access = crate::ir::AccessSpec::unrestricted("unrelated-writer");
        writer.actions = vec![case.processes[1].actions.remove(1)];
        match &mut writer.actions[0].operation {
            ActionOp::PublishBlob { bytes, .. } => *bytes = root_payload.clone(),
            _ => unreachable!(),
        }
        match &mut case.processes[2].actions[0].operation {
            ActionOp::ReadBlob { expected, .. } => *expected = root_payload.clone(),
            _ => unreachable!(),
        }
        let sink = &case.processes[2];
        observation.executions[2].request_id = case.execution_request_id(sink);
        observation.executions[2].results[0].length = Some(root_payload.len());
        observation.executions[2].results[0].digest = Some(crate::codegen::digest(&root_payload));
        let mut writer_execution = observation.executions[1].clone();
        writer_execution.process = writer.id.clone();
        writer_execution.request_id = case.execution_request_id(&writer);
        let mut write = observation.executions[1].results.remove(1);
        write.process = writer.id.clone();
        write.step = 0;
        write.length = Some(root_payload.len());
        write.digest = Some(crate::codegen::digest(&root_payload));
        writer_execution.results = vec![write];
        case.topology = TopologyFamily::Fixed;
        case.routes[0].edges[1].source_role = writer.id.clone();
        case.processes.push(writer);
        observation.executions.push(writer_execution);
        // Swapping global polling order does not invent process-local causality.
        observation.executions.reverse();
        let mut split = ledger_for_case(&case);
        split.observe_case(&case, &observation).unwrap();
        assert!(!split.observed.contains_key(&relay));
        split.assert_observed_closed().unwrap();
    }

    #[test]
    fn persisted_coverage_requires_the_exact_live_plan_build_deployment_and_fixture() {
        let (case, observation) = observed_blob_chain();
        let expected = ledger_for_case(&case);
        let mut completed = expected.clone();
        completed.observe_case(&case, &observation).unwrap();
        let encoded = serde_json::to_vec(&completed).unwrap();
        let decode = || serde_json::from_slice::<CoverageLedger>(&encoded).unwrap();
        let mut resumed = decode();
        assert!(resumed.assert_observed_closed().is_err());
        resumed.validate_resume(&expected).unwrap();
        resumed.assert_observed_closed().unwrap();

        for changed in 0..4 {
            let mut next = expected.clone();
            let identity = next.identity.as_mut().unwrap();
            match changed {
                0 => identity.deployment_generation = "redeployment-b".to_owned(),
                1 => identity.fixture_mapping_digest = crate::codegen::digest(b"recreated fixture"),
                2 => identity.artifacts_digest = crate::codegen::digest(b"different binaries"),
                _ => identity.plan_digest = crate::codegen::digest(b"different immutable plan"),
            }
            let mut stale = decode();
            assert!(stale.validate_resume(&next).is_err());
            assert!(stale.assert_observed_closed().is_err());
            assert!(!next.completed_cases.contains(&case.id));
            next.observe_case(&case, &observation).unwrap();
        }
    }

    #[test]
    fn corrupt_completion_ids_counts_and_attempt_identity_cannot_resume() {
        let (case, observation) = observed_blob_chain();
        let expected = ledger_for_case(&case);
        let mut completed = expected.clone();
        completed.observe_case(&case, &observation).unwrap();
        let corruptions: &[fn(&mut CoverageLedger)] = &[
            |ledger| {
                ledger.completed_cases.insert("unplanned".to_owned());
            },
            |ledger| {
                *ledger.observed.values_mut().next().unwrap() += 1;
            },
            |ledger| {
                ledger.completed_evidence.clear();
            },
            |ledger| {
                let evidence = ledger.completed_evidence.values_mut().next().unwrap();
                *evidence.execution_ids.values_mut().next().unwrap() = "another-attempt".to_owned();
            },
            |ledger| {
                ledger
                    .completed_evidence
                    .values_mut()
                    .next()
                    .unwrap()
                    .identity_digest = crate::codegen::digest(b"another deployment");
            },
            |ledger| {
                let evidence = ledger.completed_evidence.values_mut().next().unwrap();
                *evidence.request_ids.values_mut().next().unwrap() = String::new();
            },
        ];
        for corrupt in corruptions {
            let mut forged: CoverageLedger =
                serde_json::from_slice(&serde_json::to_vec(&completed).unwrap()).unwrap();
            corrupt(&mut forged);
            assert!(forged.validate_resume(&expected).is_err());
            assert!(forged.assert_observed_closed().is_err());
        }
    }

    #[test]
    fn completion_proves_exact_template_to_attempt_transformation() {
        let (case, mut observation) = observed_blob_chain();
        let executed = case.for_attempt(7, true);
        for (execution, program) in observation.executions.iter_mut().zip(&executed.processes) {
            execution.request_id = executed.execution_request_id(program);
            for (result, action) in execution.results.iter_mut().zip(&program.actions) {
                result.path = action.operation.path().to_owned();
            }
        }
        let expected = ledger_for_case(&case);
        let mut ledger = expected.clone();
        let budget = crate::budget::Budget::new(std::time::Duration::from_secs(30));
        let mut changed = executed.clone();
        changed.processes[0]
            .access
            .execution_id
            .push_str("-not-the-attempt");
        assert!(
            ledger
                .observe_attempt(&case, &changed, &observation, 7, true, &budget)
                .is_err()
        );
        ledger
            .observe_attempt(&case, &executed, &observation, 7, true, &budget)
            .unwrap();
        let mut restored: CoverageLedger =
            serde_json::from_slice(&serde_json::to_vec(&ledger).unwrap()).unwrap();
        restored.validate_resume(&expected).unwrap();
        restored.assert_observed_closed().unwrap();
    }

    #[test]
    fn authorization_requires_allowed_and_denied_results_in_the_same_restricted_process() {
        let (mut case, mut observation) = observed_blob_chain();
        case.topology = TopologyFamily::Fixed;
        case.routes.clear();
        case.processes.truncate(1);
        observation.executions.truncate(1);
        case.scenarios.insert(CoverageScenario::Authorization);
        let allowed = case.processes[0].actions[0].operation.path().to_owned();
        case.processes[0].access.write_prefixes = vec![allowed];
        let denied_path = "/cases/observed-chain/denied".to_owned();
        case.processes[0].actions.push(crate::ir::Action::error(
            ActionOp::PublishBlob {
                path: denied_path.clone(),
                bytes: b"denied".to_vec(),
            },
            libc::EACCES,
        ));
        let mut denied = observation.executions[0].results[0].clone();
        denied.step = 1;
        denied.path = denied_path;
        denied.outcome = "expected_error".to_owned();
        denied.errno = Some(libc::EACCES);
        denied.length = None;
        denied.digest = None;
        observation.executions[0].results.push(denied);
        let mut complete = ledger_for_case(&case);
        complete.observe_case(&case, &observation).unwrap();
        assert_eq!(
            complete.observed.get(&CoverageKey::Scenario {
                scenario: CoverageScenario::Authorization,
            }),
            Some(&1)
        );

        for missing in 0..2 {
            let mut incomplete_case = case.clone();
            let mut incomplete_observation = observation.clone();
            incomplete_case.processes[0].actions.remove(missing);
            incomplete_observation.executions[0].results.remove(missing);
            incomplete_observation.executions[0].results[0].step = 0;
            let mut incomplete = ledger_for_case(&incomplete_case);
            assert!(
                incomplete
                    .observe_case(&incomplete_case, &incomplete_observation)
                    .is_err()
            );
        }

        let mut unrelated = case.processes[0].clone();
        unrelated.id = "unrelated-allowed".to_owned();
        unrelated.access = crate::ir::AccessSpec::unrestricted("unrelated-allowed");
        unrelated.actions = vec![case.processes[0].actions.remove(0)];
        let mut allowed_execution = observation.executions[0].clone();
        allowed_execution.process = unrelated.id.clone();
        allowed_execution.request_id = case.execution_request_id(&unrelated);
        let mut allowed_result = observation.executions[0].results.remove(0);
        allowed_result.process = unrelated.id.clone();
        allowed_execution.results = vec![allowed_result];
        observation.executions[0].results[0].step = 0;
        case.processes.push(unrelated);
        observation.executions.push(allowed_execution);
        let mut split = ledger_for_case(&case);
        assert!(split.observe_case(&case, &observation).is_err());
        assert!(split.observed.is_empty());
    }

    #[test]
    fn reader_drop_credits_release_without_an_invented_terminal_call() {
        let (mut case, mut observation) = observed_blob_chain();
        case.topology = TopologyFamily::Fixed;
        case.routes.clear();
        case.processes.truncate(2);
        observation.executions.truncate(2);
        case.processes[1].actions.truncate(1);
        observation.executions[1].results.truncate(1);
        let ActionOp::ReadBlob { path, expected } = case.processes[1].actions[0].operation.clone()
        else {
            unreachable!();
        };
        case.processes[1].actions[0].operation = ActionOp::DescriptorRead {
            path,
            expected,
            flags: libc::O_RDONLY,
            offset: 0,
            method: DescriptorReadMethod::Read,
            finish: DescriptorFinish::Drop,
        };
        let result = &mut observation.executions[1].results[0];
        result.action = ActionClass::Descriptor.as_str().to_owned();
        result.descriptor = Some(DescriptorObservation::Read {
            method: DescriptorReadMethod::Read,
            finish: DescriptorFinish::Drop,
            terminal_results: Vec::new(),
        });
        let mut ledger = ledger_for_case(&case);
        ledger.observe_case(&case, &observation).unwrap();
        assert_eq!(
            ledger.observed.get(&CoverageKey::DescriptorTerminal {
                direction: "read".to_owned(),
                finish: "drop".to_owned(),
            }),
            Some(&1)
        );

        let Some(DescriptorObservation::Read {
            terminal_results, ..
        }) = &mut observation.executions[1].results[0].descriptor
        else {
            unreachable!();
        };
        terminal_results.push(DescriptorTerminalResult::Ok);
        let mut forged = ledger_for_case(&case);
        assert!(forged.observe_case(&case, &observation).is_err());
        assert!(forged.observed.is_empty());
    }
}
