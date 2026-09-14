//! Versioned campaign planning and durable artifact contracts.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::budget::Budget;
use crate::corpus::{
    SEEDED_MODEL_PATH, generated_campaign_cases, generated_campaign_cases_budgeted,
};
use crate::coverage::CoverageLedger;
use crate::harness::ClusterHarness;
use crate::ir::{
    Action, ActionOp, AttemptPaths, BehaviorCase, CASE_SCHEMA_VERSION, CaseResources,
    GENERATOR_VERSION, validate_artifact_id,
};

pub const CAMPAIGN_SCHEMA_VERSION: u32 = 2;
pub const ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub const INITIAL_NODE_COUNT: usize = 5;
pub const NORMAL_CASE_COUNT: usize = 128;
pub const RECOVERY_CASE_COUNT: usize = 16;
pub const SURVIVOR_CASE_COUNT: usize = 32;
pub const PREPARATION_DEADLINE_SECS: u64 = 10 * 60;
pub const CLEANUP_DEADLINE_SECS: u64 = provisioning::PAID_CLEANUP_RESERVE_MS / 1_000;
pub const CAMPAIGN_DEADLINE_SECS: u64 = 5 * 60;
pub const WORKLOAD_DEADLINE_SECS: u64 = 285;
pub const DIAGNOSTIC_DEADLINE_SECS: u64 = 10;
pub const LOCAL_CLEANUP_RESERVE_SECS: u64 = 5;

const MAX_CAMPAIGN_PAYLOAD_BYTES: u64 = 512 * 1024 * 1024;
const MAX_CAMPAIGN_ALLOCATION_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMode {
    Mock,
    Real,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CampaignLimits {
    pub fixture_lifetime_secs: u64,
    pub total_cost_usd: f64,
    pub total_hourly_price_usd: f64,
    pub case_deadline_secs: u64,
    pub max_campaign_payload_bytes: u64,
    pub max_campaign_allocation_bytes: u64,
    pub max_case_race_states: u32,
}

impl CampaignLimits {
    pub fn validate(&self, paid: bool) -> Result<(), String> {
        if self.fixture_lifetime_secs == 0
            || self.case_deadline_secs == 0
            || self.max_campaign_payload_bytes == 0
            || self.max_campaign_allocation_bytes == 0
            || self.max_case_race_states == 0
        {
            return Err(
                "campaign lifetime, deadlines, and resource bounds must be nonzero".to_owned(),
            );
        }
        if self.max_campaign_payload_bytes > MAX_CAMPAIGN_PAYLOAD_BYTES
            || self.max_campaign_allocation_bytes > MAX_CAMPAIGN_ALLOCATION_BYTES
        {
            return Err(
                "campaign bounds exceed the 512 MiB payload or 8 GiB allocation ceiling".to_owned(),
            );
        }
        for (name, value) in [
            ("total_cost_usd", self.total_cost_usd),
            ("total_hourly_price_usd", self.total_hourly_price_usd),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(format!("{name} must be finite and non-negative"));
            }
            if paid && value == 0.0 {
                return Err(format!(
                    "{name} must be an explicit positive paid-run ceiling"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OfferPolicy {
    pub gpu_model: Option<String>,
    pub min_gpu_ram_mb: Option<u64>,
    pub min_compute_cap: Option<u64>,
    pub min_reliability: Option<f64>,
    pub min_download_mbps: Option<f64>,
    pub min_upload_mbps: Option<f64>,
    pub max_hourly_price_per_node: Option<f64>,
    pub blacklist_hosts: Vec<u64>,
}

impl OfferPolicy {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("min_reliability", self.min_reliability),
            ("min_download_mbps", self.min_download_mbps),
            ("min_upload_mbps", self.min_upload_mbps),
            ("max_hourly_price_per_node", self.max_hourly_price_per_node),
        ] {
            if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
                return Err(format!("{name} must be finite and non-negative"));
            }
        }
        if self.min_reliability.is_some_and(|value| value > 1.0) {
            return Err("min_reliability must not exceed one".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CampaignConfig {
    pub provider: ProviderMode,
    pub seed: u64,
    pub runtime_image: String,
    pub limits: CampaignLimits,
    pub offers: OfferPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PersistedFixtureEntry {
    Blob { path: String, expected: Vec<u8> },
    QuiescentStream { path: String },
}

impl PersistedFixtureEntry {
    pub fn path(&self) -> &str {
        match self {
            Self::Blob { path, .. } | Self::QuiescentStream { path } => path,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCase {
    pub id: String,
    pub pre_restart: BehaviorCase,
    pub post_restart: BehaviorCase,
    pub persisted_entries: Vec<PersistedFixtureEntry>,
}

impl RecoveryCase {
    pub fn validate(&self) -> Result<(), String> {
        validate_artifact_id(&self.id, "recovery")?;
        self.pre_restart.validate()?;
        self.post_restart.validate()?;
        if self.pre_restart.id == self.post_restart.id
            || self.pre_restart.live_nodes != self.post_restart.live_nodes
        {
            return Err(
                "recovery segments require distinct identities and the same exact live set"
                    .to_owned(),
            );
        }
        let owned = self.pre_restart.owned_paths();
        let mut persisted = BTreeSet::new();
        let mut blob = false;
        let mut stream = false;
        for entry in &self.persisted_entries {
            if !persisted.insert(entry.path())
                || !owned.contains(entry.path())
                || !self
                    .post_restart
                    .read_only_fixture_paths
                    .contains(entry.path())
            {
                return Err("persisted entries must be unique pre-restart-owned, post-restart read-only paths".to_owned());
            }
            match entry {
                PersistedFixtureEntry::Blob { .. } => blob = true,
                PersistedFixtureEntry::QuiescentStream { .. } => stream = true,
            }
        }
        if !blob || !stream {
            return Err(
                "recovery must identify both persisted blob and quiescent stream entries"
                    .to_owned(),
            );
        }
        Ok(())
    }

    pub(crate) fn for_attempt(&self, attempt: u64) -> Self {
        let paths = AttemptPaths::new(&[&self.pre_restart, &self.post_restart], attempt);
        let mut scoped = Self {
            id: self.id.clone(),
            pre_restart: self.pre_restart.with_attempt_paths(attempt, Some(&paths)),
            post_restart: self.post_restart.with_attempt_paths(attempt, Some(&paths)),
            persisted_entries: self.persisted_entries.clone(),
        };
        for entry in &mut scoped.persisted_entries {
            match entry {
                PersistedFixtureEntry::Blob { path, .. }
                | PersistedFixtureEntry::QuiescentStream { path } => {
                    *path = paths.map(path);
                }
            }
        }
        scoped
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CampaignResourcePlan {
    pub case_count: usize,
    pub workload_segment_count: usize,
    pub regression_count: usize,
    pub readiness_gate_count: usize,
    pub recovery_transition_count: usize,
    pub destructive_transition_count: usize,
    pub recovery_namespace_snapshot_count: usize,
    pub auxiliary_probes_share_workload_budget: bool,
    pub workload_budget_secs: u64,
    pub diagnostic_budget_secs: u64,
    pub fixture_cleanup_budget_secs: u64,
    pub action_count: u64,
    pub process_count: u64,
    pub payload_bytes: u64,
    pub allocation_bytes: u64,
    pub observation_count: u64,
    pub planned_runtime_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CampaignPlan {
    pub schema_version: u32,
    pub case_schema_version: u32,
    pub generator_version: u32,
    pub campaign_id: String,
    pub seed: u64,
    pub expected_initial_nodes: BTreeSet<u64>,
    pub first_survivors: BTreeSet<u64>,
    pub second_survivors: BTreeSet<u64>,
    pub regressions: Vec<BehaviorCase>,
    pub normal: Vec<BehaviorCase>,
    pub recovery: Vec<RecoveryCase>,
    pub four_node: Vec<BehaviorCase>,
    pub three_node: Vec<BehaviorCase>,
    pub normal_coverage: CoverageLedger,
    pub four_node_coverage: CoverageLedger,
    pub three_node_coverage: CoverageLedger,
    pub resources: CampaignResourcePlan,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactEnvelope<T> {
    pub artifact_schema_version: u32,
    pub kind: String,
    pub campaign_id: String,
    pub written_unix_ms: u64,
    pub payload: T,
}

impl<T> ArtifactEnvelope<T> {
    pub fn new(
        kind: impl Into<String>,
        campaign_id: impl Into<String>,
        payload: T,
    ) -> Result<Self, String> {
        let campaign_id = campaign_id.into();
        validate_artifact_id(&campaign_id, "campaign")?;
        let written_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock precedes epoch: {error}"))?
            .as_millis()
            .try_into()
            .map_err(|_| "artifact timestamp exceeded u64".to_owned())?;
        Ok(Self {
            artifact_schema_version: ARTIFACT_SCHEMA_VERSION,
            kind: kind.into(),
            campaign_id,
            written_unix_ms,
            payload,
        })
    }

    pub fn validate(&self, expected_kind: &str) -> Result<(), String> {
        if self.artifact_schema_version != ARTIFACT_SCHEMA_VERSION {
            return Err(format!(
                "artifact schema {} is incompatible with supported schema {ARTIFACT_SCHEMA_VERSION}",
                self.artifact_schema_version
            ));
        }
        if self.kind != expected_kind {
            return Err(format!(
                "artifact kind {:?} does not match expected {expected_kind:?}",
                self.kind
            ));
        }
        validate_artifact_id(&self.campaign_id, "campaign")?;
        Ok(())
    }
}

impl CampaignPlan {
    pub fn build(config: &CampaignConfig, regressions: Vec<BehaviorCase>) -> Result<Self, String> {
        Self::build_inner(config, regressions, None)
    }

    pub fn build_with_budget(
        config: &CampaignConfig,
        regressions: Vec<BehaviorCase>,
        budget: &Budget,
    ) -> Result<Self, String> {
        Self::build_inner(config, regressions, Some(budget))
    }

    fn build_inner(
        config: &CampaignConfig,
        regressions: Vec<BehaviorCase>,
        budget: Option<&Budget>,
    ) -> Result<Self, String> {
        if let Some(budget) = budget {
            budget.check("campaign plan generation and admission")?;
        }
        config
            .limits
            .validate(config.provider == ProviderMode::Real)?;
        config.offers.validate()?;
        if config.runtime_image.trim().is_empty() {
            return Err("runtime image must not be empty".to_owned());
        }

        let expected_initial_nodes = BTreeSet::from([1, 2, 3, 4, 5]);
        let first_survivors = BTreeSet::from([1, 2, 3, 4]);
        let second_survivors = BTreeSet::from([1, 2, 3]);
        let campaign_id = format!("vastai-e2e-{:016x}", config.seed);

        let normal = generate_exact_cases(
            config.seed.rotate_left(7),
            NORMAL_CASE_COUNT,
            &expected_initial_nodes,
            "normal",
            config.limits.max_case_race_states,
            budget,
        )?;
        let recovery = generate_recovery_cases(
            config.seed.rotate_left(13),
            &expected_initial_nodes,
            config.limits.max_case_race_states,
            budget,
        )?;
        let four_node = generate_exact_cases(
            config.seed.rotate_left(19),
            SURVIVOR_CASE_COUNT,
            &first_survivors,
            "four-node",
            config.limits.max_case_race_states,
            budget,
        )?;
        let three_node = generate_exact_cases(
            config.seed.rotate_left(29),
            SURVIVOR_CASE_COUNT,
            &second_survivors,
            "three-node",
            config.limits.max_case_race_states,
            budget,
        )?;

        for regression in &regressions {
            if let Some(budget) = budget {
                budget.check("validate compatible persisted regression")?;
            }
            regression.validate()?;
            if regression.live_nodes != expected_initial_nodes {
                return Err(format!(
                    "regression {} requires incompatible live set {:?}",
                    regression.id, regression.live_nodes
                ));
            }
        }
        let normal_coverage = planned_coverage(&normal, &expected_initial_nodes, true, budget)?;
        let four_node_coverage = planned_coverage(&four_node, &first_survivors, false, budget)?;
        let three_node_coverage = planned_coverage(&three_node, &second_survivors, false, budget)?;

        let every_case = regressions
            .iter()
            .chain(&normal)
            .chain(
                recovery
                    .iter()
                    .flat_map(|case| [&case.pre_restart, &case.post_restart]),
            )
            .chain(&four_node)
            .chain(&three_node);
        let totals = campaign_resources(every_case)?.checked_add(auxiliary_campaign_resources(
            &expected_initial_nodes,
            &recovery,
        )?)?;
        let CaseResources {
            action_count,
            process_count,
            payload_bytes,
            allocation_bytes,
            observation_count,
        } = totals;
        if payload_bytes > config.limits.max_campaign_payload_bytes {
            return Err(format!(
                "planned campaign payload {payload_bytes} exceeds bound {}",
                config.limits.max_campaign_payload_bytes
            ));
        }
        if allocation_bytes > config.limits.max_campaign_allocation_bytes {
            return Err(format!(
                "planned campaign allocation {allocation_bytes} exceeds bound {}",
                config.limits.max_campaign_allocation_bytes
            ));
        }
        let workload_cases = NORMAL_CASE_COUNT
            + RECOVERY_CASE_COUNT
            + SURVIVOR_CASE_COUNT
            + SURVIVOR_CASE_COUNT
            + regressions.len();
        // Segments, probes, transitions and diagnosis share enforced parent
        // deadlines. A per-operation ceiling is not a charge per successful case.
        let workload_segment_count = workload_cases + RECOVERY_CASE_COUNT;
        let fixture_cleanup_budget_secs = if config.provider == ProviderMode::Real {
            CLEANUP_DEADLINE_SECS
        } else {
            LOCAL_CLEANUP_RESERVE_SECS
        };
        let planned_runtime_secs = if config.provider == ProviderMode::Real {
            PREPARATION_DEADLINE_SECS + CAMPAIGN_DEADLINE_SECS + CLEANUP_DEADLINE_SECS
        } else {
            CAMPAIGN_DEADLINE_SECS
        };
        if planned_runtime_secs > config.limits.fixture_lifetime_secs {
            return Err(format!(
                "planned campaign requires {planned_runtime_secs}s but fixture lifetime ceiling is {}s",
                config.limits.fixture_lifetime_secs
            ));
        }

        Ok(Self {
            schema_version: CAMPAIGN_SCHEMA_VERSION,
            case_schema_version: CASE_SCHEMA_VERSION,
            generator_version: GENERATOR_VERSION,
            campaign_id,
            seed: config.seed,
            expected_initial_nodes,
            first_survivors,
            second_survivors,
            regressions,
            normal,
            recovery,
            four_node,
            three_node,
            normal_coverage,
            four_node_coverage,
            three_node_coverage,
            resources: CampaignResourcePlan {
                case_count: workload_cases,
                workload_segment_count,
                regression_count: workload_cases
                    - NORMAL_CASE_COUNT
                    - RECOVERY_CASE_COUNT
                    - 2 * SURVIVOR_CASE_COUNT,
                readiness_gate_count: 1,
                recovery_transition_count: RECOVERY_CASE_COUNT,
                destructive_transition_count: 2,
                recovery_namespace_snapshot_count: 2 * RECOVERY_CASE_COUNT,
                auxiliary_probes_share_workload_budget: true,
                workload_budget_secs: WORKLOAD_DEADLINE_SECS,
                diagnostic_budget_secs: DIAGNOSTIC_DEADLINE_SECS,
                fixture_cleanup_budget_secs,
                action_count,
                process_count,
                payload_bytes,
                allocation_bytes,
                observation_count,
                planned_runtime_secs,
            },
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_artifact_id(&self.campaign_id, "campaign")?;
        if self.schema_version != CAMPAIGN_SCHEMA_VERSION
            || self.case_schema_version != CASE_SCHEMA_VERSION
            || self.generator_version != GENERATOR_VERSION
        {
            return Err("campaign schema or generator version is incompatible".to_owned());
        }
        if self.expected_initial_nodes.len() != INITIAL_NODE_COUNT
            || self.normal.len() != NORMAL_CASE_COUNT
            || self.recovery.len() != RECOVERY_CASE_COUNT
            || self.four_node.len() != SURVIVOR_CASE_COUNT
            || self.three_node.len() != SURVIVOR_CASE_COUNT
        {
            return Err("campaign shape differs from the fixed VastAI E2E contract".to_owned());
        }
        if self.first_survivors.len() != INITIAL_NODE_COUNT - 1
            || self.second_survivors.len() != INITIAL_NODE_COUNT - 2
            || !self.first_survivors.is_subset(&self.expected_initial_nodes)
            || !self.second_survivors.is_subset(&self.first_survivors)
        {
            return Err(
                "each destructive transition must remove exactly one existing node".to_owned(),
            );
        }
        let logical_count = NORMAL_CASE_COUNT
            + RECOVERY_CASE_COUNT
            + 2 * SURVIVOR_CASE_COUNT
            + self.regressions.len();
        if self.resources.case_count != logical_count
            || self.resources.workload_segment_count != logical_count + RECOVERY_CASE_COUNT
            || self.resources.regression_count != self.regressions.len()
            || self.resources.readiness_gate_count != 1
            || self.resources.recovery_transition_count != RECOVERY_CASE_COUNT
            || self.resources.destructive_transition_count != 2
            || self.resources.recovery_namespace_snapshot_count != 2 * RECOVERY_CASE_COUNT
            || !self.resources.auxiliary_probes_share_workload_budget
            || self.resources.workload_budget_secs != WORKLOAD_DEADLINE_SECS
            || self.resources.diagnostic_budget_secs != DIAGNOSTIC_DEADLINE_SECS
        {
            return Err(
                "campaign resource accounting differs from bounded phase contract".to_owned(),
            );
        }
        validate_phase(&self.normal, &self.expected_initial_nodes)?;
        validate_phase(&self.regressions, &self.expected_initial_nodes)?;
        validate_phase(&self.four_node, &self.first_survivors)?;
        validate_phase(&self.three_node, &self.second_survivors)?;
        for (cases, nodes, full, ledger) in [
            (
                &self.normal,
                &self.expected_initial_nodes,
                true,
                &self.normal_coverage,
            ),
            (
                &self.four_node,
                &self.first_survivors,
                false,
                &self.four_node_coverage,
            ),
            (
                &self.three_node,
                &self.second_survivors,
                false,
                &self.three_node_coverage,
            ),
        ] {
            ledger.validate_plan(&planned_coverage(cases, nodes, full, None)?)?;
        }
        for recovery in &self.recovery {
            recovery.validate()?;
            if recovery.pre_restart.live_nodes != self.expected_initial_nodes
                || recovery.post_restart.live_nodes != self.expected_initial_nodes
            {
                return Err(format!(
                    "recovery case {} has an incompatible live set",
                    recovery.id
                ));
            }
        }
        let totals = campaign_resources(
            self.regressions
                .iter()
                .chain(&self.normal)
                .chain(
                    self.recovery
                        .iter()
                        .flat_map(|case| [&case.pre_restart, &case.post_restart]),
                )
                .chain(&self.four_node)
                .chain(&self.three_node),
        )?
        .checked_add(auxiliary_campaign_resources(
            &self.expected_initial_nodes,
            &self.recovery,
        )?)?;
        if self.resources.action_count != totals.action_count
            || self.resources.process_count != totals.process_count
            || self.resources.payload_bytes != totals.payload_bytes
            || self.resources.allocation_bytes != totals.allocation_bytes
            || self.resources.observation_count != totals.observation_count
        {
            return Err("campaign resource totals differ from checked case resources".to_owned());
        }
        if totals.payload_bytes > MAX_CAMPAIGN_PAYLOAD_BYTES
            || totals.allocation_bytes > MAX_CAMPAIGN_ALLOCATION_BYTES
        {
            return Err("campaign resources exceed the hard admission ceilings".to_owned());
        }
        Ok(())
    }

    pub fn admit_selected_hourly_cost(
        &self,
        limits: &CampaignLimits,
        selected_hourly_cost: f64,
    ) -> Result<(), String> {
        if !selected_hourly_cost.is_finite() || selected_hourly_cost < 0.0 {
            return Err("selected hourly cost is invalid".to_owned());
        }
        if selected_hourly_cost > limits.total_hourly_price_usd {
            return Err(format!(
                "selected hourly cost ${selected_hourly_cost:.6} exceeds ceiling ${:.6}",
                limits.total_hourly_price_usd
            ));
        }
        let worst_case = selected_hourly_cost * limits.fixture_lifetime_secs as f64 / 3_600.0;
        if worst_case > limits.total_cost_usd {
            return Err(format!(
                "selected worst-case cost ${worst_case:.6} exceeds ceiling ${:.6}",
                limits.total_cost_usd
            ));
        }
        Ok(())
    }

    /// Builds a structurally consistent over-budget plan for the scripted
    /// pre-provider admission check. The resulting plan is deliberately
    /// invalid and must be rejected by [`Self::validate`].
    #[doc(hidden)]
    pub fn inject_scripted_resource_overflow(&mut self) -> Result<(), String> {
        if [
            &self.normal_coverage,
            &self.four_node_coverage,
            &self.three_node_coverage,
        ]
        .into_iter()
        .any(|ledger| ledger.identity().is_some())
        {
            return Err(
                "scripted resource overflow must be injected before coverage is bound".to_owned(),
            );
        }
        for case in self
            .normal
            .iter_mut()
            .chain(&mut self.four_node)
            .chain(&mut self.three_node)
        {
            saturate_case_allocation(case)?;
        }
        for recovery in &mut self.recovery {
            saturate_case_allocation(&mut recovery.pre_restart)?;
            saturate_case_allocation(&mut recovery.post_restart)?;
        }

        self.normal_coverage =
            planned_coverage(&self.normal, &self.expected_initial_nodes, true, None)?;
        self.four_node_coverage =
            planned_coverage(&self.four_node, &self.first_survivors, false, None)?;
        self.three_node_coverage =
            planned_coverage(&self.three_node, &self.second_survivors, false, None)?;

        let totals = campaign_resources(
            self.regressions
                .iter()
                .chain(&self.normal)
                .chain(
                    self.recovery
                        .iter()
                        .flat_map(|case| [&case.pre_restart, &case.post_restart]),
                )
                .chain(&self.four_node)
                .chain(&self.three_node),
        )?
        .checked_add(auxiliary_campaign_resources(
            &self.expected_initial_nodes,
            &self.recovery,
        )?)?;
        self.resources.action_count = totals.action_count;
        self.resources.process_count = totals.process_count;
        self.resources.payload_bytes = totals.payload_bytes;
        self.resources.allocation_bytes = totals.allocation_bytes;
        self.resources.observation_count = totals.observation_count;
        if totals.allocation_bytes <= MAX_CAMPAIGN_ALLOCATION_BYTES {
            return Err(
                "scripted resource overflow could not exceed the campaign allocation ceiling"
                    .to_owned(),
            );
        }
        Ok(())
    }
}
const MAX_CASE_ALLOCATION_BYTES: u64 = 64 * 1024 * 1024;

fn saturate_case_allocation(case: &mut BehaviorCase) -> Result<(), String> {
    let resources = case.resource_summary()?;
    let ceiling = case
        .resource_bounds
        .max_allocation_bytes
        .min(MAX_CASE_ALLOCATION_BYTES);
    let additional = ceiling.saturating_sub(resources.allocation_bytes);
    if additional == 0 {
        return Ok(());
    }
    let action = Action::exception(
        ActionOp::MappingExportClose {
            path: SEEDED_MODEL_PATH.to_owned(),
            length: additional,
        },
        crate::ir::PythonException::BufferError,
    );
    let process_index = if resources.action_count < u64::from(case.resource_bounds.max_actions)
        && resources.action_count < 64
    {
        case.processes
            .first_mut()
            .ok_or_else(|| format!("case {} has no process for scripted admission", case.id))?
            .actions
            .push(action);
        0
    } else {
        // Full random-DAG cases always contain a zero-allocation route wait.
        // Replacing one keeps the action bound fixed while preserving the
        // route's independently witnessed read and write endpoints.
        let replacement = case
            .processes
            .iter()
            .enumerate()
            .find_map(|(process, program)| {
                program
                    .actions
                    .iter()
                    .position(|action| {
                        matches!(
                            &action.operation,
                            ActionOp::Lookup { path, .. } if path == SEEDED_MODEL_PATH
                        )
                    })
                    .map(|step| (process, step))
            })
            .or_else(|| {
                case.processes
                    .iter()
                    .enumerate()
                    .find_map(|(process, program)| {
                        program
                            .actions
                            .iter()
                            .position(|action| {
                                matches!(action.operation, ActionOp::AwaitEntry { .. })
                            })
                            .map(|step| (process, step))
                    })
            })
            .ok_or_else(|| {
                format!(
                    "case {} has no zero-allocation action for scripted admission",
                    case.id
                )
            })?;
        case.processes[replacement.0].actions[replacement.1] = action;
        replacement.0
    };
    let process = &mut case.processes[process_index];
    process.access.read_prefixes.push("/models".to_owned());
    process.access.read_prefixes.sort();
    process.access.read_prefixes.dedup();
    case.read_only_fixture_paths
        .insert(SEEDED_MODEL_PATH.to_owned());
    case.validate()
}

fn generate_exact_cases(
    seed: u64,
    count: usize,
    live_nodes: &BTreeSet<u64>,
    phase: &str,
    max_race_states: u32,
    budget: Option<&Budget>,
) -> Result<Vec<BehaviorCase>, String> {
    let node_count = u8::try_from(live_nodes.len())
        .map_err(|_| "live-node count exceeds generator range".to_owned())?;
    let contiguous = (1..=u64::from(node_count)).collect::<Vec<_>>();
    let exact = live_nodes.iter().copied().collect::<Vec<_>>();
    let mapping = contiguous.into_iter().zip(exact).collect::<Vec<_>>();
    let mut cases = match budget {
        Some(budget) => generated_campaign_cases_budgeted(seed, count, node_count, budget)?,
        None => generated_campaign_cases(seed, count, node_count)?,
    };
    for (index, case) in cases.iter_mut().enumerate() {
        if let Some(budget) = budget {
            budget.check("map campaign case to exact logical node identities")?;
        }
        case.id = format!("{phase}-{index:03}-{}", case.id);
        case.live_nodes = live_nodes.clone();
        case.resource_bounds.max_race_states = max_race_states;
        // Recovery phases share one attempt. Distinct seeds are not an
        // identity boundary (e.g. rotating seed zero still gives zero).
        let scope = format!("/cases/{phase}-{index:03}");
        let fixtures = &case.read_only_fixture_paths;
        let scope_path = |path: &str| {
            if fixtures.contains(path) {
                path.to_owned()
            } else if path == "/cases" {
                scope.clone()
            } else if let Some(suffix) = path.strip_prefix("/cases/") {
                format!("{scope}/{suffix}")
            } else {
                path.to_owned()
            }
        };
        for process in &mut case.processes {
            let fixture_grants = fixtures
                .iter()
                .filter(|path| {
                    process.access.read_prefixes.iter().any(|prefix| {
                        path.as_str() == prefix
                            || path
                                .strip_prefix(prefix.as_str())
                                .is_some_and(|rest| rest.starts_with('/'))
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            let old_execution = format!("/runs/{}", process.access.execution_id);
            process.access.execution_id =
                format!("{phase}-{index:03}-{}", process.access.execution_id);
            let new_execution = format!("/runs/{}", process.access.execution_id);
            for prefix in process
                .access
                .read_prefixes
                .iter_mut()
                .chain(&mut process.access.write_prefixes)
            {
                *prefix = scope_path(prefix);
                if prefix == &old_execution {
                    *prefix = new_execution.clone();
                } else if let Some(rest) = prefix.strip_prefix(old_execution.as_str())
                    && rest.starts_with('/')
                {
                    *prefix = format!("{new_execution}{rest}");
                }
            }
            process.access.read_prefixes.extend(fixture_grants);
            process.access.read_prefixes.sort();
            process.access.read_prefixes.dedup();
            for action in &mut process.actions {
                action.operation.map_paths(|path| *path = scope_path(path));
            }
            process.logical_node_id = mapping
                .iter()
                .find_map(|(from, to)| (*from == process.logical_node_id).then_some(*to))
                .ok_or_else(|| format!("case {} references unmapped node", case.id))?;
        }
        for route in &mut case.routes {
            for edge in &mut route.edges {
                edge.path = scope_path(&edge.path);
                edge.source = mapping
                    .iter()
                    .find_map(|(from, to)| (*from == edge.source).then_some(*to))
                    .ok_or_else(|| format!("case {} route source is unmapped", case.id))?;
                edge.destination = mapping
                    .iter()
                    .find_map(|(from, to)| (*from == edge.destination).then_some(*to))
                    .ok_or_else(|| format!("case {} route destination is unmapped", case.id))?;
            }
            for path in &mut route.join_inputs {
                *path = scope_path(path);
            }
        }
        if let crate::ir::FailureInjection::SlowProcess {
            parked_path,
            release_path,
            ..
        } = &mut case.failure
        {
            *parked_path = scope_path(parked_path);
            *release_path = scope_path(release_path);
        }
        case.validate()?;
    }
    Ok(cases)
}

fn generate_recovery_cases(
    seed: u64,
    live_nodes: &BTreeSet<u64>,
    max_race_states: u32,
    budget: Option<&Budget>,
) -> Result<Vec<RecoveryCase>, String> {
    let before = generate_exact_cases(
        seed,
        RECOVERY_CASE_COUNT,
        live_nodes,
        "recovery-pre",
        max_race_states,
        budget,
    )?;
    let after = generate_exact_cases(
        seed.rotate_left(31),
        RECOVERY_CASE_COUNT,
        live_nodes,
        "recovery-post",
        max_race_states,
        budget,
    )?;
    before
        .into_iter()
        .zip(after)
        .enumerate()
        .map(|(index, (mut pre_restart, mut post_restart))| {
            if let Some(budget) = budget {
                budget.check("construct exact persisted recovery namespace facts")?;
            }
            let blob_path = format!("/cases/recovery-pre-{index:03}/persisted-blob");
            let stream_path = format!("/cases/recovery-pre-{index:03}/persisted-stream");
            let blob_payload = vec![u8::try_from(index).unwrap_or(u8::MAX); 1025];
            let stream_payload = vec![u8::try_from(index).unwrap_or(u8::MAX).wrapping_add(1); 4097];
            pre_restart.processes[0]
                .actions
                .push(Action::ok(ActionOp::PublishBlob {
                    path: blob_path.clone(),
                    bytes: blob_payload.clone(),
                }));
            pre_restart.processes[0]
                .actions
                .push(Action::ok(ActionOp::Lookup {
                    path: blob_path.clone(),
                    expected_kind: "blob".to_owned(),
                }));
            pre_restart.processes[0]
                .actions
                .push(Action::ok(ActionOp::ReadBlob {
                    path: blob_path.clone(),
                    expected: blob_payload.clone(),
                }));
            pre_restart.processes[0]
                .actions
                .push(Action::ok(ActionOp::StreamRoundTrip {
                    path: stream_path.clone(),
                    chunks: vec![stream_payload],
                }));
            pre_restart.processes[0]
                .actions
                .push(Action::ok(ActionOp::WaitForQuiescent {
                    path: stream_path.clone(),
                }));
            post_restart
                .read_only_fixture_paths
                .extend([blob_path.clone(), stream_path.clone()]);
            post_restart.processes[0]
                .access
                .read_prefixes
                .extend([blob_path.clone(), stream_path.clone()]);
            // Keep the healthy branch behind the slow process's parked marker.
            let probe_start = usize::from(matches!(
                post_restart.failure,
                crate::ir::FailureInjection::SlowProcess { .. }
            ));
            post_restart.processes[0].actions.splice(
                probe_start..probe_start,
                [
                    Action::ok(ActionOp::Lookup {
                        path: blob_path.clone(),
                        expected_kind: "blob".to_owned(),
                    }),
                    Action::ok(ActionOp::ReadBlob {
                        path: blob_path.clone(),
                        expected: blob_payload.clone(),
                    }),
                    Action::ok(ActionOp::Lookup {
                        path: stream_path.clone(),
                        expected_kind: "stream".to_owned(),
                    }),
                    Action::ok(ActionOp::WaitForQuiescent {
                        path: stream_path.clone(),
                    }),
                ],
            );
            // Generation reserves four action slots for persistence probes.
            // Never widen the healthy-case contract to admit a composition.
            for case in [&pre_restart, &post_restart] {
                let resources = case.resource_summary()?;
                if !(16..=64).contains(&resources.action_count)
                    || !(2..=20).contains(&resources.process_count)
                {
                    return Err(format!(
                        "recovery case {} exceeds healthy action/process bounds",
                        case.id
                    ));
                }
            }
            pre_restart.validate()?;
            post_restart.validate()?;
            Ok(RecoveryCase {
                id: format!("recovery-{index:02}"),
                persisted_entries: vec![
                    PersistedFixtureEntry::Blob {
                        path: blob_path,
                        expected: blob_payload,
                    },
                    PersistedFixtureEntry::QuiescentStream { path: stream_path },
                ],
                pre_restart,
                post_restart,
            })
        })
        .collect()
}
fn planned_coverage(
    cases: &[BehaviorCase],
    live_nodes: &BTreeSet<u64>,
    full: bool,
    budget: Option<&Budget>,
) -> Result<CoverageLedger, String> {
    let mut ledger = if full {
        CoverageLedger::five_node(live_nodes.clone())?
    } else {
        CoverageLedger::survivor(live_nodes.clone())?
    };
    for case in cases {
        if let Some(budget) = budget {
            budget.check("close planned campaign coverage")?;
        }
        ledger.plan_case(case)?;
    }
    ledger.assert_planned_closed()?;
    Ok(ledger)
}

fn validate_phase(cases: &[BehaviorCase], live_nodes: &BTreeSet<u64>) -> Result<(), String> {
    for case in cases {
        case.validate()?;
        if &case.live_nodes != live_nodes {
            return Err(format!(
                "case {} requires {:?}, expected {live_nodes:?}",
                case.id, case.live_nodes
            ));
        }
    }
    Ok(())
}

fn auxiliary_campaign_resources(
    initial_nodes: &BTreeSet<u64>,
    recovery: &[RecoveryCase],
) -> Result<CaseResources, String> {
    let nodes = initial_nodes.iter().copied().collect::<Vec<_>>();
    // Use the executable constructors: nonce/attempt identities change paths,
    // never their requested payload, buffer, endpoint, or observation counts.
    let mut total = ClusterHarness::convergence_case(&nodes, 0)?.resource_summary()?;
    for case in recovery {
        for absent_paths in [BTreeSet::new(), case.post_restart.owned_paths()] {
            let probe = ClusterHarness::persistence_probe_case(
                &case.pre_restart,
                &case.persisted_entries,
                &nodes,
                &absent_paths,
                0,
            )?;
            total = total.checked_add(probe.resource_summary()?)?;
        }
    }
    Ok(total)
}

fn campaign_resources<'a>(
    cases: impl IntoIterator<Item = &'a BehaviorCase>,
) -> Result<CaseResources, String> {
    cases
        .into_iter()
        .try_fold(CaseResources::default(), |total, case| {
            total.checked_add(case.resource_summary()?)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(provider: ProviderMode) -> CampaignConfig {
        CampaignConfig {
            provider,
            seed: 7,
            runtime_image: "example.invalid/myelin-e2e:fixture".to_owned(),
            limits: CampaignLimits {
                fixture_lifetime_secs: 24 * 60 * 60,
                total_cost_usd: 10.0,
                total_hourly_price_usd: 2.0,
                case_deadline_secs: 30,
                max_campaign_payload_bytes: 512 * 1024 * 1024,
                max_campaign_allocation_bytes: 8 * 1024 * 1024 * 1024,
                max_case_race_states: 256,
            },
            offers: OfferPolicy {
                gpu_model: None,
                min_gpu_ram_mb: None,
                min_compute_cap: None,
                min_reliability: Some(0.95),
                min_download_mbps: None,
                min_upload_mbps: None,
                max_hourly_price_per_node: Some(0.4),
                blacklist_hosts: Vec::new(),
            },
        }
    }

    #[test]
    fn full_campaign_is_generated_before_provider_access() {
        let plan = CampaignPlan::build(&config(ProviderMode::Real), Vec::new()).unwrap();
        plan.validate().unwrap();
        assert_eq!(plan.resources.case_count, 208);
        assert_eq!(plan.resources.workload_segment_count, 224);
        assert_eq!(plan.normal.len(), 128);
        assert_eq!(plan.recovery.len(), 16);
        assert_eq!(plan.four_node.len(), 32);
        assert_eq!(plan.three_node.len(), 32);
        let mut workload_allocation = 0_u64;
        for case in plan
            .normal
            .iter()
            .chain(&plan.four_node)
            .chain(&plan.three_node)
            .chain(
                plan.recovery
                    .iter()
                    .flat_map(|case| [&case.pre_restart, &case.post_restart]),
            )
        {
            let resources = case.resource_summary().unwrap();
            assert!((16..=64).contains(&resources.action_count), "{}", case.id);
            assert!((2..=20).contains(&resources.process_count), "{}", case.id);
            workload_allocation += resources.allocation_bytes;
        }
        let readiness = ClusterHarness::convergence_case(&[1, 2, 3, 4, 5], 0)
            .unwrap()
            .resource_summary()
            .unwrap();
        let mut without_snapshots = config(ProviderMode::Real);
        without_snapshots.limits.max_campaign_allocation_bytes =
            workload_allocation + readiness.allocation_bytes;
        assert!(CampaignPlan::build(&without_snapshots, Vec::new()).is_err());
    }

    #[test]
    fn campaign_resource_ceilings_cannot_be_relaxed() {
        let mut limits = config(ProviderMode::Real).limits;
        limits.validate(true).unwrap();
        limits.max_campaign_payload_bytes += 1;
        assert!(limits.validate(true).is_err());
        limits.max_campaign_payload_bytes = MAX_CAMPAIGN_PAYLOAD_BYTES;
        limits.max_campaign_allocation_bytes += 1;
        assert!(limits.validate(true).is_err());
    }

    #[test]
    fn role_dags_preserve_noncontiguous_survivor_membership() {
        let live_nodes = BTreeSet::from([2, 7, 11]);
        let cases = generate_exact_cases(0, 32, &live_nodes, "survivor", 256, None).unwrap();
        planned_coverage(&cases, &live_nodes, false, None).unwrap();
        for case in &cases {
            assert_eq!(case.live_nodes, live_nodes);
            for route in &case.routes {
                for edge in &route.edges {
                    for (role, node) in [
                        (&edge.source_role, edge.source),
                        (&edge.destination_role, edge.destination),
                    ] {
                        let owner = case
                            .processes
                            .iter()
                            .find(|process| process.id == *role)
                            .unwrap();
                        assert!(live_nodes.contains(&node));
                        assert_eq!(owner.logical_node_id, node);
                    }
                }
            }
        }
    }

    #[test]
    fn paid_plan_rejects_missing_cost_ceiling() {
        let mut invalid = config(ProviderMode::Real);
        invalid.limits.total_cost_usd = 0.0;
        assert!(
            CampaignPlan::build(&invalid, Vec::new())
                .unwrap_err()
                .contains("ceiling")
        );
    }

    #[test]
    fn selected_cost_is_admitted_conservatively() {
        let config = config(ProviderMode::Real);
        let plan = CampaignPlan::build(&config, Vec::new()).unwrap();
        assert!(plan.admit_selected_hourly_cost(&config.limits, 0.4).is_ok());
        assert!(
            plan.admit_selected_hourly_cost(&config.limits, 2.1)
                .is_err()
        );
    }

    #[test]
    fn artifact_envelope_rejects_wrong_kind_and_schema() {
        let mut artifact = ArtifactEnvelope::new("campaign", "run", 7_u64).unwrap();
        artifact.validate("campaign").unwrap();
        artifact.kind = "other".to_owned();
        assert!(artifact.validate("campaign").is_err());
        artifact.kind = "campaign".to_owned();
        artifact.artifact_schema_version += 1;
        assert!(artifact.validate("campaign").is_err());
    }

    #[test]
    fn campaign_totals_include_roundtrip_retry_and_requested_allocations() {
        let mut case = crate::corpus::stable_corpus(2, 1).remove(0);
        case.processes.truncate(1);
        case.processes[0].actions = vec![
            Action::ok(ActionOp::StreamRoundTrip {
                path: "/cases/resources/roundtrip".to_owned(),
                chunks: vec![vec![1, 2, 3]],
            }),
            Action::ok(ActionOp::StreamReadWithRetry {
                path: "/cases/resources/retry".to_owned(),
                expected: vec![4, 5],
            }),
        ];
        let totals = campaign_resources([&case, &case]).unwrap();
        assert_eq!(totals.payload_bytes, 10);
        assert_eq!(totals.allocation_bytes, 2 * (61 + 7 * 256 * 1024));
        assert_eq!(totals.action_count, 4);
        assert_eq!(totals.process_count, 2);
    }

    #[test]
    fn recovery_attempts_share_owned_persistence_but_not_external_fixtures() {
        let blob = "/cases/recovery/persisted";
        let stream = "/cases/recovery/quiescent";
        let external = "/cases/external/fixture";
        let mut pre = crate::corpus::stable_corpus(2, 1).remove(0);
        pre.processes.truncate(1);
        pre.processes[0].actions = vec![
            Action::ok(ActionOp::PublishBlob {
                path: blob.to_owned(),
                bytes: vec![1],
            }),
            Action::ok(ActionOp::StreamRoundTrip {
                path: stream.to_owned(),
                chunks: vec![vec![2]],
            }),
        ];
        let mut post = pre.clone();
        post.id = "post-restart".to_owned();
        post.read_only_fixture_paths =
            BTreeSet::from([blob.to_owned(), stream.to_owned(), external.to_owned()]);
        post.processes[0].actions = vec![
            Action::ok(ActionOp::ReadBlob {
                path: blob.to_owned(),
                expected: vec![1],
            }),
            Action::ok(ActionOp::WaitForQuiescent {
                path: stream.to_owned(),
            }),
            Action::ok(ActionOp::ReadBlob {
                path: external.to_owned(),
                expected: vec![3],
            }),
        ];
        let recovery = RecoveryCase {
            id: "recovery".to_owned(),
            pre_restart: pre,
            post_restart: post,
            persisted_entries: vec![
                PersistedFixtureEntry::Blob {
                    path: blob.to_owned(),
                    expected: vec![1],
                },
                PersistedFixtureEntry::QuiescentStream {
                    path: stream.to_owned(),
                },
            ],
        };
        let first = recovery.for_attempt(7);
        let second = recovery.for_attempt(8);
        first.pre_restart.validate().unwrap();
        first.post_restart.validate().unwrap();
        for (step, entry) in first.persisted_entries.iter().enumerate() {
            assert_eq!(
                entry.path(),
                first.pre_restart.processes[0].actions[step]
                    .operation
                    .path()
            );
            assert_eq!(
                entry.path(),
                first.post_restart.processes[0].actions[step]
                    .operation
                    .path()
            );
            assert!(
                first
                    .post_restart
                    .read_only_fixture_paths
                    .contains(entry.path())
            );
            assert!(!first.post_restart.owned_paths().contains(entry.path()));
        }
        assert!(
            first
                .pre_restart
                .owned_paths()
                .is_disjoint(&second.pre_restart.owned_paths())
        );
        assert_eq!(
            first.post_restart.processes[0].actions[2].operation.path(),
            external
        );
        assert!(first.post_restart.owned_paths().is_empty());
    }
}
