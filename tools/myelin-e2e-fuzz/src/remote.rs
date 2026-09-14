//! Paid VastAI campaign admission, durable ownership, and deny-only state.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use swactor_vastai::{BlockingVastClient, VastClient};

use myelin_control_contract::{ControlReply, Offer, OfferSearchRequest, OfferSearchResults};
use provisioning::{
    PaidAdmissionMode, PaidAdmissionSnapshot, PaidCleanupLimits, PaidCleanupOwnership,
    PaidFixtureAdmission, PaidProcessIdentity,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::campaign::{
    ARTIFACT_SCHEMA_VERSION, ArtifactEnvelope, CampaignConfig, CampaignLimits, CampaignPlan,
};

pub const PAID_STATE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaidCampaignPhase {
    Planned,
    Acquiring,
    Prepared,
    DenyOnly,
    Quarantined,
    CleanupOnly,
    Complete,
}

impl PaidCampaignPhase {
    pub const fn permits_acquisition(self) -> bool {
        matches!(self, Self::Planned | Self::Acquiring)
    }

    pub const fn permits_workload(self) -> bool {
        matches!(self, Self::DenyOnly)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquisitionRecord {
    pub sequence: u32,
    pub operation: String,
    pub expected_offer_ids: Vec<u64>,
    pub expected_labels: BTreeSet<String>,
    pub observed_contract_ids: BTreeSet<u64>,
    pub completed: bool,
    pub written_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaidCampaignState {
    pub schema_version: u32,
    pub campaign_id: String,
    pub phase: PaidCampaignPhase,
    pub state_dir: PathBuf,
    pub selected_offer_ids: Vec<u64>,
    pub owned_labels: BTreeSet<String>,
    pub owned_contract_ids: BTreeSet<u64>,
    pub acquisition_count: u32,
    pub bootstrap_count: u32,
    pub records: Vec<AcquisitionRecord>,
    pub quarantine_reason: Option<String>,
    #[serde(default)]
    pub cleanup_ownership: Option<PaidCleanupOwnership>,
}

impl PaidCampaignState {
    pub fn planned(campaign_id: impl Into<String>, state_dir: PathBuf) -> Self {
        Self {
            schema_version: PAID_STATE_SCHEMA_VERSION,
            campaign_id: campaign_id.into(),
            phase: PaidCampaignPhase::Planned,
            state_dir,
            selected_offer_ids: Vec::new(),
            owned_labels: BTreeSet::new(),
            owned_contract_ids: BTreeSet::new(),
            acquisition_count: 0,
            bootstrap_count: 0,
            records: Vec::new(),
            quarantine_reason: None,
            cleanup_ownership: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != PAID_STATE_SCHEMA_VERSION {
            return Err(format!(
                "paid state schema {} is unsupported; expected {}",
                self.schema_version, PAID_STATE_SCHEMA_VERSION
            ));
        }
        if self.campaign_id.is_empty() {
            return Err("paid state campaign id is empty".to_owned());
        }
        if self.acquisition_count > 5 || self.bootstrap_count > 5 {
            return Err(
                "paid state exceeds the five-node acquisition/bootstrap ceiling".to_owned(),
            );
        }
        if !self.phase.permits_acquisition()
            && self.records.iter().any(|record| !record.completed)
            && !matches!(
                self.phase,
                PaidCampaignPhase::CleanupOnly | PaidCampaignPhase::Quarantined
            )
        {
            return Err("deny-only state retains an unresolved acquisition record".to_owned());
        }
        if matches!(
            self.phase,
            PaidCampaignPhase::Prepared | PaidCampaignPhase::DenyOnly
        ) && (self.acquisition_count != 5
            || self.bootstrap_count != 5
            || !(3..=5).contains(&self.owned_contract_ids.len())
            || self.owned_labels.len() != 5)
        {
            return Err(
                "prepared fixture does not own three to five surviving contracts and five labels"
                    .to_owned(),
            );
        }
        Ok(())
    }

    pub fn begin_acquisition(
        &mut self,
        offer_ids: Vec<u64>,
        labels: BTreeSet<String>,
    ) -> Result<(), String> {
        if !self.phase.permits_acquisition() {
            return Err(format!(
                "phase {:?} denies provider acquisition",
                self.phase
            ));
        }
        if offer_ids.len() != 5 || labels.len() != 5 {
            return Err("paid acquisition requires five exact offers and labels".to_owned());
        }
        if !self.records.is_empty() {
            return Err("paid acquisition may be attempted only once".to_owned());
        }
        self.phase = PaidCampaignPhase::Acquiring;
        self.selected_offer_ids = offer_ids.clone();
        self.owned_labels = labels.clone();
        self.records.push(AcquisitionRecord {
            sequence: 1,
            operation: "create-five-exact-contracts".to_owned(),
            expected_offer_ids: offer_ids,
            expected_labels: labels,
            observed_contract_ids: BTreeSet::new(),
            completed: false,
            written_unix_ms: unix_ms()?,
        });
        Ok(())
    }

    pub fn reconcile_prepared(
        &mut self,
        admission: &PaidAdmissionSnapshot,
        contracts: BTreeSet<u64>,
        labels: BTreeSet<String>,
    ) -> Result<(), String> {
        if self.phase != PaidCampaignPhase::Acquiring {
            return Err("only an acquiring campaign can become prepared".to_owned());
        }
        if contracts.len() != 5 || labels != self.owned_labels {
            return Err(format!(
                "acquisition reconciliation expected five exact labels/contracts: labels={labels:?}, contracts={contracts:?}"
            ));
        }
        self.reconcile_admission(admission)?;
        if admission.mode != PaidAdmissionMode::Prepared
            || admission
                .contracts
                .values()
                .copied()
                .collect::<BTreeSet<_>>()
                != contracts
            || admission
                .nodes
                .iter()
                .map(|node| node.label.clone())
                .collect::<BTreeSet<_>>()
                != labels
            || admission.create_reservations.len() != 5
            || admission.initial_bootstraps.len() != 5
            || !admission.rejected_creates.is_empty()
            || !admission.accounting_errors.is_empty()
        {
            return Err(
                "prepared fixture lacks actual five-creation/five-bootstrap admission evidence"
                    .to_owned(),
            );
        }
        let record = self
            .records
            .last_mut()
            .ok_or_else(|| "acquisition record is missing".to_owned())?;
        record.observed_contract_ids = contracts.clone();
        record.completed = true;
        record.written_unix_ms = unix_ms()?;
        self.owned_contract_ids = contracts;
        self.acquisition_count = admission.create_reservations.len() as u32;
        self.bootstrap_count = admission.initial_bootstraps.len() as u32;
        // The provider admission is already permanently sealed. Commit the
        // runner's prepared accounting directly as deny-only so no crash can
        // leave a durable workload state with acquisition still in transition.
        self.phase = PaidCampaignPhase::DenyOnly;
        self.validate()
    }

    pub fn quarantine(&mut self, reason: impl Into<String>) {
        self.phase = PaidCampaignPhase::Quarantined;
        self.quarantine_reason = Some(reason.into());
    }
    pub fn resume_deny_only(
        &mut self,
        admission: &PaidAdmissionSnapshot,
        contracts: BTreeSet<u64>,
    ) -> Result<(), String> {
        if self.phase != PaidCampaignPhase::Quarantined {
            return Err("only a quarantined paid fixture can be resumed".to_owned());
        }
        if !(3..=5).contains(&contracts.len()) {
            return Err(format!(
                "resumed fixture must retain three to five contracts, found {}",
                contracts.len()
            ));
        }
        self.reconcile_admission(admission)?;
        if admission.mode != PaidAdmissionMode::Prepared
            || !contracts.is_subset(&admission.contracts.values().copied().collect())
            || admission.create_reservations.len() != 5
            || admission.initial_bootstraps.len() != 5
            || !admission.accounting_errors.is_empty()
        {
            return Err(
                "resume requires original five-creation/five-bootstrap admission evidence"
                    .to_owned(),
            );
        }
        self.acquisition_count = admission.create_reservations.len() as u32;
        self.bootstrap_count = admission.initial_bootstraps.len() as u32;
        self.owned_contract_ids = contracts;
        self.phase = PaidCampaignPhase::DenyOnly;
        self.quarantine_reason = None;
        self.validate()
    }

    /// Reconcile rather than replace either durable ownership source. Exact IDs
    /// survive runner loss, including discoveries that no longer carry labels.
    pub fn reconcile_admission(&mut self, admission: &PaidAdmissionSnapshot) -> Result<(), String> {
        let labels = admission
            .nodes
            .iter()
            .map(|node| node.label.clone())
            .collect::<BTreeSet<_>>();
        if !self.owned_labels.is_empty() && self.owned_labels != labels {
            return Err("runner ownership labels disagree with durable admission".to_owned());
        }
        self.owned_labels.extend(labels);
        self.owned_contract_ids
            .extend(admission.known_contract_ids());
        self.cleanup_ownership = admission.cleanup.clone();
        Ok(())
    }

    pub fn enter_cleanup_only(&mut self) {
        self.phase = PaidCampaignPhase::CleanupOnly;
    }

    pub fn complete_cleanup(&mut self) -> Result<(), String> {
        if !self.owned_contract_ids.is_empty() {
            return Err("cannot complete cleanup while attributed contracts remain".to_owned());
        }
        if self.records.iter().any(|record| !record.completed) {
            return Err(
                "cannot complete cleanup with unresolved acquisition accounting".to_owned(),
            );
        }
        self.phase = PaidCampaignPhase::Complete;
        self.validate()
    }
}

pub fn offer_search_request(config: &CampaignConfig) -> OfferSearchRequest {
    OfferSearchRequest {
        gpu_model: config.offers.gpu_model.clone(),
        min_gpu_ram_mb: config.offers.min_gpu_ram_mb,
        min_compute_cap: config.offers.min_compute_cap,
        min_reliability: config.offers.min_reliability,
        require_verified: Some(true),
        min_download_mbps: config.offers.min_download_mbps,
        min_upload_mbps: config.offers.min_upload_mbps,
        max_hourly_price: config.offers.max_hourly_price_per_node,
        blacklist_hosts: config.offers.blacklist_hosts.clone(),
        count: Some(8),
    }
}

pub fn decode_offer_results(response: Value) -> Result<OfferSearchResults, String> {
    match serde_json::from_value::<ControlReply>(response)
        .map_err(|error| format!("decode orchestrator offer reply: {error}"))?
    {
        ControlReply::Offers(results) => Ok(results),
        ControlReply::Rejected(error) => Err(format!("offer search rejected: {error}")),
        other => Err(format!("unexpected offer search reply: {other:?}")),
    }
}

pub fn select_exact_offers(
    results: &OfferSearchResults,
    config: &CampaignConfig,
    plan: &CampaignPlan,
) -> Result<Vec<Offer>, String> {
    let mut offers = results.offers.clone();
    offers.sort_by(|left, right| {
        left.hourly_price
            .total_cmp(&right.hourly_price)
            .then_with(|| left.offer_id.cmp(&right.offer_id))
    });
    let mut selected = Vec::with_capacity(5);
    let mut hosts = BTreeSet::new();
    for offer in offers {
        if !offer_is_eligible(&offer, config)? {
            continue;
        }
        let Some(host) = offer.host_id else {
            continue;
        };
        if hosts.insert(host) {
            selected.push(offer);
        }
        if selected.len() == 5 {
            break;
        }
    }
    if selected.len() != 5 {
        return Err(format!(
            "offer search {} returned only {} eligible distinct verified hosts",
            results.search_id,
            selected.len()
        ));
    }
    let hourly = selected.iter().map(|offer| offer.hourly_price).sum();
    plan.admit_selected_hourly_cost(&config.limits, hourly)?;
    Ok(selected)
}

fn offer_is_eligible(offer: &Offer, config: &CampaignConfig) -> Result<bool, String> {
    for (name, value) in [
        ("hourly_price", Some(offer.hourly_price)),
        ("download_cost_per_tb", Some(offer.download_cost_per_tb)),
        ("upload_cost_per_tb", Some(offer.upload_cost_per_tb)),
        ("gpu_ram_mb", offer.gpu_ram_mb),
        ("reliability", offer.reliability),
        ("download_mbps", offer.download_mbps),
        ("upload_mbps", offer.upload_mbps),
    ] {
        if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
            return Err(format!("offer {} has malformed {name}", offer.offer_id));
        }
    }
    let Some(host_id) = offer.host_id else {
        return Ok(false);
    };
    if offer.verification.as_deref() != Some("verified")
        || config.offers.blacklist_hosts.contains(&host_id)
        || config.offers.gpu_model.as_ref().is_some_and(|required| {
            !offer
                .gpu_model
                .to_ascii_lowercase()
                .contains(&required.to_ascii_lowercase())
        })
        || config.offers.min_gpu_ram_mb.is_some_and(|minimum| {
            !offer
                .gpu_ram_mb
                .is_some_and(|value| value >= minimum as f64)
        })
        || config
            .offers
            .min_compute_cap
            .is_some_and(|minimum| offer.compute_cap < minimum)
        || config
            .offers
            .min_reliability
            .is_some_and(|minimum| !offer.reliability.is_some_and(|value| value >= minimum))
        || config
            .offers
            .min_download_mbps
            .is_some_and(|minimum| !offer.download_mbps.is_some_and(|value| value >= minimum))
        || config
            .offers
            .min_upload_mbps
            .is_some_and(|minimum| !offer.upload_mbps.is_some_and(|value| value >= minimum))
        || config
            .offers
            .max_hourly_price_per_node
            .is_some_and(|maximum| offer.hourly_price > maximum)
    {
        return Ok(false);
    }
    Ok(true)
}

pub fn write_paid_state(path: &Path, state: &PaidCampaignState) -> Result<(), String> {
    state.validate()?;
    durable_json(path, state)
}

pub fn read_paid_state(path: &Path) -> Result<PaidCampaignState, String> {
    let state = serde_json::from_slice::<PaidCampaignState>(
        &fs::read(path).map_err(|error| format!("read paid campaign state: {error}"))?,
    )
    .map_err(|error| format!("parse paid campaign state: {error}"))?;
    state.validate()?;
    Ok(state)
}

pub fn write_campaign_plan(path: &Path, plan: &CampaignPlan) -> Result<(), String> {
    plan.validate()?;
    let envelope = ArtifactEnvelope::new("campaign-plan", plan.campaign_id.clone(), plan)?;
    if envelope.artifact_schema_version != ARTIFACT_SCHEMA_VERSION {
        return Err("campaign artifact schema mismatch".to_owned());
    }
    durable_json(path, &envelope)
}
pub fn read_campaign_plan(path: &Path) -> Result<CampaignPlan, String> {
    let envelope =
        serde_json::from_reader::<_, ArtifactEnvelope<CampaignPlan>>(std::io::BufReader::new(
            File::open(path).map_err(|error| format!("read campaign plan: {error}"))?,
        ))
        .map_err(|error| format!("parse campaign plan: {error}"))?;
    envelope.validate("campaign-plan")?;
    envelope.payload.validate()?;
    if envelope.campaign_id != envelope.payload.campaign_id {
        return Err("campaign plan envelope identity differs from its payload".to_owned());
    }
    Ok(envelope.payload)
}

pub fn read_coverage_ledger(
    path: &Path,
    campaign_id: &str,
    phase: &str,
) -> Result<crate::coverage::CoverageLedger, String> {
    let envelope = serde_json::from_slice::<ArtifactEnvelope<crate::coverage::CoverageLedger>>(
        &fs::read(path).map_err(|error| format!("read {phase} coverage ledger: {error}"))?,
    )
    .map_err(|error| format!("parse {phase} coverage ledger: {error}"))?;
    envelope.validate(&format!("{phase}-coverage"))?;
    if envelope.campaign_id != campaign_id {
        return Err(format!(
            "{phase} coverage campaign {:?} differs from expected {campaign_id:?}",
            envelope.campaign_id
        ));
    }
    Ok(envelope.payload)
}

pub fn write_coverage_ledger(
    path: &Path,
    campaign_id: &str,
    phase: &str,
    ledger: &crate::coverage::CoverageLedger,
) -> Result<(), String> {
    let envelope =
        ArtifactEnvelope::new(format!("{phase}-coverage"), campaign_id.to_owned(), ledger)?;
    durable_json(path, &envelope)
}

/// Convert the operator ceiling and an hourly price conservatively.
fn cleanup_limits(hourly: f64, limits: &CampaignLimits) -> Result<PaidCleanupLimits, String> {
    let maximum = (limits.total_cost_usd * 1_000_000.0).floor();
    let hourly_micros = (hourly * 1_000_000.0).ceil();
    if !maximum.is_finite()
        || !hourly_micros.is_finite()
        || maximum < 1.0
        || hourly_micros < 1.0
        || maximum >= u64::MAX as f64
        || hourly_micros >= u64::MAX as f64
        || hourly > limits.total_hourly_price_usd
    {
        return Err("paid cleanup financial limits are invalid or exceed authorization".to_owned());
    }
    Ok(PaidCleanupLimits {
        maximum_cost_microusd: maximum as u64,
        hourly_price_microusd: hourly_micros as u64,
    })
}

/// Install cleanup ownership before offer search using the operator's full
/// authorized hourly ceiling. Exact selected prices are checked separately.
pub fn authorized_cleanup_limits(limits: &CampaignLimits) -> Result<PaidCleanupLimits, String> {
    cleanup_limits(limits.total_hourly_price_usd, limits)
}

/// Convert the operator ceiling and selected rental prices conservatively.
/// Nonzero metered network prices cannot be bounded by rental lifetime alone.
pub fn selected_cleanup_limits(
    offers: &[Offer],
    limits: &CampaignLimits,
) -> Result<PaidCleanupLimits, String> {
    if offers
        .iter()
        .any(|offer| offer.download_cost_per_tb != 0.0 || offer.upload_cost_per_tb != 0.0)
    {
        return Err(
            "paid spending ceiling requires zero metered upload/download charges".to_owned(),
        );
    }
    cleanup_limits(offers.iter().map(|offer| offer.hourly_price).sum(), limits)
}

/// Install detached restart supervision before provision_selected. Credentials
/// are inherited through the environment, never process arguments.
pub fn start_cleanup_owner(
    executable: &Path,
    admission_path: &Path,
    api_key_env: &str,
    limits: PaidCleanupLimits,
    preparation_remaining: Duration,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + preparation_remaining.min(Duration::from_secs(10));
    let admission = PaidFixtureAdmission::open(admission_path)?;
    let runner = PaidProcessIdentity::current()?;
    match admission.snapshot()?.cleanup {
        None => admission.install_cleanup_ownership(runner, limits)?,
        Some(ownership) if ownership.runner == runner && ownership.limits == limits => {}
        Some(_) => {
            return Err(
                "paid cleanup ownership differs from the immutable run authorization".to_owned(),
            );
        }
    }
    spawn_cleanup_supervisor(executable, admission_path, api_key_env, false, deadline)
}

/// Recovery is always deny-only. It cannot extend limits or create reservations.
pub fn recover_cleanup_owner(
    executable: &Path,
    admission_path: &Path,
    api_key_env: &str,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let admission = PaidFixtureAdmission::open(admission_path)?;
    admission.enter_cleanup_only()?;
    spawn_cleanup_supervisor(executable, admission_path, api_key_env, true, deadline)
}

fn spawn_cleanup_supervisor(
    executable: &Path,
    admission_path: &Path,
    api_key_env: &str,
    recovery: bool,
    deadline: std::time::Instant,
) -> Result<(), String> {
    use std::process::{Command, Stdio};
    let admission = PaidFixtureAdmission::open(admission_path)?;
    let spawn = || -> Result<std::process::Child, String> {
        let mut command = Command::new(executable);
        command
            .arg("--paid-cleanup-supervisor")
            .arg(admission_path)
            .arg("--api-key-env")
            .arg(api_key_env)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // Neither runner exit nor terminal loss takes down supervision.
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        #[cfg(not(unix))]
        return Err("independent paid cleanup requires Unix process supervision".to_owned());
        command
            .spawn()
            .map_err(|error| format!("start independent paid cleanup supervisor: {error}"))
    };
    let mut child = if admission.cleanup_supervisor_active()? {
        None
    } else {
        Some(spawn()?)
    };
    let result = (|| loop {
        let state = admission.snapshot()?;
        if std::time::Instant::now() < deadline
            && recovery
            && state
                .cleanup
                .as_ref()
                .is_some_and(|owner| owner.spending_stopped)
        {
            return Ok(());
        }
        let exited = child
            .as_mut()
            .map(|child| child.try_wait())
            .transpose()
            .map_err(|error| format!("observe cleanup supervisor: {error}"))?
            .flatten()
            .is_some();
        if exited {
            child = None;
        }
        // A previously observed supervisor can finish after renewed cleanup
        // invalidates its old absence receipt. Its lock remains the arbiter.
        if child.is_none()
            && std::time::Instant::now() < deadline
            && !admission.cleanup_supervisor_active()?
        {
            child = Some(spawn()?);
        }
        let ready = admission.cleanup_supervisor_active()?
            && admission.cleanup_owner_active()?
            && state.cleanup.as_ref().is_some_and(|owner| {
                owner
                    .supervisor
                    .as_ref()
                    .is_some_and(PaidProcessIdentity::is_live)
                    && owner
                        .owner
                        .as_ref()
                        .is_some_and(PaidProcessIdentity::is_live)
            });
        let expired = std::time::Instant::now() >= deadline;
        if ready || expired {
            if !ready || expired {
                admission.enter_cleanup_only()?;
                return Err(
                    "cleanup supervision failed readiness; acquisition remains denied".to_owned(),
                );
            }
            return if recovery {
                Ok(())
            } else {
                admission.require_live_cleanup_owner()
            };
        }
        std::thread::sleep(
            Duration::from_millis(25)
                .min(deadline.saturating_duration_since(std::time::Instant::now())),
        );
    })();
    if let Some(mut child) = child {
        // Detach cleanup responsibility, but retain reaping on every return.
        std::thread::spawn(move || {
            let _ = child.wait();
        });
    }
    result
}

/// Supervises an exact ledger until typed absence is durable. A worker crash
/// restarts cleanup-only immediately, without new limits or reservation slots.
/// This protects owner-process loss, not host loss or loss of both processes.
pub fn run_cleanup_supervisor(admission_path: &Path, api_key_env: &str) -> Result<(), String> {
    use std::process::{Command, Stdio};
    let admission = PaidFixtureAdmission::open(admission_path)?;
    let _supervisor_lock = admission.claim_cleanup_supervisor()?;
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    let mut first = true;
    loop {
        let state = admission.snapshot()?;
        if state
            .cleanup
            .as_ref()
            .is_some_and(|owner| owner.spending_stopped)
        {
            return Ok(());
        }
        if !first
            || state
                .cleanup
                .as_ref()
                .is_some_and(|owner| owner.owner.is_some())
        {
            admission.enter_cleanup_only()?;
        }
        first = false;
        // Adopting an already active worker never introduces a rival deletion
        // loop; its OS-held lock remains the authoritative exclusion boundary.
        while admission.cleanup_owner_active()? {
            if admission
                .snapshot()?
                .cleanup
                .as_ref()
                .is_some_and(|owner| owner.spending_stopped)
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let result = Command::new(&executable)
            .arg("--paid-cleanup-owner")
            .arg(admission_path)
            .arg("--api-key-env")
            .arg(api_key_env)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        if let Ok(mut child) = result {
            let _ = child.wait();
        }
        // Spawn errors and worker exits retain the duty and original ceilings.
        // A short bounded backoff prevents a broken executable spinning.
        admission.enter_cleanup_only()?;
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Cleanup worker entrypoint. Only the supervisor restarts this exact duty;
/// ambiguous creates and query failures never discharge ownership.
pub fn run_cleanup_owner(admission_path: &Path, api_key_env: &str) -> Result<(), String> {
    let admission = PaidFixtureAdmission::open(admission_path)?;
    let owner = admission.claim_cleanup_owner()?;
    let api_key = std::env::var(api_key_env)
        .map_err(|_| format!("required VastAI credential environment {api_key_env} is unset"))?;
    let client = BlockingVastClient::new(VastClient::new(api_key))?;
    loop {
        let stopping = owner.tick()?;
        let state = admission.snapshot()?;
        if state
            .cleanup
            .as_ref()
            .is_some_and(|ownership| ownership.complete)
        {
            return Ok(());
        }
        if stopping {
            let labels = state.cleanup_labels();
            let known_ids = state.known_contract_ids();
            let unresolved = state.unresolved_create_labels();
            let result = client.cleanup_owned_reconciled(
                &labels,
                &known_ids,
                &unresolved,
                Duration::from_secs(20),
                &mut |discovered| admission.reconcile_cleanup_discovery(discovered),
            );
            match result {
                Ok(census) => {
                    admission.complete_cleanup(&census.missing_contract_ids)?;
                    return Ok(());
                }
                Err(error) => {
                    if error.accounting_failed {
                        admission.record_cleanup_accounting_error(error.detail.clone())?;
                    }
                    if let Some(census) = error.absence {
                        admission.record_cleanup_absence(&census.missing_contract_ids)?;
                        return Err(error.detail);
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Loopback-only lifecycle fixture: the scripted provider supplies existing
/// contracts, while real admission and detached processes exercise retention.
/// This does not claim SSH/runtime readiness or perform provider acquisition.
pub fn run_scripted_retained_lifecycle(
    admission_path: &Path,
    api_key_env: &str,
) -> Result<(), String> {
    let endpoint = std::env::var("VASTAI_BASE_URL").unwrap_or_default();
    if endpoint
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_none_or(|port| port == 0)
    {
        return Err("scripted retained lifecycle requires a literal loopback endpoint".to_owned());
    }
    let generation = provisioning::plugin::DeploymentIdentity {
        artifact_digest: format!("sha256:{}", "a".repeat(64)),
        deployment_generation: "scripted-retained-generation".to_owned(),
    };
    let admission = if admission_path
        .try_exists()
        .map_err(|error| error.to_string())?
    {
        let admission = PaidFixtureAdmission::open(admission_path)?;
        let before = admission.snapshot()?;
        if admission
            .authorize_retained_deployment(false, generation.clone())
            .is_ok()
            || admission
                .reserve_retained_bootstrap(42, 1, 9000, &generation)
                .is_ok()
        {
            return Err("unauthorized scripted retained bootstrap was admitted".to_owned());
        }
        admission.authorize_retained_deployment(true, generation.clone())?;
        admission.bind_orchestrator(std::process::id())?;
        for node in &before.nodes {
            admission.reserve_retained_bootstrap(
                42,
                node.node_id,
                before.contracts[&node.node_id],
                &generation,
            )?;
            if admission
                .reserve_create(42, node.node_id, 0, node.offer_id, &node.label)
                .is_ok()
                || admission.reserve_bootstrap(42, node.node_id).is_ok()
                || admission
                    .reserve_retained_bootstrap(
                        42,
                        node.node_id,
                        before.contracts[&node.node_id],
                        &generation,
                    )
                    .is_ok()
            {
                return Err("retained deployment reopened a consumed slot".to_owned());
            }
        }
        let after = admission.snapshot()?;
        if before.contracts != after.contracts
            || before.create_reservations != after.create_reservations
            || before.initial_bootstraps != after.initial_bootstraps
        {
            return Err("retained deployment changed initial ownership accounting".to_owned());
        }
        admission
    } else {
        let nodes = (1..=5)
            .map(|node_id| provisioning::PaidNodeAdmission {
                node_id,
                offer_id: 1000 + node_id,
                host_id: 2000 + node_id,
                label: format!("scripted-retained-42-{node_id}-attempt-0"),
            })
            .collect();
        let admission = PaidFixtureAdmission::create(
            admission_path,
            42,
            nodes,
            unix_ms()? + provisioning::PAID_CLEANUP_RESERVE_MS + 45_000,
        )?;
        start_cleanup_owner(
            &std::env::current_exe().map_err(|error| error.to_string())?,
            admission_path,
            api_key_env,
            PaidCleanupLimits {
                maximum_cost_microusd: 1_000_000,
                hourly_price_microusd: 1_000_000,
            },
            Duration::from_secs(10),
        )?;
        admission.bind_orchestrator(std::process::id())?;
        for node in admission.snapshot()?.nodes {
            admission.reserve_create(42, node.node_id, 0, node.offer_id, &node.label)?;
            admission.record_contract(node.node_id, 8999 + node.node_id)?;
            admission.reserve_bootstrap(42, node.node_id)?;
        }
        admission.seal_prepared()?;
        admission
    };
    admission.retain_development_fixture(true)?;
    println!(
        "{}",
        serde_json::to_string(&admission.snapshot()?).map_err(|error| error.to_string())?
    );
    Ok(())
}

/// Foreground cleanup requests the existing supervisor, never starts a rival
/// deletion loop. The caller may time out; the independent duty continues.
pub fn await_cleanup_owner(admission_path: &Path, deadline: Duration) -> Result<(), String> {
    let admission = PaidFixtureAdmission::open(admission_path)?;
    admission.enter_cleanup_only()?;
    let until = std::time::Instant::now() + deadline;
    loop {
        let state = admission.snapshot()?;
        if state.cleanup.as_ref().is_some_and(|owner| owner.complete) {
            return Ok(());
        }
        if state
            .cleanup
            .as_ref()
            .is_some_and(|owner| owner.spending_stopped)
        {
            return Err(format!(
                "paid resources absent but accounting failed: {:?}",
                state.accounting_errors
            ));
        }
        if std::time::Instant::now() >= until {
            return Err(format!(
                "independent cleanup remains active/recoverable; unresolved={:?}; accounting_errors={:?}",
                state.unresolved_create_labels(),
                state.accounting_errors
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The only runner-level cleanup integration: reconcile durable sources before
/// touching the provider, delegate to the owner, and retain identities on error.
pub fn cleanup_paid_ownership(
    paid: &mut PaidCampaignState,
    state_path: &Path,
    executable: &Path,
    api_key_env: &str,
    deadline: Duration,
) -> Result<(), String> {
    let until = std::time::Instant::now() + deadline;
    paid.enter_cleanup_only();
    let mut errors = Vec::new();
    // Persist deny-only runner state independently of admission sealing: failure
    // of either durable source must not suppress the other cleanup boundary.
    if let Err(error) = write_paid_state(state_path, paid) {
        errors.push(error);
    }
    let result = (|| -> Result<(), String> {
        let path = paid.state_dir.join("paid-admission.json");
        if !path
            .try_exists()
            .map_err(|error| format!("inspect paid admission: {error}"))?
        {
            if paid.owned_labels.is_empty()
                && paid.owned_contract_ids.is_empty()
                && paid.records.is_empty()
            {
                return paid.complete_cleanup();
            }
            // Legacy runner-only state has no admission capability to reopen.
            // Every incomplete record stays ambiguous until its exact labels appear.
            let api_key = std::env::var(api_key_env).map_err(|_| {
                format!("required VastAI credential environment {api_key_env} is unset")
            })?;
            let client = BlockingVastClient::new(VastClient::new(api_key))?;
            let unresolved = paid
                .records
                .iter()
                .filter(|record| !record.completed)
                .flat_map(|record| record.expected_labels.iter().cloned())
                .collect();
            let labels = paid.owned_labels.clone();
            let ids = paid.owned_contract_ids.clone();
            let census = client
                .cleanup_owned_reconciled(
                    &labels,
                    &ids,
                    &unresolved,
                    until.saturating_duration_since(std::time::Instant::now()),
                    &mut |discovered| {
                        paid.owned_contract_ids.extend(discovered.keys().copied());
                        let observed_labels = discovered.values().cloned().collect::<BTreeSet<_>>();
                        for record in &mut paid.records {
                            record
                                .observed_contract_ids
                                .extend(discovered.iter().filter_map(|(&contract, label)| {
                                    record.expected_labels.contains(label).then_some(contract)
                                }));
                            if record.expected_labels.is_subset(&observed_labels) {
                                record.completed = true;
                            }
                        }
                        write_paid_state(state_path, paid)
                    },
                )
                .map_err(|error| error.detail)?;
            for record in &mut paid.records {
                if !record.completed {
                    let observed_labels = census
                        .discovered_contract_labels
                        .values()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    if !record.expected_labels.is_subset(&observed_labels) {
                        return Err("legacy paid acquisition remains unresolved".to_owned());
                    }
                    record
                        .observed_contract_ids
                        .extend(census.discovered_contract_labels.keys().copied());
                    record.completed = true;
                }
            }
            paid.owned_contract_ids.clear();
            return paid.complete_cleanup();
        }
        let admission = PaidFixtureAdmission::open(&path)?;
        let ownership =
            admission.include_cleanup_ownership(&paid.owned_contract_ids, &paid.owned_labels);
        let requested = admission.request_cleanup();
        for result in [ownership, requested] {
            if let Err(error) = result {
                if let Err(persist_error) = admission.record_cleanup_accounting_error(error.clone())
                {
                    errors.push(persist_error);
                }
                errors.push(error);
            }
        }
        let initial = admission.snapshot()?;
        if let Err(error) = paid.reconcile_admission(&initial) {
            if let Err(persist_error) = admission.record_cleanup_accounting_error(error.clone()) {
                errors.push(persist_error);
            }
            errors.push(error);
        }
        let result = if initial.cleanup.is_some() {
            loop {
                let state = admission.snapshot()?;
                if state.cleanup.as_ref().is_some_and(|owner| owner.complete) {
                    break Ok(());
                }
                if state
                    .cleanup
                    .as_ref()
                    .is_some_and(|owner| owner.spending_stopped)
                {
                    break Err(format!(
                        "paid spending stopped but cleanup accounting failed: {:?}",
                        state.accounting_errors
                    ));
                }
                if std::time::Instant::now() >= until {
                    break Err(format!(
                        "paid cleanup remains recoverable; unresolved={:?}; accounting_errors={:?}",
                        state.unresolved_create_labels(),
                        state.accounting_errors
                    ));
                }
                if !admission.cleanup_owner_active()? {
                    if let Err(error) = spawn_cleanup_supervisor(
                        executable,
                        &path,
                        api_key_env,
                        true,
                        until.min(std::time::Instant::now() + Duration::from_secs(10)),
                    ) {
                        break Err(error);
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        } else {
            // Legacy cleanup uses the same exclusive owner lock and the same
            // bounded engine; it cannot manufacture missing historical limits.
            let _owner = admission.claim_cleanup_owner()?;
            let api_key = std::env::var(api_key_env).map_err(|_| {
                format!("required VastAI credential environment {api_key_env} is unset")
            })?;
            let client = BlockingVastClient::new(VastClient::new(api_key))?;
            client
                .cleanup_owned_reconciled(
                    &initial.cleanup_labels(),
                    &initial.known_contract_ids(),
                    &initial.unresolved_create_labels(),
                    until.saturating_duration_since(std::time::Instant::now()),
                    &mut |discovered| admission.reconcile_cleanup_discovery(discovered),
                )
                .map_err(|error| error.detail)
                .and_then(|census| admission.complete_cleanup(&census.missing_contract_ids))
        };
        let final_state = admission.snapshot()?;
        paid.owned_contract_ids
            .extend(final_state.known_contract_ids());
        paid.owned_labels.extend(final_state.cleanup_labels());
        paid.cleanup_ownership = final_state.cleanup.clone();
        result?;
        for record in &mut paid.records {
            record
                .observed_contract_ids
                .extend(final_state.known_contract_ids());
            record.completed = true;
        }
        paid.acquisition_count = final_state.create_reservations.len() as u32;
        paid.bootstrap_count = final_state.initial_bootstraps.len() as u32;
        paid.owned_contract_ids.clear();
        paid.complete_cleanup()
    })();
    if let Err(error) = result {
        errors.push(error);
    }
    if !errors.is_empty() {
        paid.enter_cleanup_only();
        let cleanup_error = format!("paid cleanup failed: {}", errors.join("; "));
        paid.quarantine_reason = Some(match paid.quarantine_reason.take() {
            Some(reason) => format!("{reason}; {cleanup_error}"),
            None => cleanup_error,
        });
    }
    if let Err(error) = write_paid_state(state_path, paid) {
        errors.push(error);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn durable_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("artifact path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("create artifact parent {}: {error}", parent.display()))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| format!("create temporary artifact: {error}"))?;
    {
        let mut writer = std::io::BufWriter::new(&mut temp);
        serde_json::to_writer(&mut writer, value)
            .map_err(|error| format!("serialize durable artifact: {error}"))?;
        writer
            .write_all(b"\n")
            .and_then(|()| writer.flush())
            .map_err(|error| format!("finish durable artifact: {error}"))?;
    }
    temp.as_file_mut()
        .sync_all()
        .map_err(|error| format!("sync durable artifact: {error}"))?;
    temp.persist(path)
        .map_err(|error| format!("persist durable artifact {}: {error}", path.display()))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync artifact directory {}: {error}", parent.display()))?;
    Ok(())
}

fn unix_ms() -> Result<u64, String> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes epoch: {error}"))?
        .as_millis();
    u64::try_from(value).map_err(|_| "unix timestamp exceeds u64".to_owned())
}

pub fn conservative_selected_cost(
    offers: &[Offer],
    limits: &CampaignLimits,
) -> Result<f64, String> {
    let hourly: f64 = offers.iter().map(|offer| offer.hourly_price).sum();
    let lifetime = hourly * limits.fixture_lifetime_secs as f64 / 3_600.0;
    if !lifetime.is_finite() {
        return Err("selected worst-case cost overflowed".to_owned());
    }
    Ok(lifetime)
}

pub fn scan_artifacts_for_secret(root: &Path, secret: &[u8]) -> Result<(), String> {
    if secret.is_empty() {
        return Err("credential scan marker must not be empty".to_owned());
    }
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("inspect artifact {}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "credential scan cannot certify symlink {}",
                path.display()
            ));
        }
        if metadata.is_dir() {
            for entry in fs::read_dir(&path)
                .map_err(|error| format!("read artifact directory {}: {error}", path.display()))?
            {
                pending.push(
                    entry
                        .map_err(|error| format!("read artifact entry: {error}"))?
                        .path(),
                );
            }
            continue;
        }
        if !metadata.is_file() {
            return Err(format!(
                "credential scan cannot certify special file {}",
                path.display()
            ));
        }
        let bytes = fs::read(&path)
            .map_err(|error| format!("scan artifact {}: {error}", path.display()))?;
        if bytes.windows(secret.len()).any(|window| window == secret) {
            return Err(format!(
                "credential material was persisted in artifact {}",
                path.display()
            ));
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {

    use super::*;
    use crate::campaign::{CampaignLimits, OfferPolicy, ProviderMode};

    fn prepared_admission() -> PaidAdmissionSnapshot {
        PaidAdmissionSnapshot {
            schema_version: 3,
            run_id: 7,
            nodes: (1..=5)
                .map(|id| provisioning::PaidNodeAdmission {
                    node_id: id,
                    offer_id: id,
                    host_id: id + 100,
                    label: format!("myelin-7-{id}-attempt-0"),
                })
                .collect(),
            contracts: (1..=5).map(|id| (id, id + 10)).collect(),
            create_reservations: (1..=5)
                .map(|id| {
                    (
                        id,
                        provisioning::PaidCreateReservation {
                            attempt_id: 0,
                            reserved_unix_ms: 1,
                        },
                    )
                })
                .collect(),
            rejected_creates: BTreeSet::new(),
            initial_bootstraps: (1..=5)
                .map(|id| {
                    (
                        id,
                        provisioning::PaidBootstrapReservation {
                            reserved_unix_ms: 2,
                        },
                    )
                })
                .collect(),
            retained_deployments: Default::default(),
            mode: PaidAdmissionMode::Prepared,
            deadline_unix_ms: u64::MAX,
            cleanup: None,
            discovered_contracts: Default::default(),
            accounting_errors: BTreeSet::new(),
            recovery_contract_ids: BTreeSet::new(),
            recovery_labels: BTreeSet::new(),
        }
    }

    #[test]
    fn matching_labels_and_contracts_cannot_manufacture_bootstrap_evidence() {
        let mut admission = prepared_admission();
        admission.initial_bootstraps.remove(&5);
        let labels = admission.cleanup_labels();
        let mut state = PaidCampaignState::planned("campaign", PathBuf::from("state"));
        state
            .begin_acquisition((1..=5).collect(), labels.clone())
            .unwrap();
        assert!(
            state
                .reconcile_prepared(&admission, (11..=15).collect(), labels)
                .is_err()
        );
        assert_eq!(state.acquisition_count, 0);
        assert_eq!(state.bootstrap_count, 0);
        assert!(!state.records[0].completed);
    }

    #[test]
    fn metered_transfer_prices_cannot_hide_outside_operator_ceiling() {
        let mut offers = (0..5).map(offer).collect::<Vec<_>>();
        assert!(selected_cleanup_limits(&offers, &config().limits).is_ok());
        offers[0].download_cost_per_tb = 0.001;
        assert!(selected_cleanup_limits(&offers, &config().limits).is_err());
        offers[0].download_cost_per_tb = 0.0;
        offers[0].upload_cost_per_tb = 0.001;
        assert!(selected_cleanup_limits(&offers, &config().limits).is_err());
    }

    fn config() -> CampaignConfig {
        CampaignConfig {
            provider: ProviderMode::Real,
            seed: 9,
            runtime_image: "registry.example/myelin:test".to_owned(),
            limits: CampaignLimits {
                fixture_lifetime_secs: 24 * 60 * 60,
                total_cost_usd: 20.0,
                total_hourly_price_usd: 1.0,
                case_deadline_secs: 30,
                max_campaign_allocation_bytes: 8 * 1024 * 1024 * 1024,
                max_campaign_payload_bytes: 512 * 1024 * 1024,
                max_case_race_states: 256,
            },
            offers: OfferPolicy {
                gpu_model: None,
                min_gpu_ram_mb: None,
                min_compute_cap: Some(700),
                min_reliability: Some(0.95),
                min_download_mbps: Some(100.0),
                min_upload_mbps: None,
                max_hourly_price_per_node: Some(0.2),
                blacklist_hosts: Vec::new(),
            },
        }
    }

    fn offer(index: u64) -> Offer {
        Offer {
            offer_id: 100 + index,
            host_id: Some(200 + index),
            gpu_model: "test".to_owned(),
            gpu_ram_mb: Some(8_192.0),
            compute_cap: 800,
            verification: Some("verified".to_owned()),
            reliability: Some(0.99),
            download_mbps: Some(1_000.0),
            upload_mbps: Some(1_000.0),
            location: None,
            hourly_price: 0.1,
            download_cost_per_tb: 0.0,
            upload_cost_per_tb: 0.0,
        }
    }

    #[test]
    fn prepared_state_permanently_denies_more_acquisition() {
        let labels = (1..=5)
            .map(|node| format!("myelin-7-{node}-attempt-0"))
            .collect::<BTreeSet<_>>();
        let mut state = PaidCampaignState::planned("campaign", PathBuf::from("state"));
        state
            .begin_acquisition(vec![1, 2, 3, 4, 5], labels.clone())
            .unwrap();
        state
            .reconcile_prepared(&prepared_admission(), (11..=15).collect(), labels)
            .unwrap();
        assert!(
            state
                .begin_acquisition(vec![1, 2, 3, 4, 5], BTreeSet::new())
                .is_err()
        );
    }

    #[test]
    fn quarantined_fixture_resumes_without_another_acquisition() {
        let labels = (1..=5)
            .map(|node| format!("myelin-7-{node}-attempt-0"))
            .collect::<BTreeSet<_>>();
        let mut state = PaidCampaignState::planned("campaign", PathBuf::from("state"));
        state
            .begin_acquisition(vec![1, 2, 3, 4, 5], labels.clone())
            .unwrap();
        state
            .reconcile_prepared(&prepared_admission(), (11..=15).collect(), labels)
            .unwrap();
        state.quarantine("candidate failed");
        state
            .resume_deny_only(&prepared_admission(), (13..=15).collect())
            .unwrap();
        assert_eq!(state.phase, PaidCampaignPhase::DenyOnly);
        assert_eq!(state.acquisition_count, 5);
        assert_eq!(state.bootstrap_count, 5);
        assert_eq!(state.owned_contract_ids, (13..=15).collect());
        assert!(state.quarantine_reason.is_none());
    }

    #[test]
    fn cleanup_recovery_identity_cannot_become_a_fixture_survivor() {
        let mut admission = prepared_admission();
        let mut state = PaidCampaignState::planned("campaign", PathBuf::from("state"));
        state
            .begin_acquisition((1..=5).collect(), admission.cleanup_labels())
            .unwrap();
        state
            .reconcile_prepared(&admission, (11..=15).collect(), admission.cleanup_labels())
            .unwrap();
        state.quarantine("retained deployment");
        admission.recovery_contract_ids.insert(99);
        assert!(
            state
                .resume_deny_only(&admission, BTreeSet::from([13, 14, 99]))
                .is_err()
        );
        assert_eq!(state.phase, PaidCampaignPhase::Quarantined);
    }

    #[test]
    fn empty_census_does_not_settle_an_ambiguous_acquisition() {
        let labels = (1..=5)
            .map(|node| format!("myelin-7-{node}-attempt-0"))
            .collect::<BTreeSet<_>>();
        let mut state = PaidCampaignState::planned("campaign", PathBuf::from("state"));
        state
            .begin_acquisition(vec![1, 2, 3, 4, 5], labels)
            .unwrap();
        state.enter_cleanup_only();
        assert!(state.complete_cleanup().is_err());
        assert_eq!(state.phase, PaidCampaignPhase::CleanupOnly);
        assert!(!state.phase.permits_acquisition());
        let restored: PaidCampaignState =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        assert!(!restored.records[0].completed);
        assert!(restored.validate().is_ok());
    }

    #[test]
    fn selection_requires_five_verified_distinct_hosts_within_cost() {
        let config = config();
        let plan = CampaignPlan::build(&config, Vec::new()).unwrap();
        let results = OfferSearchResults {
            search_id: 4,
            offers: (0..5).map(offer).collect(),
        };
        assert_eq!(
            select_exact_offers(&results, &config, &plan).unwrap().len(),
            5
        );

        let mut duplicate_hosts = results.clone();
        duplicate_hosts.offers[4].host_id = duplicate_hosts.offers[0].host_id;
        assert!(
            select_exact_offers(&duplicate_hosts, &config, &plan)
                .unwrap_err()
                .contains("distinct")
        );

        let mut unverified = results;
        unverified.offers[0].verification = Some("unverified".to_owned());
        assert!(
            select_exact_offers(&unverified, &config, &plan)
                .unwrap_err()
                .contains("eligible distinct verified")
        );
    }

    #[test]
    fn credential_scan_rejects_nested_artifact_leaks() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("safe.json"), b"{\"safe\":true}").unwrap();
        let marker = b"rare-credential-marker-6e8f";
        scan_artifacts_for_secret(temp.path(), marker).unwrap();
        fs::write(temp.path().join("nested/leak.txt"), marker).unwrap();
        assert!(
            scan_artifacts_for_secret(temp.path(), marker)
                .unwrap_err()
                .contains("credential material")
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_scan_rejects_symlinked_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let marker = b"private-secret-behind-symlink";
        fs::write(outside.path().join("secret"), marker).unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), root.path().join("linked"))
            .unwrap();
        assert!(scan_artifacts_for_secret(root.path(), marker).is_err());
    }

    #[cfg(target_os = "linux")]
    mod supervisor_tests {
        use super::*;
        use std::io::Read;
        use std::os::fd::{AsFd, FromRawFd, OwnedFd};
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixStream;
        use std::process::{Child, Command, Stdio};
        use std::time::Instant;

        const CHILD_ROLE: &str = "MYELIN_SUPERVISOR_REGRESSION_ROLE";
        const CHILD_PATH: &str = "MYELIN_SUPERVISOR_REGRESSION_PATH";
        const RECOVERY_TEST: &str =
            "remote::tests::supervisor_tests::exiting_observed_supervisor_restarts_renewed_cleanup";

        struct FixtureProcess(Child);

        impl Drop for FixtureProcess {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        fn admission(path: &Path) -> PaidFixtureAdmission {
            let admission = PaidFixtureAdmission::create(
                path,
                7,
                prepared_admission().nodes,
                unix_ms().unwrap() + 3_600_000,
            )
            .unwrap();
            admission
                .install_cleanup_ownership(
                    PaidProcessIdentity::current().unwrap(),
                    PaidCleanupLimits {
                        maximum_cost_microusd: 1_000_000,
                        hourly_price_microusd: 1_000_000,
                    },
                )
                .unwrap();
            admission
        }

        fn release_after_supervisor_probe(mut events: File, mut release: UnixStream) {
            let until = Instant::now() + Duration::from_secs(10);
            let mut buffer = [0; 1024];
            loop {
                match events.read(&mut buffer) {
                    Ok(count) if count > 0 => {
                        release.write_all(&[1]).unwrap();
                        return;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    result => panic!("observe supervisor lock probe: {result:?}"),
                }
                assert!(
                    Instant::now() < until,
                    "startup never observed the held lock"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }

        #[test]
        fn exiting_observed_supervisor_restarts_renewed_cleanup() {
            if let Some(role) = std::env::var_os(CHILD_ROLE) {
                let path = PathBuf::from(std::env::var_os(CHILD_PATH).unwrap());
                let admission = PaidFixtureAdmission::open(&path).unwrap();
                let supervisor = admission.claim_cleanup_supervisor().unwrap();
                if role == "observed" {
                    admission.enter_cleanup_only().unwrap();
                    // No create was reserved: the original empty duty is settled.
                    admission.complete_cleanup(&BTreeSet::new()).unwrap();
                    let mut release =
                        UnixStream::from(std::io::stdin().as_fd().try_clone_to_owned().unwrap());
                    release
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    release.write_all(&[1]).unwrap();
                    release.read_exact(&mut [0]).unwrap();
                    // Exit based on the old receipt, after the caller renewed it.
                    drop(supervisor);
                } else {
                    assert_eq!(role, "replacement");
                    let renewed = admission.snapshot().unwrap();
                    assert!(!renewed.cleanup.as_ref().unwrap().spending_stopped);
                    assert!(renewed.create_reservations.is_empty());
                    let _owner = admission.claim_cleanup_owner().unwrap();
                    let completion =
                        File::open(path.parent().unwrap().join("allow-completion")).unwrap();
                    completion.lock().unwrap();
                    admission.complete_cleanup(&BTreeSet::new()).unwrap();
                }
                return;
            }

            let directory = tempfile::tempdir().unwrap();
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let path = directory.path().join("admission.json");
            let admission = admission(&path);
            let executable = std::env::current_exe().unwrap();
            let launcher = directory.path().join("replacement-supervisor");
            fs::write(
                &launcher,
                format!(
                    "#!/bin/sh\n{CHILD_PATH}=\"$2\" {CHILD_ROLE}=replacement exec '{}' --exact '{RECOVERY_TEST}' --nocapture\n",
                    executable.to_str().unwrap().replace('\'', "'\\''"),
                ),
            )
            .unwrap();
            fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700)).unwrap();
            let completion = File::create(directory.path().join("allow-completion")).unwrap();
            completion.lock().unwrap();

            let (mut release, child_socket) = UnixStream::pair().unwrap();
            release
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut observed = FixtureProcess(
                Command::new(&executable)
                    .args(["--exact", RECOVERY_TEST, "--nocapture"])
                    .env(CHILD_ROLE, "observed")
                    .env(CHILD_PATH, &path)
                    .stdin(Stdio::from(OwnedFd::from(child_socket)))
                    .stdout(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            release.read_exact(&mut [0]).unwrap();
            let settled = admission.snapshot().unwrap();
            let old_identity = settled
                .cleanup
                .as_ref()
                .unwrap()
                .supervisor
                .clone()
                .unwrap();
            assert_eq!(old_identity.pid, observed.0.id());
            assert!(old_identity.is_live());
            assert!(settled.cleanup.as_ref().unwrap().spending_stopped);
            admission.request_cleanup().unwrap();
            let renewed = admission.snapshot().unwrap();
            assert!(!renewed.cleanup.as_ref().unwrap().spending_stopped);

            // IN_CLOSE_WRITE arrives only after the active-lock probe has
            // finished with its independently opened fd. Releasing before that
            // probe would test an initially absent supervisor, not this race.
            let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
            assert!(fd >= 0, "{}", std::io::Error::last_os_error());
            let events = unsafe { File::from_raw_fd(fd) };
            let lock_path = std::ffi::CString::new(
                directory
                    .path()
                    .join("admission.json.supervisor.lock")
                    .as_os_str()
                    .as_bytes(),
            )
            .unwrap();
            assert!(
                unsafe { libc::inotify_add_watch(fd, lock_path.as_ptr(), libc::IN_CLOSE_WRITE) }
                    >= 0,
                "{}",
                std::io::Error::last_os_error(),
            );
            let releasing = std::thread::spawn(move || {
                release_after_supervisor_probe(events, release);
            });
            let recovered = spawn_cleanup_supervisor(
                &launcher,
                &path,
                "MYELIN_SUPERVISOR_REGRESSION_UNUSED_CREDENTIAL",
                true,
                Instant::now() + Duration::from_secs(5),
            );
            releasing.join().unwrap();
            assert!(observed.0.wait().unwrap().success());
            assert!(!old_identity.is_live());
            recovered.unwrap();
            let ready = admission.snapshot().unwrap();
            let replacement = ready.cleanup.as_ref().unwrap();
            assert!(admission.cleanup_supervisor_active().unwrap());
            assert!(admission.cleanup_owner_active().unwrap());
            assert!(replacement.supervisor.as_ref().unwrap().is_live());
            assert_eq!(replacement.owner, replacement.supervisor);
            assert_ne!(replacement.supervisor.as_ref(), Some(&old_identity));
            assert!(!replacement.spending_stopped);
            drop(completion);
            await_cleanup_owner(&path, Duration::from_secs(5)).unwrap();

            let completed = admission.snapshot().unwrap();
            let ownership = completed.cleanup.as_ref().unwrap();
            assert!(ownership.complete);
            assert!(ownership.spending_stopped);
            assert_ne!(ownership.supervisor.as_ref(), Some(&old_identity));
            assert_eq!(completed.mode, PaidAdmissionMode::CleanupOnly);
            assert_eq!(completed.create_reservations, renewed.create_reservations);
            assert_eq!(ownership.limits, renewed.cleanup.as_ref().unwrap().limits);
            assert_eq!(
                ownership.stop_unix_ms,
                renewed.cleanup.as_ref().unwrap().stop_unix_ms,
            );
            assert!(
                admission
                    .reserve_create(7, 1, 0, 1, "myelin-7-1-attempt-0")
                    .is_err()
            );
        }

        #[test]
        fn expired_readiness_cannot_authorize_acquisition() {
            let directory = tempfile::tempdir().unwrap();
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let path = directory.path().join("admission.json");
            let admission = admission(&path);
            let _supervisor = admission.claim_cleanup_supervisor().unwrap();
            let _owner = admission.claim_cleanup_owner().unwrap();
            admission.bind_orchestrator(std::process::id()).unwrap();
            admission.require_live_cleanup_owner().unwrap();
            let before = admission.snapshot().unwrap();

            let result = spawn_cleanup_supervisor(
                &directory.path().join("must-not-spawn"),
                &path,
                "MYELIN_SUPERVISOR_REGRESSION_UNUSED_CREDENTIAL",
                false,
                Instant::now(),
            );
            assert!(result.is_err(), "live ownership overrode an expired caller");
            assert!(admission.cleanup_supervisor_active().unwrap());
            assert!(admission.cleanup_owner_active().unwrap());
            assert!(admission.require_live_cleanup_owner().is_err());
            assert!(
                admission
                    .reserve_create(7, 1, 0, 1, "myelin-7-1-attempt-0")
                    .is_err()
            );
            let after = admission.snapshot().unwrap();
            assert_eq!(after.mode, PaidAdmissionMode::CleanupOnly);
            assert_eq!(after.create_reservations, before.create_reservations);
            assert_eq!(after.cleanup, before.cleanup);
        }

        #[test]
        fn expired_recovery_cannot_accept_a_completed_receipt() {
            let directory = tempfile::tempdir().unwrap();
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let path = directory.path().join("admission.json");
            let admission = admission(&path);
            let _supervisor = admission.claim_cleanup_supervisor().unwrap();
            admission.enter_cleanup_only().unwrap();
            admission.complete_cleanup(&BTreeSet::new()).unwrap();
            let before = admission.snapshot().unwrap();

            let result = spawn_cleanup_supervisor(
                &directory.path().join("must-not-spawn"),
                &path,
                "MYELIN_SUPERVISOR_REGRESSION_UNUSED_CREDENTIAL",
                true,
                Instant::now(),
            );
            assert!(result.is_err(), "old completion overrode an expired caller");
            let after = admission.snapshot().unwrap();
            assert_eq!(after.mode, PaidAdmissionMode::CleanupOnly);
            assert_eq!(after.cleanup, before.cleanup);
            assert!(admission.cleanup_supervisor_active().unwrap());
        }
    }
}
