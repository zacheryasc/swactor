//! Durable, fail-closed admission for one five-node paid fixture.
//!
//! The caller supplies a private, pre-existing directory. The sibling `.lock`
//! file is created once and is never replaced or removed by this protocol. Every
//! transaction opens it independently (including cloned handles), locks it, then
//! atomically replaces and fsyncs the state. No lock escapes a synchronous method.
//! Reservations are not leases: an interrupted or failed external operation never
//! releases its slot. Expected labels therefore remain discoverable even when a
//! create response, contract recording, or the creating process is lost.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::plugin::DeploymentIdentity;

pub const PAID_CLEANUP_RESERVE_MS: u64 = 5 * 60 * 1000;
const ORCHESTRATOR_RECOVERY_LIMIT_MS: u64 = 10 * 60 * 1000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaidNodeAdmission {
    pub node_id: u64,
    pub offer_id: u64,
    pub host_id: u64,
    pub label: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaidAdmissionMode {
    /// Durable presearch ownership exists, but no provider acquisition is authorized.
    Selecting,
    Preparing,
    Prepared,
    CleanupOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaidCreateReservation {
    pub attempt_id: u64,
    pub reserved_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaidBootstrapReservation {
    pub reserved_unix_ms: u64,
}

/// One explicit quiescent generation transition, never an acquisition lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaidRetainedDeployment {
    pub artifact_digest: String,
    pub contracts: BTreeMap<u64, u64>,
    pub runner: PaidProcessIdentity,
    pub orchestrator: Option<PaidProcessIdentity>,
    pub bootstraps: BTreeMap<u64, PaidBootstrapReservation>,
}

/// Immutable integer micro-dollar ceilings. `hourly_price_microusd` is the
/// authorized aggregate hourly maximum used conservatively before selection;
/// selected offers must also have zero metered transfer prices.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaidCleanupLimits {
    pub maximum_cost_microusd: u64,
    pub hourly_price_microusd: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaidProcessIdentity {
    pub pid: u32,
    pub start_ticks: u64,
    pub boot_id: String,
}

impl PaidProcessIdentity {
    pub fn current() -> Result<Self, String> {
        Self::read(std::process::id())
    }

    fn read(pid: u32) -> Result<Self, String> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).map_err(io_error)?;
        let fields = stat
            .rsplit_once(") ")
            .ok_or_else(|| "malformed cleanup process identity".to_owned())?
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        if fields
            .first()
            .is_none_or(|state| matches!(*state, "Z" | "X"))
        {
            return Err("cleanup process is not running".to_owned());
        }
        let start_ticks = fields
            .get(19)
            .ok_or_else(|| "cleanup process start identity is absent".to_owned())?
            .parse()
            .map_err(|error| format!("cleanup process start identity: {error}"))?;
        Ok(Self {
            pid,
            start_ticks,
            boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .map_err(io_error)?
                .trim()
                .to_owned(),
        })
    }

    pub fn is_live(&self) -> bool {
        Self::read(self.pid).is_ok_and(|identity| identity == *self)
    }

    /// Zombies remain present until their parent has reaped them. Any observation
    /// error other than a missing/reused PID conservatively keeps the barrier held.
    fn incarnation_present(&self) -> bool {
        let stat = match fs::read_to_string(format!("/proc/{}/stat", self.pid)) {
            Ok(stat) => stat,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return false,
            Err(_) => return true,
        };
        let Some(fields) = stat.rsplit_once(") ").map(|(_, fields)| fields) else {
            return true;
        };
        let Some(start_ticks) = fields
            .split_whitespace()
            .nth(19)
            .and_then(|ticks| ticks.parse::<u64>().ok())
        else {
            return true;
        };
        let boot_id = match fs::read_to_string("/proc/sys/kernel/random/boot_id") {
            Ok(boot_id) => boot_id,
            Err(_) => return true,
        };
        start_ticks == self.start_ticks && boot_id.trim() == self.boot_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaidCleanupOwnership {
    pub runner: PaidProcessIdentity,
    pub owner: Option<PaidProcessIdentity>,
    #[serde(default)]
    pub supervisor: Option<PaidProcessIdentity>,
    #[serde(default)]
    pub orchestrator: Option<PaidProcessIdentity>,
    #[serde(default)]
    pub orchestrator_recovery_until_unix_ms: Option<u64>,
    pub limits: PaidCleanupLimits,
    pub started_unix_ms: u64,
    pub stop_unix_ms: u64,
    pub heartbeat_unix_ms: u64,
    pub retained_development_fixture: bool,
    pub complete: bool,
    #[serde(default)]
    pub spending_stopped: bool,
    /// Provider deletion is durably held until the bound orchestrator is stopped.
    #[serde(default)]
    pub orchestrator_shutdown_pending: bool,
}

impl PaidCleanupOwnership {
    fn live_at(&self, now: u64) -> bool {
        !self.complete
            && now < self.stop_unix_ms
            && now >= self.started_unix_ms
            && now >= self.heartbeat_unix_ms
            && now.saturating_sub(self.heartbeat_unix_ms) <= 30_000
            && self
                .owner
                .as_ref()
                .is_some_and(PaidProcessIdentity::is_live)
    }
}

/// An OS-held exclusive lock, not a PID claim. Dropping/crashing releases only
/// the supervisor lock; durable reservations and cleanup obligations remain.
pub struct PaidCleanupOwner {
    admission: PaidFixtureAdmission,
    identity: PaidProcessIdentity,
    _lock: File,
}

impl PaidCleanupOwner {
    /// Returns true when spending-stop cleanup must run. An orderly shutdown
    /// barrier withholds provider deletion only while the original runner,
    /// supervisor, deadline, and bound orchestrator all remain live.
    pub fn tick(&self) -> Result<bool, String> {
        self.admission.update(false, |state, now| {
            let ownership = state
                .cleanup
                .as_mut()
                .ok_or_else(|| "cleanup ownership is absent".to_owned())?;
            if ownership.owner.as_ref() != Some(&self.identity) {
                return Err("cleanup owner incarnation changed".to_owned());
            }
            let clock_reversed =
                now < ownership.started_unix_ms || now < ownership.heartbeat_unix_ms;
            ownership.heartbeat_unix_ms = now;
            let shutdown_pending = ownership.orchestrator_shutdown_pending;
            let automatic_cleanup = now >= ownership.stop_unix_ms
                || clock_reversed
                || ownership
                    .supervisor
                    .as_ref()
                    .is_some_and(|process| !process.is_live())
                || ownership
                    .orchestrator_recovery_until_unix_ms
                    .is_some_and(|until| now >= until || !ownership.runner.is_live())
                || (!ownership.retained_development_fixture
                    && (!ownership.runner.is_live()
                        || (ownership.orchestrator_recovery_until_unix_ms.is_none()
                            && ownership.orchestrator.as_ref().is_some_and(|process| {
                                if shutdown_pending {
                                    !process.incarnation_present()
                                } else {
                                    !process.is_live()
                                }
                            }))));
            if automatic_cleanup {
                state.mode = PaidAdmissionMode::CleanupOnly;
                ownership.orchestrator_shutdown_pending = false;
            }
            Ok(())
        })?;
        let state = self.admission.snapshot()?;
        Ok(state.mode == PaidAdmissionMode::CleanupOnly
            && state
                .cleanup
                .as_ref()
                .is_none_or(|ownership| !ownership.orchestrator_shutdown_pending))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaidAdmissionSnapshot {
    pub schema_version: u32,
    pub run_id: u64,
    pub nodes: Vec<PaidNodeAdmission>,
    /// Logical node -> exact provider contract. Never removed during cleanup.
    pub contracts: BTreeMap<u64, u64>,
    /// Logical node -> consumed create slot, including ambiguous outcomes.
    pub create_reservations: BTreeMap<u64, PaidCreateReservation>,
    /// Consumed create slots with an authoritative provider rejection.
    /// Empty/missing census results never populate this set.
    #[serde(default)]
    pub rejected_creates: BTreeSet<u64>,
    /// Logical node -> the single initial logical bootstrap session.
    pub initial_bootstraps: BTreeMap<u64, PaidBootstrapReservation>,
    #[serde(default)]
    pub retained_deployments: BTreeMap<String, PaidRetainedDeployment>,
    pub mode: PaidAdmissionMode,
    pub deadline_unix_ms: u64,
    #[serde(default)]
    pub cleanup: Option<PaidCleanupOwnership>,
    /// All attributable identities, including duplicates that cannot be mapped.
    #[serde(default)]
    pub discovered_contracts: BTreeMap<u64, String>,
    #[serde(default)]
    pub accounting_errors: BTreeSet<String>,
    #[serde(default)]
    pub recovery_contract_ids: BTreeSet<u64>,
    #[serde(default)]
    pub recovery_labels: BTreeSet<String>,
}

impl PaidAdmissionSnapshot {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != 3 {
            return Err("unsupported paid admission schema".to_owned());
        }
        if self.nodes.is_empty() {
            if !matches!(
                self.mode,
                PaidAdmissionMode::Selecting | PaidAdmissionMode::CleanupOnly
            ) || self.cleanup.is_none()
                || !self.contracts.is_empty()
                || !self.create_reservations.is_empty()
                || !self.rejected_creates.is_empty()
                || !self.initial_bootstraps.is_empty()
                || !self.retained_deployments.is_empty()
            {
                return Err("invalid acquisition-free paid admission state".to_owned());
            }
            if self.mode == PaidAdmissionMode::Selecting
                && (!self.discovered_contracts.is_empty()
                    || !self.accounting_errors.is_empty()
                    || !self.recovery_contract_ids.is_empty()
                    || !self.recovery_labels.is_empty()
                    || self.cleanup.as_ref().is_some_and(|owner| {
                        owner.complete
                            || owner.spending_stopped
                            || owner.retained_development_fixture
                            || owner.orchestrator_recovery_until_unix_ms.is_some()
                            || owner.orchestrator_shutdown_pending
                    }))
            {
                return Err("presearch paid admission already entered recovery".to_owned());
            }
        } else {
            validate_nodes(&self.nodes)?;
            if self.mode == PaidAdmissionMode::Selecting {
                return Err("selecting paid admission cannot own provider nodes".to_owned());
            }
        }
        if self.deadline_unix_ms == 0 {
            return Err("paid admission deadline is absent".to_owned());
        }
        if self.recovery_contract_ids.contains(&0)
            || self.recovery_labels.iter().any(|label| !valid_label(label))
        {
            return Err("invalid durable recovery ownership scope".to_owned());
        }
        if let Some(owner) = &self.cleanup {
            if owner.stop_unix_ms > self.deadline_unix_ms
                || owner.started_unix_ms >= owner.stop_unix_ms
                || owner.limits.maximum_cost_microusd == 0
                || owner.limits.hourly_price_microusd == 0
                || (owner.orchestrator_shutdown_pending
                    && (self.mode != PaidAdmissionMode::CleanupOnly
                        || owner.orchestrator.is_none()
                        || owner.complete
                        || owner.spending_stopped
                        || owner.retained_development_fixture))
            {
                return Err("invalid immutable paid cleanup limits".to_owned());
            }
        }
        let mut contracts = BTreeSet::new();
        for (&node_id, reservation) in &self.create_reservations {
            if !self.nodes.iter().any(|node| node.node_id == node_id)
                // Attempt 0 is the orchestrator's initial provisioning
                // attempt; admission labels reserve `...-attempt-0`.
                || reservation.reserved_unix_ms >= self.deadline_unix_ms
            {
                return Err("invalid paid create reservation".to_owned());
            }
        }
        for node_id in &self.rejected_creates {
            if !self.create_reservations.contains_key(node_id)
                || self.contracts.contains_key(node_id)
            {
                return Err("paid create rejection contradicts ownership accounting".to_owned());
            }
        }
        for (&node_id, &contract_id) in &self.contracts {
            if !self.create_reservations.contains_key(&node_id)
                || contract_id == 0
                || !contracts.insert(contract_id)
            {
                return Err("invalid or duplicated paid contract mapping".to_owned());
            }
        }
        for (&node_id, reservation) in &self.initial_bootstraps {
            if !self.contracts.contains_key(&node_id)
                || reservation.reserved_unix_ms >= self.deadline_unix_ms
            {
                return Err("invalid paid bootstrap reservation".to_owned());
            }
        }
        for (generation, deployment) in &self.retained_deployments {
            validate_deployment_identity(&deployment.artifact_digest, generation)?;
            if deployment.contracts != self.contracts
                || deployment.contracts.len() != 5
                || deployment.bootstraps.iter().any(|(node, reservation)| {
                    !deployment.contracts.contains_key(node)
                        || reservation.reserved_unix_ms >= self.deadline_unix_ms
                })
            {
                return Err("invalid retained paid deployment scope".to_owned());
            }
        }
        if self.mode == PaidAdmissionMode::Prepared
            && (self.contracts.len() != 5 || self.initial_bootstraps.len() != 5)
        {
            return Err("prepared paid admission is incomplete".to_owned());
        }
        Ok(())
    }

    pub fn known_contract_ids(&self) -> BTreeSet<u64> {
        self.contracts
            .values()
            .copied()
            .chain(self.discovered_contracts.keys().copied())
            .chain(self.recovery_contract_ids.iter().copied())
            .collect()
    }

    pub fn unresolved_create_labels(&self) -> BTreeSet<String> {
        self.nodes
            .iter()
            .filter(|node| {
                self.create_reservations.contains_key(&node.node_id)
                    && !self.contracts.contains_key(&node.node_id)
                    && !self.rejected_creates.contains(&node.node_id)
            })
            .map(|node| node.label.clone())
            .collect()
    }

    pub fn cleanup_labels(&self) -> BTreeSet<String> {
        self.nodes
            .iter()
            .map(|node| node.label.clone())
            .chain(self.recovery_labels.iter().cloned())
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct PaidFixtureAdmission {
    path: PathBuf,
    lock_path: PathBuf,
    #[cfg(unix)]
    lock_identity: (u64, u64),
}

impl PaidFixtureAdmission {
    /// Initializes a legacy authorization whose exact topology is already known.
    /// New paid runs should use [`Self::create_cleanup_only`] before offer search.
    pub fn create(
        path: impl AsRef<Path>,
        run_id: u64,
        nodes: Vec<PaidNodeAdmission>,
        deadline_unix_ms: u64,
    ) -> Result<Self, String> {
        validate_nodes(&nodes)?;
        if deadline_unix_ms <= unix_ms()? {
            return Err("paid admission requires a future deadline".to_owned());
        }
        Self::initialize(
            path.as_ref(),
            new_snapshot(
                run_id,
                nodes,
                PaidAdmissionMode::Preparing,
                deadline_unix_ms,
                None,
            ),
        )
    }

    /// Creates immutable run and cleanup ownership before provider offer search.
    /// The selecting state owns no labels or acquisition slots and cannot admit
    /// provider work until [`Self::bind_selected_nodes`] commits one exact topology.
    pub fn create_cleanup_only(
        path: impl AsRef<Path>,
        run_id: u64,
        deadline_unix_ms: u64,
        runner: PaidProcessIdentity,
        limits: PaidCleanupLimits,
    ) -> Result<Self, String> {
        let now = unix_ms()?;
        if deadline_unix_ms <= now {
            return Err("paid admission requires a future deadline".to_owned());
        }
        let cleanup = new_cleanup_ownership(runner, limits, deadline_unix_ms, now)?;
        Self::initialize(
            path.as_ref(),
            new_snapshot(
                run_id,
                Vec::new(),
                PaidAdmissionMode::Selecting,
                deadline_unix_ms,
                Some(cleanup),
            ),
        )
    }

    /// A previously used lock path can never be reinitialized, even if its state
    /// is missing following a crash or deletion.
    fn initialize(path: &Path, state: PaidAdmissionSnapshot) -> Result<Self, String> {
        state.validate()?;
        let path = private_state_path(path)?;
        if path.try_exists().map_err(io_error)? || fs::symlink_metadata(&path).is_ok() {
            return Err("paid admission state already exists".to_owned());
        }
        let lock_path = sibling(&path, ".lock");
        let lock = private_options()
            .write(true)
            .read(true)
            .create_new(true)
            .open(&lock_path)
            .map_err(io_error)?;
        // Failure from this point deliberately leaves the lock inode in place.
        lock.lock().map_err(io_error)?;
        lock.sync_all().map_err(io_error)?;
        sync_parent(&path)?;
        let admission = Self {
            path,
            lock_path,
            #[cfg(unix)]
            lock_identity: file_identity(&lock)?,
        };
        admission.persist(&state)?;
        Ok(admission)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = private_state_path(path.as_ref())?;
        let lock_path = sibling(&path, ".lock");
        let lock = open_private_file(&lock_path, true)?;
        let admission = Self {
            path,
            lock_path,
            #[cfg(unix)]
            lock_identity: file_identity(&lock)?,
        };
        admission.snapshot()?;
        Ok(admission)
    }

    pub fn snapshot(&self) -> Result<PaidAdmissionSnapshot, String> {
        let _lock = self.lock()?;
        self.load()
    }

    /// Installs immutable limits before a cleanup supervisor can authorize work.
    /// Installing ownership over legacy/live reservations is recovery-only.
    pub fn install_cleanup_ownership(
        &self,
        runner: PaidProcessIdentity,
        limits: PaidCleanupLimits,
    ) -> Result<(), String> {
        self.update(false, |state, now| {
            if state.cleanup.is_some() {
                return Err("paid cleanup ownership and limits cannot be replaced".to_owned());
            }
            let ownership = new_cleanup_ownership(runner, limits, state.deadline_unix_ms, now)?;
            if !state.create_reservations.is_empty() {
                state.mode = PaidAdmissionMode::CleanupOnly;
            }
            state.cleanup = Some(ownership);
            Ok(())
        })
    }

    pub fn claim_cleanup_owner(&self) -> Result<PaidCleanupOwner, String> {
        let lock = self.claim_process_lock(".owner.lock")?;
        let identity = PaidProcessIdentity::current()?;
        self.update(false, |state, now| {
            let Some(ownership) = state.cleanup.as_mut() else {
                state.mode = PaidAdmissionMode::CleanupOnly;
                return Ok(());
            };
            if ownership.owner.is_some() || now >= ownership.stop_unix_ms {
                state.mode = PaidAdmissionMode::CleanupOnly;
                ownership.retained_development_fixture = false;
                ownership.orchestrator_shutdown_pending = false;
            }
            ownership.owner = Some(identity.clone());
            ownership.heartbeat_unix_ms = now;
            Ok(())
        })?;
        Ok(PaidCleanupOwner {
            admission: self.clone(),
            identity,
            _lock: lock,
        })
    }

    /// The detached restart supervisor owns a different exclusive OS lock from
    /// its replaceable cleanup worker. Restarting a supervisor is deny-only too.
    pub fn claim_cleanup_supervisor(&self) -> Result<File, String> {
        let lock = self.claim_process_lock(".supervisor.lock")?;
        let identity = PaidProcessIdentity::current()?;
        self.update(false, |state, _| {
            let ownership = state.cleanup.as_mut().ok_or_else(|| {
                "cleanup supervisor requires original immutable limits".to_owned()
            })?;
            if ownership.supervisor.is_some() || ownership.owner.is_some() {
                state.mode = PaidAdmissionMode::CleanupOnly;
                ownership.retained_development_fixture = false;
                ownership.orchestrator_shutdown_pending = false;
            }
            ownership.supervisor = Some(identity);
            Ok(())
        })?;
        Ok(lock)
    }

    pub fn cleanup_supervisor_active(&self) -> Result<bool, String> {
        self.process_lock_active(".supervisor.lock")
    }

    fn claim_process_lock(&self, suffix: &str) -> Result<File, String> {
        let path = sibling(&self.path, suffix);
        let lock = if fs::symlink_metadata(&path).is_ok() {
            open_private_file(&path, true)?
        } else {
            let file = private_options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            sync_parent(&path)?;
            file
        };
        lock.try_lock()
            .map_err(|error| format!("paid process owner already active: {error}"))?;
        Ok(lock)
    }

    /// Lock ownership, unlike heartbeat expiry, remains observable during slow
    /// provider cleanup and after the original spending deadline.
    pub fn cleanup_owner_active(&self) -> Result<bool, String> {
        self.process_lock_active(".owner.lock")
    }

    fn process_lock_active(&self, suffix: &str) -> Result<bool, String> {
        let path = sibling(&self.path, suffix);
        if !path.try_exists().map_err(io_error)? {
            return Ok(false);
        }
        let lock = open_private_file(&path, true)?;
        match lock.try_lock() {
            Ok(()) => Ok(false),
            Err(std::fs::TryLockError::WouldBlock) => Ok(true),
            Err(error) => Err(format!("observe cleanup ownership lock: {error}")),
        }
    }

    /// Permanently denies provider work before an orderly orchestrator stop.
    /// Cleanup ownership remains active, but provider deletion is held until
    /// [`Self::release_cleanup_after_orchestrator_stopped`] seals the barrier.
    pub fn begin_cleanup_shutdown(&self) -> Result<(), String> {
        self.require_live_cleanup_owner()?;
        let runner = PaidProcessIdentity::current()?;
        self.update(false, |state, now| {
            let ownership = state
                .cleanup
                .as_mut()
                .filter(|ownership| ownership.live_at(now))
                .ok_or_else(|| "cleanup shutdown requires live original ownership".to_owned())?;
            if ownership.runner != runner {
                return Err("cleanup shutdown requires the original runner".to_owned());
            }
            state.mode = PaidAdmissionMode::CleanupOnly;
            ownership.retained_development_fixture = false;
            ownership.complete = false;
            ownership.spending_stopped = false;
            ownership.orchestrator_shutdown_pending = ownership.orchestrator.is_some();
            Ok(())
        })
    }

    /// Releases provider cleanup only after the bound process incarnation has
    /// disappeared. The caller must invoke this after it has waited/reaped.
    pub fn release_cleanup_after_orchestrator_stopped(&self) -> Result<(), String> {
        let runner = PaidProcessIdentity::current()?;
        self.update(false, |state, _| {
            if state.mode != PaidAdmissionMode::CleanupOnly {
                return Err("cleanup release requires prior durable deny-only shutdown".to_owned());
            }
            let ownership = state
                .cleanup
                .as_mut()
                .ok_or_else(|| "cleanup release requires original ownership".to_owned())?;
            if ownership.runner != runner {
                return Err("cleanup release requires the original runner".to_owned());
            }
            if !ownership.orchestrator_shutdown_pending {
                return Ok(());
            }
            if ownership
                .orchestrator
                .as_ref()
                .is_some_and(PaidProcessIdentity::incarnation_present)
            {
                return Err("provider cleanup is held until the orchestrator is reaped".to_owned());
            }
            ownership.orchestrator_shutdown_pending = false;
            Ok(())
        })
    }

    /// A new foreground cleanup requires new provider absence, not a cached
    /// completion receipt. Exact historical identities are never removed.
    pub fn request_cleanup(&self) -> Result<(), String> {
        self.update(false, |state, _| {
            state.mode = PaidAdmissionMode::CleanupOnly;
            if let Some(owner) = state.cleanup.as_mut() {
                owner.complete = false;
                owner.spending_stopped = false;
            }
            Ok(())
        })
    }

    pub fn include_cleanup_ownership(
        &self,
        ids: &BTreeSet<u64>,
        labels: &BTreeSet<String>,
    ) -> Result<(), String> {
        self.update(false, |state, _| {
            if ids.contains(&0) || labels.iter().any(|label| !valid_label(label)) {
                return Err("invalid exact durable runner cleanup ownership".to_owned());
            }
            state.recovery_contract_ids.extend(ids);
            state.recovery_labels.extend(labels.iter().cloned());
            Ok(())
        })
    }

    pub fn require_live_cleanup_owner(&self) -> Result<(), String> {
        let state = self.snapshot()?;
        if state
            .cleanup
            .as_ref()
            .is_some_and(|owner| owner.live_at(unix_ms().unwrap_or(u64::MAX)))
            && self.cleanup_owner_active()?
            && matches!(
                state.mode,
                PaidAdmissionMode::Preparing | PaidAdmissionMode::Prepared
            )
        {
            Ok(())
        } else {
            Err("paid work requires a live independent cleanup owner at original expiry".to_owned())
        }
    }

    /// Bind an exact process incarnation before any create/bootstrap reservation.
    /// The first binding is allowed in the acquisition-free selecting phase;
    /// rebinding requires explicit retained development or bounded formal recovery.
    pub fn bind_orchestrator(&self, pid: u32) -> Result<(), String> {
        let selecting = self.snapshot()?.mode == PaidAdmissionMode::Selecting;
        if selecting {
            if !self.cleanup_owner_active()? {
                self.enter_cleanup_only()?;
                return Err("orchestrator binding requires a live cleanup owner".to_owned());
            }
        } else {
            self.require_live_cleanup_owner()?;
        }
        let identity = match PaidProcessIdentity::read(pid) {
            Ok(identity) => identity,
            Err(error) if selecting => {
                self.enter_cleanup_only()?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let runner = PaidProcessIdentity::current()?;
        let mut rejected_selecting = false;
        self.update(false, |state, now| {
            let owner_live = state
                .cleanup
                .as_ref()
                .is_some_and(|owner| owner.live_at(now));
            if !owner_live {
                if state.mode == PaidAdmissionMode::Selecting {
                    state.mode = PaidAdmissionMode::CleanupOnly;
                    rejected_selecting = true;
                    return Ok(());
                }
                return Err("orchestrator binding requires a live cleanup owner".to_owned());
            }
            let denied = {
                let owner = state.cleanup.as_ref().expect("live ownership checked");
                state.mode == PaidAdmissionMode::CleanupOnly
                    || (!owner.retained_development_fixture && owner.runner != runner)
                    || owner
                        .orchestrator_recovery_until_unix_ms
                        .is_some_and(|until| now >= until)
                    || (owner
                        .orchestrator
                        .as_ref()
                        .is_some_and(|old| old != &identity)
                        && owner.orchestrator_recovery_until_unix_ms.is_none()
                        && !owner.retained_development_fixture)
            };
            if denied {
                if state.mode == PaidAdmissionMode::Selecting {
                    state.mode = PaidAdmissionMode::CleanupOnly;
                    state
                        .cleanup
                        .as_mut()
                        .expect("live ownership checked")
                        .retained_development_fixture = false;
                    rejected_selecting = true;
                    return Ok(());
                }
                return Err("orchestrator rebinding lacks a live authorized transition".to_owned());
            }
            let owner = state.cleanup.as_mut().expect("live ownership checked");
            owner.runner = runner;
            for deployment in state.retained_deployments.values_mut() {
                if deployment.runner == owner.runner && deployment.orchestrator.is_none() {
                    deployment.orchestrator = Some(identity.clone());
                }
            }
            owner.orchestrator = Some(identity);
            owner.orchestrator_recovery_until_unix_ms = None;
            owner.retained_development_fixture = false;
            Ok(())
        })?;
        if rejected_selecting {
            Err("orchestrator binding closed presearch acquisition".to_owned())
        } else {
            Ok(())
        }
    }

    /// Atomically binds the only five selected offers and opens their one-shot
    /// create slots. Ownership, deadlines, limits, and lock identity are unchanged.
    pub fn bind_selected_nodes(&self, nodes: Vec<PaidNodeAdmission>) -> Result<(), String> {
        validate_nodes(&nodes)?;
        if !self.cleanup_owner_active()? || !self.cleanup_supervisor_active()? {
            self.enter_cleanup_only()?;
            return Err(
                "selected offers require live cleanup ownership and restart supervision".to_owned(),
            );
        }
        let runner = PaidProcessIdentity::current()?;
        let mut rejected = false;
        self.update(false, |state, now| {
            if state.mode != PaidAdmissionMode::Selecting
                || !state.nodes.is_empty()
                || !state.contracts.is_empty()
                || !state.create_reservations.is_empty()
                || !state.rejected_creates.is_empty()
                || !state.initial_bootstraps.is_empty()
                || !state.retained_deployments.is_empty()
                || !state.discovered_contracts.is_empty()
                || !state.accounting_errors.is_empty()
                || !state.recovery_contract_ids.is_empty()
                || !state.recovery_labels.is_empty()
            {
                return Err("selected paid offers can be bound exactly once".to_owned());
            }
            let live = state.cleanup.as_ref().is_some_and(|owner| {
                owner.live_at(now)
                    && owner.runner == runner
                    && owner.runner.is_live()
                    && owner.orchestrator_recovery_until_unix_ms.is_none()
                    && owner
                        .supervisor
                        .as_ref()
                        .is_some_and(PaidProcessIdentity::is_live)
                    && owner
                        .orchestrator
                        .as_ref()
                        .is_some_and(PaidProcessIdentity::is_live)
                    && !owner.spending_stopped
            });
            if now >= state.deadline_unix_ms || !live {
                state.mode = PaidAdmissionMode::CleanupOnly;
                if let Some(owner) = state.cleanup.as_mut() {
                    owner.retained_development_fixture = false;
                    owner.orchestrator_shutdown_pending = false;
                }
                rejected = true;
                return Ok(());
            }
            state.nodes = nodes;
            state.mode = PaidAdmissionMode::Preparing;
            Ok(())
        })?;
        if rejected {
            Err("selected offers require the original live cleanup ownership".to_owned())
        } else {
            Ok(())
        }
    }

    /// Authorize one intentional orchestrator restart, never acquisition.
    /// Runner loss or the original spending-stop deadline still forces cleanup.
    pub fn begin_orchestrator_recovery(&self) -> Result<(), String> {
        self.require_live_cleanup_owner()?;
        let runner = PaidProcessIdentity::current()?;
        self.update(false, |state, now| {
            let owner = state
                .cleanup
                .as_mut()
                .filter(|owner| owner.live_at(now))
                .ok_or_else(|| "orchestrator recovery requires a live cleanup owner".to_owned())?;
            if state.mode != PaidAdmissionMode::Prepared
                || owner.runner != runner
                || owner.orchestrator_recovery_until_unix_ms.is_some()
                || !owner
                    .orchestrator
                    .as_ref()
                    .is_some_and(PaidProcessIdentity::is_live)
            {
                return Err(
                    "formal recovery requires a live prepared orchestrator and runner".to_owned(),
                );
            }
            owner.orchestrator_recovery_until_unix_ms = Some(
                now.saturating_add(ORCHESTRATOR_RECOVERY_LIMIT_MS)
                    .min(owner.stop_unix_ms),
            );
            Ok(())
        })
    }

    pub fn retain_development_fixture(&self, explicitly_authorized: bool) -> Result<(), String> {
        self.require_live_cleanup_owner()?;
        if !self.cleanup_supervisor_active()? {
            return Err("retention requires an independent restart supervisor".to_owned());
        }
        self.update(false, |state, now| {
            if !explicitly_authorized || state.mode != PaidAdmissionMode::Prepared {
                return Err(
                    "retention requires explicit development authorization and prepared admission"
                        .to_owned(),
                );
            }
            let owner = state
                .cleanup
                .as_mut()
                .filter(|owner| owner.live_at(now))
                .ok_or_else(|| "retention requires a live cleanup owner".to_owned())?;
            if !owner
                .supervisor
                .as_ref()
                .is_some_and(PaidProcessIdentity::is_live)
            {
                return Err("retention requires a live restart supervisor".to_owned());
            }
            owner.retained_development_fixture = true;
            Ok(())
        })
    }

    /// Called only by explicit retained development, before starting a new
    /// orchestrator. Historical slots and exact contract identities never move.
    pub fn authorize_retained_deployment(
        &self,
        explicitly_authorized: bool,
        deployment: DeploymentIdentity,
    ) -> Result<(), String> {
        validate_deployment_identity(
            &deployment.artifact_digest,
            &deployment.deployment_generation,
        )?;
        self.require_live_cleanup_owner()?;
        if !self.cleanup_supervisor_active()? {
            return Err("retained deployment requires restart supervision".to_owned());
        }
        let runner = PaidProcessIdentity::current()?;
        self.update(false, |state, now| {
            let owner = state
                .cleanup
                .as_ref()
                .filter(|owner| owner.live_at(now))
                .ok_or("retained deployment requires a live cleanup owner")?;
            if !explicitly_authorized
                || state.mode != PaidAdmissionMode::Prepared
                || !owner.retained_development_fixture
                || owner.orchestrator_recovery_until_unix_ms.is_some()
                || owner
                    .orchestrator
                    .as_ref()
                    .is_none_or(PaidProcessIdentity::is_live)
                || (owner.runner != runner && owner.runner.is_live())
                || !owner
                    .supervisor
                    .as_ref()
                    .is_some_and(PaidProcessIdentity::is_live)
                || state
                    .retained_deployments
                    .contains_key(&deployment.deployment_generation)
                || state
                    .retained_deployments
                    .values()
                    .any(|prior| prior.orchestrator.is_none())
            {
                return Err(
                    "retained deployment lacks an explicit quiescent fresh generation transition"
                        .to_owned(),
                );
            }
            state.retained_deployments.insert(
                deployment.deployment_generation,
                PaidRetainedDeployment {
                    artifact_digest: deployment.artifact_digest,
                    contracts: state.contracts.clone(),
                    runner,
                    orchestrator: None,
                    bootstraps: BTreeMap::new(),
                },
            );
            Ok(())
        })
    }

    pub fn reserve_retained_bootstrap(
        &self,
        run_id: u64,
        node_id: u64,
        contract_id: u64,
        deployment: &DeploymentIdentity,
    ) -> Result<(), String> {
        self.require_live_cleanup_owner()?;
        let orchestrator = PaidProcessIdentity::current()?;
        self.update(false, |state, now| {
            let owner = state
                .cleanup
                .as_ref()
                .filter(|owner| owner.live_at(now))
                .ok_or("retained bootstrap requires a live cleanup owner")?;
            if state.mode != PaidAdmissionMode::Prepared
                || state.run_id != run_id
                || !owner.runner.is_live()
                || owner.orchestrator.as_ref() != Some(&orchestrator)
                || owner.orchestrator_recovery_until_unix_ms.is_some()
            {
                return Err(
                    "retained bootstrap is outside its bound generation transition".to_owned(),
                );
            }
            let authorization = state
                .retained_deployments
                .get_mut(&deployment.deployment_generation)
                .filter(|authorization| {
                    authorization.artifact_digest == deployment.artifact_digest
                        && authorization.runner == owner.runner
                        && authorization.orchestrator.as_ref() == Some(&orchestrator)
                        && authorization.contracts.get(&node_id) == Some(&contract_id)
                })
                .ok_or("retained bootstrap has no exact deployment authorization")?;
            if authorization.bootstraps.contains_key(&node_id) {
                return Err("retained generation bootstrap slot is permanently consumed".to_owned());
            }
            authorization.bootstraps.insert(
                node_id,
                PaidBootstrapReservation {
                    reserved_unix_ms: now,
                },
            );
            Ok(())
        })
    }

    /// Persist every discovery before deletion, even if duplicate accounting is
    /// invalid. Errors poison acceptance but must never block spending-stop.
    pub fn reconcile_cleanup_discovery(
        &self,
        discovered: &BTreeMap<u64, String>,
    ) -> Result<(), String> {
        self.update(false, |state, _| {
            for (&contract, label) in discovered {
                let Some(node) = state.nodes.iter().find(|node| &node.label == label) else {
                    if !state.recovery_labels.contains(label) {
                        return Err("cleanup discovery escaped exact authorized labels".to_owned());
                    }
                    state.discovered_contracts.insert(contract, label.clone());
                    state
                        .accounting_errors
                        .insert(format!("unmapped durable runner label {label}"));
                    continue;
                };
                state.discovered_contracts.insert(contract, label.clone());
                if !state.create_reservations.contains_key(&node.node_id)
                    || state.rejected_creates.contains(&node.node_id)
                    || state
                        .contracts
                        .get(&node.node_id)
                        .is_some_and(|old| *old != contract)
                    || state
                        .contracts
                        .iter()
                        .any(|(id, old)| *id != node.node_id && *old == contract)
                {
                    state.accounting_errors.insert(format!(
                        "ambiguous owned contract {contract} for node {}",
                        node.node_id
                    ));
                } else {
                    state.contracts.insert(node.node_id, contract);
                }
            }
            Ok(())
        })
    }

    pub fn record_cleanup_accounting_error(&self, error: String) -> Result<(), String> {
        self.update(false, |state, _| {
            state.accounting_errors.insert(error);
            Ok(())
        })
    }

    pub fn record_cleanup_absence(&self, absent_ids: &BTreeSet<u64>) -> Result<(), String> {
        self.update(false, |state, _| {
            if state.mode != PaidAdmissionMode::CleanupOnly
                || state
                    .cleanup
                    .as_ref()
                    .is_some_and(|owner| owner.orchestrator_shutdown_pending)
                || !state.unresolved_create_labels().is_empty()
                || !state.known_contract_ids().is_subset(absent_ids)
            {
                return Err(
                    "cleanup cannot stop discovery without exact absence and resolved acceptance"
                        .to_owned(),
                );
            }
            if let Some(owner) = state.cleanup.as_mut() {
                owner.spending_stopped = true;
            }
            Ok(())
        })
    }

    pub fn complete_cleanup(&self, absent_ids: &BTreeSet<u64>) -> Result<(), String> {
        let mut accounting_failed = false;
        self.update(false, |state, _| {
            if state.mode != PaidAdmissionMode::CleanupOnly
                || state
                    .cleanup
                    .as_ref()
                    .is_some_and(|owner| owner.orchestrator_shutdown_pending)
                || !state.unresolved_create_labels().is_empty()
                || !state.known_contract_ids().is_subset(absent_ids)
            {
                return Err(
                    "paid cleanup lacks exact absence or authoritative acquisition accounting"
                        .to_owned(),
                );
            }
            accounting_failed = !state.accounting_errors.is_empty();
            if let Some(owner) = state.cleanup.as_mut() {
                // Publish one terminal receipt: success must never be observed
                // as spending-stopped with completion still pending.
                owner.spending_stopped = true;
                owner.complete = !accounting_failed;
            }
            Ok(())
        })?;
        if accounting_failed {
            Err("paid cleanup lacks authoritative acquisition accounting".to_owned())
        } else {
            Ok(())
        }
    }

    pub fn reserve_create(
        &self,
        run_id: u64,
        node_id: u64,
        attempt_id: u64,
        offer_id: u64,
        label: &str,
    ) -> Result<(), String> {
        self.update(true, |state, now| {
            if run_id != state.run_id {
                return Err("paid create run identity mismatch".to_owned());
            }
            let node = state
                .nodes
                .iter()
                .find(|node| node.node_id == node_id)
                .ok_or_else(|| "paid create node was not selected".to_owned())?;
            if node.offer_id != offer_id || node.label != label {
                return Err("paid create offer or label differs from selected topology".to_owned());
            }
            if state.create_reservations.contains_key(&node_id) {
                return Err("paid create slot is permanently consumed".to_owned());
            }
            state.create_reservations.insert(
                node_id,
                PaidCreateReservation {
                    attempt_id,
                    reserved_unix_ms: now,
                },
            );
            Ok(())
        })
    }

    /// Records only an authoritative rejected create response, never a timeout
    /// or missing contract. The create slot remains permanently consumed.
    pub fn record_create_rejection(&self, node_id: u64) -> Result<(), String> {
        self.update(false, |state, _| {
            if !state.create_reservations.contains_key(&node_id)
                || state.contracts.contains_key(&node_id)
            {
                return Err("paid create rejection requires an unresolved consumed slot".to_owned());
            }
            state.rejected_creates.insert(node_id);
            Ok(())
        })
    }

    /// Records a late response or cleanup discovery even in deny-only mode.
    /// Re-observing the same mapping is idempotent; reuse or remapping is denied.
    pub fn record_contract(&self, node_id: u64, contract_id: u64) -> Result<(), String> {
        self.update(false, |state, _| {
            if contract_id == 0 || !state.create_reservations.contains_key(&node_id) {
                return Err(
                    "paid contract requires a consumed create slot and nonzero id".to_owned(),
                );
            }
            if state.rejected_creates.contains(&node_id) {
                return Err("paid contract contradicts a definitive create rejection".to_owned());
            }
            if let Some(existing) = state.contracts.get(&node_id) {
                return if *existing == contract_id {
                    Ok(())
                } else {
                    Err("paid contract mapping cannot change".to_owned())
                };
            }
            if state
                .contracts
                .values()
                .any(|existing| *existing == contract_id)
            {
                return Err("paid contract is already assigned to another node".to_owned());
            }
            state.contracts.insert(node_id, contract_id);
            Ok(())
        })
    }

    pub fn reserve_bootstrap(&self, run_id: u64, node_id: u64) -> Result<(), String> {
        self.update(true, |state, now| {
            if state.run_id != run_id || !state.contracts.contains_key(&node_id) {
                return Err("paid bootstrap requires the exact run and a known contract".to_owned());
            }
            if state.initial_bootstraps.contains_key(&node_id) {
                return Err("paid initial bootstrap slot is permanently consumed".to_owned());
            }
            state.initial_bootstraps.insert(
                node_id,
                PaidBootstrapReservation {
                    reserved_unix_ms: now,
                },
            );
            Ok(())
        })
    }

    /// Call only after readiness and readiness-workload cleanup have succeeded.
    pub fn seal_prepared(&self) -> Result<(), String> {
        self.update(true, |state, _| {
            if state.contracts.len() != 5 || state.initial_bootstraps.len() != 5 {
                return Err(
                    "paid preparation needs five contracts and five initial bootstraps".to_owned(),
                );
            }
            state.mode = PaidAdmissionMode::Prepared;
            Ok(())
        })
    }

    pub fn enter_cleanup_only(&self) -> Result<(), String> {
        self.update(false, |state, _| {
            state.mode = PaidAdmissionMode::CleanupOnly;
            if let Some(owner) = state.cleanup.as_mut() {
                owner.orchestrator_shutdown_pending = false;
            }
            Ok(())
        })
    }

    fn lock(&self) -> Result<File, String> {
        private_state_path(&self.path)?;
        let lock = open_private_file(&self.lock_path, true)?;
        #[cfg(unix)]
        if file_identity(&lock)? != self.lock_identity {
            return Err("paid admission lock inode changed".to_owned());
        }
        lock.lock().map_err(io_error)?;
        Ok(lock)
    }

    fn load(&self) -> Result<PaidAdmissionSnapshot, String> {
        let mut state: PaidAdmissionSnapshot =
            serde_json::from_reader(open_private_file(&self.path, false)?)
                .map_err(|error| format!("decode paid admission: {error}"))?;
        // Legacy reservations never imply rejection: keep every unresolved
        // outcome ambiguous when adopting a schema-1 durable cleanup record.
        if state.schema_version <= 2 {
            state.schema_version = 3;
            state.mode = PaidAdmissionMode::CleanupOnly;
        }
        state.validate()?;
        Ok(state)
    }

    fn update(
        &self,
        admits_work: bool,
        change: impl FnOnce(&mut PaidAdmissionSnapshot, u64) -> Result<(), String>,
    ) -> Result<(), String> {
        let _lock = self.lock()?;
        let mut state = self.load()?;
        let now = unix_ms()?;
        if admits_work {
            if state.mode != PaidAdmissionMode::Preparing {
                return Err("paid admission is permanently deny-only".to_owned());
            }
            if now >= state.deadline_unix_ms {
                state.mode = PaidAdmissionMode::CleanupOnly;
                if let Some(owner) = state.cleanup.as_mut() {
                    owner.orchestrator_shutdown_pending = false;
                }
                self.persist(&state)?;
                return Err("paid preparation deadline expired".to_owned());
            }
            if !state.cleanup.as_ref().is_some_and(|owner| {
                owner.live_at(now)
                    && owner.runner.is_live()
                    && owner.orchestrator_recovery_until_unix_ms.is_none()
                    && owner
                        .orchestrator
                        .as_ref()
                        .is_some_and(PaidProcessIdentity::is_live)
            }) || !self.cleanup_owner_active()?
            {
                state.mode = PaidAdmissionMode::CleanupOnly;
                if let Some(owner) = state.cleanup.as_mut() {
                    owner.orchestrator_shutdown_pending = false;
                }
                self.persist(&state)?;
                return Err(
                    "paid work denied: durable cleanup owner is absent, stale or expired"
                        .to_owned(),
                );
            }
        }
        change(&mut state, now)?;
        state.validate()?;
        self.persist(&state)
    }

    fn persist(&self, state: &PaidAdmissionSnapshot) -> Result<(), String> {
        // A stale temporary is not authoritative: no external work is allowed
        // until both the rename and directory fsync have returned successfully.
        let temporary = sibling(&self.path, ".tmp");
        if fs::symlink_metadata(&temporary).is_ok() {
            fs::remove_file(&temporary).map_err(io_error)?;
        }
        let mut file = private_options()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(io_error)?;
        serde_json::to_writer(&mut file, state)
            .map_err(|error| format!("encode paid admission: {error}"))?;
        file.flush().map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        fs::rename(&temporary, &self.path).map_err(io_error)?;
        sync_parent(&self.path)
    }
}

fn new_snapshot(
    run_id: u64,
    nodes: Vec<PaidNodeAdmission>,
    mode: PaidAdmissionMode,
    deadline_unix_ms: u64,
    cleanup: Option<PaidCleanupOwnership>,
) -> PaidAdmissionSnapshot {
    PaidAdmissionSnapshot {
        schema_version: 3,
        run_id,
        nodes,
        contracts: BTreeMap::new(),
        create_reservations: BTreeMap::new(),
        rejected_creates: BTreeSet::new(),
        initial_bootstraps: BTreeMap::new(),
        retained_deployments: BTreeMap::new(),
        mode,
        deadline_unix_ms,
        cleanup,
        discovered_contracts: BTreeMap::new(),
        accounting_errors: BTreeSet::new(),
        recovery_contract_ids: BTreeSet::new(),
        recovery_labels: BTreeSet::new(),
    }
}

fn new_cleanup_ownership(
    runner: PaidProcessIdentity,
    limits: PaidCleanupLimits,
    deadline_unix_ms: u64,
    now: u64,
) -> Result<PaidCleanupOwnership, String> {
    if limits.maximum_cost_microusd == 0 || limits.hourly_price_microusd == 0 {
        return Err("paid cleanup requires positive financial limits".to_owned());
    }
    let financial_ms = (u128::from(limits.maximum_cost_microusd) * 3_600_000
        / u128::from(limits.hourly_price_microusd))
    .min(u128::from(u64::MAX)) as u64;
    let spending_deadline = now.saturating_add(financial_ms).min(deadline_unix_ms);
    let stop_unix_ms = spending_deadline.saturating_sub(PAID_CLEANUP_RESERVE_MS);
    if stop_unix_ms <= now {
        return Err("paid ceilings cannot cover the five-minute cleanup reserve".to_owned());
    }
    Ok(PaidCleanupOwnership {
        runner,
        owner: None,
        supervisor: None,
        orchestrator: None,
        orchestrator_recovery_until_unix_ms: None,
        limits,
        started_unix_ms: now,
        stop_unix_ms,
        heartbeat_unix_ms: 0,
        retained_development_fixture: false,
        complete: false,
        spending_stopped: false,
        orchestrator_shutdown_pending: false,
    })
}

fn validate_deployment_identity(artifact_digest: &str, generation: &str) -> Result<(), String> {
    if artifact_digest
        .strip_prefix("sha256:")
        .is_none_or(|digest| {
            digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        || generation.is_empty()
        || !generation
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("invalid retained deployment identity".to_owned());
    }
    Ok(())
}

fn validate_nodes(nodes: &[PaidNodeAdmission]) -> Result<(), String> {
    if nodes.len() != 5 {
        return Err("paid admission requires exactly five selected nodes".to_owned());
    }
    let mut ids = BTreeSet::new();
    let mut offers = BTreeSet::new();
    let mut hosts = BTreeSet::new();
    let mut labels = BTreeSet::new();
    for node in nodes {
        if node.offer_id == 0
            || node.host_id == 0
            || !valid_label(&node.label)
            || !ids.insert(node.node_id)
            || !offers.insert(node.offer_id)
            || !hosts.insert(node.host_id)
            || !labels.insert(&node.label)
        {
            return Err(
                "paid admission requires distinct nodes, offers, hosts, and nonempty labels"
                    .to_owned(),
            );
        }
    }
    Ok(())
}

fn valid_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 128
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn unix_ms() -> Result<u64, String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("paid admission clock: {error}"))?;
    u64::try_from(elapsed.as_millis()).map_err(|error| format!("paid admission clock: {error}"))
}

fn private_state_path(path: &Path) -> Result<PathBuf, String> {
    let name = path
        .file_name()
        .ok_or_else(|| "paid admission path has no file name".to_owned())?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).map_err(io_error)?;
    let metadata = fs::metadata(&parent).map_err(io_error)?;
    if !metadata.is_dir() {
        return Err("paid admission parent is not a directory".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("paid admission directory must be private (0700)".to_owned());
        }
    }
    #[cfg(not(unix))]
    return Err("paid admission requires Unix private-state permissions".to_owned());
    Ok(parent.join(name))
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn open_private_file(path: &Path, writable: bool) -> Result<File, String> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("paid admission state and lock must be regular files".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 || metadata.nlink() != 1 {
            return Err("paid admission files must be private and not hard-linked".to_owned());
        }
    }
    OpenOptions::new()
        .read(true)
        .write(writable)
        .open(path)
        .map_err(io_error)
}

#[cfg(unix)]
fn file_identity(file: &File) -> Result<(u64, u64), String> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata().map_err(io_error)?;
    Ok((metadata.dev(), metadata.ino()))
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .expect("validated admission file name")
        .to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

fn sync_parent(path: &Path) -> Result<(), String> {
    File::open(path.parent().expect("validated admission parent"))
        .and_then(|file| file.sync_all())
        .map_err(io_error)
}

fn io_error(error: std::io::Error) -> String {
    format!("paid admission storage: {error}")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};

    struct Fixture(
        PathBuf,
        std::cell::RefCell<Option<PaidCleanupOwner>>,
        std::cell::RefCell<Option<File>>,
    );

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "paid-admission-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
            Self(
                path,
                std::cell::RefCell::new(None),
                std::cell::RefCell::new(None),
            )
        }

        fn path(&self) -> PathBuf {
            self.0.join("admission.json")
        }

        fn create(&self) -> PaidFixtureAdmission {
            let admission = PaidFixtureAdmission::create(
                self.path(),
                42,
                nodes(),
                unix_ms().unwrap() + PAID_CLEANUP_RESERVE_MS + 60_000,
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
            *self.2.borrow_mut() = Some(admission.claim_cleanup_supervisor().unwrap());
            *self.1.borrow_mut() = Some(admission.claim_cleanup_owner().unwrap());
            admission.bind_orchestrator(std::process::id()).unwrap();
            admission
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // The fixture is no longer usable and all test processes have exited.
            self.1.get_mut().take();
            self.2.get_mut().take();
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn nodes() -> Vec<PaidNodeAdmission> {
        (1..=5)
            .map(|node_id| PaidNodeAdmission {
                node_id,
                offer_id: node_id + 100,
                host_id: node_id + 200,
                label: format!("paid-42-{node_id}-attempt-1"),
            })
            .collect()
    }

    fn reserve(admission: &PaidFixtureAdmission, node_id: u64) -> Result<(), String> {
        admission.reserve_create(
            42,
            node_id,
            1,
            node_id + 100,
            &format!("paid-42-{node_id}-attempt-1"),
        )
    }

    fn prepare(admission: &PaidFixtureAdmission) {
        for id in 1..=5 {
            reserve(admission, id).unwrap();
            admission.record_contract(id, id + 900).unwrap();
            admission.reserve_bootstrap(42, id).unwrap();
        }
        admission.seal_prepared().unwrap();
    }

    #[test]
    fn missing_cleanup_owner_denies_before_first_create() {
        let fixture = Fixture::new();
        let admission =
            PaidFixtureAdmission::create(fixture.path(), 42, nodes(), unix_ms().unwrap() + 60_000)
                .unwrap();
        assert!(reserve(&admission, 1).is_err());
        let state = admission.snapshot().unwrap();
        assert!(state.create_reservations.is_empty());
        assert_eq!(state.mode, PaidAdmissionMode::CleanupOnly);
    }

    #[test]
    fn cleanup_reserve_is_inside_both_operator_ceilings() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        let state = admission.snapshot().unwrap();
        let owner = state.cleanup.unwrap();
        assert_eq!(
            owner.stop_unix_ms + PAID_CLEANUP_RESERVE_MS,
            state.deadline_unix_ms
        );
        assert!(
            u128::from(owner.stop_unix_ms + PAID_CLEANUP_RESERVE_MS - owner.started_unix_ms)
                * u128::from(owner.limits.hourly_price_microusd)
                <= u128::from(owner.limits.maximum_cost_microusd) * 3_600_000
        );

        let tight = Fixture::new();
        let admission =
            PaidFixtureAdmission::create(tight.path(), 42, nodes(), unix_ms().unwrap() + 3_600_000)
                .unwrap();
        assert!(
            admission
                .install_cleanup_ownership(
                    PaidProcessIdentity::current().unwrap(),
                    PaidCleanupLimits {
                        maximum_cost_microusd: 1,
                        hourly_price_microusd: 1_000_000
                    },
                )
                .is_err()
        );
        assert!(admission.snapshot().unwrap().create_reservations.is_empty());
    }

    #[test]
    fn unexpected_orchestrator_loss_closes_unused_acquisition_slots() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        reserve(&admission, 1).unwrap();
        admission
            .update(false, |state, _| {
                state
                    .cleanup
                    .as_mut()
                    .unwrap()
                    .orchestrator
                    .as_mut()
                    .unwrap()
                    .start_ticks += 1;
                Ok(())
            })
            .unwrap();
        assert!(fixture.1.borrow().as_ref().unwrap().tick().unwrap());
        assert!(reserve(&admission, 2).is_err());
        assert_eq!(admission.snapshot().unwrap().create_reservations.len(), 1);
    }

    #[test]
    fn formal_recovery_allows_one_bounded_restart_not_acquisition() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        prepare(&admission);
        admission.begin_orchestrator_recovery().unwrap();
        assert!(admission.begin_orchestrator_recovery().is_err());
        admission
            .update(false, |state, _| {
                state
                    .cleanup
                    .as_mut()
                    .unwrap()
                    .orchestrator
                    .as_mut()
                    .unwrap()
                    .start_ticks += 1;
                Ok(())
            })
            .unwrap();
        assert!(!fixture.1.borrow().as_ref().unwrap().tick().unwrap());
        assert!(reserve(&admission, 1).is_err());
        admission.bind_orchestrator(std::process::id()).unwrap();
        assert!(!fixture.1.borrow().as_ref().unwrap().tick().unwrap());
        admission.begin_orchestrator_recovery().unwrap();
        admission
            .update(false, |state, now| {
                state
                    .cleanup
                    .as_mut()
                    .unwrap()
                    .orchestrator_recovery_until_unix_ms = Some(now);
                Ok(())
            })
            .unwrap();
        assert!(fixture.1.borrow().as_ref().unwrap().tick().unwrap());
        assert!(admission.bind_orchestrator(std::process::id()).is_err());
    }

    #[test]
    fn owner_recovery_preserves_limits_and_never_reopens_reservations() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        reserve(&admission, 1).unwrap();
        let before = admission.snapshot().unwrap();
        assert!(
            admission.claim_cleanup_owner().is_err(),
            "two live owners acquired one fixture"
        );
        fixture.1.borrow_mut().take();
        assert!(!admission.cleanup_owner_active().unwrap());
        assert!(admission.require_live_cleanup_owner().is_err());
        let recovered = PaidFixtureAdmission::open(fixture.path()).unwrap();
        let owner = recovered.claim_cleanup_owner().unwrap();
        assert!(owner.tick().unwrap());
        assert!(reserve(&recovered, 2).is_err());
        let after = recovered.snapshot().unwrap();
        assert_eq!(after.create_reservations, before.create_reservations);
        assert_eq!(
            after.cleanup.as_ref().unwrap().limits,
            before.cleanup.as_ref().unwrap().limits
        );
        assert_eq!(
            after.cleanup.as_ref().unwrap().stop_unix_ms,
            before.cleanup.as_ref().unwrap().stop_unix_ms
        );
        assert!(
            after
                .unresolved_create_labels()
                .contains("paid-42-1-attempt-1")
        );
        assert!(recovered.complete_cleanup(&BTreeSet::new()).is_err());
        recovered
            .reconcile_cleanup_discovery(&BTreeMap::from([(
                9001,
                "paid-42-1-attempt-1".to_owned(),
            )]))
            .unwrap();
        recovered.complete_cleanup(&BTreeSet::from([9001])).unwrap();
        assert_eq!(
            recovered.snapshot().unwrap().known_contract_ids(),
            BTreeSet::from([9001])
        );
    }

    #[test]
    fn runner_loss_before_response_accounting_keeps_discovery_obligation() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        reserve(&admission, 1).unwrap();
        // Model the exact PID incarnation disappearing, including PID reuse.
        admission
            .update(false, |state, _| {
                state.cleanup.as_mut().unwrap().runner.start_ticks += 1;
                Ok(())
            })
            .unwrap();
        assert!(fixture.1.borrow().as_ref().unwrap().tick().unwrap());
        assert!(admission.complete_cleanup(&BTreeSet::new()).is_err());
        assert!(reserve(&admission, 2).is_err());
        admission
            .reconcile_cleanup_discovery(&BTreeMap::from([(
                9001,
                "paid-42-1-attempt-1".to_owned(),
            )]))
            .unwrap();
        assert!(admission.complete_cleanup(&BTreeSet::new()).is_err());
        admission.complete_cleanup(&BTreeSet::from([9001])).unwrap();
    }

    #[test]
    fn retained_fixture_requires_explicit_live_owner_and_original_expiry() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        prepare(&admission);
        let original = admission.snapshot().unwrap().cleanup.unwrap();
        assert!(admission.retain_development_fixture(false).is_err());
        admission.retain_development_fixture(true).unwrap();
        admission
            .update(false, |state, _| {
                state.cleanup.as_mut().unwrap().runner.start_ticks += 1;
                Ok(())
            })
            .unwrap();
        assert!(!fixture.1.borrow().as_ref().unwrap().tick().unwrap());
        let retained = admission.snapshot().unwrap().cleanup.unwrap();
        assert_eq!(retained.stop_unix_ms, original.stop_unix_ms);
        assert_eq!(retained.limits, original.limits);
        // Model arrival at the immutable stop instant without sleeping.
        admission
            .update(false, |state, now| {
                state.cleanup.as_mut().unwrap().stop_unix_ms = now;
                Ok(())
            })
            .unwrap();
        assert!(fixture.1.borrow().as_ref().unwrap().tick().unwrap());
        assert!(admission.retain_development_fixture(true).is_err());
        assert!(admission.reserve_bootstrap(42, 1).is_err());
    }

    #[test]
    fn retained_generation_requires_quiescence_and_never_reopens_initial_slots() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        prepare(&admission);
        admission.retain_development_fixture(true).unwrap();
        let mut prior = Command::new("sleep").arg("30").spawn().unwrap();
        admission.bind_orchestrator(prior.id()).unwrap();
        admission.retain_development_fixture(true).unwrap();
        let deployment = DeploymentIdentity {
            artifact_digest: format!("sha256:{}", "a".repeat(64)),
            deployment_generation: "retained-fresh".to_owned(),
        };
        assert!(
            admission
                .authorize_retained_deployment(true, deployment.clone())
                .is_err()
        );
        prior.kill().unwrap();
        prior.wait().unwrap();
        assert!(
            admission
                .authorize_retained_deployment(false, deployment.clone())
                .is_err()
        );
        assert!(
            admission
                .reserve_retained_bootstrap(42, 1, 901, &deployment)
                .is_err()
        );
        let before = admission.snapshot().unwrap();
        admission
            .authorize_retained_deployment(true, deployment.clone())
            .unwrap();
        admission.bind_orchestrator(std::process::id()).unwrap();
        let mut wrong = deployment.clone();
        wrong.artifact_digest = format!("sha256:{}", "b".repeat(64));
        assert!(
            admission
                .reserve_retained_bootstrap(42, 1, 901, &wrong)
                .is_err()
        );
        assert!(
            admission
                .reserve_retained_bootstrap(42, 1, 902, &deployment)
                .is_err()
        );
        for id in 1..=5 {
            admission
                .reserve_retained_bootstrap(42, id, 900 + id, &deployment)
                .unwrap();
            assert!(admission.reserve_bootstrap(42, id).is_err());
            assert!(reserve(&admission, id).is_err());
            assert!(
                admission
                    .reserve_retained_bootstrap(42, id, 900 + id, &deployment)
                    .is_err()
            );
        }
        assert!(
            admission
                .authorize_retained_deployment(true, deployment)
                .is_err()
        );
        let after = admission.snapshot().unwrap();
        assert_eq!(after.contracts, before.contracts);
        assert_eq!(after.create_reservations, before.create_reservations);
        assert_eq!(after.initial_bootstraps, before.initial_bootstraps);
        admission.enter_cleanup_only().unwrap();
        assert!(admission.retain_development_fixture(true).is_err());
    }

    #[test]
    fn runner_loss_after_preparation_stops_nonretained_fixture() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        prepare(&admission);
        admission.begin_orchestrator_recovery().unwrap();
        admission
            .update(false, |state, _| {
                state.cleanup.as_mut().unwrap().runner.start_ticks += 1;
                Ok(())
            })
            .unwrap();
        assert!(fixture.1.borrow().as_ref().unwrap().tick().unwrap());
        assert_eq!(
            admission.snapshot().unwrap().known_contract_ids(),
            (901..=905).collect()
        );
    }

    #[test]
    fn duplicate_discovery_preserves_all_ids_but_never_passes_accounting() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        reserve(&admission, 1).unwrap();
        admission.enter_cleanup_only().unwrap();
        admission
            .reconcile_cleanup_discovery(&BTreeMap::from([
                (9001, "paid-42-1-attempt-1".to_owned()),
                (9002, "paid-42-1-attempt-1".to_owned()),
            ]))
            .unwrap();
        let absent = BTreeSet::from([9001, 9002]);
        assert!(admission.complete_cleanup(&absent).is_err());
        let snapshot = admission.snapshot().unwrap();
        assert_eq!(snapshot.known_contract_ids(), absent);
        assert!(snapshot.cleanup.as_ref().unwrap().spending_stopped);
        assert!(!snapshot.cleanup.as_ref().unwrap().complete);
        assert!(!snapshot.accounting_errors.is_empty());
    }

    #[test]
    fn repeated_cleanup_requires_new_absence_and_retains_exact_ids() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        reserve(&admission, 1).unwrap();
        admission.record_contract(1, 9001).unwrap();
        admission.enter_cleanup_only().unwrap();
        admission.complete_cleanup(&BTreeSet::from([9001])).unwrap();
        admission.request_cleanup().unwrap();
        let snapshot = admission.snapshot().unwrap();
        assert!(!snapshot.cleanup.as_ref().unwrap().complete);
        assert!(!snapshot.cleanup.as_ref().unwrap().spending_stopped);
        assert!(admission.complete_cleanup(&BTreeSet::new()).is_err());
        assert_eq!(snapshot.known_contract_ids(), BTreeSet::from([9001]));
    }

    #[test]
    fn definitive_rejection_settles_uncertainty_without_reauthorizing_create() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        assert!(admission.record_create_rejection(1).is_err());
        reserve(&admission, 1).unwrap();
        admission.record_create_rejection(1).unwrap();
        let reopened = PaidFixtureAdmission::open(fixture.path()).unwrap();
        assert!(reopened.snapshot().unwrap().rejected_creates.contains(&1));
        assert!(reserve(&reopened, 1).is_err());
        assert!(reopened.record_contract(1, 9000).is_err());
        reserve(&reopened, 2).unwrap();
        reopened.record_contract(2, 9001).unwrap();
        assert!(reopened.record_create_rejection(2).is_err());
    }

    #[test]
    fn legacy_cleanup_reservations_remain_ambiguous() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        reserve(&admission, 1).unwrap();
        let mut legacy = serde_json::to_value(admission.snapshot().unwrap()).unwrap();
        legacy["schema_version"] = serde_json::json!(1);
        legacy.as_object_mut().unwrap().remove("rejected_creates");
        fs::write(fixture.path(), serde_json::to_vec(&legacy).unwrap()).unwrap();
        let reopened = PaidFixtureAdmission::open(fixture.path()).unwrap();
        let snapshot = reopened.snapshot().unwrap();
        assert!(snapshot.create_reservations.contains_key(&1));
        assert!(snapshot.rejected_creates.is_empty());
        assert!(reserve(&reopened, 1).is_err());
        reopened.record_contract(1, 9000).unwrap();
    }

    #[test]
    fn rejected_topologies_never_initialize_an_authorization() {
        let fixture = Fixture::new();
        let deadline = unix_ms().unwrap() + 60_000;
        let mut invalid = nodes();
        invalid.pop();
        assert!(PaidFixtureAdmission::create(fixture.path(), 42, invalid, deadline).is_err());
        for field in 0..4 {
            let mut invalid = nodes();
            match field {
                0 => invalid[1].node_id = invalid[0].node_id,
                1 => invalid[1].offer_id = invalid[0].offer_id,
                2 => invalid[1].host_id = invalid[0].host_id,
                _ => invalid[1].label = invalid[0].label.clone(),
            }
            assert!(PaidFixtureAdmission::create(fixture.path(), 42, invalid, deadline).is_err());
        }
        assert!(PaidFixtureAdmission::create(fixture.path(), 42, nodes(), 1).is_err());
        let admission = fixture.create();
        reserve(&admission, 1).unwrap();
        assert!(PaidFixtureAdmission::create(fixture.path(), 42, nodes(), deadline).is_err());
        assert!(reserve(&PaidFixtureAdmission::open(fixture.path()).unwrap(), 1).is_err());
    }

    #[test]
    fn reservations_survive_reopen_and_cannot_remap_or_release() {
        let fixture = Fixture::new();
        let first = fixture.create();
        let second = PaidFixtureAdmission::open(fixture.path()).unwrap();
        assert!(
            first
                .reserve_create(43, 1, 1, 101, "paid-42-1-attempt-1")
                .is_err()
        );
        assert!(
            first
                .reserve_create(42, 1, 1, 102, "paid-42-1-attempt-1")
                .is_err()
        );
        assert!(
            first
                .reserve_create(42, 1, 1, 101, "different-label")
                .is_err()
        );
        reserve(&first, 1).unwrap();
        drop(first);
        assert!(reserve(&second, 1).is_err());
        assert!(
            second
                .reserve_create(42, 1, 2, 101, "paid-42-1-attempt-1")
                .is_err()
        );
        assert!(second.reserve_bootstrap(42, 1).is_err());
        assert!(second.record_contract(2, 900).is_err());
        second.record_contract(1, 900).unwrap();
        assert!(second.record_contract(1, 901).is_err());
        reserve(&second, 2).unwrap();
        assert!(second.record_contract(2, 900).is_err());
        second.reserve_bootstrap(42, 1).unwrap();
        assert!(
            PaidFixtureAdmission::open(fixture.path())
                .unwrap()
                .reserve_bootstrap(42, 1)
                .is_err()
        );
        let snapshot = second.snapshot().unwrap();
        assert_eq!(snapshot.contracts, BTreeMap::from([(1, 900)]));
        assert_eq!(
            snapshot
                .create_reservations
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(snapshot.nodes, nodes());
    }

    #[test]
    fn cloned_and_independent_handles_serialize_the_same_slot() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        let start = Arc::new(Barrier::new(12));
        let mut workers = Vec::new();
        for index in 0..12 {
            let handle = if index % 2 == 0 {
                admission.clone()
            } else {
                PaidFixtureAdmission::open(fixture.path()).unwrap()
            };
            let start = start.clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                reserve(&handle, 1).is_ok()
            }));
        }
        let successes = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|success| *success)
            .count();
        assert_eq!(successes, 1);
        assert_eq!(admission.snapshot().unwrap().create_reservations.len(), 1);
    }

    #[test]
    fn concurrent_processes_share_one_bound_after_abrupt_exit() {
        const CHILD_PATH: &str = "PROVISIONING_ADMISSION_TEST_CHILD_PATH";
        const CHILD_NODE: &str = "PROVISIONING_ADMISSION_TEST_CHILD_NODE";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            let node_id = std::env::var(CHILD_NODE).unwrap().parse().unwrap();
            let admission = PaidFixtureAdmission::open(path).unwrap();
            // Abrupt exit deliberately omits destructors after durable admission,
            // exactly the response-loss window before any contract is recorded.
            std::process::exit(if reserve(&admission, node_id).is_ok() {
                0
            } else {
                17
            });
        }
        let fixture = Fixture::new();
        let admission = fixture.create();
        let lock_identity = fs::metadata(sibling(&fixture.path(), ".lock"))
            .unwrap()
            .ino();
        let mut children = Vec::new();
        for index in 0..15 {
            children.push(Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "paid_admission::tests::concurrent_processes_share_one_bound_after_abrupt_exit"])
                .env(CHILD_PATH, fixture.path())
                .env(CHILD_NODE, (index % 5 + 1).to_string())
                .stdout(Stdio::null())
                .spawn().unwrap());
        }
        let mut successes = 0;
        for mut child in children {
            match child.wait().unwrap().code() {
                Some(0) => successes += 1,
                Some(17) => {}
                status => panic!("admission child failed unexpectedly: {status:?}"),
            }
        }
        assert_eq!(successes, 5);
        let snapshot = PaidFixtureAdmission::open(fixture.path())
            .unwrap()
            .snapshot()
            .unwrap();
        assert_eq!(
            snapshot
                .create_reservations
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        assert!(snapshot.contracts.is_empty());
        assert_eq!(snapshot.nodes, nodes());
        assert_eq!(
            fs::metadata(sibling(&fixture.path(), ".lock"))
                .unwrap()
                .ino(),
            lock_identity
        );
        for node in &snapshot.nodes {
            assert!(reserve(&admission, node.node_id).is_err());
        }
    }

    #[test]
    fn preparation_boundary_crashes_preserve_cleanup_and_deny_reacquisition() {
        const CHILD_PATH: &str = "PROVISIONING_BOUNDARY_CRASH_PATH";
        const CHILD_STAGE: &str = "PROVISIONING_BOUNDARY_CRASH_STAGE";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            let stage: u8 = std::env::var(CHILD_STAGE).unwrap().parse().unwrap();
            let admission = PaidFixtureAdmission::open(path).unwrap();
            admission
                .update(false, |state, _| {
                    state.cleanup.as_mut().unwrap().runner = PaidProcessIdentity::current()?;
                    Ok(())
                })
                .unwrap();
            for node in 1..=5 {
                if stage >= 1 {
                    reserve(&admission, node).unwrap();
                }
                if stage >= 2 {
                    admission.record_contract(node, node + 900).unwrap();
                }
                if stage >= 3 {
                    admission.reserve_bootstrap(42, node).unwrap();
                }
            }
            if stage >= 4 {
                admission.seal_prepared().unwrap();
            }
            // No destructors or cooperative cleanup at the persisted boundary.
            std::process::exit(0);
        }
        // Before reservation; before response accounting; before bootstrap;
        // after bootstrap/readiness but before commit; after prepared commit.
        for stage in 0_u8..=4 {
            let fixture = Fixture::new();
            let admission = fixture.create();
            let status = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "paid_admission::tests::preparation_boundary_crashes_preserve_cleanup_and_deny_reacquisition"])
                .env(CHILD_PATH, fixture.path())
                .env(CHILD_STAGE, stage.to_string())
                .stdout(Stdio::null())
                .status().unwrap();
            assert!(status.success(), "boundary {stage}: {status}");
            assert!(fixture.1.borrow().as_ref().unwrap().tick().unwrap());
            let recovered = PaidFixtureAdmission::open(fixture.path()).unwrap();
            for node in 1..=5 {
                assert!(reserve(&recovered, node).is_err());
                assert!(recovered.reserve_bootstrap(42, node).is_err());
            }
            let contracts = if stage == 0 {
                BTreeSet::new()
            } else {
                (901..=905).collect()
            };
            if stage == 1 {
                // A request may have reached the provider before the crash:
                // no response is needed to retain exact-label discovery duty.
                assert!(recovered.complete_cleanup(&BTreeSet::new()).is_err());
                recovered
                    .reconcile_cleanup_discovery(
                        &nodes()
                            .into_iter()
                            .map(|node| (node.node_id + 900, node.label))
                            .collect(),
                    )
                    .unwrap();
            }
            if stage > 0 {
                assert!(recovered.complete_cleanup(&BTreeSet::new()).is_err());
            }
            recovered.complete_cleanup(&contracts).unwrap();
            assert!(admission.snapshot().unwrap().cleanup.unwrap().complete);
            assert!(reserve(&admission, 1).is_err());
        }
    }

    #[test]
    fn sealing_and_cleanup_are_irreversible_but_late_discovery_is_preserved() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        let stale = PaidFixtureAdmission::open(fixture.path()).unwrap();
        assert!(admission.seal_prepared().is_err());
        for node_id in 1..=5 {
            reserve(&admission, node_id).unwrap();
            admission.record_contract(node_id, node_id + 900).unwrap();
        }
        assert!(admission.seal_prepared().is_err());
        for node_id in 1..=5 {
            admission.reserve_bootstrap(42, node_id).unwrap();
        }
        admission.seal_prepared().unwrap();
        assert_eq!(stale.snapshot().unwrap().mode, PaidAdmissionMode::Prepared);
        assert!(reserve(&stale, 1).is_err());
        assert!(stale.reserve_bootstrap(42, 1).is_err());
        stale.enter_cleanup_only().unwrap();
        assert!(admission.seal_prepared().is_err());
        assert_eq!(
            admission.snapshot().unwrap().mode,
            PaidAdmissionMode::CleanupOnly
        );

        let incomplete_fixture = Fixture::new();
        let incomplete = incomplete_fixture.create();
        reserve(&incomplete, 1).unwrap();
        incomplete.enter_cleanup_only().unwrap();
        assert!(reserve(&incomplete, 2).is_err());
        incomplete.record_contract(1, 999).unwrap();
        assert!(incomplete.reserve_bootstrap(42, 1).is_err());
        assert_eq!(
            incomplete.snapshot().unwrap().contracts,
            BTreeMap::from([(1, 999)])
        );
    }

    #[test]
    fn expired_deadline_becomes_durable_cleanup_only() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        // Advance only this fixture's deadline, without sleeps or wall-clock races.
        admission
            .update(false, |state, _| {
                state.deadline_unix_ms = 1;
                let owner = state.cleanup.as_mut().unwrap();
                owner.started_unix_ms = 0;
                owner.stop_unix_ms = 1;
                Ok(())
            })
            .unwrap();
        assert!(reserve(&admission, 1).is_err());
        let reopened = PaidFixtureAdmission::open(fixture.path()).unwrap();
        assert_eq!(
            reopened.snapshot().unwrap().mode,
            PaidAdmissionMode::CleanupOnly
        );
        assert!(reopened.seal_prepared().is_err());
        assert!(reopened.snapshot().unwrap().create_reservations.is_empty());
    }

    #[test]
    fn missing_state_cannot_reinitialize_a_used_authorization() {
        let fixture = Fixture::new();
        let admission = fixture.create();
        reserve(&admission, 1).unwrap();
        fs::remove_file(fixture.path()).unwrap();
        assert!(PaidFixtureAdmission::open(fixture.path()).is_err());
        assert!(
            PaidFixtureAdmission::create(fixture.path(), 42, nodes(), unix_ms().unwrap() + 60_000)
                .is_err()
        );
        assert_eq!(
            fs::metadata(&admission.lock_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
