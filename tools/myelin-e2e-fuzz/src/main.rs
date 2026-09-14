use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use myelin_e2e_fuzz::{
    BehaviorCase, Budget, CAMPAIGN_DEADLINE_SECS, CLEANUP_DEADLINE_SECS, CampaignConfig,
    CampaignLimits, CampaignPlan, ClusterHarness, ClusterHarnessConfig, CoverageLedger,
    DIAGNOSTIC_DEADLINE_SECS, DeploymentBoundary, DeploymentBundle, FixturePathSnapshot,
    HarnessProvider, LOCAL_CLEANUP_RESERVE_SECS, OfferPolicy, PREPARATION_DEADLINE_SECS,
    PaidCampaignPhase, PaidCampaignState, ProviderMode, RawDockerFleet, RawPriorState,
    RecoveryCase, WORKLOAD_DEADLINE_SECS, authorized_cleanup_limits, conservative_selected_cost,
    decode_offer_results, failure_corpus, offer_search_request, random_short_dags_budgeted,
    read_campaign_plan, read_coverage_ledger, read_paid_state, render_case_python,
    scan_artifacts_for_secret, select_exact_offers, stable_corpus, write_campaign_plan,
    write_coverage_ledger, write_paid_state,
};
use myelin_e2e_fuzz::{GateImageIdentity, immutable_registry_reference, runtime_image_identity};
use sha2::{Digest as _, Sha256};

struct Options {
    seed: u64,
    nodes: u8,
    random_cases: usize,
    max_cases: Option<usize>,
    failure_cases: bool,
    smoke_only: bool,
    recovery_only: bool,
    artifacts: PathBuf,
    image: Option<String>,
    build_image: bool,
    replay: Option<PathBuf>,
    shrink: bool,
    deadline: Duration,
    campaign: bool,
    deployment_e2e: bool,
    prepare_artifacts: bool,
    deployment_nodes: u8,
    paid_vastai: bool,
    authorize_paid_vastai: bool,
    retain_paid_fixture: bool,
    gate_attestation: Option<PathBuf>,
    scripted_provider: bool,
    scripted_campaign_resource_overflow: bool,
    cleanup_only: Option<PathBuf>,
    resume_paid: Option<PathBuf>,
    paid_cleanup_owner: Option<PathBuf>,
    paid_cleanup_supervisor: Option<PathBuf>,
    scripted_retained_lifecycle: Option<PathBuf>,
    fixture_lifetime_secs: Option<u64>,
    max_total_cost_usd: Option<f64>,
    max_hourly_cost_usd: Option<f64>,
    ssh_identity: Option<PathBuf>,
    api_key_env: String,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock precedes epoch: {error}"))?
            .as_secs();
        let mut options = Self {
            seed,
            nodes: 2,
            random_cases: 16,
            max_cases: None,
            failure_cases: false,
            smoke_only: false,
            recovery_only: false,
            artifacts: PathBuf::from("target/e2e-behavioral-fuzz"),
            image: None,
            build_image: true,
            replay: None,
            shrink: true,
            deadline: Duration::ZERO,
            campaign: false,
            deployment_e2e: false,
            prepare_artifacts: false,
            deployment_nodes: 5,
            paid_vastai: false,
            authorize_paid_vastai: false,
            retain_paid_fixture: false,
            gate_attestation: None,
            scripted_provider: false,
            scripted_campaign_resource_overflow: false,
            cleanup_only: None,
            resume_paid: None,
            paid_cleanup_owner: None,
            paid_cleanup_supervisor: None,
            scripted_retained_lifecycle: None,
            fixture_lifetime_secs: None,
            max_total_cost_usd: None,
            max_hourly_cost_usd: None,
            ssh_identity: None,
            api_key_env: "VASTAI_API_KEY".to_owned(),
        };
        let mut args = std::env::args().skip(1);
        while let Some(argument) = args.next() {
            let mut next = |name: &str| {
                args.next()
                    .ok_or_else(|| format!("{name} requires a value"))
            };
            match argument.as_str() {
                "--paid-cleanup-owner" => {
                    options.paid_cleanup_owner = Some(PathBuf::from(next("--paid-cleanup-owner")?))
                }
                "--paid-cleanup-supervisor" => {
                    options.paid_cleanup_supervisor =
                        Some(PathBuf::from(next("--paid-cleanup-supervisor")?))
                }
                "--scripted-retained-lifecycle" => {
                    options.scripted_retained_lifecycle =
                        Some(PathBuf::from(next("--scripted-retained-lifecycle")?))
                }
                "--seed" => {
                    options.seed = next("--seed")?
                        .parse()
                        .map_err(|error| format!("invalid --seed: {error}"))?;
                }
                "--nodes" => {
                    options.nodes = next("--nodes")?
                        .parse()
                        .map_err(|error| format!("invalid --nodes: {error}"))?;
                }
                "--random-cases" => {
                    options.random_cases = next("--random-cases")?
                        .parse()
                        .map_err(|error| format!("invalid --random-cases: {error}"))?;
                }
                "--max-cases" => {
                    options.max_cases = Some(
                        next("--max-cases")?
                            .parse()
                            .map_err(|error| format!("invalid --max-cases: {error}"))?,
                    );
                }
                "--smoke-only" => options.smoke_only = true,
                "--artifacts" => options.artifacts = PathBuf::from(next("--artifacts")?),
                "--image" => options.image = Some(next("--image")?),
                "--no-build-image" => options.build_image = false,
                "--replay" => options.replay = Some(PathBuf::from(next("--replay")?)),
                "--failure-cases" => options.failure_cases = true,
                "--recovery-only" => options.recovery_only = true,
                "--no-shrink" => options.shrink = false,
                "--campaign" => options.campaign = true,
                "--deployment-e2e" => options.deployment_e2e = true,
                "--prepare-artifacts" => options.prepare_artifacts = true,
                "--deployment-nodes" => {
                    options.deployment_nodes = next("--deployment-nodes")?
                        .parse()
                        .map_err(|error| format!("invalid --deployment-nodes: {error}"))?;
                }
                "--paid-vastai" => {
                    options.campaign = true;
                    options.paid_vastai = true;
                }
                "--authorize-paid-vastai" => options.authorize_paid_vastai = true,
                "--retain-paid-fixture" => options.retain_paid_fixture = true,
                "--gate-attestation" => {
                    options.gate_attestation = Some(PathBuf::from(next("--gate-attestation")?))
                }
                "--scripted-provider" => options.scripted_provider = true,
                "--scripted-campaign-resource-overflow" => {
                    options.scripted_campaign_resource_overflow = true
                }
                "--cleanup-only" => {
                    options.cleanup_only = Some(PathBuf::from(next("--cleanup-only")?))
                }
                "--resume-paid" => {
                    options.resume_paid = Some(PathBuf::from(next("--resume-paid")?))
                }
                "--fixture-lifetime-secs" => {
                    options.fixture_lifetime_secs =
                        Some(next("--fixture-lifetime-secs")?.parse().map_err(|error| {
                            format!("invalid --fixture-lifetime-secs: {error}")
                        })?);
                }
                "--max-total-cost-usd" => {
                    options.max_total_cost_usd = Some(
                        next("--max-total-cost-usd")?
                            .parse()
                            .map_err(|error| format!("invalid --max-total-cost-usd: {error}"))?,
                    );
                }
                "--max-hourly-cost-usd" => {
                    options.max_hourly_cost_usd = Some(
                        next("--max-hourly-cost-usd")?
                            .parse()
                            .map_err(|error| format!("invalid --max-hourly-cost-usd: {error}"))?,
                    );
                }
                "--ssh-identity" => {
                    options.ssh_identity = Some(PathBuf::from(next("--ssh-identity")?))
                }
                "--api-key-env" => options.api_key_env = next("--api-key-env")?,
                "--deadline-secs" => {
                    options.deadline = Duration::from_secs(
                        next("--deadline-secs")?
                            .parse()
                            .map_err(|error| format!("invalid --deadline-secs: {error}"))?,
                    );
                }
                "--help" | "-h" => {
                    println!(
                        "myelin-e2e-fuzz [--campaign [--paid-vastai --authorize-paid-vastai]] \
                         [--resume-paid PAID_STATE.json --authorize-paid-vastai] \
                         [--fixture-lifetime-secs N] [--max-total-cost-usd N] \
                         [--max-hourly-cost-usd N] [--ssh-identity PATH] [--api-key-env NAME] \
                         [--gate-attestation ordered-acceptance.json] [--retain-paid-fixture] \
                         [--scripted-provider [--scripted-campaign-resource-overflow]] \
                         [--cleanup-only PAID_STATE.json] [--deployment-e2e \
                         [--deployment-nodes 2..5]] [--seed N] [--nodes 2..5] \
                         [--random-cases N] [--max-cases N] [--smoke-only] [--failure-cases] \
                         [--recovery-only] [--artifacts PATH] [--image TAG] [--no-build-image] \
                         [--prepare-artifacts] [--no-shrink] [--replay CASE.json] --deadline-secs N"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown option {other:?}")),
            }
        }
        if options.scripted_campaign_resource_overflow
            && (!options.paid_vastai
                || !options.scripted_provider
                || options.cleanup_only.is_some()
                || options.resume_paid.is_some()
                || options.paid_cleanup_owner.is_some()
                || options.paid_cleanup_supervisor.is_some()
                || options.scripted_retained_lifecycle.is_some()
                || options.deployment_e2e
                || options.prepare_artifacts)
        {
            return Err(
                "--scripted-campaign-resource-overflow requires only the scripted paid campaign mode"
                    .to_owned(),
            );
        }
        if options.retain_paid_fixture
            && ((!options.paid_vastai && options.resume_paid.is_none())
                || options.cleanup_only.is_some()
                || options.paid_cleanup_owner.is_some()
                || options.paid_cleanup_supervisor.is_some()
                || options.scripted_retained_lifecycle.is_some()
                || options.deployment_e2e
                || options.prepare_artifacts)
        {
            return Err(
                "--retain-paid-fixture requires paid preparation or explicit paid resume"
                    .to_owned(),
            );
        }
        if options.prepare_artifacts
            && (options.campaign
                || options.deployment_e2e
                || options.paid_vastai
                || options.resume_paid.is_some()
                || options.cleanup_only.is_some()
                || options.paid_cleanup_owner.is_some()
                || options.paid_cleanup_supervisor.is_some()
                || options.scripted_retained_lifecycle.is_some()
                || options.failure_cases
                || options.smoke_only
                || options.recovery_only
                || options.replay.is_some())
        {
            return Err(
                "--prepare-artifacts cannot be combined with execution or cleanup modes".to_owned(),
            );
        }
        if options.paid_cleanup_owner.is_some()
            || options.paid_cleanup_supervisor.is_some()
            || options.scripted_retained_lifecycle.is_some()
        {
            return Ok(options);
        }
        if options.deadline.is_zero() {
            return Err("--deadline-secs is required and must be nonzero".to_owned());
        }
        if options.deployment_e2e && !(2..=5).contains(&options.deployment_nodes) {
            return Err("--deployment-nodes must be between 2 and 5".to_owned());
        }
        if options.cleanup_only.is_some() {
            return Ok(options);
        }
        if options.resume_paid.is_some() {
            if !options.authorize_paid_vastai {
                return Err("paid fixture resume requires --authorize-paid-vastai".to_owned());
            }
            if options.image.is_none() || options.ssh_identity.is_none() {
                return Err("paid fixture resume requires --image and --ssh-identity".to_owned());
            }
            return Ok(options);
        }
        if options.campaign {
            if options.fixture_lifetime_secs.is_none() {
                return Err("--campaign requires --fixture-lifetime-secs".to_owned());
            }
            if options.paid_vastai {
                if !options.authorize_paid_vastai {
                    return Err("paid execution requires --authorize-paid-vastai".to_owned());
                }
                if options.image.is_none()
                    || options.max_total_cost_usd.is_none()
                    || options.max_hourly_cost_usd.is_none()
                    || options.ssh_identity.is_none()
                {
                    return Err(
                        "paid execution requires --image, both cost ceilings, and --ssh-identity"
                            .to_owned(),
                    );
                }
            }
        } else if !(2..=5).contains(&options.nodes) {
            return Err("--nodes must be between 2 and 5".to_owned());
        }
        Ok(options)
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("myelin-e2e-behavioral-fuzz: {error}");
        std::process::exit(1);
    }
}

static TIMING_ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
static TIMING_SPANS: Mutex<Vec<serde_json::Value>> = Mutex::new(Vec::new());

struct PhaseTimer {
    name: String,
    started: Instant,
}

impl PhaseTimer {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            started: Instant::now(),
        }
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        myelin_e2e_fuzz::record_execution_stage(&self.name, elapsed, 0, 0);
        if let Ok(mut spans) = TIMING_SPANS.lock() {
            spans.push(serde_json::json!({
                "stage": self.name,
                "start_us": self.started.duration_since(*TIMING_ORIGIN).as_micros(),
                "elapsed_us": elapsed.as_micros(),
            }));
        }
    }
}

fn run() -> Result<(), String> {
    let started = *TIMING_ORIGIN;
    let options = Options::parse()?;
    if let Some(admission) = &options.scripted_retained_lifecycle {
        return myelin_e2e_fuzz::run_scripted_retained_lifecycle(admission, &options.api_key_env);
    }
    if let Some(admission) = &options.paid_cleanup_supervisor {
        return myelin_e2e_fuzz::run_cleanup_supervisor(admission, &options.api_key_env);
    }
    if let Some(admission) = &options.paid_cleanup_owner {
        return myelin_e2e_fuzz::run_cleanup_owner(admission, &options.api_key_env);
    }
    let result = run_options(&options);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("timing wall clock: {error}"))?
        .as_millis();
    let mut evidence = serde_json::json!({
        "schema_version": 1,
        "clock": "monotonic",
        "elapsed_us": started.elapsed().as_micros(),
        "passed": result.is_ok(),
        "pending_predicate": result.as_ref().err(),
        "seed": options.seed,
        "machine": fs::read_to_string("/proc/sys/kernel/hostname").ok(),
        "storage": storage_evidence(),
        "cpu": fs::read_to_string("/proc/cpuinfo").ok().and_then(|cpu| cpu.lines()
            .find(|line| line.starts_with("model name")).map(str::to_owned)),
        "memory": fs::read_to_string("/proc/self/status").ok().map(|status| status.lines()
            .filter(|line| line.starts_with("VmHWM:") || line.starts_with("VmPeak:"))
            .map(str::to_owned).collect::<Vec<_>>()),
        "configuration": {
            "case_deadline_secs": options.deadline.as_secs(),
            "campaign_secs": CAMPAIGN_DEADLINE_SECS,
            "workload_secs": WORKLOAD_DEADLINE_SECS,
            "diagnosis_secs": DIAGNOSTIC_DEADLINE_SECS,
            "local_cleanup_reserve_secs": LOCAL_CLEANUP_RESERVE_SECS,
            "paid": options.paid_vastai || options.resume_paid.is_some(),
        },
        "spans": TIMING_SPANS.lock().map_err(|_| "timing spans poisoned".to_owned())?.as_slice(),
        "resources": myelin_e2e_fuzz::execution_evidence(),
    });
    if let Ok(secret) = std::env::var(&options.api_key_env) {
        if !secret.is_empty() {
            redact_evidence(&mut evidence, &secret);
        }
    }
    let timing = durable_json(
        &options
            .artifacts
            .join("timing-runs")
            .join(format!("{stamp}-{}.json", std::process::id())),
        &evidence,
    );
    let final_scan = (|| {
        if !(options.paid_vastai || options.resume_paid.is_some() || options.cleanup_only.is_some())
        {
            return Ok(());
        }
        let secret = std::env::var(&options.api_key_env)
            .map_err(|_| "paid credential environment is unset".to_owned())?;
        let mut roots = vec![options.artifacts.clone()];
        if let Some(state_path) = options
            .resume_paid
            .as_ref()
            .or(options.cleanup_only.as_ref())
        {
            roots.push(
                state_path
                    .parent()
                    .ok_or("paid state parent missing")?
                    .to_path_buf(),
            );
            roots.push(read_paid_state(state_path)?.state_dir);
        } else {
            for entry in fs::read_dir(&options.artifacts)
                .map_err(|error| format!("scan paid state roots: {error}"))?
            {
                let path = entry
                    .map_err(|error| format!("scan paid state entry: {error}"))?
                    .path()
                    .join("paid-state.json");
                if path.is_file() {
                    roots.push(read_paid_state(&path)?.state_dir);
                }
            }
        }
        let mut failures = Vec::new();
        for root in roots {
            if root.exists() {
                if let Err(error) = scan_artifacts_for_secret(&root, secret.as_bytes()) {
                    failures.push(error);
                }
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    })();
    merge_execution_cleanup(result, merge_execution_cleanup(timing, final_scan))
}

fn redact_evidence(value: &mut serde_json::Value, secret: &str) {
    match value {
        serde_json::Value::String(text) => *text = text.replace(secret, "[redacted]"),
        serde_json::Value::Array(values) => {
            for value in values {
                redact_evidence(value, secret);
            }
        }
        serde_json::Value::Object(fields) => {
            let original = std::mem::take(fields);
            for (key, mut value) in original {
                redact_evidence(&mut value, secret);
                fields.insert(key.replace(secret, "[redacted]"), value);
            }
        }
        _ => {}
    }
}

fn storage_evidence() -> serde_json::Value {
    let mut status = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c".".as_ptr(), status.as_mut_ptr()) } != 0 {
        return serde_json::json!({"error": std::io::Error::last_os_error().to_string()});
    }
    let status = unsafe { status.assume_init() };
    serde_json::json!({
        "filesystem_id": status.f_fsid,
        "block_size": status.f_bsize,
        "fragment_size": status.f_frsize,
        "blocks": status.f_blocks,
        "available_blocks": status.f_bavail,
        "available_inodes": status.f_favail,
    })
}

fn run_options(options: &Options) -> Result<(), String> {
    let workspace = std::env::current_dir()
        .map_err(|error| format!("read current workspace directory: {error}"))?;
    if options.cleanup_only.is_none() {
        if let Some(path) = std::env::var_os("MYELIN_E2E_ARTIFACT_IDENTITY") {
            let expected: myelin_e2e_fuzz::PreparedArtifactIdentity = serde_json::from_reader(
                fs::File::open(path)
                    .map_err(|error| format!("open required artifact identity: {error}"))?,
            )
            .map_err(|error| format!("decode required artifact identity: {error}"))?;
            myelin_e2e_fuzz::require_prepared_artifacts(
                &workspace,
                &expected,
                &Budget::new(options.deadline),
            )?;
        }
    }
    if options.prepare_artifacts {
        let budget = Budget::new(options.deadline);
        let _phase = PhaseTimer::new("artifact-preparation");
        myelin_e2e_fuzz::stage_deployment_payload(&workspace, &options.artifacts, &budget)?;
        return budget.check("complete immutable binary and wheel preparation");
    }
    if let Some(state_path) = &options.cleanup_only {
        return run_cleanup_only(state_path, &options.api_key_env, options.deadline);
    }
    if options.paid_vastai || options.resume_paid.is_some() {
        validate_paid_gates(options, &workspace)?;
    }
    if let Some(state_path) = &options.resume_paid {
        return run_resume_paid(state_path, &options, workspace);
    }
    if options.deployment_e2e {
        return run_deployment_e2e(&options, workspace);
    }
    if options.campaign {
        return run_campaign(&options, workspace);
    }
    let total = Budget::new(Duration::from_secs(CAMPAIGN_DEADLINE_SECS));
    let execution = total.child(Duration::from_secs(
        CAMPAIGN_DEADLINE_SECS - LOCAL_CLEANUP_RESERVE_SECS,
    ));
    let mut cases = if let Some(replay) = &options.replay {
        vec![
            serde_json::from_slice::<BehaviorCase>(
                &fs::read(replay).map_err(|error| format!("read replay case: {error}"))?,
            )
            .map_err(|error| format!("parse replay case: {error}"))?,
        ]
    } else if options.failure_cases {
        failure_corpus(options.nodes, options.seed)
    } else {
        let mut stable = stable_corpus(options.nodes, options.seed);
        if options.smoke_only {
            stable.truncate(usize::from(options.nodes) * (usize::from(options.nodes) - 1) * 2);
        } else {
            stable.extend(random_short_dags_budgeted(
                options.seed.rotate_left(17),
                options.random_cases,
                options.nodes,
                &execution,
            )?);
        }
        stable
    };
    if let Some(max_cases) = options.max_cases {
        cases.truncate(max_cases);
    }
    if options.recovery_only {
        cases.clear();
    }
    if cases.is_empty() && !options.recovery_only {
        return Err("no behavioral cases selected".to_owned());
    }

    for case in &cases {
        case.validate()?;
    }
    fs::create_dir_all(&options.artifacts)
        .map_err(|error| format!("create artifact root: {error}"))?;
    let fixture_node_count = if options.replay.is_some() {
        cases
            .first()
            .map(BehaviorCase::node_count)
            .ok_or_else(|| "replay case list is empty".to_owned())?
    } else {
        options.nodes
    };
    let config = ClusterHarnessConfig {
        workspace,
        artifacts: options.artifacts.clone(),
        node_count: fixture_node_count,
        seed: options.seed,
        image: options.image.clone(),
        build_image: options.build_image,
        deadline: options.deadline,
        provider: HarnessProvider::LocalMock,
        selected_offer_ids: (1..=u64::from(fixture_node_count)).collect(),
        state_dir: None,
        reset_state: true,
        offer_search_id: None,
        provision: true,
        adopt_only: false,
        relay_url: None,
        run_id: 1,
    };
    let mut harness = ClusterHarness::start_with_budgets(config, execution.clone(), total.clone())?;
    let result = (|| {
        let preparation = PhaseTimer::new("fixture-preparation");
        harness.verify_running_recovery()?;
        if !options.recovery_only {
            harness.verify_workload_convergence()?;
        }
        drop(preparation);
        let workload = execution.child(
            execution
                .remaining("standalone workload allocation")?
                .saturating_sub(Duration::from_secs(DIAGNOSTIC_DEADLINE_SECS))
                .min(Duration::from_secs(WORKLOAD_DEADLINE_SECS)),
        );
        let diagnosis = execution.clone();
        harness.set_execution_budget(workload.clone());
        for (index, case) in cases.iter().enumerate() {
            workload.check(&format!("admit case {}", case.id))?;
            println!("case {}/{}: {}", index + 1, cases.len(), case.id);
            run_campaign_case(
                &mut harness,
                case,
                &options.artifacts,
                &options.artifacts,
                options.shrink,
                &diagnosis,
            )?;
        }
        harness.verify_missing_resource_recovery()?;
        workload.check("complete standalone workload")
    })();
    let _cleanup = PhaseTimer::new("fixture-cleanup");
    let cleanup_budget = total.child(Duration::from_secs(LOCAL_CLEANUP_RESERVE_SECS));
    let teardown = harness.teardown_with_budget(cleanup_budget);
    merge_execution_cleanup(result, teardown)?;
    total.check("complete standalone run and cleanup")
}

fn durable_write(path: &Path, contents: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("artifact path {} has no parent", path.display()))?;
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "create durable artifact parent {}: {error}",
            parent.display()
        )
    })?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| format!("create durable artifact temporary file: {error}"))?;
    temp.write_all(contents)
        .map_err(|error| format!("write durable artifact: {error}"))?;
    temp.as_file_mut()
        .sync_all()
        .map_err(|error| format!("sync durable artifact: {error}"))?;
    temp.persist(path).map_err(|error| {
        format!(
            "persist durable artifact {}: {}",
            path.display(),
            error.error
        )
    })?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "sync durable artifact directory {}: {error}",
                parent.display()
            )
        })
}

fn durable_json(path: &Path, value: &impl serde::Serialize) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("artifact {} has no parent", path.display()))?;
    fs::create_dir_all(parent).map_err(|error| format!("create JSON artifact parent: {error}"))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| format!("create JSON temporary file: {error}"))?;
    {
        let mut writer = std::io::BufWriter::new(temp.as_file_mut());
        serde_json::to_writer(&mut writer, value)
            .map_err(|error| format!("stream JSON artifact: {error}"))?;
        writer
            .flush()
            .map_err(|error| format!("flush JSON artifact: {error}"))?;
    }
    temp.as_file()
        .sync_all()
        .map_err(|error| format!("sync JSON artifact: {error}"))?;
    temp.persist(path)
        .map_err(|error| format!("persist JSON artifact: {}", error.error))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync JSON artifact directory: {error}"))
}

fn claim_campaign_directory(path: &Path, paid: bool) -> Result<(), String> {
    if !paid {
        return fs::create_dir_all(path)
            .map_err(|error| format!("create campaign directory: {error}"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| "campaign directory has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| format!("create campaign parent: {error}"))?;
    // Never open or replace any ownership/plan file until this exclusive claim succeeds.
    fs::create_dir(path).map_err(|error| format!(
        "refuse existing or unclaimable paid campaign {}: {error}; use cleanup-only or explicit resume",
        path.display()))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync paid campaign claim: {error}"))
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OrderedGateAttestation {
    schema_version: u32,
    state: String,
    completed_unix_ms: u64,
    expires_unix_ms: u64,
    build_artifacts: BTreeMap<String, String>,
    deployment_artifacts: myelin_e2e_fuzz::PreparedArtifactIdentity,
    source_digest: String,
    image_identities: BTreeMap<String, GateImageIdentity>,
    configuration: GateConfiguration,
    stages: Vec<GateStage>,
    runs: Vec<GateRun>,
    build_elapsed_secs: f64,
    elapsed_secs: f64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct GateConfiguration {
    warm_runs: usize,
    image: String,
    seed: u64,
    case_deadline_secs: u64,
    build_images: bool,
    workspace: PathBuf,
    artifacts: PathBuf,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct GateStage {
    name: String,
    state: String,
    exit_code: Option<i32>,
    started_unix_ms: u64,
    completed_unix_ms: u64,
    command: Vec<String>,
    elapsed_secs: f64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct GateRun {
    index: usize,
    state: String,
    gate_a: String,
    inside_target: bool,
    campaign_elapsed_secs: f64,
    elapsed_secs: f64,
}

fn expected_gate_stages(evidence: &OrderedGateAttestation) -> Vec<(String, Vec<String>)> {
    let config = &evidence.configuration;
    let binary = config
        .workspace
        .join("target/release/myelin-e2e-fuzz")
        .to_string_lossy()
        .into_owned();
    let strings = |values: &[&str]| {
        values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
    };
    let artifact = |name: &str| config.artifacts.join(name).to_string_lossy().into_owned();
    let mut tests = strings(&["cargo", "test"]);
    for package in [
        "swactor",
        "myelin-e2e-fuzz",
        "swactor-vastai",
        "myelin-control-contract",
        "provisioning",
        "myelin",
        "data-plane",
        "iroh-driver",
        "distribution",
        "swactor-process",
        "swactor-process-context",
    ] {
        tests.extend(strings(&["-p", package]));
    }
    tests.push("--tests".to_owned());
    let mut no_run = tests.clone();
    no_run.push("--no-run".to_owned());
    let mut stages = vec![
        (
            "build".to_owned(),
            strings(&[
                "cargo",
                "build",
                "--release",
                "-p",
                "myelin",
                "--bins",
                "-p",
                "myelin-e2e-fuzz",
            ]),
        ),
        ("build-test-binaries".to_owned(), no_run),
        (
            "build-deployment-artifacts".to_owned(),
            strings(&[
                &binary,
                "--prepare-artifacts",
                "--deadline-secs",
                "3600",
                "--artifacts",
                &artifact("build-deployment-artifacts"),
            ]),
        ),
    ];
    if config.build_images {
        let mut parent: Option<&str> = None;
        for (role, suffix, image) in [
            ("base", ".base", "myelin-node-base:cuda12.6"),
            ("node", "", "myelin-node:latest"),
            ("e2e", ".e2e", config.image.as_str()),
        ] {
            let mut command = strings(&[
                "docker",
                "build",
                "-f",
                &format!("apps/myelin/node-image/Dockerfile{suffix}"),
                "-t",
                image,
            ]);
            if let Some(parent) = parent {
                let tag = format!("myelin-e2e-parent:{}", parent.trim_start_matches("sha256:"));
                stages.push((
                    format!("build-image-{role}-parent"),
                    strings(&["docker", "tag", parent, &tag]),
                ));
                command.extend(strings(&["--build-arg", &format!("BASE_IMAGE={tag}")]));
            }
            for (key, value) in [
                ("provenance-version", "1"),
                (
                    "source-build-input-digest",
                    evidence
                        .deployment_artifacts
                        .source_build_input_digest
                        .as_str(),
                ),
                ("image-role", role),
            ] {
                command.extend(strings(&[
                    "--label",
                    &format!("org.swactor.myelin.e2e.{key}={value}"),
                ]));
            }
            if let Some(parent) = parent {
                command.extend(strings(&[
                    "--label",
                    &format!("org.swactor.myelin.e2e.parent-image-id={parent}"),
                ]));
            }
            command.push(".".to_owned());
            stages.push((format!("build-image-{role}"), command));
            parent = evidence
                .image_identities
                .get(image)
                .map(|identity| identity.id.as_str());
        }
    }
    for index in 1..=config.warm_runs {
        let common = strings(&[
            &binary,
            "--seed",
            &config.seed.to_string(),
            "--deadline-secs",
            &config.case_deadline_secs.to_string(),
            "--no-build-image",
            "--image",
            &config.image,
        ]);
        for (suffix, flags) in [
            (
                "gate-a",
                strings(&["--deployment-e2e", "--deployment-nodes", "5"]),
            ),
            (
                "campaign",
                strings(&["--campaign", "--fixture-lifetime-secs", "43200"]),
            ),
            (
                "failure-cases",
                strings(&["--nodes", "5", "--failure-cases"]),
            ),
        ] {
            let mut command = common.clone();
            command.extend(flags);
            command.extend(strings(&[
                "--artifacts",
                &artifact(&format!("warm-{index}/{suffix}")),
            ]));
            stages.push((format!("warm-{index}-{suffix}"), command));
        }
        stages.push((format!("warm-{index}-contract-model-safety"), tests.clone()));
        stages.push((
            format!("warm-{index}-scripted-provider"),
            strings(&[
                "bash",
                "tools/myelin-e2e-fuzz/scripted_safety_gate.sh",
                &artifact(&format!("warm-{index}/scripted-provider")),
            ]),
        ));
    }
    stages.push((
        "verify-qualified-deployment-artifacts".to_owned(),
        strings(&[
            &binary,
            "--prepare-artifacts",
            "--deadline-secs",
            &config.case_deadline_secs.to_string(),
            "--artifacts",
            &artifact("verify-qualified-deployment-artifacts"),
        ]),
    ));
    stages
}

fn valid_gate_duration(seconds: f64) -> bool {
    seconds.is_finite() && seconds >= 0.0
}

fn validate_gate_attestation(evidence: &OrderedGateAttestation, now: u64) -> Result<(), String> {
    if evidence.schema_version != 5
        || evidence.state != "passed"
        || evidence.completed_unix_ms > now
        || evidence.expires_unix_ms <= now
        || evidence.expires_unix_ms <= evidence.completed_unix_ms
        || evidence.expires_unix_ms - evidence.completed_unix_ms > 86_400_000
        || evidence.configuration.warm_runs < 3
        || evidence.runs.len() != evidence.configuration.warm_runs
        || evidence.configuration.case_deadline_secs == 0
        || !evidence.configuration.workspace.is_absolute()
        || !evidence.configuration.artifacts.is_absolute()
        || !valid_gate_duration(evidence.build_elapsed_secs)
        || !valid_gate_duration(evidence.elapsed_secs)
    {
        return Err(
            "paid gate attestation is missing, incomplete, stale, or incompatible".to_owned(),
        );
    }
    let expected_images = [
        "myelin-node-base:cuda12.6",
        "myelin-node:latest",
        evidence.configuration.image.as_str(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if evidence
        .image_identities
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != expected_images
        || expected_images.len() != 3
        || evidence.image_identities.values().any(|identity| {
            identity.os.is_empty()
                || identity.architecture.is_empty()
                || identity.provenance_version != 1
                || identity.source_build_input_digest
                    != evidence.deployment_artifacts.source_build_input_digest
                || identity.id.strip_prefix("sha256:").is_none_or(|digest| {
                    digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
        })
    {
        return Err("paid gate evidence omitted an exact deployment runtime image".to_owned());
    }
    let source_input = &evidence.deployment_artifacts.source_build_input_digest;
    if source_input.len() != 64 || !source_input.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("qualified source/build input digest is missing or malformed".to_owned());
    }
    let base = &evidence.image_identities["myelin-node-base:cuda12.6"];
    let node = &evidence.image_identities["myelin-node:latest"];
    let runtime = &evidence.image_identities[&evidence.configuration.image];
    if base.image_role != "base"
        || base.parent_image_id.is_some()
        || node.image_role != "node"
        || node.parent_image_id.as_ref() != Some(&base.id)
        || runtime.image_role != "e2e"
        || runtime.parent_image_id.as_ref() != Some(&node.id)
    {
        return Err(
            "runtime images lack the qualified build-time source and parent provenance".to_owned(),
        );
    }
    let expected = expected_gate_stages(evidence);
    if evidence.stages.len() != expected.len() {
        return Err(
            "paid authorization requires every build, warm gate and final verification stage"
                .to_owned(),
        );
    }
    let mut previous = 0;
    for (stage, (name, command)) in evidence.stages.iter().zip(expected) {
        if stage.name != name {
            return Err(format!(
                "missing, duplicate or out-of-order required gate {name}"
            ));
        }
        if stage.command != command {
            return Err(format!(
                "required gate {name} changed its qualified command or case selection"
            ));
        }
        if stage.state != "passed"
            || stage.exit_code != Some(0)
            || !valid_gate_duration(stage.elapsed_secs)
            || stage.started_unix_ms < previous
            || stage.completed_unix_ms < stage.started_unix_ms
            || stage.completed_unix_ms > evidence.completed_unix_ms
        {
            return Err("paid gate stages failed or were not executed in order".to_owned());
        }
        previous = stage.completed_unix_ms;
    }
    let build_count = if evidence.configuration.build_images {
        8
    } else {
        3
    };
    if evidence.build_elapsed_secs
        < evidence.stages[..build_count]
            .iter()
            .map(|stage| stage.elapsed_secs)
            .sum::<f64>()
        || evidence.elapsed_secs
            < evidence
                .stages
                .iter()
                .map(|stage| stage.elapsed_secs)
                .sum::<f64>()
        || evidence.elapsed_secs
            < evidence.build_elapsed_secs
                + evidence
                    .runs
                    .iter()
                    .map(|run| run.elapsed_secs)
                    .sum::<f64>()
    {
        return Err("paid gate timing omits required phase execution".to_owned());
    }
    for (offset, run) in evidence.runs.iter().enumerate() {
        let phases = &evidence.stages[build_count + offset * 5..build_count + (offset + 1) * 5];
        if run.index != offset + 1
            || run.state != "passed"
            || run.gate_a != "passed"
            || !run.inside_target
            || !valid_gate_duration(run.campaign_elapsed_secs)
            || !valid_gate_duration(run.elapsed_secs)
            || run.campaign_elapsed_secs > 300.0
            || run.elapsed_secs > 600.0
            || run.campaign_elapsed_secs != phases[1].elapsed_secs
            || run.elapsed_secs < phases.iter().map(|stage| stage.elapsed_secs).sum::<f64>()
        {
            return Err("paid authorization requires every complete local run inside measured campaign and total ceilings".to_owned());
        }
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<String, String> {
    let mut source = fs::File::open(path)
        .map_err(|error| format!("open artifact {}: {error}", path.display()))?;
    let mut digest = Sha256::new();
    std::io::copy(&mut source, &mut digest)
        .map_err(|error| format!("hash artifact {}: {error}", path.display()))?;
    Ok(format!("{:x}", digest.finalize()))
}

fn artifact_identity(workspace: &Path) -> Result<BTreeMap<String, String>, String> {
    ["myelin-e2e-fuzz", "myelin-orchestrator", "myelin-worker"]
        .into_iter()
        .map(|name| {
            Ok((
                name.to_owned(),
                hash_file(&workspace.join("target/release").join(name))?,
            ))
        })
        .collect()
}

fn source_identity(workspace: &Path) -> Result<String, String> {
    fn collect(root: &Path, paths: &mut Vec<PathBuf>) -> Result<(), String> {
        for entry in
            fs::read_dir(root).map_err(|error| format!("read source directory: {error}"))?
        {
            let entry = entry.map_err(|error| format!("read source entry: {error}"))?;
            let kind = entry
                .file_type()
                .map_err(|error| format!("inspect source: {error}"))?;
            if kind.is_symlink() {
                return Err(format!(
                    "source identity rejects symlink {}",
                    entry.path().display()
                ));
            }
            if kind.is_dir() {
                if !matches!(
                    entry.file_name().to_str(),
                    Some("target" | ".git" | ".venv" | "__pycache__" | "node_modules")
                ) {
                    collect(&entry.path(), paths)?;
                }
            } else if kind.is_file() {
                paths.push(entry.path());
            }
        }
        Ok(())
    }
    let mut paths = [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        ".dockerignore",
        "clippy.toml",
    ]
    .into_iter()
    .map(|name| workspace.join(name))
    .collect::<Vec<_>>();
    for directory in [
        ".cargo",
        "src",
        "tests",
        "crates",
        "xtask",
        "apps/myelin",
        "tools/myelin-e2e-fuzz",
        "tools/vastai",
        "tools/actor-control-flow-lint",
    ] {
        collect(&workspace.join(directory), &mut paths)?;
    }
    paths.sort();
    let mut digest = Sha256::new();
    for path in paths {
        digest.update(
            path.strip_prefix(workspace)
                .map_err(|error| error.to_string())?
                .as_os_str()
                .as_encoded_bytes(),
        );
        digest.update([0]);
        digest.update(hash_file(&path)?.as_bytes());
        digest.update([0]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn validate_paid_gates(options: &Options, workspace: &Path) -> Result<(), String> {
    if options.scripted_provider {
        // A test mode is not paid permission: only a literal loopback endpoint is permitted.
        let endpoint = std::env::var("VASTAI_BASE_URL").unwrap_or_default();
        let port = endpoint
            .strip_prefix("http://127.0.0.1:")
            .and_then(|port| port.parse::<u16>().ok())
            .filter(|port| *port != 0);
        return if port.is_some() {
            Ok(())
        } else {
            Err("scripted provider requires an explicit http://127.0.0.1:PORT endpoint".to_owned())
        };
    }
    let path = options.gate_attestation.as_ref()
        .ok_or_else(|| "paid execution requires --gate-attestation from ordered_acceptance.py; operator authorization is not gate evidence".to_owned())?;
    let evidence: OrderedGateAttestation = serde_json::from_reader(
        fs::File::open(path).map_err(|error| format!("open paid gate attestation: {error}"))?,
    )
    .map_err(|error| format!("decode paid gate attestation: {error}"))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("gate clock: {error}"))?
        .as_millis() as u64;
    validate_gate_attestation(&evidence, now)?;
    if !immutable_registry_reference(&evidence.configuration.image) {
        return Err("paid execution requires a qualified repository@sha256:manifest_digest image, not a mutable tag".to_owned());
    }
    if evidence.build_artifacts != artifact_identity(workspace)?
        || evidence.source_digest != source_identity(workspace)?
        || evidence.configuration.workspace
            != workspace
                .canonicalize()
                .map_err(|error| format!("resolve qualified workspace: {error}"))?
        || hash_file(&std::env::current_exe().map_err(|error| error.to_string())?)?
            != *evidence
                .build_artifacts
                .get("myelin-e2e-fuzz")
                .ok_or("missing tested harness digest")?
        || options.image.as_deref() != Some(evidence.configuration.image.as_str())
    {
        return Err(
            "paid gate evidence does not attest the current binaries, sources and image".to_owned(),
        );
    }
    let budget = Budget::new(options.deadline);
    for (name, expected) in &evidence.image_identities {
        if runtime_image_identity(name, &budget)? != *expected {
            return Err(format!(
                "runtime image {name} changed since ordered local gates"
            ));
        }
    }
    let runtime = &evidence.image_identities[&evidence.configuration.image];
    if !runtime.repo_digests.contains(&evidence.configuration.image) {
        return Err(
            "qualified runtime image is not bound to its registry manifest digest".to_owned(),
        );
    }
    myelin_e2e_fuzz::require_prepared_artifacts(
        workspace,
        &evidence.deployment_artifacts,
        &budget,
    )?;
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RegressionWitness {
    schema_version: u32,
    case_id: String,
    replay_verified: bool,
    signature: Option<myelin_e2e_fuzz::FailureSignature>,
    artifacts: BTreeMap<String, String>,
}

fn persist_regression(
    root: &Path,
    case: &BehaviorCase,
    error: &str,
    signature: Option<&myelin_e2e_fuzz::FailureSignature>,
    replay_verified: bool,
) -> Result<(), String> {
    case.validate()?;
    if replay_verified && signature.is_none() {
        return Err("reusable regression requires a typed replay signature".to_owned());
    }
    let directory = root.join("regressions");
    fs::create_dir_all(&directory)
        .map_err(|error| format!("create regression directory: {error}"))?;
    // Pending artifacts never appear in the reusable corpus. Rename commits the complete
    // directory only after every file and the versioned manifest have reached durable storage.
    let pending = tempfile::Builder::new()
        .prefix(".pending-regression-")
        .tempdir_in(root)
        .map_err(|error| format!("stage regression witness: {error}"))?;
    let mut artifacts = BTreeMap::new();
    durable_json(&pending.path().join("case.json"), case)?;
    durable_write(&pending.path().join("failure.txt"), error.as_bytes())?;
    durable_json(&pending.path().join("signature.json"), &signature)?;
    for process in &case.processes {
        durable_write(
            &pending.path().join(format!("{}.py", process.id)),
            render_case_python(case, process, Duration::from_secs(WORKLOAD_DEADLINE_SECS))
                .as_bytes(),
        )?;
    }
    for entry in fs::read_dir(pending.path()).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        artifacts.insert(
            entry.file_name().to_string_lossy().into_owned(),
            hash_file(&entry.path())?,
        );
    }
    let witness = RegressionWitness {
        schema_version: 1,
        case_id: case.id.clone(),
        replay_verified,
        signature: signature.cloned(),
        artifacts,
    };
    durable_json(&pending.path().join("witness.json"), &witness)?;
    let target = directory.join(&case.id);
    if target.exists() {
        let existing = read_regression_witness(&target, replay_verified)?;
        if existing.1 == witness {
            return Ok(());
        }
        return Err(format!(
            "refuse to overwrite committed regression witness {}",
            target.display()
        ));
    }
    fs::rename(pending.path(), &target)
        .map_err(|error| format!("commit regression witness: {error}"))?;
    fs::File::open(&directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync committed regression directory: {error}"))?;
    fs::File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync regression staging directory: {error}"))
}

fn read_regression_witness(
    directory: &Path,
    require_replay: bool,
) -> Result<(BehaviorCase, RegressionWitness), String> {
    let read_json = |name: &str| -> Result<serde_json::Value, String> {
        let path = directory.join(name);
        if !fs::symlink_metadata(&path)
            .map_err(|error| format!("incomplete regression {}: {error}", path.display()))?
            .is_file()
        {
            return Err(format!(
                "regression artifact is not a regular file: {}",
                path.display()
            ));
        }
        serde_json::from_reader(fs::File::open(&path).map_err(|error| error.to_string())?)
            .map_err(|error| format!("invalid regression artifact {}: {error}", path.display()))
    };
    let witness: RegressionWitness = serde_json::from_value(read_json("witness.json")?)
        .map_err(|error| format!("incompatible regression manifest: {error}"))?;
    if witness.schema_version != 1
        || (require_replay && (!witness.replay_verified || witness.signature.is_none()))
        || witness
            .signature
            .as_ref()
            .is_some_and(|signature| signature.invariant.is_empty())
    {
        return Err(format!(
            "incomplete or incompatible regression witness {}",
            directory.display()
        ));
    }
    let case: BehaviorCase = serde_json::from_value(read_json("case.json")?)
        .map_err(|error| format!("invalid regression case: {error}"))?;
    case.validate()?;
    let signature: Option<myelin_e2e_fuzz::FailureSignature> =
        serde_json::from_value(read_json("signature.json")?)
            .map_err(|error| format!("invalid regression signature: {error}"))?;
    if case.id != witness.case_id || signature != witness.signature {
        return Err("regression case/signature does not match committed witness".to_owned());
    }
    let mut required = BTreeSet::from([
        "case.json".to_owned(),
        "failure.txt".to_owned(),
        "signature.json".to_owned(),
    ]);
    required.extend(
        case.processes
            .iter()
            .map(|process| format!("{}.py", process.id)),
    );
    if witness.artifacts.keys().cloned().collect::<BTreeSet<_>>() != required {
        return Err(
            "regression manifest does not enumerate every required witness artifact".to_owned(),
        );
    }
    for (name, expected) in &witness.artifacts {
        let path = directory.join(name);
        if !fs::symlink_metadata(&path)
            .map_err(|error| format!("incomplete regression {}: {error}", path.display()))?
            .is_file()
            || hash_file(&path)? != *expected
        {
            return Err(format!(
                "regression artifact hash mismatch: {}",
                path.display()
            ));
        }
    }
    for process in &case.processes {
        let generated =
            render_case_python(&case, process, Duration::from_secs(WORKLOAD_DEADLINE_SECS));
        if witness.artifacts[&format!("{}.py", process.id)]
            != format!("{:x}", Sha256::digest(generated.as_bytes()))
        {
            return Err(
                "regression generated program is incompatible with the current renderer".to_owned(),
            );
        }
    }
    required.insert("witness.json".to_owned());
    let actual = fs::read_dir(directory)
        .map_err(|error| error.to_string())?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| error.to_string())?;
    if actual != required {
        return Err("regression witness contains uncommitted artifacts".to_owned());
    }
    Ok((case, witness))
}

fn run_cleanup_only(
    state_path: &Path,
    api_key_env: &str,
    requested_deadline: Duration,
) -> Result<(), String> {
    let mut state = read_paid_state(state_path)?;
    state.enter_cleanup_only();
    let initial_persistence = write_paid_state(state_path, &state);
    let deadline = if requested_deadline.is_zero() {
        Duration::from_secs(5 * 60)
    } else {
        requested_deadline
    };
    let cleanup = myelin_e2e_fuzz::cleanup_paid_ownership(
        &mut state,
        state_path,
        &std::env::current_exe().map_err(|error| error.to_string())?,
        api_key_env,
        deadline,
    );
    let persistence = write_paid_state(state_path, &state);
    merge_execution_cleanup(
        initial_persistence,
        merge_execution_cleanup(cleanup, persistence),
    )
}

fn run_campaign(options: &Options, workspace: PathBuf) -> Result<(), String> {
    let _campaign = PhaseTimer::new("campaign-total");
    let total = Budget::new(Duration::from_secs(if options.paid_vastai {
        PREPARATION_DEADLINE_SECS + CAMPAIGN_DEADLINE_SECS + CLEANUP_DEADLINE_SECS
    } else {
        CAMPAIGN_DEADLINE_SECS
    }));
    let provider = if options.paid_vastai {
        ProviderMode::Real
    } else {
        ProviderMode::Mock
    };
    let runtime_image = options
        .image
        .clone()
        .unwrap_or_else(|| format!("myelin-e2e-campaign:{}", options.seed));
    let limits = CampaignLimits {
        fixture_lifetime_secs: options
            .fixture_lifetime_secs
            .expect("campaign lifetime validated by parser"),
        total_cost_usd: options.max_total_cost_usd.unwrap_or(0.0),
        total_hourly_price_usd: options.max_hourly_cost_usd.unwrap_or(0.0),
        case_deadline_secs: options.deadline.as_secs(),
        max_campaign_payload_bytes: 512 * 1024 * 1024,
        max_campaign_allocation_bytes: 8 * 1024 * 1024 * 1024,
        max_case_race_states: 262_144,
    };
    let config = CampaignConfig {
        provider,
        seed: options.seed,
        runtime_image: runtime_image.clone(),
        limits,
        offers: OfferPolicy {
            gpu_model: None,
            min_gpu_ram_mb: None,
            min_compute_cap: Some(700),
            min_reliability: Some(0.95),
            min_download_mbps: Some(100.0),
            min_upload_mbps: None,
            max_hourly_price_per_node: options.max_hourly_cost_usd.map(|ceiling| ceiling / 5.0),
            blacklist_hosts: vec![59017],
        },
    };
    let generation = PhaseTimer::new("campaign-generation-and-oracle");
    let regressions = load_regressions(&options.artifacts)?;
    let mut plan = CampaignPlan::build_with_budget(&config, regressions, &total)?;
    if options.scripted_campaign_resource_overflow {
        plan.inject_scripted_resource_overflow()?;
        return match plan.validate() {
            Err(error) => Err(error),
            Ok(()) => {
                Err("scripted campaign resource overflow was unexpectedly admitted".to_owned())
            }
        };
    }
    plan.validate()?;
    drop(generation);
    total.check("campaign generation and resource admission")?;
    let campaign_root = options.artifacts.join(&plan.campaign_id);
    claim_campaign_directory(&campaign_root, options.paid_vastai)?;
    let serialization = PhaseTimer::new("campaign-plan-persistence");
    write_campaign_plan(&campaign_root.join("campaign-plan.json"), &plan)?;
    drop(serialization);
    durable_json(
        &campaign_root.join("campaign-resource-plan.json"),
        &plan.resources,
    )?;
    total.check("campaign plan persistence")?;

    if options.paid_vastai {
        durable_json(
            &campaign_root.join("paid-authorization.json"),
            &serde_json::json!({
                "schema_version": 1,
                "operator_authorized": options.authorize_paid_vastai,
                "scripted_loopback_only": options.scripted_provider,
                "retain_development_fixture": options.retain_paid_fixture,
                "ordered_attestation_digest": options.gate_attestation.as_ref().map(|path| hash_file(path)).transpose()?,
                "plan_digest": hash_file(&campaign_root.join("campaign-plan.json"))?,
                "limits": config.limits,
            }),
        )?;
        run_paid_campaign(options, workspace, config, plan, campaign_root, &total)
    } else {
        run_mock_campaign(options, workspace, config, plan, campaign_root, &total)
    }
}

/// A local `iroh-relay --dev` process. Isolated fixture nodes reach each
/// other only through a relay, exactly like remote nodes behind NAT; running
/// one on the host (reachable from every node bridge via its gateway) keeps
/// the E2E self-contained and gives relay-path traffic LAN latency.
struct LocalRelay {
    url: String,
    child: std::process::Child,
}

impl LocalRelay {
    fn start(fleet: &RawDockerFleet, budget: &Budget) -> Result<Option<Self>, String> {
        if std::env::var("MYELIN_E2E_RELAY_URL").is_ok() {
            // An explicit relay overrides the local one.
            return Ok(None);
        }
        if which("iroh-relay").is_none() {
            return Ok(None);
        }
        let gateway = fleet.host_gateway()?;
        budget.check("launch local relay")?;
        let address = format!("{gateway}:3340")
            .parse::<std::net::SocketAddr>()
            .map_err(|error| format!("relay address: {error}"))?;
        let mut child = std::process::Command::new("iroh-relay")
            .arg("--dev")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|error| format!("spawn local iroh relay: {error}"))?;
        let url = format!("http://{gateway}:3340");
        let readiness = budget.child(Duration::from_secs(30));
        let result = (|| {
            loop {
                let remaining = readiness.remaining("local relay socket ready or child exit")?;
                if let Some(status) = child
                    .try_wait()
                    .map_err(|error| format!("relay child status: {error}"))?
                {
                    return Err(format!("local relay exited before readiness: {status}"));
                }
                if std::net::TcpStream::connect_timeout(
                    &address,
                    remaining.min(Duration::from_millis(100)),
                )
                .is_ok()
                {
                    return Ok(());
                }
                // The relay has no readiness notification contract. Reconcile
                // its socket while waking immediately for its actual pidfd exit.
                wait_process_event(child.id(), remaining.min(Duration::from_millis(25)))?;
            }
        })();
        if let Err(error) = result {
            let relay = Self { url, child };
            return merge_execution_cleanup(Err(error), relay.stop(budget));
        }
        Ok(Some(Self { url, child }))
    }

    fn stop(mut self, budget: &Budget) -> Result<(), String> {
        if self
            .child
            .try_wait()
            .map_err(|error| format!("observe local relay: {error}"))?
            .is_some()
        {
            return Ok(());
        }
        let signal = self
            .child
            .kill()
            .map_err(|error| format!("stop local relay: {error}"));
        let exit = wait_process_exit(self.child.id(), budget);
        // Always try to reap, even when the inherited deadline has expired.
        let reap = self
            .child
            .try_wait()
            .map_err(|error| format!("reap local relay: {error}"))
            .and_then(|status| status.ok_or_else(|| "local relay remains unreaped".to_owned()))
            .map(|_| ());
        merge_execution_cleanup(merge_execution_cleanup(signal, exit), reap)
    }
}

impl Drop for LocalRelay {
    fn drop(&mut self) {
        // Retain Child ownership through forced reaping, including startup
        // failure paths. No new cleanup deadline is created by this fallback.
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn which(program: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(program))
        .find(|candidate| candidate.is_file())
}

fn deployment_relay_url(local: Option<&LocalRelay>) -> Option<String> {
    if let Some(relay) = local {
        return Some(relay.url.clone());
    }
    std::env::var("MYELIN_E2E_RELAY_URL").ok()
}

/// Deployment E2E over the raw SSH fleet fixture.
///
/// Proves the properties the remote path needs: blank nodes converge through
/// one SSH bootstrap transaction per node, workers report the deployment
/// identity, no SSH child survives bootstrap, and a second generation
/// refreshes the same containers through reconciliation alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeploymentInterruption {
    SshClientLoss,
    OrchestratorLoss,
}

const DEPLOYMENT_GATE_EVIDENCE_SCHEMA_VERSION: u32 = 4;

#[derive(serde::Serialize)]
struct DeploymentGateEvidence {
    schema_version: u32,
    gate: String,
    run_id: u64,
    node_count: u8,
    initial_fixture: myelin_e2e_fuzz::RawFleetSnapshot,
    rounds: Vec<DeploymentRoundEvidence>,
    cleanup: DeploymentCleanupEvidence,
    passed: bool,
    pending_prior_census: Vec<myelin_e2e_fuzz::RawNodeCensus>,
    pending_deployment_generation: Option<String>,
    failure: Option<String>,
}

#[derive(serde::Serialize)]
struct DeploymentRoundEvidence {
    deployment_generation: String,
    artifact_digest: String,
    executable_digest: String,
    prior_states: BTreeMap<u64, String>,
    interruption: Option<DeploymentInterruptionEvidence>,
    temporarily_unreachable_node: Option<u64>,
    fixture: myelin_e2e_fuzz::RawFleetSnapshot,
    raw_census: Vec<myelin_e2e_fuzz::RawNodeCensus>,
    fleet_status: serde_json::Value,
    telemetry: DeploymentTelemetryEvidence,
    matching_receipt_required_for_runtime_admission: bool,
    stale_processes_and_descendants_absent: bool,
    ssh_session_independent: bool,
    fresh_membership_routing_readiness_telemetry: bool,
    directed_all_pairs_behavior_passed: bool,
}

#[derive(serde::Serialize)]
struct DeploymentTelemetryEvidence {
    archives: BTreeSet<PathBuf>,
    provider_receipts: BTreeMap<u64, serde_json::Value>,
    creating_event_count: usize,
    creating_events: Vec<serde_json::Value>,
    bootstrapping_events: Vec<serde_json::Value>,
}

#[derive(serde::Serialize)]
struct DeploymentInterruptionEvidence {
    kind: String,
    boundary: String,
}

#[derive(serde::Serialize)]
struct LocalProcessIdentity {
    pid: u32,
    start_ticks: u64,
    #[serde(skip)]
    pidfd: std::os::fd::OwnedFd,
}

#[derive(serde::Serialize)]
struct DeploymentCleanupEvidence {
    attempted: bool,
    complete: bool,
    remaining_resources: Option<Vec<String>>,
    orphan_processes: Vec<LocalProcessIdentity>,
    error: Option<String>,
}

fn run_deployment_e2e(options: &Options, workspace: PathBuf) -> Result<(), String> {
    let _gate_timer = PhaseTimer::new("deployment-gate-total");
    // The fixture spans every sequential redeployment, not one workload campaign.
    // Retain each operation's deadline and reserve cleanup outside all rounds.
    let rounds = 10 + 2 * 5_u32.div_ceil(u32::from(options.deployment_nodes));
    let execution_duration = options
        .deadline
        .saturating_mul(rounds)
        .saturating_add(Duration::from_secs(PREPARATION_DEADLINE_SECS));
    let total =
        Budget::new(execution_duration.saturating_add(Duration::from_secs(CLEANUP_DEADLINE_SECS)));
    let execution = total.child(execution_duration);
    // Crashed orchestrators must not hand orphaned SSH clients to an external
    // init process where the fixture can no longer account for or reap them.
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
        return Err(format!(
            "enable deployment child reaping: {}",
            std::io::Error::last_os_error()
        ));
    }
    let root = options.artifacts.join("deployment-e2e");
    fs::create_dir_all(&root).map_err(|error| format!("create deployment artifacts: {error}"))?;
    let fixture_dir = root.join("fixture");
    fs::create_dir_all(&fixture_dir)
        .map_err(|error| format!("create fixture artifacts: {error}"))?;
    let state_dir = myelin_e2e_fuzz::private_fixture_dir(&root)?;
    let run_id = options.seed | (1 << 62);

    let node_count = options.deployment_nodes;
    let first_prior_states = [
        RawPriorState::NoWorkerOrTrustworthyMetadata,
        RawPriorState::HealthyStaleWorker,
        RawPriorState::DeadWorkerWithStaleMetadata,
        RawPriorState::MultipleStaleWorkersAndDescendants,
        RawPriorState::InterruptedTransferAndPartialInstall,
    ];
    let second_prior_states = [
        RawPriorState::CorruptActiveBinary,
        RawPriorState::CorruptActivationPointer,
        RawPriorState::CorruptDeploymentDescriptor,
        RawPriorState::DeadWorkerWithStaleMetadata,
        RawPriorState::MultipleStaleWorkersAndDescendants,
    ];
    let payload = myelin_e2e_fuzz::stage_deployment_payload(&workspace, &root, &execution)?;
    let generation_one = format!("deploy-{run_id}-a");
    let generation_two = format!("deploy-{run_id}-b");
    let generation_three = format!("deploy-{run_id}-c");
    let bundle_one =
        myelin_e2e_fuzz::assemble_deployment_bundle(&root, &payload, &generation_one, &execution)?;
    myelin_e2e_fuzz::distinguish_deployment_payload(&payload, &generation_two)?;
    let mut prior_state_bundles = Vec::new();
    for (generation, states) in [
        (&generation_two, first_prior_states.as_slice()),
        (&generation_three, second_prior_states.as_slice()),
    ] {
        if generation == &generation_three {
            myelin_e2e_fuzz::distinguish_deployment_payload(&payload, generation)?;
        }
        for (index, chunk) in states.chunks(usize::from(node_count)).enumerate() {
            let identity = if index == 0 {
                generation.clone()
            } else {
                format!("{generation}-prior-state-{index}")
            };
            let mut assigned_states = chunk.to_vec();
            assigned_states.resize(usize::from(node_count), RawPriorState::HealthyStaleWorker);
            prior_state_bundles.push((
                myelin_e2e_fuzz::assemble_deployment_bundle(
                    &root, &payload, &identity, &execution,
                )?,
                assigned_states,
            ));
        }
    }
    let bundle_two = &prior_state_bundles[0].0;
    let bundle_three =
        &prior_state_bundles[first_prior_states.len().div_ceil(usize::from(node_count))].0;
    let mut interruption_bundles = Vec::new();
    for interruption in [
        DeploymentInterruption::SshClientLoss,
        DeploymentInterruption::OrchestratorLoss,
    ] {
        let interruption_name = match interruption {
            DeploymentInterruption::SshClientLoss => "ssh",
            DeploymentInterruption::OrchestratorLoss => "orchestrator",
        };
        for boundary in [
            DeploymentBoundary::TransferStarted,
            DeploymentBoundary::BeforeLaunch,
            DeploymentBoundary::AfterLaunch,
            DeploymentBoundary::BeforeReceipt,
        ] {
            let generation = format!(
                "deploy-{run_id}-fault-{interruption_name}-{}",
                boundary.as_remote_name()
            );
            interruption_bundles.push((
                interruption,
                boundary,
                myelin_e2e_fuzz::assemble_deployment_bundle(
                    &root,
                    &payload,
                    &generation,
                    &execution,
                )?,
            ));
        }
    }
    let unreachable_bundle = myelin_e2e_fuzz::assemble_deployment_bundle(
        &root,
        &payload,
        &format!("deploy-{run_id}-temporarily-unreachable"),
        &execution,
    )?;
    if bundle_one.artifact_digest == bundle_two.artifact_digest
        || bundle_one.executable_digest == bundle_two.executable_digest
        || bundle_one.artifact_digest == bundle_three.artifact_digest
        || bundle_one.executable_digest == bundle_three.executable_digest
        || bundle_two.artifact_digest == bundle_three.artifact_digest
        || bundle_two.executable_digest == bundle_three.executable_digest
        || interruption_bundles.iter().any(|(_, _, bundle)| {
            bundle.artifact_digest != bundle_three.artifact_digest
                || bundle.executable_digest != bundle_three.executable_digest
        })
        || unreachable_bundle.artifact_digest != bundle_three.artifact_digest
        || unreachable_bundle.executable_digest != bundle_three.executable_digest
    {
        return Err(
            "generations A, B, and C must have distinct payloads; interruption rounds must reuse C bytes".to_owned(),
        );
    }

    let fixture_timer = PhaseTimer::new("raw-fixture-construction");
    let fleet = RawDockerFleet::ensure(
        &workspace,
        &fixture_dir,
        u32::from(node_count),
        options.build_image,
        &execution,
        &total,
    )?;
    drop(fixture_timer);
    let immutable_fixture = match fleet.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => return merge_execution_cleanup(Err(error), fleet.teardown()),
    };
    let mut evidence = DeploymentGateEvidence {
        schema_version: DEPLOYMENT_GATE_EVIDENCE_SCHEMA_VERSION,
        gate: "retained-node-redeployment".to_owned(),
        run_id,
        node_count,
        initial_fixture: immutable_fixture.clone(),
        rounds: Vec::new(),
        cleanup: DeploymentCleanupEvidence {
            attempted: false,
            complete: false,
            remaining_resources: None,
            orphan_processes: Vec::new(),
            error: None,
        },
        passed: false,
        pending_prior_census: Vec::new(),
        pending_deployment_generation: Some(generation_one.clone()),
        failure: None,
    };
    let evidence_path = root.join("gate-a-evidence.json");
    let mut relay = None;
    let manifest = RawDockerFleet::manifest_path(&fixture_dir);
    let result = (|| {
        write_deployment_gate_evidence(&evidence_path, &evidence)?;
        relay = LocalRelay::start(&fleet, &execution)?;
        let deployment = DeploymentRoundContext {
            options,
            workspace: &workspace,
            fixture_dir: &fixture_dir,
            state_dir: &state_dir,
            manifest: &manifest,
            fleet: &fleet,
            relay: relay.as_ref(),
            run_id,
            node_count,
            immutable_fixture: &immutable_fixture,
            cleanup: &total,
        };
        let mut telemetry_before = deployment_telemetry_files(&fixture_dir)?;
        let mut harness =
            start_deployment_round(&deployment, &bundle_one, true, true, &execution, false)?;
        evidence.rounds.push(capture_deployment_round(
            &deployment,
            &harness,
            &bundle_one,
            &telemetry_before,
            DeploymentRoundObservation {
                allow_creating: true,
                prior_states: uniform_prior_states(
                    &fleet,
                    "no_worker_or_trustworthy_deployment_metadata",
                ),
                interruption: None,
                temporarily_unreachable_node: None,
            },
        )?);
        evidence.pending_deployment_generation = None;
        write_deployment_gate_evidence(&evidence_path, &evidence)?;
        harness.retain_remote_fixture(true)?;
        telemetry_before = deployment_telemetry_files(&fixture_dir)?;

        for (bundle, prior_states) in &prior_state_bundles {
            let _round_timer =
                PhaseTimer::new(format!("deployment-round-{}", bundle.deployment_generation));
            fleet.record_prior_processes()?;
            evidence.pending_prior_census = fleet.census()?;
            evidence.pending_deployment_generation = Some(bundle.deployment_generation.clone());
            write_deployment_gate_evidence(&evidence_path, &evidence)?;
            inject_prior_states(&fleet, prior_states)?;
            evidence.pending_prior_census = fleet.census()?;
            write_deployment_gate_evidence(&evidence_path, &evidence)?;
            let mut refreshed =
                start_deployment_round(&deployment, bundle, false, false, &execution, false)?;
            evidence.rounds.push(capture_deployment_round(
                &deployment,
                &refreshed,
                bundle,
                &telemetry_before,
                DeploymentRoundObservation {
                    allow_creating: false,
                    prior_states: enumerated_prior_states(&fleet, prior_states)?,
                    interruption: None,
                    temporarily_unreachable_node: None,
                },
            )?);
            evidence.pending_prior_census.clear();
            evidence.pending_deployment_generation = None;
            write_deployment_gate_evidence(&evidence_path, &evidence)?;
            refreshed.retain_remote_fixture(true)?;
            telemetry_before = deployment_telemetry_files(&fixture_dir)?;
        }

        for (interruption, boundary, bundle) in &interruption_bundles {
            let _round_timer =
                PhaseTimer::new(format!("deployment-round-{}", bundle.deployment_generation));
            fleet.record_prior_processes()?;
            evidence.pending_prior_census = fleet.census()?;
            evidence.pending_deployment_generation = Some(bundle.deployment_generation.clone());
            write_deployment_gate_evidence(&evidence_path, &evidence)?;
            let mut interrupted = run_interrupted_deployment_round(
                &deployment,
                bundle,
                *boundary,
                *interruption,
                &execution,
            )?;
            evidence.rounds.push(capture_deployment_round(
                &deployment,
                &interrupted,
                bundle,
                &telemetry_before,
                DeploymentRoundObservation {
                    allow_creating: false,
                    prior_states: uniform_prior_states(&fleet, "healthy_stale_worker"),
                    interruption: Some(DeploymentInterruptionEvidence {
                        kind: match interruption {
                            DeploymentInterruption::SshClientLoss => "ssh_client_loss",
                            DeploymentInterruption::OrchestratorLoss => "orchestrator_loss",
                        }
                        .to_owned(),
                        boundary: boundary.as_remote_name().to_owned(),
                    }),
                    temporarily_unreachable_node: None,
                },
            )?);
            evidence.pending_prior_census.clear();
            evidence.pending_deployment_generation = None;
            write_deployment_gate_evidence(&evidence_path, &evidence)?;
            interrupted.retain_remote_fixture(true)?;
            telemetry_before = deployment_telemetry_files(&fixture_dir)?;
        }

        fleet.record_prior_processes()?;
        evidence.pending_deployment_generation =
            Some(unreachable_bundle.deployment_generation.clone());
        evidence.pending_prior_census = fleet.census()?;
        write_deployment_gate_evidence(&evidence_path, &evidence)?;
        let mut recovered =
            run_temporarily_unreachable_round(&deployment, &unreachable_bundle, &execution)?;
        evidence.rounds.push(capture_deployment_round(
            &deployment,
            &recovered,
            &unreachable_bundle,
            &telemetry_before,
            DeploymentRoundObservation {
                allow_creating: false,
                prior_states: uniform_prior_states(
                    &fleet,
                    "temporarily_unreachable_with_prior_state_intact",
                ),
                interruption: None,
                temporarily_unreachable_node: Some(1),
            },
        )?);
        evidence.pending_prior_census.clear();
        evidence.pending_deployment_generation = None;
        write_deployment_gate_evidence(&evidence_path, &evidence)?;
        execution.check("complete all Gate A rounds")?;
        recovered.retain_remote_fixture(true)
    })();
    evidence.failure = result.as_ref().err().cloned();
    evidence.cleanup.attempted = true;
    let before_cleanup = write_deployment_gate_evidence(&evidence_path, &evidence);
    let diagnostics = if result.is_err() {
        durable_json(
            &root.join("failure-diagnostics.json"),
            &fleet.failure_diagnostics(&execution.child(Duration::from_secs(5))),
        )
    } else {
        Ok(())
    };
    let _cleanup_timer = PhaseTimer::new("deployment-fixture-cleanup");
    let cleanup_budget = total.child(Duration::from_secs(CLEANUP_DEADLINE_SECS));
    // Capture orphan identities before spawning cleanup commands. The orphan
    // owner must never discover and kill a live Docker client or reap the relay
    // out from under its Child owner.
    let orphan_snapshot = capture_local_deployment_processes(
        relay.as_ref().map(|relay| relay.child.id()),
        &mut evidence.cleanup.orphan_processes,
        &cleanup_budget,
    );
    let (relay_cleanup, fleet_teardown, raw_remaining, local_cleanup) =
        std::thread::scope(|scope| {
            let raw = scope.spawn(|| {
                let teardown = fleet.teardown_with_budget(&cleanup_budget);
                let remaining = fleet.remaining_resources_with_budget(&cleanup_budget);
                (teardown, remaining)
            });
            let relay = scope.spawn(|| relay.map_or(Ok(()), |relay| relay.stop(&cleanup_budget)));
            let local_cleanup = merge_execution_cleanup(
                orphan_snapshot,
                cleanup_local_deployment_processes(
                    &mut evidence.cleanup.orphan_processes,
                    &cleanup_budget,
                ),
            );
            let relay_cleanup = relay
                .join()
                .unwrap_or_else(|_| Err("relay cleanup owner panicked".to_owned()));
            let (fleet_teardown, remaining) = raw.join().unwrap_or_else(|_| {
                (
                    Err("raw cleanup owner panicked".to_owned()),
                    Err("raw cleanup census unavailable after owner panic".to_owned()),
                )
            });
            (relay_cleanup, fleet_teardown, remaining, local_cleanup)
        });
    // All managed cleanup children are now relinquished. Capture any late
    // orphan (including a failed relay reap) and reconcile without a new budget.
    let final_snapshot = capture_local_deployment_processes(
        None,
        &mut evidence.cleanup.orphan_processes,
        &cleanup_budget,
    );
    let final_local_cleanup = merge_execution_cleanup(
        final_snapshot,
        cleanup_local_deployment_processes(&mut evidence.cleanup.orphan_processes, &cleanup_budget),
    );
    let local_remaining = process_descendants(std::process::id()).map(|pids| {
        pids.into_iter()
            .map(|pid| format!("process:{pid}"))
            .collect::<Vec<_>>()
    });
    let remaining_resources = match (raw_remaining, local_remaining) {
        (Ok(mut raw), Ok(local)) => {
            raw.extend(local);
            Ok(raw)
        }
        (Err(raw), Err(local)) => Err(format!("{raw}; {local}")),
        (Err(error), _) | (_, Err(error)) => Err(error),
    };
    evidence.cleanup.remaining_resources = remaining_resources.as_ref().ok().cloned();
    let mut cleanup_errors = Vec::new();
    for result in [
        &relay_cleanup,
        &fleet_teardown,
        &local_cleanup,
        &final_local_cleanup,
    ] {
        if let Err(error) = result {
            cleanup_errors.push(error.clone());
        }
    }
    match &remaining_resources {
        Ok(remaining) if !remaining.is_empty() => {
            cleanup_errors.push(format!(
                "fixture resources remain after cleanup: {remaining:?}"
            ));
        }
        Err(error) => cleanup_errors.push(format!("prove fixture cleanup: {error}")),
        Ok(_) => {}
    }
    evidence.cleanup.complete = cleanup_errors.is_empty();
    evidence.cleanup.error = (!cleanup_errors.is_empty()).then(|| cleanup_errors.join("; "));

    let mut errors = Vec::new();
    if let Err(error) = diagnostics {
        errors.push(format!("persist worker failure diagnostics: {error}"));
    }
    errors.extend(cleanup_errors);
    if let Err(error) = total.check("complete Gate A deployment and cleanup") {
        errors.push(error);
    }
    if let Err(error) = result {
        errors.push(error);
    }
    if let Err(error) = before_cleanup {
        errors.push(error);
    }
    evidence.passed = errors.is_empty() && evidence.cleanup.complete;
    evidence.failure = (!errors.is_empty()).then(|| errors.join("; "));
    if let Err(error) = write_deployment_gate_evidence(&evidence_path, &evidence) {
        errors.push(error);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

struct LocalProcessStat {
    start_ticks: u64,
    parent_pid: u32,
    state: char,
}

fn local_process_stat(pid: u32) -> Result<Option<LocalProcessStat>, String> {
    let stat = match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("inspect local deployment process {pid}: {error}")),
    };
    let mut fields = stat
        .rsplit_once(')')
        .ok_or_else(|| format!("process {pid} stat omitted fields"))?
        .1
        .split_whitespace();
    let state = fields
        .next()
        .and_then(|state| state.chars().next())
        .ok_or_else(|| format!("process {pid} stat omitted state"))?;
    let parent_pid = fields
        .next()
        .ok_or_else(|| format!("process {pid} stat omitted parent"))?
        .parse::<u32>()
        .map_err(|error| format!("parse process {pid} parent: {error}"))?;
    let start_ticks = fields
        .nth(17)
        .ok_or_else(|| format!("process {pid} stat omitted start ticks"))?
        .parse::<u64>()
        .map_err(|error| format!("parse process {pid} start ticks: {error}"))?;
    Ok(Some(LocalProcessStat {
        start_ticks,
        parent_pid,
        state,
    }))
}

fn pin_local_process(
    pid: u32,
    expected_start_ticks: u64,
) -> Result<Option<LocalProcessIdentity>, String> {
    let Some(pidfd) = open_process_exit_fd(pid)? else {
        return Ok(None);
    };
    if !local_process_stat(pid)?.is_some_and(|stat| stat.start_ticks == expected_start_ticks) {
        return Ok(None);
    }
    // The descriptor, not a later /proc lookup, owns signal and reap authority.
    Ok(Some(LocalProcessIdentity {
        pid,
        start_ticks: expected_start_ticks,
        pidfd,
    }))
}

fn signal_local_process(process: &LocalProcessIdentity, signal: i32) -> Result<(), String> {
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            std::os::fd::AsRawFd::as_raw_fd(&process.pidfd),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(format!(
            "signal owned deployment process {}:{}: {error}",
            process.pid, process.start_ticks
        ))
    }
}

fn capture_local_children(
    parent: &LocalProcessIdentity,
    excluded_pid: Option<u32>,
    observed: &[LocalProcessIdentity],
) -> Result<Vec<LocalProcessIdentity>, String> {
    let mut captured = Vec::new();
    for pid in process_children(parent.pid)? {
        if Some(pid) == excluded_pid {
            continue;
        }
        let Some(stat) = local_process_stat(pid)? else {
            continue;
        };
        if stat.parent_pid != parent.pid {
            continue;
        }
        let mut already_owned = false;
        for process in observed {
            if process.pid == pid
                && process.start_ticks == stat.start_ticks
                && process_fd_events(&process.pidfd, process.pid, Duration::ZERO)? & libc::POLLHUP
                    == 0
            {
                already_owned = true;
                break;
            }
        }
        if already_owned {
            continue;
        }
        let Some(process) = pin_local_process(pid, stat.start_ticks)? else {
            continue;
        };
        // Both sides of the parent-child edge must still name the pinned
        // incarnations. A recycled parent PID grants no discovery authority.
        if !local_process_stat(pid)?.is_some_and(|current| {
            current.start_ticks == stat.start_ticks && current.parent_pid == parent.pid
        }) || process_fd_events(&parent.pidfd, parent.pid, Duration::ZERO)? != 0
        {
            continue;
        }
        captured.push(process);
    }
    Ok(captured)
}

fn capture_local_deployment_processes(
    excluded_pid: Option<u32>,
    observed: &mut Vec<LocalProcessIdentity>,
    budget: &Budget,
) -> Result<(), String> {
    let pid = std::process::id();
    let stat = local_process_stat(pid)?
        .ok_or_else(|| "local deployment cleanup owner disappeared".to_owned())?;
    let owner = pin_local_process(pid, stat.start_ticks)?
        .ok_or_else(|| "local deployment cleanup owner changed identity".to_owned())?;
    let mut settled = false;
    loop {
        budget.check("capture stopped local deployment descendants")?;
        let before = observed.len();
        let children = capture_local_children(&owner, excluded_pid, observed)?;
        observed.extend(children);
        let mut all_stopped = true;
        let mut index = 0;
        while index < observed.len() {
            let process = &observed[index];
            if process_fd_events(&process.pidfd, process.pid, Duration::ZERO)? == 0 {
                if local_process_stat(process.pid)?.is_some_and(|stat| {
                    stat.start_ticks == process.start_ticks
                        && !matches!(stat.state, 'T' | 't' | 'Z' | 'X')
                }) {
                    signal_local_process(process, libc::SIGSTOP)?;
                    all_stopped = false;
                }
                let children = capture_local_children(process, excluded_pid, observed)?;
                observed.extend(children);
            }
            index += 1;
        }
        // Two settled scans include children reparented while the first scan
        // was stopping their parent. No owned process can fork after capture.
        if before == observed.len() && all_stopped {
            if settled {
                return Ok(());
            }
            settled = true;
        } else {
            settled = false;
        }
        if !all_stopped {
            // pidfds notify exit, not job-control stops; reconcile stop state.
            budget.wait(
                Duration::from_millis(1),
                "capture stopped local deployment descendants",
            )?;
        }
    }
}

fn cleanup_local_deployment_processes(
    observed: &mut Vec<LocalProcessIdentity>,
    budget: &Budget,
) -> Result<(), String> {
    let mut errors = Vec::new();
    for process in observed.iter() {
        if let Err(error) = signal_local_process(process, libc::SIGKILL) {
            errors.push(error);
        }
    }
    loop {
        let mut pending = Vec::new();
        let mut wait_on = None;
        for process in observed.iter() {
            // Non-direct descendants can become our children only after their
            // parent exits. P_PIDFD never reaps a replacement numeric PID.
            let mut status = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PIDFD,
                    std::os::fd::AsRawFd::as_raw_fd(&process.pidfd) as libc::id_t,
                    &mut status,
                    libc::WEXITED | libc::WNOHANG,
                )
            };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if !matches!(
                    error.raw_os_error(),
                    Some(libc::ECHILD | libc::EINTR | libc::ESRCH)
                ) {
                    errors.push(format!(
                        "reap owned deployment process {}:{}: {error}",
                        process.pid, process.start_ticks
                    ));
                }
            }
            let events = process_fd_events(&process.pidfd, process.pid, Duration::ZERO)?;
            // POLLIN proves exit; POLLHUP additionally proves this exact
            // incarnation was reaped, even by its original non-direct parent.
            if events & libc::POLLHUP == 0 {
                pending.push(format!("process:{}:{}", process.pid, process.start_ticks));
                if events & libc::POLLIN == 0 {
                    wait_on = Some(process);
                }
            }
        }
        if !errors.is_empty() {
            return Err(errors.join("; "));
        }
        if pending.is_empty() {
            return Ok(());
        }
        let predicate = format!("reap deployment descendants {pending:?}");
        let remaining = budget.remaining(&predicate)?;
        if let Some(process) = wait_on {
            process_fd_events(
                &process.pidfd,
                process.pid,
                remaining.min(Duration::from_millis(100)),
            )?;
        } else {
            // Exit-ready pidfds otherwise spin while an ancestor finishes
            // reparenting/reaping. All authority stays on the retained fds.
            budget.wait(Duration::from_millis(5), &predicate)?;
        }
    }
}

#[cfg(test)]
mod local_process_tests {
    use super::*;

    fn blocked_child() -> std::process::Child {
        std::process::Command::new("sh")
            .args(["-c", "read line; exit 23"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    }

    #[test]
    fn pinning_rejects_a_changed_start_identity() {
        let mut child = blocked_child();
        let stat = local_process_stat(child.id()).unwrap().unwrap();
        let pinned = pin_local_process(child.id(), stat.start_ticks + 1);
        child.stdin.take().unwrap().write_all(b"finish\n").unwrap();
        let status = child.wait().unwrap();
        assert!(pinned.unwrap().is_none());
        assert_eq!(status.code(), Some(23));
    }

    #[test]
    fn cleanup_never_adopts_a_replacement_numeric_pid() {
        let mut original = blocked_child();
        let stat = local_process_stat(original.id()).unwrap().unwrap();
        let mut captured = pin_local_process(original.id(), stat.start_ticks)
            .unwrap()
            .unwrap();
        original.kill().unwrap();
        original.wait().unwrap();

        let mut replacement = blocked_child();
        // Model PID recycling without relying on allocator timing: numeric
        // metadata now names another child, but the retained kernel handle
        // still names the original, already-reaped incarnation.
        captured.pid = replacement.id();
        captured.start_ticks = local_process_stat(replacement.id())
            .unwrap()
            .unwrap()
            .start_ticks;
        let cleanup = cleanup_local_deployment_processes(
            &mut vec![captured],
            &Budget::new(Duration::from_secs(5)),
        );
        let released = replacement.stdin.take().unwrap().write_all(b"finish\n");
        let status = replacement.wait();
        cleanup.unwrap();
        released.unwrap();
        assert_eq!(status.unwrap().code(), Some(23));
    }

    #[test]
    fn cleanup_reaps_stopped_non_direct_descendants() {
        const CHILD_TEST: &str = "MYELIN_LOCAL_DESCENDANT_CLEANUP_TEST";
        if std::env::var_os(CHILD_TEST).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "local_process_tests::cleanup_reaps_stopped_non_direct_descendants",
                    "--nocapture",
                ])
                .env(CHILD_TEST, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        // Re-execution confines the process-wide subreaper setting and the
        // self-descendant census to this test, even under parallel cargo test.
        assert_eq!(
            unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) },
            0
        );
        let mut parent = std::process::Command::new("sh")
            .args(["-c", "sleep 60 & echo $!; kill -STOP $$; wait"])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let parent_pid = parent.id();
        let mut child_pid = String::new();
        std::io::BufReader::new(parent.stdout.take().unwrap())
            .read_line(&mut child_pid)
            .unwrap();
        let child_pid = child_pid.trim().parse::<u32>().unwrap();
        let non_direct = local_process_stat(child_pid)
            .unwrap()
            .is_some_and(|stat| stat.parent_pid == parent_pid);
        let mut managed = blocked_child();
        let mut observed = Vec::new();
        let budget = Budget::new(Duration::from_secs(5));
        let capture =
            capture_local_deployment_processes(Some(managed.id()), &mut observed, &budget);
        // Child ownership is explicitly relinquished to the captured pidfds.
        drop(parent);
        let captured_both = [parent_pid, child_pid]
            .iter()
            .all(|pid| observed.iter().any(|process| process.pid == *pid));
        let cleanup = cleanup_local_deployment_processes(&mut observed, &budget);
        let released = managed.stdin.take().unwrap().write_all(b"finish\n");
        let managed_status = managed.wait();
        capture.unwrap();
        cleanup.unwrap();
        released.unwrap();
        assert_eq!(managed_status.unwrap().code(), Some(23));
        assert!(non_direct && captured_both);
        for pid in [parent_pid, child_pid] {
            assert!(local_process_stat(pid).unwrap().is_none());
        }
        assert!(process_descendants(std::process::id()).unwrap().is_empty());
    }
}

struct DeploymentRoundContext<'a> {
    options: &'a Options,
    workspace: &'a Path,
    fixture_dir: &'a Path,
    state_dir: &'a Path,
    manifest: &'a Path,
    fleet: &'a RawDockerFleet,
    relay: Option<&'a LocalRelay>,
    run_id: u64,
    node_count: u8,
    immutable_fixture: &'a myelin_e2e_fuzz::RawFleetSnapshot,
    cleanup: &'a Budget,
}

fn start_deployment_round(
    context: &DeploymentRoundContext<'_>,
    bundle: &DeploymentBundle,
    reset_state: bool,
    provision: bool,
    budget: &Budget,
    unobserved: bool,
) -> Result<ClusterHarness, String> {
    let config = ClusterHarnessConfig {
        workspace: context.workspace.to_path_buf(),
        artifacts: context.fixture_dir.to_path_buf(),
        node_count: context.node_count,
        seed: context.options.seed,
        image: Some("myelin-raw-deploy".to_owned()),
        build_image: false,
        deadline: context.options.deadline,
        provider: HarnessProvider::StaticSsh {
            manifest: context.manifest.to_path_buf(),
            identity: context.fleet.identity.clone(),
            bundle: bundle.tar_path.clone(),
        },
        selected_offer_ids: Vec::new(),
        state_dir: Some(context.state_dir.to_path_buf()),
        reset_state,
        offer_search_id: None,
        provision,
        relay_url: deployment_relay_url(context.relay),
        adopt_only: false,
        run_id: context.run_id,
    };
    let harness = ClusterHarness::start_deployment_unobserved(
        config,
        budget.clone(),
        context.cleanup.clone(),
    )?;
    if unobserved {
        Ok(harness)
    } else {
        complete_deployment_round(harness, context.fleet, bundle, context.immutable_fixture)
    }
}

fn complete_deployment_round(
    mut harness: ClusterHarness,
    fleet: &RawDockerFleet,
    bundle: &DeploymentBundle,
    immutable_fixture: &myelin_e2e_fuzz::RawFleetSnapshot,
) -> Result<ClusterHarness, String> {
    let _round_timer = PhaseTimer::new(format!(
        "deployment-convergence-{}",
        bundle.deployment_generation
    ));
    let result = (|| {
        harness.complete_deployment()?;
        harness.verify_workload_convergence()?;
        let census = assert_raw_deployment(fleet, bundle)?;
        assert_fleet_identity(&harness.fleet_snapshot()?, &census, bundle)?;
        fleet.assert_unchanged(immutable_fixture)?;
        assert_no_ssh_children(harness.orchestrator_pid())?;
        harness
            .execution_budget()
            .check("deployment generation fully proved")
    })();
    match result {
        Ok(()) => Ok(harness),
        Err(error) => merge_execution_cleanup(Err(error), harness.retain_remote_fixture(true)),
    }
}

fn run_interrupted_deployment_round(
    context: &DeploymentRoundContext<'_>,
    bundle: &DeploymentBundle,
    boundary: DeploymentBoundary,
    interruption: DeploymentInterruption,
    budget: &Budget,
) -> Result<ClusterHarness, String> {
    let round = budget.child(budget.remaining("admit deployment interruption")?);
    context
        .fleet
        .hold_boundary(&bundle.deployment_generation, boundary)?;
    // The real orchestrator owns the concurrent SSH deployment task. The
    // runner owns its Child directly: no scoped threads or unbounded joins.
    let launched = start_deployment_round(context, bundle, false, false, &round, true);
    let mut harness = match launched {
        Ok(harness) => harness,
        Err(error) => {
            return merge_execution_cleanup(
                Err(error),
                context.fleet.release_boundary_with_budget(
                    &bundle.deployment_generation,
                    boundary,
                    context.cleanup,
                ),
            );
        }
    };
    let injection = (|| {
        context.fleet.wait_for_boundary(
            0,
            &bundle.deployment_generation,
            boundary,
            round
                .remaining("SSH interruption boundary")?
                .min(Duration::from_secs(60)),
        )?;
        match interruption {
            DeploymentInterruption::SshClientLoss => kill_ssh_children(harness.orchestrator_pid()),
            DeploymentInterruption::OrchestratorLoss => kill_process(
                harness.orchestrator_pid(),
                "orchestrator at deployment boundary",
            ),
        }
    })();
    let release = context.fleet.release_boundary_with_budget(
        &bundle.deployment_generation,
        boundary,
        context.cleanup,
    );
    if let Err(error) = merge_execution_cleanup(injection, release) {
        round.cancel();
        return merge_execution_cleanup(Err(error), harness.retain_remote_fixture(true));
    }
    match interruption {
        DeploymentInterruption::SshClientLoss => {
            complete_deployment_round(harness, context.fleet, bundle, context.immutable_fixture)
        }
        DeploymentInterruption::OrchestratorLoss => {
            // Only the injected loss permits this explicit retained restart;
            // elapsed budget exhaustion is never a product-terminal success.
            if harness.complete_deployment().is_ok() {
                return merge_execution_cleanup(
                    Err(format!(
                        "deployment unexpectedly completed after orchestrator loss at {boundary:?}"
                    )),
                    harness.retain_remote_fixture(true),
                );
            }
            harness.retain_remote_fixture(true)?;
            round.check("restart retained deployment after injected orchestrator loss")?;
            start_deployment_round(context, bundle, false, false, &round, false)
        }
    }
}
fn run_temporarily_unreachable_round(
    context: &DeploymentRoundContext<'_>,
    bundle: &DeploymentBundle,
    budget: &Budget,
) -> Result<ClusterHarness, String> {
    let _round_timer = PhaseTimer::new("deployment-round-temporarily-unreachable");
    let round = budget.child(budget.remaining("admit temporary node unreachability")?);
    context.fleet.set_node_reachable(0, false)?;
    let launched = start_deployment_round(context, bundle, false, false, &round, true);
    let mut harness = match launched {
        Ok(harness) => harness,
        Err(error) => {
            return merge_execution_cleanup(
                Err(error),
                context
                    .fleet
                    .set_node_reachable_with_budget(0, true, context.cleanup),
            );
        }
    };
    let observation = round
        .remaining("healthy peer transfer while one node unreachable")
        .and_then(|remaining| {
            context.fleet.wait_for_boundary(
                1,
                &bundle.deployment_generation,
                DeploymentBoundary::TransferStarted,
                remaining.min(Duration::from_secs(60)),
            )
        });
    let reachable = context
        .fleet
        .set_node_reachable_with_budget(0, true, context.cleanup);
    if let Err(error) = merge_execution_cleanup(observation, reachable) {
        round.cancel();
        return merge_execution_cleanup(Err(error), harness.retain_remote_fixture(true));
    }
    complete_deployment_round(harness, context.fleet, bundle, context.immutable_fixture)
}

fn open_process_exit_fd(pid: u32) -> Result<Option<std::os::fd::OwnedFd>, String> {
    use std::os::fd::FromRawFd as _;
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
    if fd < 0 {
        let error = std::io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(None)
        } else {
            Err(format!("open exit notification for process {pid}: {error}"))
        };
    }
    Ok(Some(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) }))
}

fn process_fd_events(
    fd: &std::os::fd::OwnedFd,
    pid: u32,
    duration: Duration,
) -> Result<libc::c_short, String> {
    use std::os::fd::AsRawFd as _;
    let mut descriptor = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let started = Instant::now();
    let ready = loop {
        let remaining = duration.saturating_sub(started.elapsed());
        let timeout = remaining.as_millis().min(i32::MAX as u128) as i32;
        let ready = unsafe { libc::poll(&mut descriptor, 1, timeout) };
        if ready >= 0 || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
            break ready;
        }
        // An interrupted zero-time probe must not grant discovery authority
        // by pretending an exited parent is still alive.
    };
    if ready < 0 {
        return Err(format!(
            "wait process {pid} exit: {}",
            std::io::Error::last_os_error()
        ));
    }
    if ready <= 0 {
        return Ok(0);
    }
    if descriptor.revents & (libc::POLLERR | libc::POLLNVAL) != 0 {
        return Err(format!("invalid exit notification for process {pid}"));
    }
    Ok(descriptor.revents & (libc::POLLIN | libc::POLLHUP))
}

fn wait_process_event(pid: u32, duration: Duration) -> Result<bool, String> {
    let Some(fd) = open_process_exit_fd(pid)? else {
        return Ok(true);
    };
    process_fd_events(&fd, pid, duration).map(|events| events != 0)
}

fn wait_process_exit(pid: u32, budget: &Budget) -> Result<(), String> {
    loop {
        let remaining = budget.remaining(&format!("process {pid} exit and reap"))?;
        if wait_process_event(pid, remaining.min(Duration::from_millis(100)))? {
            return Ok(());
        }
    }
}

fn process_children(parent_pid: u32) -> Result<Vec<u32>, String> {
    let mut children = BTreeSet::new();
    let task_dir = format!("/proc/{parent_pid}/task");
    let tasks = match std::fs::read_dir(&task_dir) {
        Ok(tasks) => tasks,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("read process tasks {task_dir}: {error}")),
    };
    for task in tasks {
        let task = task.map_err(|error| format!("inspect process task: {error}"))?;
        let task_children = match std::fs::read_to_string(task.path().join("children")) {
            Ok(children) => children,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "read child processes for task {}: {error}",
                    task.path().display()
                ));
            }
        };
        children.extend(
            task_children
                .split_whitespace()
                .filter_map(|pid| pid.parse::<u32>().ok()),
        );
    }
    Ok(children.into_iter().collect())
}

fn process_descendants(root_pid: u32) -> Result<Vec<u32>, String> {
    let mut descendants = BTreeSet::new();
    let mut pending = vec![root_pid];
    while let Some(parent_pid) = pending.pop() {
        for child in process_children(parent_pid)? {
            if descendants.insert(child) {
                pending.push(child);
            }
        }
    }
    Ok(descendants.into_iter().collect())
}

fn kill_ssh_children(orchestrator_pid: u32) -> Result<(), String> {
    let ssh_children = process_descendants(orchestrator_pid)?
        .into_iter()
        .filter(|pid| {
            std::fs::read_to_string(format!("/proc/{pid}/comm"))
                .is_ok_and(|comm| matches!(comm.trim(), "ssh" | "scp"))
        })
        .collect::<Vec<_>>();
    if ssh_children.is_empty() {
        return Err(format!(
            "orchestrator {orchestrator_pid} had no SSH descendant at the deployment boundary"
        ));
    }
    for pid in ssh_children {
        kill_process(pid, "SSH deployment client")?;
    }
    Ok(())
}

fn kill_process(pid: u32, description: &str) -> Result<(), String> {
    if unsafe { libc::kill(pid as i32, libc::SIGKILL) } == 0
        || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    {
        Ok(())
    } else {
        Err(format!(
            "kill {description} {pid}: {}",
            std::io::Error::last_os_error()
        ))
    }
}

fn uniform_prior_states(fleet: &RawDockerFleet, state: &str) -> BTreeMap<u64, String> {
    fleet
        .nodes
        .iter()
        .map(|node| (u64::from(node.slot) + 1, state.to_owned()))
        .collect()
}

fn enumerated_prior_states(
    fleet: &RawDockerFleet,
    states: &[RawPriorState],
) -> Result<BTreeMap<u64, String>, String> {
    if states.len() != fleet.nodes.len() {
        return Err(format!(
            "{} prior states cannot cover {} raw nodes",
            states.len(),
            fleet.nodes.len()
        ));
    }
    fleet
        .nodes
        .iter()
        .zip(states)
        .map(|(node, state)| {
            let value = serde_json::to_value(state)
                .map_err(|error| format!("serialize injected prior state: {error}"))?;
            let state = value
                .as_str()
                .ok_or_else(|| format!("injected prior state is not a string: {value}"))?;
            Ok((u64::from(node.slot) + 1, state.to_owned()))
        })
        .collect()
}

struct DeploymentRoundObservation {
    allow_creating: bool,
    prior_states: BTreeMap<u64, String>,
    interruption: Option<DeploymentInterruptionEvidence>,
    temporarily_unreachable_node: Option<u64>,
}

fn capture_deployment_round(
    context: &DeploymentRoundContext<'_>,
    harness: &ClusterHarness,
    bundle: &DeploymentBundle,
    telemetry_before: &BTreeSet<PathBuf>,
    observation: DeploymentRoundObservation,
) -> Result<DeploymentRoundEvidence, String> {
    let raw_census = assert_raw_deployment(context.fleet, bundle)?;
    let fleet_status = harness.fleet_snapshot()?;
    assert_fleet_identity(&fleet_status, &raw_census, bundle)?;
    assert_no_ssh_children(harness.orchestrator_pid())?;
    let archives = deployment_telemetry_files(context.fixture_dir)?
        .difference(telemetry_before)
        .cloned()
        .collect();
    let telemetry = capture_deployment_telemetry(archives, &raw_census, bundle)?;
    if !observation.allow_creating && telemetry.creating_event_count != 0 {
        return Err(format!(
            "retained deployment {} attempted {} acquisitions: {:?}; archives={:?}",
            bundle.deployment_generation,
            telemetry.creating_event_count,
            telemetry.creating_events,
            telemetry.archives
        ));
    }
    Ok(DeploymentRoundEvidence {
        deployment_generation: bundle.deployment_generation.clone(),
        artifact_digest: bundle.artifact_digest.clone(),
        executable_digest: bundle.executable_digest.clone(),
        prior_states: observation.prior_states,
        interruption: observation.interruption,
        temporarily_unreachable_node: observation.temporarily_unreachable_node,
        fixture: context.fleet.snapshot()?,
        raw_census,
        fleet_status,
        telemetry,
        matching_receipt_required_for_runtime_admission: true,
        stale_processes_and_descendants_absent: true,
        ssh_session_independent: true,
        fresh_membership_routing_readiness_telemetry: true,
        directed_all_pairs_behavior_passed: true,
    })
}

fn write_deployment_gate_evidence(
    path: &Path,
    evidence: &DeploymentGateEvidence,
) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(evidence)
        .map_err(|error| format!("serialize deployment gate evidence: {error}"))?;
    let temporary = path.with_extension("json.tmp");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| format!("create deployment gate evidence: {error}"))?;
    file.write_all(&bytes)
        .map_err(|error| format!("write deployment gate evidence: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("sync deployment gate evidence: {error}"))?;
    std::fs::rename(&temporary, path)
        .map_err(|error| format!("commit deployment gate evidence: {error}"))?;
    let parent = path
        .parent()
        .ok_or_else(|| format!("deployment evidence path {} has no parent", path.display()))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync deployment evidence directory: {error}"))
}

fn inject_prior_states(fleet: &RawDockerFleet, states: &[RawPriorState]) -> Result<(), String> {
    if states.len() != fleet.nodes.len() {
        return Err(format!(
            "{} prior states do not exactly cover {} raw nodes",
            states.len(),
            fleet.nodes.len()
        ));
    }
    for (node, state) in fleet.nodes.iter().zip(states.iter().copied()) {
        fleet.inject_prior_state(node.slot, state)?;
    }
    Ok(())
}

fn assert_raw_deployment(
    fleet: &RawDockerFleet,
    expected: &DeploymentBundle,
) -> Result<Vec<myelin_e2e_fuzz::RawNodeCensus>, String> {
    let census = fleet.census()?;
    if census.len() != fleet.nodes.len() {
        return Err(format!(
            "raw census returned {} nodes for a {}-node fixture",
            census.len(),
            fleet.nodes.len()
        ));
    }
    let expected_descriptor = serde_json::json!({
        "artifact_digest": expected.artifact_digest,
        "deployment_generation": expected.deployment_generation,
        "executable_digest": expected.executable_digest,
    });
    let release_suffix = expected
        .artifact_digest
        .strip_prefix("sha256:")
        .ok_or_else(|| "deployment bundle artifact digest is not sha256-prefixed".to_owned())?;
    for node in &census {
        if !node.surviving_prior_processes.is_empty() {
            return Err(format!(
                "node {} retained prior process incarnations: {:?}",
                node.logical_node_id, node.surviving_prior_processes
            ));
        }
        if node.worker_processes.len() != 1 {
            return Err(format!(
                "node {} has {} worker processes, expected exactly one: {:?}",
                node.logical_node_id,
                node.worker_processes.len(),
                node.worker_processes
            ));
        }
        let worker = &node.worker_processes[0];
        if node.agent_pid != Some(worker.pid) {
            return Err(format!(
                "node {} agent pid {:?} differs from sole worker {}",
                node.logical_node_id, node.agent_pid, worker.pid
            ));
        }
        if worker.executable_digest.as_deref() != Some(&expected.executable_digest)
            || node.active_executable_digest.as_deref() != Some(&expected.executable_digest)
        {
            return Err(format!(
                "node {} executable identity mismatch: worker={:?}, active={:?}, expected={}",
                node.logical_node_id,
                worker.executable_digest,
                node.active_executable_digest,
                expected.executable_digest
            ));
        }
        if node.worker_group_processes.len() != 1
            || node.worker_group_processes[0].pid != worker.pid
        {
            return Err(format!(
                "node {} retained stale worker descendants or group members: {:?}",
                node.logical_node_id, node.worker_group_processes
            ));
        }
        let link = node.active_link_target.as_deref().unwrap_or_default();
        if !link.ends_with(&format!("/releases/{release_suffix}")) {
            return Err(format!(
                "node {} active link {link:?} does not select artifact {}",
                node.logical_node_id, expected.artifact_digest
            ));
        }
        let descriptor = node
            .active_deployment_raw
            .as_deref()
            .ok_or_else(|| {
                format!(
                    "node {} has no active deployment descriptor",
                    node.logical_node_id
                )
            })
            .and_then(|raw| {
                serde_json::from_str::<serde_json::Value>(raw).map_err(|error| {
                    format!(
                        "node {} active deployment descriptor is invalid: {error}",
                        node.logical_node_id
                    )
                })
            })?;
        if descriptor != expected_descriptor {
            return Err(format!(
                "node {} active descriptor {descriptor} differs from {expected_descriptor}",
                node.logical_node_id
            ));
        }
    }
    Ok(census)
}
fn assert_fleet_identity(
    fleet: &serde_json::Value,
    census: &[myelin_e2e_fuzz::RawNodeCensus],
    expected: &DeploymentBundle,
) -> Result<(), String> {
    let nodes = fleet
        .pointer("/FleetStatus/nodes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("fleet status has no nodes: {fleet}"))?;
    if nodes.len() != census.len() {
        return Err(format!(
            "expected {} fleet nodes, saw {}",
            census.len(),
            nodes.len()
        ));
    }
    let mut observed = BTreeSet::new();
    for node in nodes {
        let node_id = node
            .get("logical_node_id")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("fleet node has no logical identity: {node}"))?;
        let raw = census
            .iter()
            .find(|raw| raw.logical_node_id == node_id)
            .ok_or_else(|| format!("fleet node {node_id} has no matching raw node"))?;
        if !observed.insert(node_id) {
            return Err(format!(
                "fleet reports logical node {node_id} more than once"
            ));
        }
        let phase = node.get("phase").and_then(serde_json::Value::as_str);
        if phase != Some("running") {
            return Err(format!(
                "node {node_id} is {phase:?}, expected running; node={node}"
            ));
        }
        let digest = node
            .pointer("/runtime/artifact_digest")
            .and_then(serde_json::Value::as_str);
        let generation = node
            .pointer("/runtime/deployment_generation")
            .and_then(serde_json::Value::as_str);
        if digest != Some(&expected.artifact_digest)
            || generation != Some(&expected.deployment_generation)
        {
            return Err(format!(
                "node {node_id} reports deployment ({digest:?}, {generation:?}), expected ({}, {})",
                expected.artifact_digest, expected.deployment_generation
            ));
        }
        let worker = &raw.worker_processes[0];
        let incarnation = format!(
            "{}:{}:{}",
            worker.pid, worker.start_ticks, expected.deployment_generation
        );
        let hash = Sha256::digest(incarnation.as_bytes());
        let mut identity_bytes = [0_u8; 8];
        identity_bytes.copy_from_slice(&hash[..8]);
        let readiness_id = u64::from_be_bytes(identity_bytes);
        if node
            .pointer("/runtime/readiness_id")
            .and_then(serde_json::Value::as_u64)
            != Some(readiness_id)
        {
            return Err(format!(
                "node {node_id} runtime readiness is not bound to incarnation {incarnation}"
            ));
        }
    }
    Ok(())
}

fn deployment_telemetry_files(fixture_dir: &Path) -> Result<BTreeSet<PathBuf>, String> {
    let mut files = BTreeSet::new();
    for entry in fs::read_dir(fixture_dir)
        .map_err(|error| format!("read deployment telemetry directory: {error}"))?
    {
        let entry = entry.map_err(|error| format!("inspect deployment telemetry: {error}"))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("telemetry-") && name.ends_with(".jsonl") {
            files.insert(entry.path());
        }
    }
    Ok(files)
}

fn capture_deployment_telemetry(
    archives: BTreeSet<PathBuf>,
    census: &[myelin_e2e_fuzz::RawNodeCensus],
    expected: &DeploymentBundle,
) -> Result<DeploymentTelemetryEvidence, String> {
    let mut receipts = BTreeMap::new();
    let mut creating_events = Vec::new();
    let mut bootstrapping_events = Vec::new();
    for archive in &archives {
        let file = fs::File::open(archive)
            .map_err(|error| format!("open deployment telemetry: {error}"))?;
        let mut reader = std::io::BufReader::new(file);
        let mut line = String::new();
        loop {
            line.clear();
            if reader
                .read_line(&mut line)
                .map_err(|error| format!("read deployment telemetry: {error}"))?
                == 0
            {
                break;
            }
            // A live archive can end in an uncommitted partial frame.
            if !line.ends_with('\n') || !line.contains("myelin.provisioning.") {
                continue;
            }
            let frame: serde_json::Value = serde_json::from_str(&line)
                .map_err(|error| format!("decode deployment telemetry frame: {error}"))?;
            let channel = frame
                .get("channel")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if channel != "myelin.provisioning.events"
                && !(channel.starts_with("myelin.provisioning.logs.node.")
                    && channel.ends_with(".provider"))
            {
                continue;
            }
            let archived = frame
                .pointer("/payload/value")
                .ok_or_else(|| "deployment telemetry frame has no decoded payload".to_owned())?;
            let payload = match archived {
                serde_json::Value::String(text) => serde_json::from_str::<serde_json::Value>(text)
                    .map_err(|error| format!("decode deployment telemetry payload: {error}"))?,
                value => value.clone(),
            };
            if channel == "myelin.provisioning.events" {
                match payload
                    .pointer("/event/kind")
                    .and_then(serde_json::Value::as_str)
                {
                    Some("Creating") => creating_events.push(payload),
                    Some("Bootstrapping") => bootstrapping_events.push(payload),
                    _ => {}
                }
                continue;
            }
            let Some(provider_line) = payload
                .pointer("/line/line")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            if !provider_line.contains("MyelinBootstrapDispatched") {
                continue;
            }
            let dispatched: serde_json::Value = serde_json::from_str(provider_line)
                .map_err(|error| format!("decode deployment provider line: {error}"))?;
            if dispatched.get("type").and_then(serde_json::Value::as_str)
                != Some("MyelinBootstrapDispatched")
                || dispatched
                    .get("deployment_generation")
                    .and_then(serde_json::Value::as_str)
                    != Some(&expected.deployment_generation)
            {
                continue;
            }
            let Some(node_id) = dispatched
                .get("node_id")
                .and_then(serde_json::Value::as_u64)
            else {
                continue;
            };
            let Some(node) = census.iter().find(|node| node.logical_node_id == node_id) else {
                continue;
            };
            let worker = &node.worker_processes[0];
            let incarnation = format!(
                "{}:{}:{}",
                worker.pid, worker.start_ticks, expected.deployment_generation
            );
            let receipt = dispatched
                .get("receipt")
                .ok_or_else(|| format!("node {node_id} dispatched without a receipt"))?;
            let expected_receipt = serde_json::json!({
                "type": "MyelinBootstrapReceipt",
                "artifact_digest": expected.artifact_digest,
                "deployment_generation": expected.deployment_generation,
                "executable_digest": expected.executable_digest,
                "pid": worker.pid,
                "process_start_ticks": worker.start_ticks,
                "incarnation": incarnation,
                "worker_count": 1,
            });
            if receipt == &expected_receipt {
                receipts.insert(node_id, receipt.clone());
            }
        }
    }
    for node in census {
        if !bootstrapping_events.iter().any(|event| {
            event
                .pointer("/event/node_id")
                .and_then(serde_json::Value::as_u64)
                == Some(node.logical_node_id)
        }) {
            return Err(format!(
                "node {} has no Bootstrapping phase telemetry for this deployment round",
                node.logical_node_id
            ));
        }
        if !receipts.contains_key(&node.logical_node_id) {
            return Err(format!(
                "node {} has no matching provider launch receipt for generation {} and its live process incarnation",
                node.logical_node_id, expected.deployment_generation
            ));
        }
    }
    Ok(DeploymentTelemetryEvidence {
        archives,
        provider_receipts: receipts,
        creating_event_count: creating_events.len(),
        creating_events,
        bootstrapping_events,
    })
}

fn assert_no_ssh_children(orchestrator_pid: u32) -> Result<(), String> {
    for pid in process_descendants(orchestrator_pid)? {
        let comm = std::fs::read_to_string(format!("/proc/{pid}/comm"))
            .unwrap_or_default()
            .trim()
            .to_owned();
        if comm == "ssh" || comm == "scp" {
            return Err(format!(
                "orchestrator {orchestrator_pid} still runs {comm} descendant {pid} after bootstrap"
            ));
        }
    }
    Ok(())
}

fn run_mock_campaign(
    options: &Options,
    workspace: PathBuf,
    config: CampaignConfig,
    plan: CampaignPlan,
    campaign_root: PathBuf,
    total: &Budget,
) -> Result<(), String> {
    let fixture = ClusterHarnessConfig {
        workspace,
        artifacts: campaign_root.join("fixture"),
        node_count: 5,
        seed: plan.seed,
        image: Some(config.runtime_image),
        build_image: options.build_image,
        deadline: options.deadline,
        provider: HarnessProvider::LocalMock,
        selected_offer_ids: (1..=5).collect(),
        state_dir: Some(myelin_e2e_fuzz::private_fixture_dir(&campaign_root)?),
        reset_state: true,
        offer_search_id: None,
        provision: true,
        adopt_only: false,
        relay_url: None,
        run_id: 1,
    };
    let preparation = PhaseTimer::new("fixture-preparation");
    let execution = total.child(
        total
            .remaining("local fixture preparation")?
            .saturating_sub(Duration::from_secs(LOCAL_CLEANUP_RESERVE_SECS)),
    );
    let mut harness =
        ClusterHarness::start_with_budgets(fixture, execution.clone(), total.clone())?;
    let result = harness.verify_workload_convergence().and_then(|()| {
        execution.check("local prepared-fixture commit")?;
        drop(preparation);
        execute_campaign(
            &mut harness,
            &plan,
            &campaign_root,
            &options.artifacts,
            options.shrink,
            &execution,
            false,
        )
    });
    let _cleanup = PhaseTimer::new("fixture-cleanup");
    let cleanup = total.child(Duration::from_secs(LOCAL_CLEANUP_RESERVE_SECS));
    let teardown = harness.teardown_with_budget(cleanup);
    merge_execution_cleanup(result, teardown)?;
    total.check("complete local campaign and evidence")
}

fn run_paid_campaign(
    options: &Options,
    workspace: PathBuf,
    config: CampaignConfig,
    plan: CampaignPlan,
    campaign_root: PathBuf,
    total: &Budget,
) -> Result<(), String> {
    let state_path = campaign_root.join("paid-state.json");
    let state_dir = myelin_e2e_fuzz::private_fixture_dir(&campaign_root)?;
    // Model seeds are deliberately repeatable; paid ownership identities are not.
    // Reusing a seed in another artifact root must never reuse provider labels.
    let mut entropy = [0_u8; 8];
    fs::File::open("/dev/urandom")
        .and_then(|mut source| std::io::Read::read_exact(&mut source, &mut entropy))
        .map_err(|error| format!("allocate paid fixture identity: {error}"))?;
    let run_id = u64::from_le_bytes(entropy) | (1 << 63);
    let mut paid = PaidCampaignState::planned(plan.campaign_id.clone(), state_dir.clone());
    write_paid_state(&state_path, &paid)?;
    let execution = total.child(
        total
            .remaining("paid invocation admission")?
            .saturating_sub(Duration::from_secs(CLEANUP_DEADLINE_SECS)),
    );
    // Preparation starts before the orchestrator/provider can do any work,
    // including both searches, readiness cleanup and the durable fixture commit.
    let preparation = execution.child(Duration::from_secs(PREPARATION_DEADLINE_SECS));
    let preparation_timer = PhaseTimer::new("paid-preparation");
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes epoch: {error}"))?
        .as_millis() as u64;
    let admission_expiry = now_ms
        .checked_add(config.limits.fixture_lifetime_secs.saturating_mul(1000))
        .ok_or_else(|| "paid lifetime expiry overflowed".to_owned())?;
    let cleanup_limits = authorized_cleanup_limits(&config.limits)?;
    let admission_path = state_dir.join("paid-admission.json");
    let admission = provisioning::PaidFixtureAdmission::create_cleanup_only(
        &admission_path,
        run_id,
        admission_expiry,
        provisioning::PaidProcessIdentity::current()?,
        cleanup_limits.clone(),
    )?;
    myelin_e2e_fuzz::start_cleanup_owner(
        &std::env::current_exe().map_err(|error| error.to_string())?,
        &admission_path,
        &options.api_key_env,
        cleanup_limits,
        preparation.remaining("start independent paid cleanup owner before provider search")?,
    )?;
    let fixture = ClusterHarnessConfig {
        workspace,
        artifacts: campaign_root.join("fixture"),
        node_count: 5,
        seed: plan.seed,
        image: Some(config.runtime_image.clone()),
        build_image: false,
        deadline: options.deadline,
        provider: HarnessProvider::VastAiReal {
            ssh_identity: options
                .ssh_identity
                .clone()
                .expect("paid identity validated"),
            api_key_env: options.api_key_env.clone(),
            bundle: None,
            admission: admission_path,
        },
        selected_offer_ids: Vec::new(),
        state_dir: Some(state_dir.clone()),
        reset_state: true,
        offer_search_id: None,
        provision: false,
        adopt_only: false,
        relay_url: None,
        run_id,
    };
    let mut harness =
        ClusterHarness::start_with_budgets(fixture, preparation.clone(), total.clone())?;
    admission.bind_orchestrator(harness.orchestrator_pid())?;
    let result = (|| {
        let search_timer = PhaseTimer::new("offer-selection");
        let request = offer_search_request(&config);
        request.validate()?;
        let payload = serde_json::to_value(&request)
            .map_err(|error| format!("serialize paid offer search: {error}"))?;
        let search = preparation.child(Duration::from_secs(120));
        harness.set_execution_budget(search.clone());
        let observe = || {
            loop {
                search.check("observe eligible cheapest distinct-host offers")?;
                match harness.search_offers(payload.clone()) {
                    Ok(response) => break decode_offer_results(response),
                    Err(error) => search.wait(
                        Duration::from_millis(250),
                        &format!("provider search readiness/retry: {error}"),
                    )?,
                }
            }
        };
        let first = observe()?;
        let first_selected = select_exact_offers(&first, &config, &plan)?;
        // Both actual observations are independently eligible and cheapest.
        // Provider HTTP/429 policy controls rate; elapsed dwell is not stability.
        let confirmed = observe()?;
        let selected = select_exact_offers(&confirmed, &config, &plan)?;
        if first_selected
            .iter()
            .map(|offer| (offer.offer_id, offer.host_id, offer.hourly_price))
            .ne(selected
                .iter()
                .map(|offer| (offer.offer_id, offer.host_id, offer.hourly_price)))
        {
            return Err(
                "eligible cheapest distinct-host selection changed between observations".to_owned(),
            );
        }
        durable_json(
            &campaign_root.join("offer-stability.json"),
            &serde_json::json!({
                "schema_version": 1, "first": first_selected, "confirmed": selected,
                "first_search_id": first.search_id, "confirmed_search_id": confirmed.search_id,
            }),
        )?;
        drop(search_timer);
        harness.set_execution_budget(preparation.clone());
        let worst_case_cost = conservative_selected_cost(&selected, &config.limits)?;
        myelin_e2e_fuzz::selected_cleanup_limits(&selected, &config.limits)?;
        let offer_ids = selected
            .iter()
            .map(|offer| offer.offer_id)
            .collect::<Vec<_>>();
        if harness.fixture_run_id()? != run_id {
            return Err("paid orchestrator changed the allocated fixture identity".to_owned());
        }
        let labels = (1..=5)
            .map(|node| format!("myelin-{run_id}-{node}-attempt-0"))
            .collect::<BTreeSet<_>>();
        let admission_nodes = selected
            .iter()
            .zip(1_u64..)
            .map(|(offer, node_id)| provisioning::PaidNodeAdmission {
                node_id,
                offer_id: offer.offer_id,
                host_id: offer.host_id.expect("selection requires exact host"),
                label: format!("myelin-{run_id}-{node_id}-attempt-0"),
            })
            .collect();

        // Durable cleanup ownership precedes the capability authorizing a create.
        paid.begin_acquisition(offer_ids.clone(), labels)?;
        write_paid_state(&state_path, &paid)?;
        preparation.check("authorize five exact paid creates")?;
        admission.bind_selected_nodes(admission_nodes)?;
        paid.reconcile_admission(&admission.snapshot()?)?;
        write_paid_state(&state_path, &paid)?;
        harness.provision_selected(offer_ids, confirmed.search_id)?;
        harness.verify_workload_convergence()?;
        harness.seal_paid_admission()?;
        let contracts = harness.owned_provider_contracts()?;
        let actual_labels = harness.owned_provider_labels()?;
        paid.reconcile_prepared(
            &provisioning::PaidFixtureAdmission::open(paid.state_dir.join("paid-admission.json"))?
                .snapshot()?,
            contracts,
            actual_labels,
        )?;
        write_paid_state(&state_path, &paid)?;
        preparation.check("paid prepared-fixture commit and readiness cleanup")?;
        drop(preparation_timer);
        println!(
            "paid fixture prepared: campaign={} contracts={} worst_case_cost=${worst_case_cost:.6}",
            plan.campaign_id,
            paid.owned_contract_ids.len()
        );
        if options.retain_paid_fixture {
            return retain_paid_harness(&mut harness, &state_path, &mut paid);
        }
        let campaign = execution.child(Duration::from_secs(CAMPAIGN_DEADLINE_SECS));
        execute_campaign(
            &mut harness,
            &plan,
            &campaign_root,
            &options.artifacts,
            options.shrink,
            &campaign,
            false,
        )
    })();
    if options.retain_paid_fixture && result.is_ok() {
        println!(
            "development fixture retained at {}; campaign acceptance not attempted",
            state_path.display()
        );
        return result;
    }
    let _cleanup_timer = PhaseTimer::new("paid-fixture-cleanup");
    let cleanup = total.child(Duration::from_secs(CLEANUP_DEADLINE_SECS));
    let cleaned = cleanup_paid_harness(
        &mut harness,
        &state_path,
        &mut paid,
        options,
        &campaign_root,
        &cleanup,
    );
    let secret_scan = std::env::var(&options.api_key_env)
        .map_err(|_| "paid credential environment is unset".to_owned())
        .and_then(|secret| scan_artifacts_for_secret(&campaign_root, secret.as_bytes()));
    merge_execution_cleanup(result, merge_execution_cleanup(cleaned, secret_scan))?;
    total.check("complete paid campaign and cleanup")
}

fn retain_paid_harness(
    harness: &mut ClusterHarness,
    state_path: &Path,
    paid: &mut PaidCampaignState,
) -> Result<(), String> {
    paid.quarantine(
        "explicitly retained development fixture; fresh binary redeployment and readiness required",
    );
    write_paid_state(state_path, paid)?;
    harness.retain_remote_fixture(true)?;
    paid.reconcile_admission(
        &provisioning::PaidFixtureAdmission::open(paid.state_dir.join("paid-admission.json"))?
            .snapshot()?,
    )?;
    write_paid_state(state_path, paid)
}

#[derive(serde::Serialize)]
#[serde(deny_unknown_fields)]
struct PaidCleanupProof<'a> {
    schema_version: u32,
    kind: &'static str,
    complete: bool,
    paid_state: &'a PaidCampaignState,
    admission: Option<&'a provisioning::PaidAdmissionSnapshot>,
    errors: &'a [String],
}

fn cleanup_paid_harness(
    harness: &mut ClusterHarness,
    state_path: &Path,
    paid: &mut PaidCampaignState,
    options: &Options,
    campaign_root: &Path,
    cleanup: &Budget,
) -> Result<(), String> {
    paid.enter_cleanup_only();
    // Both durable records deny work before shutdown begins. The admission
    // barrier prevents its cleanup owner from deleting provider resources until
    // the exact orchestrator incarnation has been stopped and reaped.
    let persisted_state = write_paid_state(state_path, paid);
    let admission_path = paid.state_dir.join("paid-admission.json");
    let shutdown_barrier = if admission_path.is_file() {
        provisioning::PaidFixtureAdmission::open(&admission_path)
            .and_then(|admission| admission.begin_cleanup_shutdown())
    } else {
        Ok(())
    };
    let orchestrator_result = harness.stop_paid_orchestrator_for_cleanup(cleanup);
    let cleanup_release = if admission_path.is_file() {
        provisioning::PaidFixtureAdmission::open(&admission_path)
            .and_then(|admission| admission.release_cleanup_after_orchestrator_stopped())
    } else {
        Ok(())
    };
    let sealed_admission = seal_cleanup_admission(paid);
    let state_result = merge_execution_cleanup(
        persisted_state,
        merge_execution_cleanup(
            shutdown_barrier,
            merge_execution_cleanup(
                orchestrator_result,
                merge_execution_cleanup(cleanup_release, sealed_admission),
            ),
        ),
    );
    let provider_result = cleanup
        .remaining("independent spending-stop cleanup")
        .and_then(|remaining| {
            myelin_e2e_fuzz::cleanup_paid_ownership(
                paid,
                state_path,
                &std::env::current_exe().map_err(|error| error.to_string())?,
                &options.api_key_env,
                remaining,
            )
        });
    // The final harness phase only observes the completed durable owner receipt
    // and persists its lifecycle/absence evidence; it cannot race deletion.
    let harness_result = harness.teardown_with_budget(cleanup.clone());
    let mut errors = [&state_result, &provider_result, &harness_result]
        .into_iter()
        .filter_map(|result| result.as_ref().err().cloned())
        .collect::<Vec<_>>();
    if let Err(error) = write_paid_state(state_path, paid) {
        errors.push(error);
    }
    let admission_path = paid.state_dir.join("paid-admission.json");
    let admission = if admission_path.is_file() {
        match provisioning::PaidFixtureAdmission::open(&admission_path)
            .and_then(|admission| admission.snapshot())
        {
            Ok(admission) => Some(admission),
            Err(error) => {
                errors.push(format!("read final paid cleanup accounting: {error}"));
                None
            }
        }
    } else {
        None
    };
    let complete = errors.is_empty()
        && paid.phase == PaidCampaignPhase::Complete
        && paid.owned_contract_ids.is_empty()
        && admission.as_ref().is_none_or(|admission| {
            admission.accounting_errors.is_empty()
                && admission
                    .cleanup
                    .as_ref()
                    .is_some_and(|cleanup| cleanup.complete && cleanup.spending_stopped)
        });
    let receipt = durable_json(
        &campaign_root.join("cleanup-proof.json"),
        &PaidCleanupProof {
            schema_version: 2,
            kind: "paid-cleanup-proof",
            complete,
            paid_state: paid,
            admission: admission.as_ref(),
            errors: &errors,
        },
    );
    if let Err(error) = receipt {
        errors.push(error);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn seal_cleanup_admission(paid: &PaidCampaignState) -> Result<(), String> {
    let path = paid.state_dir.join("paid-admission.json");
    if path.is_file() {
        provisioning::PaidFixtureAdmission::open(path)?.enter_cleanup_only()?;
    }
    Ok(())
}

fn run_resume_paid(state_path: &Path, options: &Options, workspace: PathBuf) -> Result<(), String> {
    let _resume_timer = PhaseTimer::new("paid-retained-redeployment");
    let campaign_root = state_path
        .parent()
        .ok_or_else(|| "paid state path has no campaign directory".to_owned())?;
    let mut paid = read_paid_state(state_path)?;
    if paid.phase != PaidCampaignPhase::Quarantined {
        return Err(format!(
            "paid fixture resume requires quarantined state, found {:?}",
            paid.phase
        ));
    }
    if paid.acquisition_count != 5 || paid.bootstrap_count != 5 {
        return Err(
            "incomplete preparation permits cleanup-only, never resumed bootstrap".to_owned(),
        );
    }
    let admission_owner =
        provisioning::PaidFixtureAdmission::open(paid.state_dir.join("paid-admission.json"))?;
    admission_owner.require_live_cleanup_owner()?;
    let admission = admission_owner.snapshot()?;
    if admission.mode != provisioning::PaidAdmissionMode::Prepared {
        return Err("cleanup-only admission cannot authorize retained redeployment".to_owned());
    }
    let cleanup_owner = admission
        .cleanup
        .as_ref()
        .filter(|owner| owner.retained_development_fixture)
        .ok_or("resume requires an explicitly authorized retained development fixture")?;
    paid.reconcile_admission(&admission)?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("retained lifetime clock: {error}"))?
        .as_millis() as u64;
    let lifetime = Duration::from_millis(
        cleanup_owner
            .stop_unix_ms
            .saturating_add(provisioning::PAID_CLEANUP_RESERVE_MS)
            .min(admission.deadline_unix_ms)
            .saturating_sub(now_ms),
    );
    let total = Budget::new(lifetime.min(Duration::from_secs(
        PREPARATION_DEADLINE_SECS + CAMPAIGN_DEADLINE_SECS + CLEANUP_DEADLINE_SECS,
    )));
    let execution = total.child(
        total
            .remaining("retained fixture lifetime")?
            .saturating_sub(Duration::from_secs(CLEANUP_DEADLINE_SECS)),
    );
    let preparation = execution.child(Duration::from_secs(PREPARATION_DEADLINE_SECS));
    let checkpoint_path = campaign_root.join("campaign-progress.json");
    if checkpoint_path.is_file() {
        let checkpoint: CampaignCheckpoint = serde_json::from_reader(
            fs::File::open(&checkpoint_path)
                .map_err(|error| format!("read recovery checkpoint: {error}"))?,
        )
        .map_err(|error| format!("parse recovery checkpoint: {error}"))?;
        if checkpoint.pending_recovery.is_some() {
            return Err(
                "failed formal recovery permits cleanup-only; prior coverage cannot resume"
                    .to_owned(),
            );
        }
    }
    let plan = read_campaign_plan(&campaign_root.join("campaign-plan.json"))?;
    if plan.campaign_id != paid.campaign_id {
        return Err("paid state and campaign plan identities differ".to_owned());
    }
    let snapshot = serde_json::from_slice::<serde_json::Value>(
        &fs::read(paid.state_dir.join("cluster.json"))
            .map_err(|error| format!("read retained cluster snapshot: {error}"))?,
    )
    .map_err(|error| format!("parse retained cluster snapshot: {error}"))?;
    if snapshot.get("run_id").and_then(serde_json::Value::as_u64) != Some(admission.run_id) {
        return Err(
            "retained cluster snapshot and paid admission run identities differ".to_owned(),
        );
    }
    let live_nodes = snapshot
        .get("nodes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|node| {
            node.get("phase")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|phase| phase != "stopped" && phase != "orphan")
        })
        .filter_map(|node| {
            node.get("logical_node_id")
                .and_then(serde_json::Value::as_u64)
        })
        .collect::<BTreeSet<_>>();
    if live_nodes != plan.expected_initial_nodes {
        return Err(format!(
            "binary redeployment invalidates prior coverage; retained set {live_nodes:?} cannot rerun the full five-node campaign without forbidden replacement"
        ));
    }
    let node_count = u8::try_from(live_nodes.len())
        .map_err(|_| "retained fixture node count exceeds u8".to_owned())?;
    // Refresh retained paid nodes through the orchestrator's normal
    // reconciliation: a rebuilt deployment bundle with a fresh generation
    // demotes stale nodes to Bootstrapping and re-runs the SSH artifact
    // transaction. The harness never runs per-node SCP/SSH orchestration.
    let bundle_artifacts = campaign_root.join("bundle");
    std::fs::create_dir_all(&bundle_artifacts)
        .map_err(|error| format!("create bundle directory: {error}"))?;
    let payload =
        myelin_e2e_fuzz::stage_deployment_payload(&workspace, &bundle_artifacts, &preparation)?;
    let generation = format!(
        "resume-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock precedes epoch: {error}"))?
            .as_millis()
    );
    let bundle = myelin_e2e_fuzz::assemble_deployment_bundle(
        &bundle_artifacts,
        &payload,
        &generation,
        &preparation,
    )?;
    admission_owner.authorize_retained_deployment(
        true,
        provisioning::plugin::DeploymentIdentity {
            artifact_digest: bundle.artifact_digest.clone(),
            deployment_generation: bundle.deployment_generation.clone(),
        },
    )?;
    let fixture = ClusterHarnessConfig {
        workspace,
        artifacts: campaign_root.join("fixture"),
        node_count,
        seed: plan.seed,
        image: options.image.clone(),
        build_image: false,
        deadline: options.deadline,
        provider: HarnessProvider::VastAiReal {
            admission: paid.state_dir.join("paid-admission.json"),
            ssh_identity: options
                .ssh_identity
                .clone()
                .expect("resume SSH identity validated by parser"),
            api_key_env: options.api_key_env.clone(),
            bundle: Some(bundle.tar_path.clone()),
        },
        selected_offer_ids: paid.selected_offer_ids.clone(),
        state_dir: Some(paid.state_dir.clone()),
        reset_state: false,
        offer_search_id: None,
        provision: false,
        adopt_only: false,
        relay_url: None,
        run_id: admission.run_id,
    };
    let mut harness =
        ClusterHarness::start_with_budgets(fixture, preparation.clone(), total.clone())?;
    let repair = (|| {
        let before = harness.owned_provider_contracts()?;
        if before.len() != live_nodes.len() {
            return Err(format!(
                "retained fixture has {} live nodes but {} attributed contracts",
                live_nodes.len(),
                before.len()
            ));
        }
        if !paid.owned_contract_ids.is_empty() && !before.is_subset(&paid.owned_contract_ids) {
            return Err(format!(
                "retained contracts {before:?} are not a subset of durable ownership {:?}",
                paid.owned_contract_ids
            ));
        }
        harness.verify_workload_convergence()?;
        let after = harness.owned_provider_contracts()?;
        if after != before {
            return Err(format!(
                "in-place deployment changed contracts: before={before:?}, after={after:?}"
            ));
        }
        paid.resume_deny_only(
            &provisioning::PaidFixtureAdmission::open(paid.state_dir.join("paid-admission.json"))?
                .snapshot()?,
            after,
        )?;
        write_paid_state(state_path, &paid)?;
        preparation.check("retained redeployment prepared commit")
    })();
    if let Err(error) = repair {
        let diagnostics = harness.orchestrator_diagnostics();
        let failure = format!("retained paid fixture repair failed: {error}; {diagnostics}");
        paid.quarantine(format!(
            "{failure}; exact contracts retained; cases paused; only explicit in-place retry or cleanup is permitted"
        ));
        let persistence = write_paid_state(state_path, &paid);
        let retention = harness.retain_remote_fixture(true);
        let reconciliation = admission_owner
            .snapshot()
            .and_then(|admission| paid.reconcile_admission(&admission))
            .and_then(|()| write_paid_state(state_path, &paid));
        return merge_execution_cleanup(
            Err(failure),
            merge_execution_cleanup(
                persistence,
                merge_execution_cleanup(retention, reconciliation),
            ),
        );
    }
    let campaign = execution.child(Duration::from_secs(CAMPAIGN_DEADLINE_SECS));
    let execution = if options.retain_paid_fixture {
        retain_paid_harness(&mut harness, state_path, &mut paid)
    } else {
        execute_campaign(
            &mut harness,
            &plan,
            campaign_root,
            campaign_root
                .parent()
                .ok_or("campaign corpus root missing")?,
            options.shrink,
            &campaign,
            false,
        )
    };
    if options.retain_paid_fixture && execution.is_ok() {
        println!(
            "development fixture retained at {}; campaign acceptance not attempted",
            state_path.display()
        );
        return execution;
    }
    let _cleanup_timer = PhaseTimer::new("paid-fixture-cleanup");
    let cleanup_budget = total.child(Duration::from_secs(CLEANUP_DEADLINE_SECS));
    let cleanup = cleanup_paid_harness(
        &mut harness,
        state_path,
        &mut paid,
        options,
        campaign_root,
        &cleanup_budget,
    );
    merge_execution_cleanup(execution, cleanup)?;
    total.check("complete retained deployment campaign and cleanup")
}

fn execute_campaign(
    harness: &mut ClusterHarness,
    plan: &CampaignPlan,
    campaign_root: &Path,
    corpus_root: &Path,
    shrink: bool,
    campaign: &Budget,
    allow_resume: bool,
) -> Result<(), String> {
    let _workload_timer = PhaseTimer::new("campaign-workload");
    let workload = campaign.child(
        campaign
            .remaining("campaign workload allocation")?
            .saturating_sub(Duration::from_secs(DIAGNOSTIC_DEADLINE_SECS))
            .min(Duration::from_secs(WORKLOAD_DEADLINE_SECS)),
    );
    harness.set_execution_budget(workload.clone());
    let fleet = harness.fleet_snapshot()?;
    let nodes = fleet
        .pointer("/FleetStatus/nodes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "campaign identity census omitted nodes".to_owned())?;
    let mut identity = nodes.clone();
    identity.sort_by_key(|node| {
        node.get("logical_node_id")
            .and_then(serde_json::Value::as_u64)
    });
    let serialized_identity = serde_json::to_vec(&identity).map_err(|error| error.to_string())?;
    let fixture_digest = format!("{:x}", Sha256::digest(&serialized_identity));
    let binaries = artifact_identity(&std::env::current_dir().map_err(|error| error.to_string())?)?;
    let evidence_identity = myelin_e2e_fuzz::EvidenceIdentity {
        plan_digest: hash_file(&campaign_root.join("campaign-plan.json"))?,
        artifacts_digest: format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&binaries).map_err(|error| error.to_string())?)
        ),
        deployment_generation: fixture_digest,
        fixture_mapping_digest: harness.fixture_identity_digest()?,
    };
    let checkpoint_path = campaign_root.join("campaign-progress.json");
    let mut checkpoint = CampaignCheckpoint {
        schema_version: 2,
        campaign_id: plan.campaign_id.clone(),
        deployment_identity: serde_json::to_value(identity).map_err(|error| error.to_string())?,
        evidence_identity: evidence_identity.clone(),
        completed_regressions: BTreeSet::new(),
        completed_recoveries: BTreeSet::new(),
        pending_recovery: None,
    };
    if allow_resume && checkpoint_path.is_file() {
        let prior: CampaignCheckpoint = serde_json::from_reader(
            fs::File::open(&checkpoint_path)
                .map_err(|error| format!("read campaign checkpoint: {error}"))?,
        )
        .map_err(|error| format!("decode campaign checkpoint: {error}"))?;
        if prior.schema_version != checkpoint.schema_version
            || prior.campaign_id != checkpoint.campaign_id
            || prior.deployment_identity != checkpoint.deployment_identity
            || prior.evidence_identity != checkpoint.evidence_identity
            || prior.pending_recovery.is_some()
            || !prior.completed_regressions.is_subset(
                &plan
                    .regressions
                    .iter()
                    .map(|case| case.id.clone())
                    .collect(),
            )
            || !prior
                .completed_recoveries
                .is_subset(&plan.recovery.iter().map(|case| case.id.clone()).collect())
        {
            return Err("campaign checkpoint is not a fully cleaned boundary on this exact deployment; failed formal recovery cannot resume".to_owned());
        }
        checkpoint = prior;
    }
    durable_json(&checkpoint_path, &checkpoint)?;
    if !allow_resume {
        // A fresh fixture or binary redeployment invalidates every old ledger.
        for (phase, ledger) in [
            ("normal", &plan.normal_coverage),
            ("four-node", &plan.four_node_coverage),
            ("three-node", &plan.three_node_coverage),
        ] {
            let mut ledger = ledger.clone();
            ledger.bind_identity(evidence_identity.clone())?;
            write_coverage_ledger(
                &campaign_root.join(format!("{phase}-coverage.json")),
                &plan.campaign_id,
                phase,
                &ledger,
            )?;
        }
    }
    let mut live = harness.node_ids().iter().copied().collect::<BTreeSet<_>>();
    if live != plan.expected_initial_nodes
        && live != plan.first_survivors
        && live != plan.second_survivors
    {
        return Err(format!(
            "fixture nodes {live:?} do not match any persisted campaign segment"
        ));
    }

    if live == plan.expected_initial_nodes {
        for case in &plan.regressions {
            if checkpoint.completed_regressions.contains(&case.id) {
                continue;
            }
            workload.check(&format!("admit regression {}", case.id))?;
            run_campaign_case(harness, case, campaign_root, corpus_root, shrink, campaign)?;
            checkpoint.completed_regressions.insert(case.id.clone());
            durable_json(&checkpoint_path, &checkpoint)?;
        }
        let mut normal_coverage = plan.normal_coverage.clone();
        normal_coverage.bind_identity(evidence_identity.clone())?;
        run_covered_phase(
            harness,
            &plan.normal,
            &mut normal_coverage,
            campaign_root,
            corpus_root,
            &plan.campaign_id,
            "normal",
            shrink,
            campaign,
        )?;
        let recoveries = plan
            .recovery
            .iter()
            .filter(|recovery| !checkpoint.completed_recoveries.contains(&recovery.id))
            .collect::<Vec<_>>();
        if let Some(first) = recoveries.first() {
            workload.check("admit formal recovery campaign")?;
            checkpoint.pending_recovery = Some(first.id.clone());
            durable_json(&checkpoint_path, &checkpoint)?;
            run_recovery_campaign(harness, &recoveries)?;
            workload.check("commit fully cleaned recovery campaign")?;
            checkpoint
                .completed_recoveries
                .extend(recoveries.iter().map(|recovery| recovery.id.clone()));
            checkpoint.pending_recovery = None;
            durable_json(&checkpoint_path, &checkpoint)?;
        }

        let removed = plan
            .expected_initial_nodes
            .difference(&plan.first_survivors)
            .copied()
            .collect::<Vec<_>>();
        if removed.len() != 1 {
            return Err("four-node survivor plan does not remove exactly one node".to_owned());
        }
        let _transition = PhaseTimer::new("destructive-transition-five-to-four");
        workload.check("admit five-to-four destructive transition")?;
        harness.remove_node(removed[0])?;
        live = harness.node_ids().iter().copied().collect();
        if live != plan.first_survivors {
            return Err("four-node survivor set differs from the persisted plan".to_owned());
        }
    }

    if live == plan.first_survivors {
        let mut four_coverage = plan.four_node_coverage.clone();
        four_coverage.bind_identity(evidence_identity.clone())?;
        run_covered_phase(
            harness,
            &plan.four_node,
            &mut four_coverage,
            campaign_root,
            corpus_root,
            &plan.campaign_id,
            "four-node",
            shrink,
            campaign,
        )?;
        let removed = plan
            .first_survivors
            .difference(&plan.second_survivors)
            .copied()
            .collect::<Vec<_>>();
        if removed.len() != 1 {
            return Err("three-node survivor plan does not remove exactly one node".to_owned());
        }
        let _transition = PhaseTimer::new("destructive-transition-four-to-three");
        workload.check("admit four-to-three destructive transition")?;
        harness.remove_node(removed[0])?;
        live = harness.node_ids().iter().copied().collect();
        if live != plan.second_survivors {
            return Err("three-node survivor set differs from the persisted plan".to_owned());
        }
    }

    let mut three_coverage = plan.three_node_coverage.clone();
    three_coverage.bind_identity(evidence_identity)?;
    run_covered_phase(
        harness,
        &plan.three_node,
        &mut three_coverage,
        campaign_root,
        corpus_root,
        &plan.campaign_id,
        "three-node",
        shrink,
        campaign,
    )?;
    if checkpoint.completed_regressions
        != plan
            .regressions
            .iter()
            .map(|case| case.id.clone())
            .collect()
        || checkpoint.completed_recoveries
            != plan.recovery.iter().map(|case| case.id.clone()).collect()
    {
        return Err(
            "campaign completed survivor phase without full regression/recovery coverage"
                .to_owned(),
        );
    }
    workload.check("complete 208 logical cases / 224 workload segments plus regressions")
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CampaignCheckpoint {
    schema_version: u32,
    campaign_id: String,
    deployment_identity: serde_json::Value,
    evidence_identity: myelin_e2e_fuzz::EvidenceIdentity,
    completed_regressions: BTreeSet<String>,
    completed_recoveries: BTreeSet<String>,
    pending_recovery: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AcceptedAttempt {
    identity: myelin_e2e_fuzz::EvidenceIdentity,
    attempt: u64,
    executed_case: BehaviorCase,
    observation: myelin_e2e_fuzz::CaseObservation,
}

fn run_covered_phase(
    harness: &mut ClusterHarness,
    cases: &[BehaviorCase],
    ledger: &mut CoverageLedger,
    campaign_root: &Path,
    corpus_root: &Path,
    campaign_id: &str,
    phase: &str,
    shrink: bool,
    diagnosis: &Budget,
) -> Result<(), String> {
    let _phase_timer = PhaseTimer::new(format!("coverage-phase-{phase}"));
    let path = campaign_root.join(format!("{phase}-coverage.json"));
    let proofs = campaign_root.join("coverage-evidence").join(phase);
    if path.is_file() {
        let mut resumed = read_coverage_ledger(&path, campaign_id, phase)?;
        resumed.validate_resume(ledger)?;
        let mut reconstructed = ledger.clone();
        for case in cases
            .iter()
            .filter(|case| resumed.completed_cases.contains(&case.id))
        {
            let proof_path = proofs.join(format!("{}.json", case.id));
            let proof: AcceptedAttempt =
                serde_json::from_reader(fs::File::open(&proof_path).map_err(|error| {
                    format!(
                        "read accepted attempt proof {}: {error}",
                        proof_path.display()
                    )
                })?)
                .map_err(|error| format!("decode accepted attempt proof: {error}"))?;
            if reconstructed.identity() != Some(&proof.identity) {
                return Err("accepted attempt belongs to another deployment".to_owned());
            }
            reconstructed.observe_attempt(
                case,
                &proof.executed_case,
                &proof.observation,
                proof.attempt,
                true,
                harness.execution_budget(),
            )?;
        }
        if serde_json::to_value(&reconstructed).map_err(|error| error.to_string())?
            != serde_json::to_value(&resumed).map_err(|error| error.to_string())?
        {
            return Err(
                "persisted coverage is not the closure of its accepted attempt evidence".to_owned(),
            );
        }
        *ledger = reconstructed;
    } else {
        write_coverage_ledger(&path, campaign_id, phase, ledger)?;
    }
    for case in cases {
        if ledger.completed_cases.contains(&case.id) {
            continue;
        }
        harness
            .execution_budget()
            .check(&format!("admit {phase} case {}", case.id))?;
        let (executed_case, observation) =
            run_campaign_case(harness, case, campaign_root, corpus_root, shrink, diagnosis)?;
        let attempt = harness
            .last_attempt_id()
            .ok_or("coverage missing immutable attempt identity")?;
        ledger.observe_attempt(
            case,
            &executed_case,
            &observation,
            attempt,
            true,
            harness.execution_budget(),
        )?;
        durable_json(
            &proofs.join(format!("{}.json", case.id)),
            &AcceptedAttempt {
                identity: ledger
                    .identity()
                    .cloned()
                    .ok_or("coverage deployment identity missing")?,
                attempt,
                executed_case,
                observation,
            },
        )?;
        write_coverage_ledger(&path, campaign_id, phase, ledger)?;
    }
    ledger.assert_observed_closed()
}

fn run_campaign_case(
    harness: &mut ClusterHarness,
    case: &BehaviorCase,
    campaign_root: &Path,
    corpus_root: &Path,
    shrink: bool,
    diagnostic_parent: &Budget,
) -> Result<(BehaviorCase, myelin_e2e_fuzz::CaseObservation), String> {
    let _attempt_timer = PhaseTimer::new(format!("attempt-{}", case.id));
    harness
        .execution_budget()
        .check(&format!("admit workload {}", case.id))?;
    let result = harness.run_case_with_identity(case);
    // A successful attempt already persisted fresh post-cleanup health proof.
    // Failed admission paths still need the explicit check before diagnosis.
    if result.is_ok() {
        return result;
    }
    let signature = harness.last_failure_signature().cloned();
    let health = harness.assert_healthy();
    let baseline_restored = health.is_ok();
    let result = merge_execution_cleanup(result, health);
    let Err(error) = result else { return result };
    let original = format!("case {} failed: {error}", case.id);
    if let Err(persist_error) = persist_regression(
        &campaign_root.join("failures"),
        case,
        &error,
        signature.as_ref(),
        false,
    ) {
        return Err(format!(
            "{original}; persist original regression failed: {persist_error}"
        ));
    }
    if !shrink || !baseline_restored || harness.is_quarantined() {
        return Err(original);
    }
    let Some(signature) = signature else {
        return Err(format!(
            "{original}; no typed behavioral signature; infrastructure failure is not shrinkable"
        ));
    };
    let _diagnosis_timer = PhaseTimer::new(format!("diagnosis-{}", case.id));
    let workload = harness.execution_budget().clone();
    let diagnostic = diagnostic_parent.child(Duration::from_secs(DIAGNOSTIC_DEADLINE_SECS));
    harness.set_execution_budget(diagnostic.clone());
    let diagnosis = (|| {
        let mut replay = |candidate: &BehaviorCase| -> Result<
            Option<(String, myelin_e2e_fuzz::FailureSignature)>,
            String,
        > {
            diagnostic.check(&format!("diagnostic replay {}", candidate.id))?;
            if harness.is_quarantined() {
                return Err("fixture quarantined; diagnosis prohibited".to_owned());
            }
            let attempt = harness.run_case(candidate);
            let observed_signature = harness.last_failure_signature().cloned();
            let restored = harness.assert_healthy();
            if let Err(restoration) = restored {
                return Err(format!(
                    "diagnostic fixture restoration failed: {restoration}"
                ));
            }
            if harness.is_quarantined() {
                return Err("diagnostic attempt quarantined fixture".to_owned());
            }
            diagnostic.check("diagnostic candidate complete and baseline restored")?;
            attempt
                .err()
                .map(|error| {
                    observed_signature
                        .map(|signature| (error, signature))
                        .ok_or_else(|| {
                            "replay failed without a typed behavioral signature".to_owned()
                        })
                })
                .transpose()
        };
        let exact = replay(case)?.ok_or_else(|| "exact replay did not reproduce".to_owned())?;
        if exact.1 != signature {
            return Err("exact replay changed typed failure signature".to_owned());
        }
        let minimized =
            myelin_e2e_fuzz::try_shrink_failure_budgeted(case.clone(), &diagnostic, |candidate| {
                Ok(replay(candidate)?.is_some_and(|(_, observed)| observed == signature))
            })?;
        let minimized_error = replay(&minimized)?
            .ok_or_else(|| "final minimized replay did not reproduce".to_owned())?;
        if minimized_error.1 != signature {
            return Err("minimized replay changed typed failure signature".to_owned());
        }
        // Never overwrite the original witness with a candidate.
        persist_regression(
            &campaign_root.join("minimized"),
            &minimized,
            &minimized_error.0,
            Some(&signature),
            true,
        )?;
        // Only a cleaned, exactly replayed, same-signature witness enters the reusable corpus.
        persist_regression(
            corpus_root,
            &minimized,
            &minimized_error.0,
            Some(&signature),
            true,
        )
    })();
    harness.set_execution_budget(workload);
    let status = match diagnosis {
        Ok(()) => "exact replay and verified minimized witness persisted".to_owned(),
        Err(reason) => format!("diagnosis incomplete: {reason}; original witness retained"),
    };
    let persistence = durable_json(
        &campaign_root
            .join("diagnosis")
            .join(format!("{}.json", case.id)),
        &serde_json::json!({"schema_version": 2, "original_failure": error, "signature": signature, "status": status}),
    );
    Err(format!(
        "{original}; {status}{}",
        persistence
            .err()
            .map(|error| format!("; diagnostic evidence failed: {error}"))
            .unwrap_or_default()
    ))
}

struct PreparedRecoveryAttempt {
    recovery: RecoveryCase,
    before: FixturePathSnapshot,
}

fn run_recovery_campaign(
    harness: &mut ClusterHarness,
    recoveries: &[&RecoveryCase],
) -> Result<(), String> {
    let _campaign_timer = PhaseTimer::new("recovery-campaign");
    let mut prepared = Vec::<PreparedRecoveryAttempt>::with_capacity(recoveries.len());
    let result = (|| {
        for recovery in recoveries {
            let _pre_timer = PhaseTimer::new(format!("recovery-pre-{}", recovery.id));
            let recovery = harness.prepare_recovery_attempt(recovery)?;
            harness.run_case_retained_deferred(&recovery.pre_restart)?;
            let before = harness
                .retained_fixture_snapshot(&recovery.pre_restart, &recovery.persisted_entries)?;
            prepared.push(PreparedRecoveryAttempt { recovery, before });
        }
        harness.seal_retained_attempts()?;

        // Every pre-restart segment is durable before the single formal
        // orchestrator replacement. All paths are attempt-scoped, and the
        // retained-attempt census admits only their modeled persistent actors.
        let _restart_timer = PhaseTimer::new("restart-rejoin-recovery-campaign");
        harness.verify_running_recovery()?;
        drop(_restart_timer);

        for attempt in &prepared {
            let recovery = &attempt.recovery;
            let _post_timer = PhaseTimer::new(format!("recovery-post-{}", recovery.id));
            harness.run_case_retained_deferred(&recovery.post_restart)?;
            let after = harness
                .retained_fixture_snapshot(&recovery.post_restart, &recovery.persisted_entries)?;
            if after != attempt.before {
                return Err(format!(
                    "recovery {} changed persisted namespace facts: before={:?}, after={after:?}",
                    recovery.id, attempt.before
                ));
            }
        }
        harness.seal_retained_attempts()?;
        let cases = prepared
            .iter()
            .flat_map(|attempt| {
                [
                    &attempt.recovery.pre_restart,
                    &attempt.recovery.post_restart,
                ]
            })
            .collect::<Vec<_>>();
        harness.cleanup_recovery_cases(&cases)?;
        Ok(())
    })();

    if result.is_ok() {
        return Ok(());
    }
    let mut cleanup = Ok(());
    for attempt in &prepared {
        let recovery = &attempt.recovery;
        harness.abort_case_processes(&recovery.pre_restart);
        harness.abort_case_processes(&recovery.post_restart);
        cleanup = merge_execution_cleanup(cleanup, harness.cleanup_case(&recovery.pre_restart));
        cleanup = merge_execution_cleanup(cleanup, harness.cleanup_case(&recovery.post_restart));
    }
    cleanup = merge_execution_cleanup(cleanup, harness.assert_healthy());
    merge_execution_cleanup(result, cleanup)
}

fn load_regressions(root: &Path) -> Result<Vec<BehaviorCase>, String> {
    let directory = root.join("regressions");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("read regression directory: {error}")),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("read regression directory entry: {error}"))?;
        if !entry
            .file_type()
            .map_err(|error| format!("inspect regression entry: {error}"))?
            .is_dir()
        {
            return Err(format!(
                "unexpected regression entry {}",
                entry.path().display()
            ));
        }
        paths.push(entry.path());
    }
    paths.sort();
    paths
        .into_iter()
        .map(|path| read_regression_witness(&path, true).map(|(case, _)| case))
        .collect()
}

fn merge_execution_cleanup<T>(
    execution: Result<T, String>,
    cleanup: Result<(), String>,
) -> Result<T, String> {
    match (execution, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup_error)) => Err(format!("cleanup failed: {cleanup_error}")),
        (Err(error), Err(cleanup_error)) => {
            Err(format!("{error}; cleanup also failed: {cleanup_error}"))
        }
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    fn ordered_attestation() -> OrderedGateAttestation {
        let mut evidence = OrderedGateAttestation {
            schema_version: 5,
            state: "passed".to_owned(),
            completed_unix_ms: 100,
            expires_unix_ms: 200,
            build_artifacts: BTreeMap::new(),
            deployment_artifacts: myelin_e2e_fuzz::PreparedArtifactIdentity {
                schema_version: 2,
                source_build_input_digest: "a".repeat(64),
                executables: BTreeMap::new(),
                wheel_input_digest: String::new(),
                wheels: BTreeMap::new(),
                payload: BTreeMap::new(),
            },
            source_digest: String::new(),
            image_identities: [
                ("myelin-node-base:cuda12.6", "base"),
                ("myelin-node:latest", "node"),
                ("myelin-e2e:tested", "e2e"),
            ]
            .into_iter()
            .enumerate()
            .map(|(index, (name, role))| {
                (
                    name.to_owned(),
                    GateImageIdentity {
                        id: format!("sha256:{}", index.to_string().repeat(64)),
                        os: "linux".to_owned(),
                        architecture: "amd64".to_owned(),
                        variant: String::new(),
                        repo_digests: BTreeSet::new(),
                        provenance_version: 1,
                        source_build_input_digest: "a".repeat(64),
                        image_role: role.to_owned(),
                        parent_image_id: index
                            .checked_sub(1)
                            .map(|parent| format!("sha256:{}", parent.to_string().repeat(64))),
                    },
                )
            })
            .collect(),
            configuration: GateConfiguration {
                warm_runs: 3,
                image: "myelin-e2e:tested".to_owned(),
                seed: 17,
                case_deadline_secs: 120,
                build_images: false,
                workspace: PathBuf::from("/qualified/workspace"),
                artifacts: PathBuf::from("/qualified/evidence"),
            },
            stages: Vec::new(),
            runs: (1..=3)
                .map(|index| GateRun {
                    index,
                    state: "passed".to_owned(),
                    gate_a: "passed".to_owned(),
                    inside_target: true,
                    campaign_elapsed_secs: 0.001,
                    elapsed_secs: 0.01,
                })
                .collect(),
            build_elapsed_secs: 0.02,
            elapsed_secs: 0.1,
        };
        evidence.stages = expected_gate_stages(&evidence)
            .into_iter()
            .enumerate()
            .map(|(index, (name, command))| GateStage {
                name,
                command,
                state: "passed".to_owned(),
                exit_code: Some(0),
                started_unix_ms: index as u64 * 2 + 1,
                completed_unix_ms: index as u64 * 2 + 2,
                elapsed_secs: 0.001,
            })
            .collect();
        evidence
    }

    #[test]
    fn ordered_gates_reject_partial_stale_failed_and_reordered_evidence() {
        assert!(validate_gate_attestation(&ordered_attestation(), 150).is_ok());
        assert!(validate_gate_attestation(&ordered_attestation(), 200).is_err());
        assert!(validate_gate_attestation(&ordered_attestation(), 99).is_err());
        let mut partial = ordered_attestation();
        partial.stages.remove(1);
        assert!(validate_gate_attestation(&partial, 150).is_err());
        let mut failed = ordered_attestation();
        failed.stages[1].exit_code = Some(1);
        assert!(validate_gate_attestation(&failed, 150).is_err());
        let mut reordered = ordered_attestation();
        reordered.stages[0].name = "warm-1-campaign".to_owned();
        reordered.stages[1].name = "warm-1-gate-a".to_owned();
        assert!(validate_gate_attestation(&reordered, 150).is_err());
        let mut isolated = ordered_attestation();
        isolated.runs.truncate(1);
        assert!(validate_gate_attestation(&isolated, 150).is_err());
        let mut missing_base = ordered_attestation();
        missing_base
            .image_identities
            .remove("myelin-node-base:cuda12.6");
        assert!(validate_gate_attestation(&missing_base, 150).is_err());
    }

    #[test]
    fn ordered_gates_require_all_phases_and_exact_case_selection() {
        for name in [
            "build",
            "build-test-binaries",
            "build-deployment-artifacts",
            "verify-qualified-deployment-artifacts",
        ] {
            let mut evidence = ordered_attestation();
            evidence.stages.retain(|stage| stage.name != name);
            assert!(validate_gate_attestation(&evidence, 150).is_err(), "{name}");
        }
        let mut reduced = ordered_attestation();
        let gate_a = reduced
            .stages
            .iter_mut()
            .find(|stage| stage.name == "warm-1-gate-a")
            .unwrap();
        let nodes = gate_a
            .command
            .iter()
            .position(|argument| argument == "--deployment-nodes")
            .unwrap();
        gate_a.command[nodes + 1] = "2".to_owned();
        assert!(validate_gate_attestation(&reduced, 150).is_err());
        let mut incomplete_build = ordered_attestation();
        incomplete_build.configuration.build_images = true;
        assert!(validate_gate_attestation(&incomplete_build, 150).is_err());
        incomplete_build.stages = expected_gate_stages(&incomplete_build)
            .into_iter()
            .enumerate()
            .map(|(index, (name, command))| GateStage {
                name,
                command,
                state: "passed".to_owned(),
                exit_code: Some(0),
                started_unix_ms: index as u64 * 2 + 1,
                completed_unix_ms: index as u64 * 2 + 2,
                elapsed_secs: 0.001,
            })
            .collect();
        assert!(validate_gate_attestation(&incomplete_build, 150).is_ok());
        incomplete_build
            .stages
            .retain(|stage| stage.name != "build-image-node-parent");
        assert!(validate_gate_attestation(&incomplete_build, 150).is_err());
    }

    #[test]
    fn ordered_gates_enforce_measured_ceilings_and_phase_accounting() {
        let mut boundary = ordered_attestation();
        boundary.runs[0].campaign_elapsed_secs = 300.0;
        boundary.stages[4].elapsed_secs = 300.0;
        boundary.runs[0].elapsed_secs = 600.0;
        boundary.elapsed_secs = 1000.0;
        assert!(validate_gate_attestation(&boundary, 150).is_ok());
        boundary.runs[0].elapsed_secs = 600.001;
        assert!(validate_gate_attestation(&boundary, 150).is_err());
        boundary.runs[0].elapsed_secs = 600.0;
        boundary.runs[0].campaign_elapsed_secs = 300.001;
        boundary.stages[4].elapsed_secs = 300.001;
        assert!(validate_gate_attestation(&boundary, 150).is_err());
        for seconds in [-1.0, f64::NAN, f64::INFINITY] {
            let mut invalid = ordered_attestation();
            invalid.runs[0].elapsed_secs = seconds;
            assert!(validate_gate_attestation(&invalid, 150).is_err());
            let mut invalid = ordered_attestation();
            invalid.stages[0].elapsed_secs = seconds;
            assert!(validate_gate_attestation(&invalid, 150).is_err());
        }
        let mut omitted = ordered_attestation();
        omitted.runs[0].elapsed_secs = 0.001;
        assert!(validate_gate_attestation(&omitted, 150).is_err());
        let mut understated = ordered_attestation();
        understated.stages[4].elapsed_secs = 301.0;
        understated.runs[0].elapsed_secs = 500.0;
        understated.elapsed_secs = 1000.0;
        assert!(validate_gate_attestation(&understated, 150).is_err());
        let mut outside = ordered_attestation();
        outside.runs[0].inside_target = false;
        assert!(validate_gate_attestation(&outside, 150).is_err());
    }

    #[test]
    fn ordered_gates_reject_legacy_missing_timing_and_stale_image_provenance() {
        let mut legacy = ordered_attestation();
        legacy.schema_version = 4;
        assert!(validate_gate_attestation(&legacy, 150).is_err());
        for field in ["inside_target", "campaign_elapsed_secs", "elapsed_secs"] {
            let mut missing = serde_json::to_value(ordered_attestation()).unwrap();
            missing["runs"][0].as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<OrderedGateAttestation>(missing).is_err(),
                "{field}"
            );
        }
        let mut stale = ordered_attestation();
        stale
            .image_identities
            .get_mut("myelin-e2e:tested")
            .unwrap()
            .source_build_input_digest = "b".repeat(64);
        assert!(validate_gate_attestation(&stale, 150).is_err());
        let mut unrelated = ordered_attestation();
        unrelated
            .image_identities
            .get_mut("myelin-e2e:tested")
            .unwrap()
            .parent_image_id = Some(format!("sha256:{}", "f".repeat(64)));
        assert!(validate_gate_attestation(&unrelated, 150).is_err());
        let mut unproven = serde_json::to_value(ordered_attestation()).unwrap();
        unproven["image_identities"]["myelin-e2e:tested"]
            .as_object_mut()
            .unwrap()
            .remove("provenance_version");
        assert!(serde_json::from_value::<OrderedGateAttestation>(unproven).is_err());
    }

    #[test]
    fn repeat_paid_claim_preserves_all_durable_ownership_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let run = temporary.path().join("campaign");
        claim_campaign_directory(&run, true).unwrap();
        let ownership = br#"{"labels":["myelin-1-1-attempt-0"],"contracts":[42],"unresolved":[2]}"#;
        durable_write(&run.join("paid-state.json"), ownership).unwrap();
        durable_write(&run.join("campaign-plan.json"), b"immutable-plan").unwrap();
        assert!(claim_campaign_directory(&run, true).is_err());
        assert_eq!(fs::read(run.join("paid-state.json")).unwrap(), ownership);
        assert_eq!(
            fs::read(run.join("campaign-plan.json")).unwrap(),
            b"immutable-plan"
        );
    }

    #[test]
    fn promoted_regression_is_loaded_from_shared_corpus_and_corruption_fails() {
        let temporary = tempfile::tempdir().unwrap();
        let case = stable_corpus(2, 17).remove(0);
        let partial = tempfile::tempdir().unwrap();
        durable_json(
            &partial
                .path()
                .join("regressions")
                .join(&case.id)
                .join("case.json"),
            &case,
        )
        .unwrap();
        assert!(load_regressions(partial.path()).is_err());
        let signature = myelin_e2e_fuzz::FailureSignature {
            invariant: "payload".to_owned(),
            failure_class: myelin_e2e_fuzz::FailureClass::PayloadMismatch,
            causal_role: myelin_e2e_fuzz::CausalRole::Action(
                myelin_e2e_fuzz::SemanticAction::ReadBlob,
            ),
            observed_outcome: None,
        };
        persist_regression(
            temporary.path(),
            &case,
            "typed failure evidence",
            Some(&signature),
            true,
        )
        .unwrap();
        let loaded = load_regressions(temporary.path()).unwrap();
        assert_eq!(
            serde_json::to_value(&loaded).unwrap(),
            serde_json::json!([case])
        );
        let witness = temporary.path().join("regressions").join(&case.id);
        durable_write(&witness.join("signature.json"), b"null").unwrap();
        assert!(load_regressions(temporary.path()).is_err());
    }
}
