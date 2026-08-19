#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use serde_json::{Value, json};
use swactor_process::{
    child_kill, child_try_wait, child_wait, command_spawn, find_process_identities_by_environment,
    request_child_termination, terminate_process_group,
};
use swactor_vastai::test_http::{TestHttpRoute, TestHttpServer};

const CASE_DEADLINE: Duration = Duration::from_secs(10);
const SUITE_DEADLINE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(25);
const CENSUS_STABILITY: Duration = Duration::from_millis(25);
const ACTORS_PER_NODE_LIMIT: u64 = 32;
const OFFER_COUNT: usize = 8;

static SUITE_STARTED: OnceLock<Instant> = OnceLock::new();
std::thread_local! {
    static OBSERVED_HTTP_FAILURES: std::cell::RefCell<Vec<String>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn http_failure_checkpoint() -> usize {
    OBSERVED_HTTP_FAILURES.with(|failures| failures.borrow().len())
}

fn discard_http_failures_since(checkpoint: usize) {
    OBSERVED_HTTP_FAILURES.with(|failures| failures.borrow_mut().truncate(checkpoint));
}

fn record_http_failure(failure: String) {
    OBSERVED_HTTP_FAILURES.with(|failures| failures.borrow_mut().push(failure));
}

fn observed_http_failures() -> Vec<String> {
    OBSERVED_HTTP_FAILURES.with(|failures| failures.borrow().clone())
}

#[derive(Clone, Copy, Debug)]
enum RestartMode {
    Graceful,
    FlushSafeAbrupt,
}

#[derive(Clone, Debug)]
enum ExternalAction {
    Search {
        count: usize,
    },
    Provision {
        command_slot: u8,
        use_searched_offers: bool,
    },
    Query,
    Kill {
        node_slot: u8,
        command_slot: u8,
    },
    Flush,
    Restart {
        mode: RestartMode,
    },
    EndpointProbe {
        node_slot: u8,
    },
    ConcurrentQueries,
}

#[derive(Clone, Debug)]
struct E2eCase {
    seed: u64,
    node_seed: u8,
    kill_mask: u8,
    offer_offset: usize,
    actions: Vec<ExternalAction>,
}

fn e2e_case() -> impl Strategy<Value = E2eCase> {
    (
        any::<u64>(),
        any::<u8>(),
        any::<u8>(),
        0_usize..OFFER_COUNT,
        any::<bool>(),
        any::<bool>(),
        any::<u8>(),
        any::<u8>(),
        proptest::collection::vec(any::<u8>(), 11),
    )
        .prop_map(
            |(
                seed,
                node_seed,
                kill_mask,
                offer_offset,
                invalid_search,
                use_searched_offers,
                node_slot,
                command_slot,
                ordering,
            )| {
                let mut keyed_actions = vec![
                    ExternalAction::Search {
                        count: if invalid_search { 0 } else { 2 },
                    },
                    ExternalAction::Provision {
                        command_slot,
                        use_searched_offers,
                    },
                    ExternalAction::Query,
                    ExternalAction::Kill {
                        node_slot,
                        command_slot,
                    },
                    ExternalAction::Flush,
                    ExternalAction::Restart {
                        mode: RestartMode::Graceful,
                    },
                    ExternalAction::EndpointProbe { node_slot },
                    ExternalAction::ConcurrentQueries,
                    ExternalAction::Query,
                    ExternalAction::Kill {
                        node_slot: node_slot.wrapping_add(1),
                        command_slot,
                    },
                    ExternalAction::Restart {
                        mode: RestartMode::FlushSafeAbrupt,
                    },
                ]
                .into_iter()
                .enumerate()
                .map(|(index, action)| ((ordering[index], index), action))
                .collect::<Vec<_>>();
                keyed_actions.sort_by_key(|(key, _)| *key);
                E2eCase {
                    seed,
                    node_seed,
                    kill_mask,
                    offer_offset,
                    actions: keyed_actions
                        .into_iter()
                        .map(|(_, action)| action)
                        .collect(),
                }
            },
        )
}

fn e2e_proptest_config() -> ProptestConfig {
    let has_case_override = std::env::var_os("PROPTEST_CASES").is_some();
    let mut config = ProptestConfig::default();
    if !has_case_override {
        config.cases = 4;
    }
    config.failure_persistence = Some(Box::new(FileFailurePersistence::Direct(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/proptest-regressions/tests/e2e_vastai.txt"
    ))));
    config.max_shrink_iters = 0;
    config
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ActorCensus {
    actors: u64,
    orchestrator_actors: u64,
    worker_actors: u64,
    poisoned: u64,
    type_mismatches: u64,
    mailbox_depth: u64,
    orchestrator_by_type: BTreeMap<String, u64>,
    workers_by_stream: BTreeMap<String, BTreeMap<String, u64>>,
}

#[derive(Clone, Debug)]
struct OracleObservation {
    expected_survivors: BTreeSet<u64>,
    expected_stopped: BTreeSet<u64>,
    running_after_restart: BTreeSet<u64>,
    stopped_after_restart: BTreeSet<u64>,
    reachable_nodes: BTreeSet<u64>,
    provider_resources: Vec<u64>,
    provider_created: BTreeSet<u64>,
    provider_destroyed: BTreeSet<u64>,
    selected_offers: BTreeMap<u64, u64>,
    searched_offers: BTreeSet<u64>,
    actor_before_replays: ActorCensus,
    actor_after_replays: ActorCensus,
    actor_initial: ActorCensus,
    actor_after_teardown: ActorCensus,
    terminal_record_counts: BTreeMap<String, usize>,
    unexpected_process_exits: Vec<String>,
    http_failures: Vec<String>,
    teardown_resources: Vec<u64>,
    labeled_temp_resources: Vec<PathBuf>,
}

fn validate_oracle(observation: &OracleObservation) -> Result<(), String> {
    if observation
        .terminal_record_counts
        .values()
        .any(|count| *count != 1)
    {
        return Err(format!(
            "accepted reply did not have exactly one terminal record: {:?}",
            observation.terminal_record_counts
        ));
    }
    if !observation.unexpected_process_exits.is_empty() {
        return Err(format!(
            "unexpected process exit: {:?}",
            observation.unexpected_process_exits
        ));
    }
    if !observation.http_failures.is_empty() {
        return Err(format!(
            "HTTP failure or disconnect: {:?}",
            observation.http_failures
        ));
    }
    if observation.running_after_restart != observation.expected_survivors {
        return Err(format!(
            "restart-state loss: expected running {:?}, observed {:?}",
            observation.expected_survivors, observation.running_after_restart
        ));
    }
    if observation.stopped_after_restart != observation.expected_stopped {
        return Err(format!(
            "destroy set mismatch: expected stopped {:?}, observed {:?}",
            observation.expected_stopped, observation.stopped_after_restart
        ));
    }
    if observation.reachable_nodes != observation.expected_survivors {
        return Err(format!(
            "survivor reachability mismatch: expected {:?}, observed {:?}",
            observation.expected_survivors, observation.reachable_nodes
        ));
    }
    let provider_set = observation
        .provider_resources
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if provider_set.len() != observation.provider_resources.len() {
        return Err(format!(
            "duplicate provider resources: {:?}",
            observation.provider_resources
        ));
    }
    if provider_set != observation.expected_survivors {
        return Err(format!(
            "provider ledger mismatch: expected {:?}, observed {:?}",
            observation.expected_survivors, provider_set
        ));
    }
    let expected_all = observation
        .expected_survivors
        .union(&observation.expected_stopped)
        .copied()
        .collect::<BTreeSet<_>>();
    if observation.provider_created != expected_all {
        return Err(format!(
            "provider create ledger mismatch: expected {:?}, observed {:?}",
            expected_all, observation.provider_created
        ));
    }
    if observation.provider_destroyed != observation.expected_stopped {
        return Err(format!(
            "provider destroy ledger mismatch: expected {:?}, observed {:?}",
            observation.expected_stopped, observation.provider_destroyed
        ));
    }
    if observation.selected_offers.len()
        != observation
            .expected_survivors
            .len()
            .saturating_add(observation.expected_stopped.len())
        || observation
            .selected_offers
            .values()
            .any(|offer| !observation.searched_offers.contains(offer))
    {
        return Err(format!(
            "selected offer did not come from search response: selected={:?}, searched={:?}",
            observation.selected_offers, observation.searched_offers
        ));
    }
    if observation.actor_initial.poisoned != 0
        || observation.actor_before_replays.poisoned != 0
        || observation.actor_after_replays.poisoned != 0
        || observation.actor_after_teardown.poisoned != 0
    {
        return Err(format!(
            "actor poisoning observed: initial={:?}, before={:?}, after={:?}, teardown={:?}",
            observation.actor_initial,
            observation.actor_before_replays,
            observation.actor_after_replays,
            observation.actor_after_teardown,
        ));
    }
    if observation.actor_initial.type_mismatches != 0
        || observation.actor_before_replays.type_mismatches != 0
        || observation.actor_after_replays.type_mismatches != 0
        || observation.actor_after_teardown.type_mismatches != 0
    {
        return Err(format!(
            "actor message type mismatch observed: initial={:?}, before={:?}, after={:?}, teardown={:?}",
            observation.actor_initial,
            observation.actor_before_replays,
            observation.actor_after_replays,
            observation.actor_after_teardown,
        ));
    }
    for (phase, census) in [
        ("before replay", &observation.actor_before_replays),
        ("after replay", &observation.actor_after_replays),
    ] {
        let typed_worker_count = census
            .workers_by_stream
            .values()
            .flat_map(|types| types.iter())
            .map(|(actor_type, count)| {
                if actor_type == "<unknown>" {
                    Err(format!(
                        "{phase} worker census contains an unknown actor type"
                    ))
                } else {
                    Ok(*count)
                }
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();
        if census.workers_by_stream.len() != observation.expected_survivors.len()
            || typed_worker_count != census.worker_actors
        {
            return Err(format!(
                "{phase} worker census is incomplete: expected_nodes={}, census={census:?}",
                observation.expected_survivors.len()
            ));
        }
    }
    let mut before_types = observation
        .actor_before_replays
        .orchestrator_by_type
        .clone();
    let mut after_types = observation.actor_after_replays.orchestrator_by_type.clone();
    before_types.remove("myelin::orchestration::control::ControlReplyObserver");
    after_types.remove("myelin::orchestration::control::ControlReplyObserver");
    if observation.actor_before_replays.actors != observation.actor_after_replays.actors
        || observation.actor_before_replays.orchestrator_actors
            != observation.actor_after_replays.orchestrator_actors
        || observation.actor_before_replays.worker_actors
            != observation.actor_after_replays.worker_actors
        || observation.actor_before_replays.mailbox_depth
            != observation.actor_after_replays.mailbox_depth
        || observation.actor_before_replays.workers_by_stream
            != observation.actor_after_replays.workers_by_stream
        || before_types != after_types
    {
        return Err(format!(
            "steady-state actor census changed under replay: before={:?}, after={:?}",
            observation.actor_before_replays, observation.actor_after_replays
        ));
    }
    let live_node_count = observation.expected_survivors.len() as u64;
    if observation.actor_before_replays.worker_actors
        > live_node_count.saturating_mul(ACTORS_PER_NODE_LIMIT)
    {
        return Err(format!(
            "per-node actor bound exceeded: live_nodes={live_node_count}, census={:?}",
            observation.actor_before_replays
        ));
    }
    if observation.actor_after_teardown.orchestrator_actors
        != observation.actor_initial.orchestrator_actors
        || observation.actor_after_teardown.mailbox_depth != 0
    {
        return Err(format!(
            "actor census did not return to baseline: initial={:?}, teardown={:?}",
            observation.actor_initial, observation.actor_after_teardown
        ));
    }
    if !observation.teardown_resources.is_empty() || !observation.labeled_temp_resources.is_empty()
    {
        return Err(format!(
            "teardown leak: provider={:?}, labeled_temp={:?}",
            observation.teardown_resources, observation.labeled_temp_resources
        ));
    }
    Ok(())
}

struct ScenarioHarness {
    state_dir: PathBuf,
    provider_url: String,
    dashboard_port: u16,
    run_id: u64,
    provider_ledger_path: PathBuf,
    restart: usize,
    child: Option<Child>,
    log_paths: Vec<PathBuf>,
    unexpected_exits: Vec<String>,
}

impl ScenarioHarness {
    fn new(state_dir: PathBuf, provider_url: String, dashboard_port: u16, run_id: u64) -> Self {
        let provider_ledger_path = state_dir.join("mock-vastai-lifecycle.jsonl");
        Self {
            state_dir,
            provider_url,
            dashboard_port,
            run_id,
            provider_ledger_path,
            restart: 0,
            child: None,
            log_paths: Vec::new(),
            unexpected_exits: Vec::new(),
        }
    }

    fn start(&mut self) -> Result<(), String> {
        if self.child.is_some() {
            return Err("orchestrator is already running".to_owned());
        }
        let log_path = self
            .state_dir
            .join(format!("orchestrator-{}.log", self.restart));
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| format!("open {}: {error}", log_path.display()))?;
        let stderr = stdout
            .try_clone()
            .map_err(|error| format!("clone {}: {error}", log_path.display()))?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_myelin-orchestrator"));
        command
            .current_dir(&self.state_dir)
            .args([
                "--provider",
                "vastai",
                "--vastai-provisioning",
                "mock",
                "--dashboard",
                "--state-dir",
            ])
            .arg(&self.state_dir)
            .args(["--run-id", &self.run_id.to_string()])
            .env("VASTAI_BASE_URL", &self.provider_url)
            .env("VAST_API_KEY", "stateful-e2e-secret")
            .env("MYELIN_DASHBOARD_PORT", self.dashboard_port.to_string())
            .env("MYELIN_MOCK_VASTAI_LEDGER_PATH", &self.provider_ledger_path)
            .env("MYELIN_VASTAI_POLL_INTERVAL_SECS", "1")
            .env("MYELIN_IROH_RELAY_MODE", "disabled")
            .env("RUST_BACKTRACE", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        let child = command_spawn(&mut command)
            .map_err(|error| format!("spawn Cargo-built Myelin orchestrator: {error}"))?;
        self.child = Some(child);
        self.log_paths.push(log_path);
        self.restart += 1;
        Ok(())
    }
    fn ensure_running(&mut self) -> Result<(), String> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| "orchestrator process is absent".to_owned())?;
        if let Some(status) = child_try_wait(child)
            .map_err(|error| format!("inspect live orchestrator process: {error}"))?
        {
            let failure = format!("orchestrator exited unexpectedly with {status}");
            self.unexpected_exits.push(failure.clone());
            return Err(failure);
        }
        Ok(())
    }

    fn restart_gracefully(&mut self, deadline: Instant) -> Result<(), String> {
        self.stop_child_gracefully(deadline)?;
        self.start()
    }

    fn restart_abruptly(&mut self) -> Result<(), String> {
        self.stop_child_abruptly()?;
        self.start()
    }

    fn stop_child_gracefully(&mut self, deadline: Instant) -> Result<(), String> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if child_try_wait(&mut child)
            .map_err(|error| format!("inspect orchestrator process: {error}"))?
            .is_none()
        {
            request_child_termination(&child)
                .map_err(|error| format!("request graceful orchestrator stop: {error}"))?;
        }
        loop {
            if let Some(status) = child_try_wait(&mut child)
                .map_err(|error| format!("inspect graceful orchestrator stop: {error}"))?
            {
                if status.success() {
                    return Ok(());
                }
                return Err(format!(
                    "graceful orchestrator stop exited with {status}; logs={}",
                    self.logs()
                ));
            }
            if Instant::now() >= deadline {
                let _ = child_kill(&mut child);
                let _ = child_wait(&mut child);
                return Err(format!(
                    "graceful orchestrator stop exceeded case deadline; logs={}",
                    self.logs()
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    fn stop_child_abruptly(&mut self) -> Result<(), String> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if child_try_wait(&mut child)
            .map_err(|error| format!("inspect orchestrator process: {error}"))?
            .is_none()
        {
            child_kill(&mut child)
                .map_err(|error| format!("kill orchestrator process: {error}"))?;
        }
        child_wait(&mut child)
            .map_err(|error| format!("wait for orchestrator process: {error}"))?;
        Ok(())
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.dashboard_port)
    }

    fn logs(&self) -> String {
        let mut paths = self.log_paths.clone();
        if let Ok(entries) = std::fs::read_dir(self.state_dir.join("process-output")) {
            paths.extend(entries.filter_map(|entry| entry.ok().map(|entry| entry.path())));
        }
        paths.sort();
        paths
            .iter()
            .map(|path| {
                std::fs::read_to_string(path)
                    .map(|text| format!("\n--- {} ---\n{text}", path.display()))
                    .unwrap_or_else(|error| {
                        format!("\n--- {} unavailable: {error} ---", path.display())
                    })
            })
            .collect()
    }

    fn cleanup_workers(&self) {
        let environment = [("MYELIN_RUN_ID".to_owned(), self.run_id.to_string())];
        let Ok(processes) = find_process_identities_by_environment(&environment, true) else {
            return;
        };
        for process in processes {
            let _ = terminate_process_group(
                &process,
                Duration::from_secs(2),
                Duration::from_millis(20),
            );
        }
    }
    fn remove_worker_sockets(&self, nodes: &BTreeSet<u64>) -> Result<(), String> {
        for node_id in nodes {
            let path = node_socket(self.run_id, *node_id);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "remove labeled worker socket {}: {error}",
                        path.display()
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Drop for ScenarioHarness {
    fn drop(&mut self) {
        let _ = self.stop_child_abruptly();
        self.cleanup_workers();
        let possible_nodes = BTreeSet::from([1, 2]);
        let _ = self.remove_worker_sockets(&possible_nodes);
    }
}

fn production_offers() -> Value {
    let offers = (0..OFFER_COUNT)
        .map(|index| {
            json!({
                "id": 8_675_300_u64 + index as u64,
                "gpu_name": "RTX 4090",
                "num_gpus": 1,
                "gpu_ram": 24_576.0,
                "dph_total": 0.20 + index as f64 / 100.0,
                "host_id": 90_000_u64 + index as u64,
                "compute_cap": 890,
                "verification": "verified",
                "reliability2": 0.995,
                "inet_down": 1_000.0,
                "inet_up": 800.0,
                "internet_down_cost_per_tb": 1.5,
                "internet_up_cost_per_tb": 2.5,
                "geolocation": "US",
                "disk_bw": 2_000.0,
                "duration": 86_400.0,
                "rentable": true
            })
        })
        .collect::<Vec<_>>();
    json!({"offers": offers})
}

fn reserve_port() -> Result<u16, String> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("reserve dashboard port: {error}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| format!("read reserved dashboard address: {error}"))
}

fn check_deadlines(case_deadline: Instant, trace: &[String]) -> Result<(), String> {
    let now = Instant::now();
    let suite_started = *SUITE_STARTED.get_or_init(Instant::now);
    if now >= case_deadline {
        return Err(format!(
            "{}-second E2E case deadline exceeded; trace={trace:#?}",
            CASE_DEADLINE.as_secs()
        ));
    }
    if now.duration_since(suite_started) >= SUITE_DEADLINE {
        return Err(format!(
            "{}-second E2E suite deadline exceeded; trace={trace:#?}",
            SUITE_DEADLINE.as_secs()
        ));
    }
    Ok(())
}

fn wait_for<T>(
    case_deadline: Instant,
    trace: &[String],
    description: &str,
    mut probe: impl FnMut() -> Result<Option<T>, String>,
) -> Result<T, String> {
    let mut last_error = None;
    loop {
        check_deadlines(case_deadline, trace)?;
        match probe() {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(error) => last_error = Some(error),
        }
        std::thread::sleep(POLL);
        if Instant::now() >= case_deadline {
            return Err(format!(
                "timed out waiting for {description}; last_error={last_error:?}; trace={trace:#?}"
            ));
        }
    }
}

fn request_json(
    method: &str,
    url: &str,
    body: Option<&Value>,
) -> Result<(u16, Option<Value>), String> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_millis(250))
        .timeout_read(Duration::from_secs(3))
        .timeout_write(Duration::from_secs(1))
        .build();
    let request = agent.request(method, url).set("Accept", "application/json");
    let result = match body {
        Some(body) => request
            .set("Content-Type", "application/json")
            .send_string(&body.to_string()),
        None => request.call(),
    };
    let response = match result {
        Ok(response) => response,
        Err(ureq::Error::Status(_, response)) => response,
        Err(error) => {
            let failure = format!("{method} {url} disconnected: {error}");
            record_http_failure(failure.clone());
            return Err(failure);
        }
    };
    let status = response.status();
    if status >= 500 {
        record_http_failure(format!("{method} {url} returned HTTP {status}"));
    }
    let raw = response.into_string().map_err(|error| {
        let failure = format!("read {method} {url} response: {error}");
        record_http_failure(failure.clone());
        failure
    })?;
    let json = if raw.trim().is_empty() {
        None
    } else {
        Some(serde_json::from_str(&raw).map_err(|error| {
            let failure = format!("parse {method} {url} response {raw:?}: {error}");
            record_http_failure(failure.clone());
            failure
        })?)
    };
    Ok((status, json))
}

fn get_status(base_url: &str) -> Result<Value, String> {
    let url = format!("{base_url}/api/control/status");
    let (status, body) = request_json("GET", &url, None)?;
    if status != 200 {
        return Err(format!("status endpoint returned {status}: {body:?}"));
    }
    body.and_then(|body| body.get("Status").cloned())
        .ok_or_else(|| "status response has no Status model".to_owned())
}

fn search_offers(base_url: &str, count: usize) -> Result<Vec<u64>, String> {
    let url = format!("{base_url}/api/control/offers");
    let request = json!({
        "gpu_model": "RTX 4090",
        "min_gpu_ram_mb": 20_000,
        "min_compute_cap": 800,
        "min_reliability": 0.99,
        "require_verified": true,
        "min_download_mbps": 500.0,
        "min_upload_mbps": 400.0,
        "max_hourly_price": 1.0,
        "blacklist_hosts": [],
        "count": count
    });
    let (status, body) = request_json("POST", &url, Some(&request))?;
    if status != 200 {
        return Err(format!("offer search returned {status}: {body:?}"));
    }
    body.and_then(|body| body.get("Offers").cloned())
        .and_then(|offers| offers.as_array().cloned())
        .ok_or_else(|| "offer response has no Offers array".to_owned())?
        .into_iter()
        .map(|offer| {
            offer
                .get("offer_id")
                .and_then(Value::as_u64)
                .ok_or_else(|| format!("offer has no offer_id: {offer}"))
        })
        .collect()
}
fn assert_typed_prerequisite_rejection(base_url: &str) -> Result<String, String> {
    match search_offers(base_url, 0) {
        Err(error) if error.contains("409") && error.contains("error") => Ok(error),
        result => Err(format!(
            "prerequisite-invalid action did not produce a typed rejection: {result:?}"
        )),
    }
}

#[derive(Clone, Debug)]
struct AcceptedCommand {
    path: String,
    submissions: Vec<Value>,
    terminal: Option<Value>,
}

#[derive(Clone, Debug, Default)]
struct ExternalReplyCollector {
    accepted: BTreeMap<String, AcceptedCommand>,
    public_replies: Vec<String>,
    http_failures: Vec<String>,
}

impl ExternalReplyCollector {
    fn submit(&mut self, base_url: &str, path: &str, body: &Value) -> Result<(), String> {
        let command_id = body
            .get("command_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("external mutation has no command_id: {body}"))?
            .to_owned();
        let url = format!("{base_url}{path}");
        let response = request_json("POST", &url, Some(body));
        let (status, response_body) = match response {
            Ok(response) => response,
            Err(error) => {
                self.http_failures
                    .push(format!("POST {path} disconnected: {error}"));
                return Err(error);
            }
        };
        if status >= 500 {
            self.http_failures
                .push(format!("POST {path} returned {status}: {response_body:?}"));
        }
        if status != 202 {
            return Err(format!("POST {path} returned {status}: {response_body:?}"));
        }
        self.public_replies
            .push(format!("POST {path} command_id={command_id} status=202"));
        self.accepted
            .entry(command_id)
            .or_insert_with(|| AcceptedCommand {
                path: path.to_owned(),
                submissions: Vec::new(),
                terminal: None,
            })
            .submissions
            .push(body.clone());
        Ok(())
    }

    fn observe_status(&mut self, status: &Value) -> Result<(), String> {
        let commands = status
            .get("commands")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("status has no command ledger: {status}"))?;
        for (command_id, accepted) in &mut self.accepted {
            let matches = commands
                .iter()
                .filter(|command| {
                    command.get("command_id").and_then(Value::as_str) == Some(command_id.as_str())
                })
                .collect::<Vec<_>>();
            if matches.len() > 1 {
                return Err(format!(
                    "accepted command {command_id} has {} public records: {matches:?}",
                    matches.len()
                ));
            }
            let Some(record) = matches.first() else {
                continue;
            };
            if record
                .get("state")
                .and_then(Value::as_str)
                .is_some_and(|state| matches!(state, "succeeded" | "failed"))
            {
                if accepted
                    .terminal
                    .as_ref()
                    .is_some_and(|terminal| terminal != *record)
                {
                    return Err(format!(
                        "accepted command {command_id} changed terminal reply: before={:?}, after={record}",
                        accepted.terminal
                    ));
                }
                accepted.terminal = Some((*record).clone());
            }
        }
        Ok(())
    }

    fn all_terminal(&self) -> bool {
        self.accepted
            .values()
            .all(|accepted| accepted.terminal.is_some())
    }

    fn terminal(&self, command_id: &str) -> Option<&Value> {
        self.accepted
            .get(command_id)
            .and_then(|accepted| accepted.terminal.as_ref())
    }
}

fn flush_control(base_url: &str) -> Result<(), String> {
    let url = format!("{base_url}/api/control/flush");
    let (status, response) = request_json("POST", &url, None)?;
    if status != 200 || response != Some(Value::String("Flushed".to_owned())) {
        return Err(format!("control flush returned {status}: {response:?}"));
    }
    Ok(())
}

fn wait_for_accepted_replies(
    harness: &ScenarioHarness,
    case_deadline: Instant,
    trace: &[String],
    replies: &mut ExternalReplyCollector,
) -> Result<Value, String> {
    wait_for(case_deadline, trace, "all accepted command replies", || {
        let status = get_status(&harness.base_url())?;
        replies.observe_status(&status)?;
        Ok(replies.all_terminal().then_some(status))
    })
}

fn concurrent_status_requests(port: u16) -> Result<(), String> {
    let request =
        b"GET /api/control/status HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: application/json\r\nConnection: close\r\n\r\n";
    let mut streams = Vec::new();
    for _ in 0..2 {
        let mut stream = TcpStream::connect(("127.0.0.1", port))
            .map_err(|error| format!("connect concurrent dashboard request: {error}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(|error| format!("set concurrent dashboard read timeout: {error}"))?;
        stream
            .write_all(request)
            .map_err(|error| format!("write concurrent dashboard request: {error}"))?;
        stream
            .flush()
            .map_err(|error| format!("flush concurrent dashboard request: {error}"))?;
        streams.push(stream);
    }
    for mut stream in streams {
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .map_err(|error| format!("read concurrent dashboard response: {error}"))?;
        let status = response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse::<u16>().ok())
            .ok_or_else(|| format!("malformed concurrent dashboard response: {response:?}"))?;
        if status != 200 {
            return Err(format!(
                "concurrent dashboard request returned {status}: {response:?}"
            ));
        }
    }
    Ok(())
}

fn node_sets(status: &Value) -> Result<(BTreeSet<u64>, BTreeSet<u64>), String> {
    let nodes = status
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status has no nodes: {status}"))?;
    let mut running = BTreeSet::new();
    let mut stopped = BTreeSet::new();
    for node in nodes {
        let id = node
            .get("logical_node_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("node has no logical_node_id: {node}"))?;
        match node.get("phase").and_then(Value::as_str) {
            Some("running") => {
                if node.get("runtime").is_none_or(Value::is_null) {
                    return Err(format!("running node {id} has no runtime readiness facts"));
                }
                running.insert(id);
            }
            Some("stopped") => {
                stopped.insert(id);
            }
            _ => {}
        }
    }
    Ok((running, stopped))
}

fn selected_offer_map(status: &Value) -> Result<BTreeMap<u64, u64>, String> {
    status
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status has no nodes: {status}"))?
        .iter()
        .map(|node| {
            let node_id = node
                .get("logical_node_id")
                .and_then(Value::as_u64)
                .ok_or_else(|| format!("node has no logical_node_id: {node}"))?;
            let offer_id = node
                .get("selected_offer_id")
                .and_then(Value::as_u64)
                .ok_or_else(|| format!("node {node_id} has no selected_offer_id"))?;
            Ok((node_id, offer_id))
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
struct ProviderSnapshot {
    live: Vec<u64>,
    created: Vec<u64>,
    destroyed: Vec<u64>,
    events: Vec<Value>,
}

fn provider_ledger_snapshot(path: &PathBuf, run_id: u64) -> Result<ProviderSnapshot, String> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(format!(
                "read independent provider ledger {}: {error}",
                path.display()
            ));
        }
    };
    let mut resources = BTreeMap::<String, u64>::new();
    let mut snapshot = ProviderSnapshot::default();
    for (index, line) in contents.lines().enumerate() {
        let event: Value = serde_json::from_str(line).map_err(|error| {
            format!(
                "parse independent provider ledger {} line {}: {error}; line={line:?}",
                path.display(),
                index + 1
            )
        })?;
        if event.get("run_id").and_then(Value::as_u64) != Some(run_id) {
            return Err(format!(
                "independent provider ledger contains a foreign run: {event}"
            ));
        }
        let event_type = event
            .get("event")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("provider lifecycle observation has no event: {event}"))?;
        let node_id = event
            .get("node_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("provider lifecycle observation has no node_id: {event}"))?;
        let provider_ref = event
            .get("provider_ref")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!("provider lifecycle observation has no provider_ref: {event}")
            })?;
        match event_type {
            "created" => {
                if event
                    .get("selected_offer_id")
                    .and_then(Value::as_u64)
                    .is_none_or(|offer_id| offer_id == 0)
                {
                    return Err(format!(
                        "provider create did not preserve a selected offer: {event}"
                    ));
                }
                snapshot.created.push(node_id);
                if let Some(previous_node) = resources.insert(provider_ref.to_owned(), node_id) {
                    return Err(format!(
                        "duplicate provider resource {provider_ref}: previous node {previous_node}, event={event}"
                    ));
                }
            }
            "adopted" | "recreated" => match resources.get(provider_ref) {
                Some(resource_node) if *resource_node == node_id => {}
                _ => {
                    return Err(format!(
                        "provider recovered a resource absent from the independent ledger: {event}; live={resources:?}"
                    ));
                }
            },
            "destroyed" => {
                snapshot.destroyed.push(node_id);
                if resources.remove(provider_ref) != Some(node_id) {
                    return Err(format!(
                        "provider destroyed a resource absent from the independent ledger: {event}; live={resources:?}"
                    ));
                }
            }
            other => {
                return Err(format!(
                    "unknown independent provider lifecycle event {other:?}: {event}"
                ));
            }
        }
        snapshot.events.push(event);
    }
    snapshot.live = resources.into_values().collect();
    Ok(snapshot)
}

fn node_socket(run_id: u64, node_id: u64) -> PathBuf {
    std::env::temp_dir().join(format!("myelin-node-debug-join-{run_id}-{node_id}.sock"))
}

fn process_count(run_id: u64, node_id: u64) -> Result<usize, String> {
    find_process_identities_by_environment(
        &[
            ("MYELIN_RUN_ID".to_owned(), run_id.to_string()),
            ("MYELIN_LOGICAL_NODE_ID".to_owned(), node_id.to_string()),
        ],
        true,
    )
    .map(|matches| matches.len())
    .map_err(|error| format!("find worker process for node {node_id}: {error}"))
}

fn reachable_nodes(run_id: u64, all_nodes: &BTreeSet<u64>) -> Result<BTreeSet<u64>, String> {
    let mut reachable = BTreeSet::new();
    for &node_id in all_nodes {
        let count = process_count(run_id, node_id)?;
        if count > 1 {
            return Err(format!("node {node_id} owns {count} worker processes"));
        }
        if count == 1 && UnixStream::connect(node_socket(run_id, node_id)).is_ok() {
            reachable.insert(node_id);
        }
    }
    Ok(reachable)
}

fn node_endpoint_request(run_id: u64, node_id: u64, request: &str) -> Result<Value, String> {
    let mut stream = UnixStream::connect(node_socket(run_id, node_id))
        .map_err(|error| format!("connect node {node_id} debug endpoint: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .map_err(|error| format!("set node {node_id} endpoint read timeout: {error}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .map_err(|error| format!("set node {node_id} endpoint write timeout: {error}"))?;
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("write node {node_id} endpoint request: {error}"))?;
    stream
        .flush()
        .map_err(|error| format!("flush node {node_id} endpoint request: {error}"))?;
    let mut response = String::new();
    BufReader::new(stream)
        .read_line(&mut response)
        .map_err(|error| format!("read node {node_id} endpoint response: {error}"))?;
    serde_json::from_str(&response)
        .map_err(|error| format!("parse node {node_id} endpoint response: {error}"))
}

fn probe_node_endpoint(run_id: u64, node_id: u64, status: &Value) -> Result<(), String> {
    let node = status
        .get("nodes")
        .and_then(Value::as_array)
        .and_then(|nodes| {
            nodes
                .iter()
                .find(|node| node.get("logical_node_id").and_then(Value::as_u64) == Some(node_id))
        })
        .ok_or_else(|| format!("status has no node {node_id}: {status}"))?;
    let endpoint_json = node
        .pointer("/runtime/endpoint")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("running node {node_id} has no endpoint: {node}"))?;
    let _: Value = serde_json::from_str(endpoint_json)
        .map_err(|error| format!("parse node {node_id} endpoint: {error}"))?;
    let response = node_endpoint_request(run_id, node_id, "{}\n")?;
    if response.get("type").and_then(Value::as_str) != Some("JoinRejected")
        || response.get("error").and_then(Value::as_str) != Some("MalformedCommand")
    {
        return Err(format!(
            "node {node_id} endpoint did not complete a typed readiness round trip: {response}"
        ));
    }
    Ok(())
}

fn actor_census(
    harness: &ScenarioHarness,
    worker_nodes: &BTreeSet<u64>,
) -> Result<ActorCensus, String> {
    let base_url = harness.base_url();
    let actor_url = format!("{base_url}/api/control/actors");
    let (actor_status, actor_body) = request_json("GET", &actor_url, None)?;
    if actor_status != 200 {
        return Err(format!(
            "live orchestrator actor census returned {actor_status}: {actor_body:?}"
        ));
    }
    let actor_body =
        actor_body.ok_or_else(|| "live orchestrator actor census returned no body".to_owned())?;
    let mut census = ActorCensus::default();
    census.orchestrator_actors = actor_body
        .get("actors")
        .and_then(Value::as_array)
        .map_or(0, |actors| actors.len() as u64);
    census.actors = census.orchestrator_actors;
    if let Some(workers) = actor_body.get("workers").and_then(Value::as_array) {
        for worker in workers {
            census.mailbox_depth = census.mailbox_depth.saturating_add(
                worker
                    .get("mailbox_depth")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
            census.poisoned = census
                .poisoned
                .saturating_add(worker.get("panics").and_then(Value::as_u64).unwrap_or(0));
            census.type_mismatches = census.type_mismatches.saturating_add(
                worker
                    .get("type_mismatches")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
        }
    }

    let url = format!("{base_url}/api/view/fleet");
    let (status, body) = request_json("GET", &url, None)?;
    if status != 200 {
        return Err(format!("fleet actor census returned {status}: {body:?}"));
    }
    let body = body.ok_or_else(|| "fleet actor census returned no body".to_owned())?;
    let live = body
        .get("live")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("fleet actor census has no live array: {body}"))?;
    if live.is_empty() {
        return Err("fleet actor census has no live streams".to_owned());
    }
    for node in live {
        let Some(summary) = node.get("actor_summary") else {
            continue;
        };
        let _actors = summary.get("actors").and_then(Value::as_u64).unwrap_or(0);
        let orchestrator = node
            .pointer("/stream/origin")
            .and_then(Value::as_str)
            .is_some_and(|origin| origin == "orchestrator");
        let mut by_type = BTreeMap::new();
        if let Some(roster) = node.get("roster").and_then(Value::as_array) {
            for actor in roster {
                let actor_type = actor
                    .get("actor_type")
                    .and_then(Value::as_str)
                    .unwrap_or("<unknown>")
                    .to_owned();
                *by_type.entry(actor_type).or_insert(0) += 1;
            }
        }
        if orchestrator {
            for (actor_type, count) in by_type {
                *census.orchestrator_by_type.entry(actor_type).or_insert(0) += count;
            }
        }
    }
    for &node_id in worker_nodes {
        let response =
            node_endpoint_request(harness.run_id, node_id, "{\"type\":\"RuntimeStats\"}\n")?;
        if response.get("type").and_then(Value::as_str) != Some("RuntimeStats") {
            return Err(format!(
                "node {node_id} rejected runtime census request: {response}"
            ));
        }
        let stats = response
            .get("stats")
            .ok_or_else(|| format!("node {node_id} runtime census has no stats: {response}"))?;
        let details = stats
            .get("actors")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("node {node_id} runtime census has no actors: {stats}"))?;
        if details.is_empty() {
            return Err(format!(
                "node {node_id} runtime census is not populated yet"
            ));
        }
        let actors = details.len() as u64;
        census.worker_actors = census.worker_actors.saturating_add(actors);
        census.actors = census.actors.saturating_add(actors);
        census.poisoned = census.poisoned.saturating_add(
            details
                .iter()
                .filter(|actor| actor.get("poisoned").and_then(Value::as_bool) == Some(true))
                .count() as u64,
        );
        census.type_mismatches = census.type_mismatches.saturating_add(
            stats
                .get("workers")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|worker| worker.get("type_mismatches").and_then(Value::as_u64))
                .sum::<u64>(),
        );
        census.mailbox_depth = census.mailbox_depth.saturating_add(
            details
                .iter()
                .filter_map(|actor| actor.get("mailbox_depth").and_then(Value::as_u64))
                .sum(),
        );
        let mut by_type = BTreeMap::new();
        for actor in details {
            let actor_type = actor
                .get("actor_type")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>")
                .to_owned();
            *by_type.entry(actor_type).or_insert(0) += 1;
        }
        census
            .workers_by_stream
            .insert(node_id.to_string(), by_type);
    }
    let typed_orchestrator_actors = census.orchestrator_by_type.values().sum::<u64>();
    if typed_orchestrator_actors == 0 && !worker_nodes.is_empty() {
        return Err(format!(
            "live orchestrator roster is not populated yet: fleet={body}"
        ));
    }
    if typed_orchestrator_actors != 0 && typed_orchestrator_actors != census.orchestrator_actors {
        return Err(format!(
            "orchestrator actor roster disagrees with the live runtime: \
             roster={typed_orchestrator_actors}, runtime={}; fleet={body}; runtime_stats={actor_body}",
            census.orchestrator_actors
        ));
    }
    if census.orchestrator_actors == 0 {
        return Err(format!(
            "live orchestrator actor census has no actors: {actor_body}"
        ));
    }
    Ok(census)
}

fn wait_for_stable_actor_census(
    harness: &ScenarioHarness,
    case_deadline: Instant,
    trace: &[String],
    worker_nodes: &BTreeSet<u64>,
) -> Result<ActorCensus, String> {
    let mut candidate: Option<(ActorCensus, Instant)> = None;
    wait_for(case_deadline, trace, "stable actor census", || {
        let census = actor_census(harness, worker_nodes)?;
        if census.mailbox_depth != 0 {
            candidate = None;
            return Ok(None);
        }
        match &candidate {
            Some((previous, since)) if previous == &census => {
                Ok((since.elapsed() >= CENSUS_STABILITY).then_some(census))
            }
            _ => {
                candidate = Some((census, Instant::now()));
                Ok(None)
            }
        }
    })
}

fn wait_for_baseline_actor_census(
    harness: &ScenarioHarness,
    case_deadline: Instant,
    trace: &[String],
    baseline: &ActorCensus,
) -> Result<ActorCensus, String> {
    wait_for(case_deadline, trace, "baseline actor census", || {
        let census = actor_census(harness, &BTreeSet::new())?;
        Ok((census.actors == baseline.actors
            && census.orchestrator_actors == baseline.orchestrator_actors
            && census.worker_actors == baseline.worker_actors
            && census.poisoned == baseline.poisoned
            && census.type_mismatches == baseline.type_mismatches
            && census.mailbox_depth == baseline.mailbox_depth)
            .then_some(census))
    })
}

fn wait_for_state(
    harness: &ScenarioHarness,
    case_deadline: Instant,
    trace: &[String],
    expected_running: &BTreeSet<u64>,
    expected_stopped: &BTreeSet<u64>,
) -> Result<Value, String> {
    wait_for(case_deadline, trace, "durable node state", || {
        let status = get_status(&harness.base_url())?;
        let (running, stopped) = node_sets(&status)?;
        Ok((running == *expected_running && stopped == *expected_stopped).then_some(status))
    })
}

fn wait_for_reachability(
    harness: &ScenarioHarness,
    case_deadline: Instant,
    trace: &[String],
    all_nodes: &BTreeSet<u64>,
    expected: &BTreeSet<u64>,
) -> Result<BTreeSet<u64>, String> {
    wait_for(case_deadline, trace, "worker reachability", || {
        let reachable = reachable_nodes(harness.run_id, all_nodes)?;
        Ok((reachable == *expected).then_some(reachable))
    })
}

fn wait_for_provider_ledger(
    harness: &ScenarioHarness,
    case_deadline: Instant,
    trace: &[String],
    expected_created: &BTreeSet<u64>,
    expected_destroyed: &BTreeSet<u64>,
    expected_live: &BTreeSet<u64>,
) -> Result<ProviderSnapshot, String> {
    wait_for(
        case_deadline,
        trace,
        "independent mock-provider lifecycle ledger",
        || {
            let snapshot = provider_ledger_snapshot(&harness.provider_ledger_path, harness.run_id)?;
            let live_set = snapshot.live.iter().copied().collect::<BTreeSet<_>>();
            let created_set = snapshot.created.iter().copied().collect::<BTreeSet<_>>();
            let destroyed_set = snapshot.destroyed.iter().copied().collect::<BTreeSet<_>>();
            Ok((created_set == *expected_created
                && snapshot.created.len() == expected_created.len()
                && destroyed_set == *expected_destroyed
                && snapshot.destroyed.len() == expected_destroyed.len()
                && live_set == *expected_live
                && snapshot.live.len() == expected_live.len())
            .then_some(snapshot))
        },
    )
}
#[derive(Clone, Debug)]
struct LifecyclePlan {
    selected_offers: Vec<u64>,
    all_nodes: BTreeSet<u64>,
    stopped: BTreeSet<u64>,
    survivors: BTreeSet<u64>,
}

fn lifecycle_plan(case: &E2eCase, searched: &[u64]) -> Result<LifecyclePlan, String> {
    let usable = searched.len().min(2);
    if usable == 0 {
        return Err("cannot derive lifecycle plan without a usable search result".to_owned());
    }
    let node_count = 1 + usize::from(case.node_seed) % usable;
    let selected_offers = (0..node_count)
        .map(|offset| searched[(case.offer_offset + offset) % searched.len()])
        .collect::<Vec<_>>();
    let all_nodes = (1..=node_count as u64).collect::<BTreeSet<_>>();
    let protected_survivor = 1 + case.seed % node_count as u64;
    let mut stopped = BTreeSet::new();
    for node_id in &all_nodes {
        if *node_id != protected_survivor
            && case.kill_mask & (1_u8 << ((*node_id as usize - 1) % 8)) != 0
        {
            stopped.insert(*node_id);
        }
    }
    let survivors = all_nodes
        .difference(&stopped)
        .copied()
        .collect::<BTreeSet<_>>();
    Ok(LifecyclePlan {
        selected_offers,
        all_nodes,
        stopped,
        survivors,
    })
}

fn labeled_temp_resources(harness: &ScenarioHarness, all_nodes: &BTreeSet<u64>) -> Vec<PathBuf> {
    let mut leaked = all_nodes
        .iter()
        .map(|node_id| node_socket(harness.run_id, *node_id))
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    let registry_path = harness.state_dir.join("process-nodes.json");
    let registry_leaked = match std::fs::read_to_string(&registry_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Ok(contents) => !matches!(
            serde_json::from_str::<Value>(&contents),
            Ok(Value::Object(registry)) if registry.is_empty()
        ),
        Err(_) => true,
    };
    if registry_leaked {
        leaked.push(registry_path);
    }
    leaked
}

fn setup_failure_with_diagnostics(error: String, case: &E2eCase, trace: &[String]) -> String {
    format!(
        "{error}; seed={}; generated_actions={:#?}; trace={trace:#?}; \
         public_replies=[]; http_failures=[]; process_logs=<not launched>; \
         census=<not launched>; reachability=<not launched>; provider_ledger=<not launched>",
        case.seed, case.actions
    )
}

fn failure_with_diagnostics(
    error: String,
    case: &E2eCase,
    trace: &[String],
    replies: &ExternalReplyCollector,
    census_snapshots: &[ActorCensus],
    harness: &ScenarioHarness,
    provider: &TestHttpServer,
) -> String {
    let status = get_status(&harness.base_url());
    let node_sets = status
        .as_ref()
        .ok()
        .and_then(|status| node_sets(status).ok());
    let observed_nodes = node_sets
        .as_ref()
        .map(|(running, stopped)| running.union(stopped).copied().collect())
        .unwrap_or_default();
    let reachability = reachable_nodes(harness.run_id, &observed_nodes);
    let census = node_sets
        .as_ref()
        .map(|(running, _)| actor_census(harness, running))
        .transpose();
    let provider_ledger = provider_ledger_snapshot(&harness.provider_ledger_path, harness.run_id);
    let worker_processes = find_process_identities_by_environment(
        &[("MYELIN_RUN_ID".to_owned(), harness.run_id.to_string())],
        true,
    );
    let worker_sockets = observed_nodes
        .iter()
        .map(|node_id| (*node_id, node_socket(harness.run_id, *node_id).exists()))
        .collect::<BTreeMap<_, _>>();
    format!(
        "{error}; seed={}; generated_actions={:#?}; trace={trace:#?}; \
         public_replies={:#?}; http_failures={:#?}; observed_http_failures={:#?}; \
         unexpected_process_exits={:#?}; \
         status={status:#?}; census_snapshots={census_snapshots:#?}; current_census={census:#?}; \
         reachability={reachability:#?}; worker_processes={worker_processes:#?}; \
         worker_sockets={worker_sockets:#?}; provider_ledger={provider_ledger:#?}; \
         provider_http_requests={:#?}; process_logs={}",
        case.seed,
        case.actions,
        replies.public_replies,
        replies.http_failures,
        observed_http_failures(),
        harness.unexpected_exits,
        provider.requests(),
        harness.logs(),
    )
}

fn run_stateful_case(case: E2eCase) -> Result<(), String> {
    let case_started = Instant::now();
    let case_deadline = case_started + CASE_DEADLINE;
    let mut trace = vec![format!(
        "seed={} generated_actions={:#?}",
        case.seed, case.actions
    )];
    discard_http_failures_since(0);
    check_deadlines(case_deadline, &trace)
        .map_err(|error| setup_failure_with_diagnostics(error, &case, &trace))?;

    let provider = TestHttpServer::start(vec![TestHttpRoute::json(
        "GET",
        "/api/v0/bundles/",
        200,
        production_offers(),
    )])
    .map_err(|error| setup_failure_with_diagnostics(error, &case, &trace))?;
    let temp = tempfile::tempdir().map_err(|error| {
        setup_failure_with_diagnostics(format!("create E2E state dir: {error}"), &case, &trace)
    })?;
    let run_id = 7_000_000_u64 + case.seed % 1_000_000;
    let dashboard_port =
        reserve_port().map_err(|error| setup_failure_with_diagnostics(error, &case, &trace))?;
    let mut harness = ScenarioHarness::new(
        temp.path().to_path_buf(),
        provider.uri(),
        dashboard_port,
        run_id,
    );
    let mut replies = ExternalReplyCollector::default();
    let mut census_snapshots = Vec::new();

    let execution = (|| -> Result<OracleObservation, String> {
        harness.start()?;
        trace.push("launch direct Cargo-built Myelin orchestrator".to_owned());
        let readiness_http_checkpoint = http_failure_checkpoint();
        wait_for(case_deadline, &trace, "dashboard readiness", || {
            get_status(&harness.base_url()).map(Some)
        })?;
        discard_http_failures_since(readiness_http_checkpoint);
        harness.ensure_running()?;
        let mut generated_restart_mode = None;
        concurrent_status_requests(harness.dashboard_port)?;
        trace.push("two concurrent independent status requests completed".to_owned());
        let actor_initial =
            wait_for_stable_actor_census(&harness, case_deadline, &trace, &BTreeSet::new())?;
        census_snapshots.push(actor_initial.clone());

        let mut searched = Vec::new();
        let mut generated_restarts = 0_usize;
        let mut typed_rejections = 0_usize;
        for (action_index, action) in case.actions.clone().into_iter().enumerate() {
            check_deadlines(case_deadline, &trace)?;
            harness.ensure_running()?;
            trace.push(format!("generated action {action_index}: {action:?}"));
            match action {
                ExternalAction::Search { count } => match search_offers(&harness.base_url(), count)
                {
                    Ok(offers) if count > 0 => {
                        searched = offers;
                        trace.push(format!("search returned usable offers {searched:?}"));
                    }
                    Ok(offers) => {
                        return Err(format!(
                            "prerequisite-invalid search unexpectedly succeeded: {offers:?}"
                        ));
                    }
                    Err(error)
                        if count == 0 && error.contains("409") && error.contains("error") =>
                    {
                        typed_rejections += 1;
                        trace.push(format!("typed invalid-search rejection: {error}"));
                    }
                    Err(error) => {
                        replies
                            .http_failures
                            .push(format!("offer search failed: {error}"));
                        return Err(error);
                    }
                },
                ExternalAction::Provision {
                    command_slot,
                    use_searched_offers,
                } => {
                    let Some(plan) = use_searched_offers
                        .then(|| lifecycle_plan(&case, &searched).ok())
                        .flatten()
                    else {
                        let rejection = assert_typed_prerequisite_rejection(&harness.base_url())?;
                        typed_rejections += 1;
                        trace.push(format!(
                            "provision rejected before usable search selection: {rejection}"
                        ));
                        continue;
                    };
                    let request = json!({
                        "command_id": format!("generated-provision-{command_slot}"),
                        "count": plan.selected_offers.len(),
                        "selected_offer_ids": plan.selected_offers,
                    });
                    replies.submit(&harness.base_url(), "/api/control/provision", &request)?;
                }
                ExternalAction::Query => {
                    let status = get_status(&harness.base_url()).map_err(|error| {
                        replies
                            .http_failures
                            .push(format!("status query failed: {error}"));
                        error
                    })?;
                    replies.observe_status(&status)?;
                }
                ExternalAction::Kill {
                    node_slot,
                    command_slot,
                } => {
                    let Some(plan) = lifecycle_plan(&case, &searched).ok() else {
                        let rejection = assert_typed_prerequisite_rejection(&harness.base_url())?;
                        typed_rejections += 1;
                        trace.push(format!(
                            "kill rejected before usable search selection: {rejection}"
                        ));
                        continue;
                    };
                    let Some(target) = (!plan.stopped.is_empty()).then(|| {
                        let index = usize::from(node_slot) % plan.stopped.len();
                        *plan.stopped.iter().nth(index).expect("bounded kill index")
                    }) else {
                        let rejection = assert_typed_prerequisite_rejection(&harness.base_url())?;
                        typed_rejections += 1;
                        trace.push(format!(
                            "kill rejected because its generated subset is empty: {rejection}"
                        ));
                        continue;
                    };
                    let status = get_status(&harness.base_url())?;
                    replies.observe_status(&status)?;
                    let (running, stopped) = node_sets(&status)?;
                    if !running.contains(&target) && !stopped.contains(&target) {
                        let rejection = assert_typed_prerequisite_rejection(&harness.base_url())?;
                        typed_rejections += 1;
                        trace.push(format!(
                            "kill of node {target} rejected before provision: {rejection}"
                        ));
                        continue;
                    }
                    let request = json!({
                        "command_id": format!("generated-kill-{command_slot}"),
                        "logical_node_id": target,
                    });
                    replies.submit(&harness.base_url(), "/api/control/kill", &request)?;
                }
                ExternalAction::Flush => {
                    flush_control(&harness.base_url()).map_err(|error| {
                        replies
                            .http_failures
                            .push(format!("control flush failed: {error}"));
                        error
                    })?;
                    replies
                        .public_replies
                        .push("POST /api/control/flush reply=Flushed".to_owned());
                }
                ExternalAction::Restart { mode } => {
                    if replies.accepted.is_empty() {
                        trace.push(format!(
                            "restart {mode:?} rejected by external model before any acknowledged mutation"
                        ));
                        continue;
                    }
                    if generated_restarts == 1 {
                        trace.push(format!(
                            "restart {mode:?} rejected by the bounded external model after one generated restart"
                        ));
                        continue;
                    }
                    flush_control(&harness.base_url())?;
                    replies.public_replies.push(format!(
                        "POST /api/control/flush reply=Flushed before {mode:?}"
                    ));
                    match mode {
                        RestartMode::Graceful => {
                            harness.restart_gracefully(case_deadline)?;
                        }
                        RestartMode::FlushSafeAbrupt => {
                            harness.restart_abruptly()?;
                        }
                    }
                    generated_restarts += 1;
                    generated_restart_mode = Some(mode);
                    let readiness_http_checkpoint = http_failure_checkpoint();
                    wait_for(
                        case_deadline,
                        &trace,
                        "dashboard after generated restart",
                        || get_status(&harness.base_url()).map(Some),
                    )?;
                    discard_http_failures_since(readiness_http_checkpoint);
                    trace.push(format!(
                        "completed generated {mode:?} restart between acknowledged operations"
                    ));
                }
                ExternalAction::EndpointProbe { node_slot } => {
                    let status = get_status(&harness.base_url())?;
                    replies.observe_status(&status)?;
                    let (running, _) = node_sets(&status)?;
                    if !running.is_empty() {
                        let index = usize::from(node_slot) % running.len();
                        let node_id = *running.iter().nth(index).expect("bounded probe index");
                        probe_node_endpoint(run_id, node_id, &status)?;
                        typed_rejections += 1;
                        trace.push(format!(
                            "node {node_id} returned typed MalformedCommand rejection"
                        ));
                    } else {
                        trace.push(
                            "endpoint probe rejected by external model before provision".to_owned(),
                        );
                    }
                }
                ExternalAction::ConcurrentQueries => {
                    concurrent_status_requests(harness.dashboard_port).map_err(|error| {
                        replies
                            .http_failures
                            .push(format!("concurrent status requests failed: {error}"));
                        error
                    })?;
                }
            }
        }

        if searched.is_empty() {
            searched = search_offers(&harness.base_url(), 2)?;
            trace.push(format!(
                "convergence search returned usable offers {searched:?}"
            ));
        }
        let plan = lifecycle_plan(&case, &searched)?;
        if plan.selected_offers.len() > 2 || plan.survivors.is_empty() {
            return Err(format!("invalid bounded lifecycle plan: {plan:?}"));
        }

        let mut status = if replies.accepted.is_empty() {
            get_status(&harness.base_url())?
        } else {
            wait_for_accepted_replies(&harness, case_deadline, &trace, &mut replies)?
        };
        for (command_id, accepted) in &replies.accepted {
            if let Some(terminal) = &accepted.terminal
                && terminal.get("state").and_then(Value::as_str) == Some("failed")
            {
                if terminal.get("error").and_then(Value::as_str).is_none() {
                    return Err(format!(
                        "failed command {command_id} has no typed rejection: {terminal}"
                    ));
                }
                typed_rejections += 1;
            }
        }

        let (running, stopped) = node_sets(&status)?;
        let observed = running.union(&stopped).copied().collect::<BTreeSet<_>>();
        if observed.is_empty() {
            let request = json!({
                "command_id": format!("converge-provision-{}", case.seed),
                "count": plan.selected_offers.len(),
                "selected_offer_ids": plan.selected_offers,
            });
            replies.submit(&harness.base_url(), "/api/control/provision", &request)?;
            wait_for_accepted_replies(&harness, case_deadline, &trace, &mut replies)?;
            let terminal = replies
                .terminal(request["command_id"].as_str().expect("command id"))
                .expect("accepted command has terminal reply");
            if terminal.get("state").and_then(Value::as_str) != Some("succeeded") {
                return Err(format!("convergence provision failed: {terminal}"));
            }
        } else if observed != plan.all_nodes {
            return Err(format!(
                "accepted replies produced an unexpected node set: expected {:?}, observed {observed:?}, status={status}",
                plan.all_nodes
            ));
        }

        status = wait_for(
            case_deadline,
            &trace,
            "provisioned state from accepted replies",
            || {
                let status = get_status(&harness.base_url())?;
                let (running, stopped) = node_sets(&status)?;
                let observed = running.union(&stopped).copied().collect::<BTreeSet<_>>();
                Ok((observed == plan.all_nodes).then_some(status))
            },
        )?;
        let (_, already_stopped) = node_sets(&status)?;
        if !already_stopped.is_subset(&plan.stopped) {
            return Err(format!(
                "generated successful kills exceeded the survivor-preserving subset: desired={:?}, observed={already_stopped:?}",
                plan.stopped
            ));
        }
        for node_id in plan.stopped.difference(&already_stopped) {
            let request = json!({
                "command_id": format!("converge-kill-{}-{node_id}", case.seed),
                "logical_node_id": node_id,
            });
            replies.submit(&harness.base_url(), "/api/control/kill", &request)?;
        }
        wait_for_accepted_replies(&harness, case_deadline, &trace, &mut replies)?;
        for command_id in replies
            .accepted
            .keys()
            .filter(|command_id| command_id.starts_with("converge-kill-"))
        {
            let terminal = replies
                .terminal(command_id)
                .expect("accepted kill has terminal reply");
            if terminal.get("state").and_then(Value::as_str) != Some("succeeded") {
                return Err(format!("convergence kill failed: {terminal}"));
            }
        }

        status = wait_for_state(
            &harness,
            case_deadline,
            &trace,
            &plan.survivors,
            &plan.stopped,
        )?;
        wait_for_reachability(
            &harness,
            case_deadline,
            &trace,
            &plan.all_nodes,
            &plan.survivors,
        )?;
        wait_for_provider_ledger(
            &harness,
            case_deadline,
            &trace,
            &plan.all_nodes,
            &plan.stopped,
            &plan.survivors,
        )?;
        let selected_offer_by_node = selected_offer_map(&status)?;
        let expected_offer_by_node = plan
            .selected_offers
            .iter()
            .enumerate()
            .map(|(index, offer_id)| (index as u64 + 1, *offer_id))
            .collect::<BTreeMap<_, _>>();
        if selected_offer_by_node != expected_offer_by_node {
            return Err(format!(
                "public selected offers differ from searched selection: expected={expected_offer_by_node:?}, observed={selected_offer_by_node:?}"
            ));
        }

        if typed_rejections == 0 {
            match search_offers(&harness.base_url(), 0) {
                Err(error) if error.contains("409") && error.contains("error") => {
                    typed_rejections += 1;
                    trace.push(format!("typed convergence rejection: {error}"));
                }
                result => {
                    return Err(format!(
                        "could not establish a typed prerequisite rejection: {result:?}"
                    ));
                }
            }
        }
        if typed_rejections == 0 {
            return Err("no typed rejection was observed".to_owned());
        }

        let convergence_restarts = match generated_restart_mode {
            Some(RestartMode::Graceful) => vec![RestartMode::FlushSafeAbrupt],
            Some(RestartMode::FlushSafeAbrupt) => vec![RestartMode::Graceful],
            None if case.seed & 1 == 0 => {
                vec![RestartMode::Graceful, RestartMode::FlushSafeAbrupt]
            }
            None => vec![RestartMode::FlushSafeAbrupt, RestartMode::Graceful],
        };
        for mode in convergence_restarts {
            flush_control(&harness.base_url())?;
            replies.public_replies.push(format!(
                "POST /api/control/flush reply=Flushed before convergence {mode:?}"
            ));
            match mode {
                RestartMode::Graceful => harness.restart_gracefully(case_deadline)?,
                RestartMode::FlushSafeAbrupt => harness.restart_abruptly()?,
            }
            let readiness_http_checkpoint = http_failure_checkpoint();
            wait_for(
                case_deadline,
                &trace,
                "dashboard after convergence restart",
                || get_status(&harness.base_url()).map(Some),
            )?;
            discard_http_failures_since(readiness_http_checkpoint);
            trace.push(format!("convergence tail completed {mode:?} restart"));
        }
        status = wait_for_state(
            &harness,
            case_deadline,
            &trace,
            &plan.survivors,
            &plan.stopped,
        )?;
        wait_for_reachability(
            &harness,
            case_deadline,
            &trace,
            &plan.all_nodes,
            &plan.survivors,
        )?;
        wait_for_provider_ledger(
            &harness,
            case_deadline,
            &trace,
            &plan.all_nodes,
            &plan.stopped,
            &plan.survivors,
        )?;
        for node_id in &plan.survivors {
            probe_node_endpoint(run_id, *node_id, &status)?;
        }

        let actor_before_replays =
            wait_for_stable_actor_census(&harness, case_deadline, &trace, &plan.survivors)?;
        census_snapshots.push(actor_before_replays.clone());
        let replay_requests = replies
            .accepted
            .values()
            .filter_map(|accepted| {
                accepted
                    .submissions
                    .first()
                    .cloned()
                    .map(|request| (accepted.path.clone(), request))
            })
            .collect::<Vec<_>>();
        for (path, request) in replay_requests {
            replies.submit(&harness.base_url(), &path, &request)?;
        }
        for _ in 0..2 {
            let read = get_status(&harness.base_url())?;
            replies.observe_status(&read)?;
        }
        concurrent_status_requests(harness.dashboard_port)?;
        status = wait_for_accepted_replies(&harness, case_deadline, &trace, &mut replies)?;
        let actor_after_replays =
            wait_for_stable_actor_census(&harness, case_deadline, &trace, &plan.survivors)?;
        census_snapshots.push(actor_after_replays.clone());
        let (running_after_restart, stopped_after_restart) = node_sets(&status)?;
        let reachable = reachable_nodes(run_id, &plan.all_nodes)?;
        let provider_snapshot =
            provider_ledger_snapshot(&harness.provider_ledger_path, harness.run_id)?;

        for node_id in &plan.survivors {
            let request = json!({
                "command_id": format!("teardown-kill-{}-{node_id}", case.seed),
                "logical_node_id": node_id,
            });
            replies.submit(&harness.base_url(), "/api/control/kill", &request)?;
            replies.submit(&harness.base_url(), "/api/control/kill", &request)?;
        }
        wait_for_accepted_replies(&harness, case_deadline, &trace, &mut replies)?;
        wait_for_state(
            &harness,
            case_deadline,
            &trace,
            &BTreeSet::new(),
            &plan.all_nodes,
        )?;
        wait_for_reachability(
            &harness,
            case_deadline,
            &trace,
            &plan.all_nodes,
            &BTreeSet::new(),
        )?;
        let final_provider = wait_for_provider_ledger(
            &harness,
            case_deadline,
            &trace,
            &plan.all_nodes,
            &plan.all_nodes,
            &BTreeSet::new(),
        )?;
        let actor_after_teardown =
            wait_for_baseline_actor_census(&harness, case_deadline, &trace, &actor_initial)?;
        census_snapshots.push(actor_after_teardown.clone());
        let final_status = get_status(&harness.base_url())?;
        replies.observe_status(&final_status)?;
        if !replies.all_terminal() {
            return Err(format!(
                "accepted replies remained nonterminal: {:?}",
                replies.accepted
            ));
        }
        let commands = final_status
            .get("commands")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("final status has no command ledger: {final_status}"))?;
        let terminal_record_counts = replies
            .accepted
            .keys()
            .map(|command_id| {
                let count = commands
                    .iter()
                    .filter(|command| {
                        command.get("command_id").and_then(Value::as_str)
                            == Some(command_id.as_str())
                            && command
                                .get("state")
                                .and_then(Value::as_str)
                                .is_some_and(|state| matches!(state, "succeeded" | "failed"))
                    })
                    .count();
                (command_id.clone(), count)
            })
            .collect::<BTreeMap<_, _>>();

        flush_control(&harness.base_url())?;
        harness.stop_child_gracefully(case_deadline)?;
        wait_for(
            case_deadline,
            &trace,
            "all worker processes stopped",
            || {
                let live = plan
                    .all_nodes
                    .iter()
                    .map(|node_id| process_count(run_id, *node_id))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .sum::<usize>();
                Ok((live == 0).then_some(()))
            },
        )?;
        let labeled_temp_resources = labeled_temp_resources(&harness, &plan.all_nodes);
        let mut http_failures = observed_http_failures();
        http_failures.extend(replies.http_failures.clone());

        Ok(OracleObservation {
            expected_survivors: plan.survivors,
            expected_stopped: plan.stopped,
            running_after_restart,
            stopped_after_restart,
            reachable_nodes: reachable,
            provider_resources: provider_snapshot.live,
            provider_created: provider_snapshot.created.into_iter().collect(),
            provider_destroyed: provider_snapshot.destroyed.into_iter().collect(),
            selected_offers: selected_offer_by_node,
            searched_offers: searched.into_iter().collect(),
            actor_before_replays,
            actor_after_replays,
            actor_initial,
            actor_after_teardown,
            terminal_record_counts,
            unexpected_process_exits: harness.unexpected_exits.clone(),
            http_failures,
            teardown_resources: final_provider.live,
            labeled_temp_resources,
        })
    })();

    let observation = execution.map_err(|error| {
        failure_with_diagnostics(
            error,
            &case,
            &trace,
            &replies,
            &census_snapshots,
            &harness,
            &provider,
        )
    })?;
    validate_oracle(&observation).map_err(|error| {
        failure_with_diagnostics(
            format!("{error}; observation={observation:#?}"),
            &case,
            &trace,
            &replies,
            &census_snapshots,
            &harness,
            &provider,
        )
    })?;
    if !provider
        .requests()
        .iter()
        .any(|request| request.method == "GET" && request.path == "/api/v0/bundles/")
    {
        return Err(failure_with_diagnostics(
            "provider search boundary was never exercised".to_owned(),
            &case,
            &trace,
            &replies,
            &census_snapshots,
            &harness,
            &provider,
        ));
    }

    let teardown_logs = harness.logs();
    let teardown_ledger = provider_ledger_snapshot(&harness.provider_ledger_path, harness.run_id);
    let provider_requests = provider.requests();
    drop(harness);
    drop(provider);
    temp.close().map_err(|error| {
        format!(
            "remove labeled E2E state directory: {error}; seed={}; actions={:#?}; \
             replies={:#?}; observation={observation:#?}; ledger={teardown_ledger:#?}; \
             provider_requests={provider_requests:#?}; logs={teardown_logs}",
            case.seed, case.actions, replies.public_replies
        )
    })?;
    Ok(())
}

proptest! {
    #![proptest_config(e2e_proptest_config())]

    #[test]
    #[ignore = "nightly process E2E; run with --ignored"]
    fn stateful_vastai_dashboard_control_survives_restarts(case in e2e_case()) {
        if let Err(error) = run_stateful_case(case) {
            prop_assert!(false, "{error}");
        }
    }
}

#[test]
fn e2e_oracle_rejects_controlled_lifecycle_faults() {
    let baseline = OracleObservation {
        expected_survivors: BTreeSet::from([2]),
        expected_stopped: BTreeSet::from([1]),
        running_after_restart: BTreeSet::from([2]),
        stopped_after_restart: BTreeSet::from([1]),
        reachable_nodes: BTreeSet::from([2]),
        provider_resources: vec![2],
        provider_created: BTreeSet::from([1, 2]),
        provider_destroyed: BTreeSet::from([1]),
        selected_offers: BTreeMap::from([(1, 101), (2, 102)]),
        searched_offers: BTreeSet::from([101, 102]),
        actor_before_replays: ActorCensus {
            actors: 10,
            orchestrator_actors: 5,
            worker_actors: 5,
            orchestrator_by_type: BTreeMap::from([("orchestrator".to_owned(), 5)]),
            workers_by_stream: BTreeMap::from([(
                "worker-2".to_owned(),
                BTreeMap::from([("worker".to_owned(), 5)]),
            )]),
            ..ActorCensus::default()
        },
        actor_after_replays: ActorCensus {
            actors: 10,
            orchestrator_actors: 5,
            worker_actors: 5,
            orchestrator_by_type: BTreeMap::from([("orchestrator".to_owned(), 5)]),
            workers_by_stream: BTreeMap::from([(
                "worker-2".to_owned(),
                BTreeMap::from([("worker".to_owned(), 5)]),
            )]),
            ..ActorCensus::default()
        },
        actor_initial: ActorCensus {
            actors: 5,
            orchestrator_actors: 5,
            orchestrator_by_type: BTreeMap::from([("orchestrator".to_owned(), 5)]),
            ..ActorCensus::default()
        },
        actor_after_teardown: ActorCensus {
            actors: 5,
            orchestrator_actors: 5,
            orchestrator_by_type: BTreeMap::from([("orchestrator".to_owned(), 5)]),
            ..ActorCensus::default()
        },
        terminal_record_counts: BTreeMap::from([("command-1".to_owned(), 1)]),
        unexpected_process_exits: Vec::new(),
        http_failures: Vec::new(),
        teardown_resources: Vec::new(),
        labeled_temp_resources: Vec::new(),
    };
    validate_oracle(&baseline).expect("baseline oracle observation");

    let mut survivor_destroy = baseline.clone();
    survivor_destroy.provider_resources.clear();
    survivor_destroy.provider_destroyed.insert(2);
    assert!(
        validate_oracle(&survivor_destroy)
            .unwrap_err()
            .contains("provider ledger mismatch")
    );

    let mut restart_loss = baseline.clone();
    restart_loss.running_after_restart.clear();
    assert!(
        validate_oracle(&restart_loss)
            .unwrap_err()
            .contains("restart-state loss")
    );
    let mut duplicate_provider = baseline.clone();
    duplicate_provider.provider_resources.push(2);
    assert!(
        validate_oracle(&duplicate_provider)
            .unwrap_err()
            .contains("duplicate provider resources")
    );

    let mut actor_poison = baseline.clone();
    actor_poison.actor_after_replays.poisoned = 1;
    assert!(
        validate_oracle(&actor_poison)
            .unwrap_err()
            .contains("actor poisoning")
    );
    let mut unexpected_exit = baseline.clone();
    unexpected_exit
        .unexpected_process_exits
        .push("exit status 1".to_owned());
    assert!(
        validate_oracle(&unexpected_exit)
            .unwrap_err()
            .contains("unexpected process exit")
    );

    let mut http_failure = baseline.clone();
    http_failure
        .http_failures
        .push("HTTP 503 or disconnect".to_owned());
    assert!(
        validate_oracle(&http_failure)
            .unwrap_err()
            .contains("HTTP failure or disconnect")
    );

    let mut duplicate_terminal = baseline.clone();
    duplicate_terminal
        .terminal_record_counts
        .insert("command-1".to_owned(), 2);
    assert!(
        validate_oracle(&duplicate_terminal)
            .unwrap_err()
            .contains("exactly one terminal record")
    );

    let mut actor_leak = baseline.clone();
    actor_leak.actor_after_replays.actors = 13;
    assert!(
        validate_oracle(&actor_leak)
            .unwrap_err()
            .contains("steady-state actor census")
    );
    let mut teardown_leak = baseline;
    teardown_leak.teardown_resources.push(2);
    teardown_leak
        .labeled_temp_resources
        .push(PathBuf::from("process-nodes.json"));
    assert!(
        validate_oracle(&teardown_leak)
            .unwrap_err()
            .contains("teardown leak")
    );
}
