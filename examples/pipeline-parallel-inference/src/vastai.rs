//! vast.ai REST API client for pipeline-parallel inference.
//!
//! Forked from `examples/single-gpu-inference/src/vastai.rs`. The single-instance
//! `create_instance` is extended to carry `STAGE` and `NUM_STAGES` env vars (in
//! addition to `SEED_ADDR` / `SEED_RELAY`). On top of that, this module adds two
//! orchestration helpers:
//!
//! - [`create_pipeline_instances`] rents one instance per stage, passing
//!   `STAGE=i` and `NUM_STAGES=N` through to each. If any creation fails after
//!   one or more have already succeeded, the already-created instances are
//!   destroyed best-effort before the error is returned.
//! - [`destroy_all_instances`] issues a DELETE per contract id and reports
//!   per-id results. A failure on one id does not prevent the attempt on the
//!   rest — every id is always tried.
//!
//! All functions accept a `base_url` so tests can point at a wiremock server.

use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

/// Identifier for a rented vast.ai instance.
#[derive(Debug, Clone)]
pub struct InstanceInfo {
    pub contract_id: u64,
}

/// Diagnostics env-var bundle forwarded to rented stage containers.
///
/// `pp-gpu-node::diag::install_from_env` reads `SWACTOR_DIAG_*` on boot
/// inside each container to decide whether to enable the aggregator and
/// where to ship to. The orchestrator-side caller of [`lease_chain`]
/// builds this from its own process env (typically the same vars the
/// orchestrator itself read), and `create_instance` injects them — plus
/// the per-stage `STAGE_INDEX` / `STAGE_COUNT` / `NODE_ROLE=stage` — into
/// each container's env on creation. Leaving `collector_url` `None`
/// disables the whole forwarding path; rented containers then start with
/// no `SWACTOR_DIAG_*` vars and run as if diagnostics were off.
#[derive(Debug, Clone, Default)]
pub struct DiagEnv {
    pub collector_url: Option<String>,
    pub run_id: Option<String>,
    pub udp_echo: Option<String>,
    pub iroh_relay_url: Option<String>,
}

impl DiagEnv {
    /// Read the standard `SWACTOR_DIAG_*` vars from the current process
    /// env. Returns an instance with all fields `None` when nothing is
    /// set — callers can still pass it and `create_instance` will skip
    /// the injection.
    pub fn from_process_env() -> Self {
        fn nonempty(v: &str) -> Option<String> {
            std::env::var(v)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        }
        Self {
            collector_url: nonempty("SWACTOR_DIAG_COLLECTOR_URL"),
            run_id: nonempty("SWACTOR_DIAG_RUN_ID"),
            udp_echo: nonempty("SWACTOR_DIAG_UDP_ECHO"),
            iroh_relay_url: nonempty(crate::relay_config::ENV_IROH_RELAY_URL),
        }
    }

    /// `true` when there is anything worth propagating into stage container
    /// env. A custom iroh relay alone (no collector URL) is enough — that
    /// path is what makes the cluster come up; collector-only is the
    /// observability path.
    pub fn is_enabled(&self) -> bool {
        self.collector_url.is_some() || self.iroh_relay_url.is_some()
    }

    /// Override the run_id that will be injected into every rented
    /// stage's `SWACTOR_DIAG_RUN_ID`. Used by the orchestrator's
    /// `--hold` path to pin the held cluster's run_id to whatever ends
    /// up in the on-disk cluster handle (rather than whatever happens
    /// to be in the operator's shell env at lease time).
    pub fn with_run_id(mut self, run_id: String) -> Self {
        self.run_id = Some(run_id);
        self
    }
}

/// A vast.ai offer (GPU rental option) returned by [`find_offer`].
#[derive(Debug, Clone, Deserialize)]
pub struct Offer {
    pub id: u64,
    pub gpu_name: String,
    pub dph_total: f64,
    #[serde(default)]
    pub gpu_ram: Option<f64>,
    #[serde(default)]
    pub geolocation: Option<String>,
    /// Inbound bandwidth price ($/TB). vast.ai bills *downloads to the
    /// instance* — i.e. every Docker image pull — at this rate, and it is
    /// excluded from `dph_total`, so a cheap-by-the-hour host can still
    /// double the bill on a fat image. Defaults to 0.0 if the offer omits it.
    #[serde(default, rename = "internet_down_cost_per_tb")]
    pub inet_down_cost_per_tb: f64,
    /// Outbound bandwidth price ($/TB). Surfaced alongside the download price
    /// so neither direction is hidden; uploads are usually negligible here.
    #[serde(default, rename = "internet_up_cost_per_tb")]
    pub inet_up_cost_per_tb: f64,
    /// Marketplace host that owns the machine. Used to blacklist providers
    /// that gouge on bandwidth, since bandwidth price is a per-host policy.
    #[serde(default)]
    pub host_id: Option<u64>,
}

/// Connection details for a running instance.
#[derive(Debug, Clone)]
pub struct RunningInstance {
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    offers: Vec<Offer>,
}

#[derive(Debug, Deserialize)]
struct CreateResponse {
    new_contract: u64,
}

#[derive(Debug, Deserialize)]
struct InstanceResponse {
    instances: InstanceStatus,
}

#[derive(Debug, Deserialize)]
struct InstanceStatus {
    actual_status: Option<String>,
    intended_status: Option<String>,
    #[serde(default)]
    status_msg: Option<String>,
    #[serde(default)]
    public_ipaddr: Option<String>,
    #[serde(default)]
    ssh_port: Option<u16>,
}

/// Find the cheapest offer matching a GPU type, excluding specific offer IDs.
///
/// Forked from `single-gpu-inference::vastai::find_offer` — same filters
/// (rentable, reliability, CUDA version, direct ports, inet speed, geo).
pub async fn find_offer(
    client: &Client,
    base_url: &str,
    api_key: &str,
    gpu_name: &str,
    exclude_ids: &[u64],
) -> Result<Offer, String> {
    // Filter goals beyond "rentable, fast, verified":
    //   - reliability2 >= 0.995 (>= 0.99 still surfaces hosts that recurrently
    //     fail container init; tightening shrinks the candidate pool to
    //     hosts with very few historical job failures).
    //   - cuda_max_good >= 12.6 matches our CUDA-12.6 base image. Cheaper
    //     offers without a modern host CUDA stack were the source of the
    //     `unresolvable CDI devices` failures we saw earlier.
    // The cheap-card selector is normally `gpu_name == <model>` (e.g. "RTX
    // 3060"), which implicitly bounds cost because that model is cheap. When
    // `PP_GPU_MIN_RAM_MB` is set we instead select by VRAM so the pipeline can
    // span a *heterogeneous* set of cards: every PP stage is an independent
    // process exchanging fp16 hidden state over the wire, so stages need not
    // share a GPU model — only enough VRAM to hold their block slice. A VRAM
    // filter alone would pull in datacenter GPUs (4090/A100/H100…) and wreck
    // the median-cost pick, so `PP_GPU_MAX_DPH` caps $/hr to keep the pool in
    // the same cheap band the single-model filter gave us, and `num_gpus == 1`
    // keeps us from renting (and paying for) a multi-GPU rig per stage.
    // Everything else — reliability, CUDA floor, verified, ports, inet, the
    // non-CN geo filter, and the median-priced pick below — is identical to
    // the single-model path.
    let mut query = serde_json::json!({
        "rentable": {"eq": true},
        "rented": {"eq": false},
        "reliability2": {"gte": 0.995},
        "cuda_max_good": {"gte": 12.6},
        "verified": {"eq": true},
        "direct_port_count": {"gte": 1},
        "inet_down": {"gte": 100.0},
    });
    match env_min_gpu_ram_mb() {
        Some(min_ram) => {
            query["gpu_ram"] = serde_json::json!({"gte": min_ram});
            query["num_gpus"] = serde_json::json!({"eq": 1});
            if let Some(max_dph) = env_max_dph() {
                query["dph_total"] = serde_json::json!({"lte": max_dph});
            }
        }
        None => {
            query["gpu_name"] = serde_json::json!({"eq": gpu_name});
        }
    }
    let url = format!(
        "{base_url}/api/v0/bundles/?q={}",
        urlencoding::encode(&query.to_string())
    );
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("find_offer request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("find_offer HTTP {status}: {body}"));
    }

    let body: SearchResponse = resp
        .json()
        .await
        .map_err(|e| format!("find_offer parse failed: {e}"))?;

    // Filter out hosts with unknown or Chinese geolocation — Docker Hub
    // and iroh relays are unreachable from behind the Great Firewall.
    let filtered: Vec<Offer> = body
        .offers
        .into_iter()
        .filter(|o| {
            o.geolocation
                .as_deref()
                .map_or(false, |g| !g.to_uppercase().contains("CN"))
        })
        .collect();

    let blacklist = blacklisted_host_ids();
    let mut candidates: Vec<Offer> = filtered
        .into_iter()
        .filter(|o| !exclude_ids.contains(&o.id))
        .filter(|o| o.host_id.map_or(true, |h| !blacklist.contains(&h)))
        .collect();
    if candidates.is_empty() {
        return Err(
            "no offers available (after geo/host-blacklist/exclusion filter)".to_string(),
        );
    }

    // Pick the median-priced offer rather than the cheapest. Cheap RTX 4090
    // offers on vast.ai have been consistently failing CDI device injection
    // at container start (per-instance dynamic CDI specs written too late
    // or with mismatched shas — the reliability score does not reflect
    // these container-runtime failures because they happen before the job
    // starts running). The median strikes a balance: it skips the bottom
    // tier of misconfigured hosts without paying for the most expensive
    // ones in the candidate set.
    // Effective price = $/hr plus the amortized-as-one-time image-pull cost
    // (`image_GB * down_$/TB / 1000`) when PP_IMAGE_SIZE_GB is set. This makes
    // the median pick rank on true cost: a host that is cheap per-hour but
    // charges $40/TB sorts below a free-bandwidth host once a 20GB pull is
    // priced in. With the flag unset, `pull_cost` is 0 and this is the old
    // dph_total ordering.
    let image_gb = env_image_size_gb();
    let pull_cost = |o: &Offer| image_gb.map_or(0.0, |gb| gb * o.inet_down_cost_per_tb / 1000.0);
    let effective = |o: &Offer| o.dph_total + pull_cost(o);
    candidates.sort_by(|a, b| effective(a).partial_cmp(&effective(b)).unwrap());
    let median_idx = candidates.len() / 2;
    let n_candidates = candidates.len();
    let picked = candidates.swap_remove(median_idx);
    // One line per stage (N small) so a heterogeneous lease is auditable: which
    // physical card each stage landed on and what it costs. Silent in the
    // single-model path too — handy when a lease picks an unexpected host.
    // When PP_IMAGE_SIZE_GB is set, append the priced-in one-time image pull so
    // the chosen $/hr and the cost it was actually ranked on are both visible.
    let pull_note = image_gb.map_or(String::new(), |gb| {
        format!(" +${:.2} pull ({:.0}GB)", pull_cost(&picked), gb)
    });
    eprintln!(
        "find_offer: selected offer {} — {} {} @ ${:.3}/hr [{}] \
         bw ${:.2}/TB down ${:.2}/TB up{} (median of {} candidates)",
        picked.id,
        picked.gpu_name,
        picked
            .gpu_ram
            .map(|r| format!("{:.0}MB", r))
            .unwrap_or_else(|| "?MB".into()),
        picked.dph_total,
        picked.geolocation.as_deref().unwrap_or("?"),
        picked.inet_down_cost_per_tb,
        picked.inet_up_cost_per_tb,
        pull_note,
        n_candidates,
    );
    Ok(picked)
}

/// `PP_GPU_MIN_RAM_MB`: when set to a positive integer, the offer search
/// selects cards by VRAM (`gpu_ram >= N` MB) instead of by exact GPU model,
/// enabling a heterogeneous cluster. Unset / blank / zero → model-name mode
/// (the historical default, unchanged).
fn env_min_gpu_ram_mb() -> Option<u64> {
    std::env::var("PP_GPU_MIN_RAM_MB")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
}

/// `PP_GPU_MAX_DPH`: optional $/hr cap, applied only in VRAM-filter mode, to
/// keep the heterogeneous pool in the cheap band (otherwise datacenter GPUs
/// dominate the median-cost pick). Unset → no cap.
fn env_max_dph() -> Option<f64> {
    std::env::var("PP_GPU_MAX_DPH")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|&v| v > 0.0)
}

/// Hosts blacklisted regardless of env: providers caught gouging on bandwidth.
/// host 59017 (machine 26024, Texas) lists $40/TB download — a ~20GB image pull
/// costs ~$0.80/instance there, which doubled an earlier run's bill.
const BLACKLISTED_HOST_IDS: &[u64] = &[59017];

/// `PP_BLACKLIST_HOSTS`: optional comma-separated host ids to exclude, merged
/// with the always-on [`BLACKLISTED_HOST_IDS`]. Blank/garbage entries ignored.
fn blacklisted_host_ids() -> std::collections::HashSet<u64> {
    let mut set: std::collections::HashSet<u64> = BLACKLISTED_HOST_IDS.iter().copied().collect();
    if let Ok(raw) = std::env::var("PP_BLACKLIST_HOSTS") {
        set.extend(raw.split(',').filter_map(|s| s.trim().parse::<u64>().ok()));
    }
    set
}

/// `PP_IMAGE_SIZE_GB`: optional size of the deploy image, in GB. When set, the
/// one-time cost of pulling the image (`image_GB * down_$/TB / 1000`) is folded
/// into each offer's effective price so the median pick is ranked on true cost,
/// not just $/hr — a host that is cheap-by-the-hour but gouges on bandwidth
/// sorts down accordingly. Unset / non-positive → rank on `dph_total` alone.
fn env_image_size_gb() -> Option<f64> {
    std::env::var("PP_IMAGE_SIZE_GB")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|&v| v > 0.0)
}

/// Poll vast.ai until the instance reaches `running`, then extract IP + port.
/// Returns error immediately on terminal statuses like `exited`.
pub async fn wait_for_running(
    client: &Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
    poll_interval: Duration,
    max_polls: u32,
) -> Result<RunningInstance, String> {
    let url = format!("{base_url}/api/v0/instances/{contract_id}/");

    for poll in 0..max_polls {
        let resp = match client
            .get(&url)
            .header("Authorization", format!("Bearer {api_key}"))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                // A network blip is transient: re-poll rather than abort. The
                // caller treats a wait_for_running error as "this host is dead"
                // and tears the instance down, so failing over one dropped
                // request would needlessly kill a healthy, still-loading node.
                eprintln!(
                    "  contract {contract_id} poll {}/{max_polls}: request error: {e} (retrying)",
                    poll + 1,
                );
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        if !resp.status().is_success() {
            // 429 (rate-limit under the request burst of an N-node lease) and
            // 5xx are transient; re-poll instead of declaring the instance
            // dead. The loop is bounded by max_polls, so a persistently failing
            // endpoint still terminates with the poll-limit error below.
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            eprintln!(
                "  contract {contract_id} poll {}/{max_polls}: HTTP {status} (retrying): {}",
                poll + 1,
                body.chars().take(80).collect::<String>(),
            );
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        let wrapper: InstanceResponse = resp
            .json()
            .await
            .map_err(|e| format!("wait_for_running parse failed: {e}"))?;
        let status = wrapper.instances;

        let actual = status.actual_status.as_deref().unwrap_or("unknown");
        let intended = status.intended_status.as_deref().unwrap_or("unknown");
        eprintln!(
            "  contract {contract_id} poll {}/{}: status={actual}",
            poll + 1,
            max_polls
        );

        if let Some(msg) = &status.status_msg {
            if msg.contains("Error") || msg.contains("failed") {
                return Err(format!("instance {contract_id} error: {msg}"));
            }
        }
        if intended == "stopped" && actual != "running" {
            let msg = status.status_msg.unwrap_or_default();
            return Err(format!("instance {contract_id} stopped: {msg}"));
        }

        match actual {
            "running" => {
                let ip = status
                    .public_ipaddr
                    .unwrap_or_else(|| "unknown".to_string());
                let port = status.ssh_port.unwrap_or(0);
                return Ok(RunningInstance { ip, port });
            }
            "exited" | "error" => {
                return Err(format!(
                    "instance {contract_id} reached terminal status: {actual}"
                ));
            }
            _ => {
                tokio::time::sleep(poll_interval).await;
            }
        }
    }

    Err(format!(
        "instance {contract_id} did not reach running within poll limit"
    ))
}

/// Request a log download URL for a running instance.
pub async fn request_logs(
    client: &Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<String, String> {
    let url = format!("{base_url}/api/v0/instances/request_logs/{contract_id}/");
    let resp = client
        .put(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("request_logs failed: {e}"))?;
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("request_logs parse failed: {e}"))?;
    body["result_url"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "no result_url in log response".to_string())
}

/// Fetch log text from a vast.ai log URL. Sleeps briefly so the host has time
/// to upload the log after `request_logs`.
pub async fn fetch_logs(client: &Client, log_url: &str) -> Result<String, String> {
    tokio::time::sleep(Duration::from_secs(5)).await;
    let resp = client
        .get(log_url)
        .send()
        .await
        .map_err(|e| format!("fetch_logs failed: {e}"))?;
    resp.text()
        .await
        .map_err(|e| format!("fetch_logs read failed: {e}"))
}

/// Create one vast.ai instance from `offer_id`, passing pipeline env vars.
///
/// `stage` and `num_stages` are forwarded as `STAGE` / `NUM_STAGES` so the
/// worker can compute its layer range on boot. `seed_addr` is forwarded so
/// the new node knows where to join the SWIM cluster. When `diag_env` is
/// `Some(..)` with a collector URL set, the corresponding `SWACTOR_DIAG_*`
/// vars are also added so the rented container reports into the same
/// diagnostics bundle as the orchestrator — see [`DiagEnv`].
pub async fn create_instance(
    client: &Client,
    base_url: &str,
    api_key: &str,
    offer_id: u64,
    stage: u32,
    num_stages: u32,
    seed_addr: &str,
    seed_relay: Option<&str>,
    image: &str,
    label: Option<&str>,
    stage_secret: Option<&str>,
    diag_env: Option<&DiagEnv>,
) -> Result<InstanceInfo, String> {
    let url = format!("{base_url}/api/v0/asks/{offer_id}/");
    let mut env = serde_json::json!({
        "SEED_ADDR": seed_addr,
        "STAGE": stage.to_string(),
        "NUM_STAGES": num_stages.to_string(),
    });
    if let Some(relay) = seed_relay {
        env["SEED_RELAY"] = serde_json::Value::String(relay.to_string());
    }
    // Pin this stage's iroh identity so it survives an in-place redeploy
    // bounce: re-read from PID 1's env on restart, the stage keeps the same
    // node id and the pipeline name registry stays valid. See
    // pp-gpu-node::stage_secret_from_env.
    if let Some(secret) = stage_secret {
        env["PP_STAGE_SECRET"] = serde_json::Value::String(secret.to_string());
    }
    // Pass through select orchestrator-side env to every rented stage so a
    // smoke run can flip e.g. stub mode or python interpreter without
    // rebuilding the docker image. Whitelist (not pass-everything) keeps
    // the container payload predictable.
    for var in ["PP_WORKER_STUB", "PYTHON", "MODEL", "CUDA", "MAX_TOKENS"] {
        if let Ok(v) = std::env::var(var) {
            if !v.trim().is_empty() {
                env[var] = serde_json::Value::String(v);
            }
        }
    }
    if let Some(diag) = diag_env {
        if let Some(url) = diag.collector_url.as_deref() {
            env["SWACTOR_DIAG_COLLECTOR_URL"] = serde_json::Value::String(url.to_string());
            env["SWACTOR_DIAG_NODE_ROLE"] = serde_json::Value::String("stage".to_string());
            env["SWACTOR_DIAG_STAGE_INDEX"] = serde_json::Value::String(stage.to_string());
            env["SWACTOR_DIAG_STAGE_COUNT"] =
                serde_json::Value::String(num_stages.to_string());
            if let Some(run_id) = diag.run_id.as_deref() {
                env["SWACTOR_DIAG_RUN_ID"] = serde_json::Value::String(run_id.to_string());
            }
            if let Some(echo) = diag.udp_echo.as_deref() {
                env["SWACTOR_DIAG_UDP_ECHO"] = serde_json::Value::String(echo.to_string());
            }
        }
        if let Some(relay_url) = diag.iroh_relay_url.as_deref() {
            env[crate::relay_config::ENV_IROH_RELAY_URL] =
                serde_json::Value::String(relay_url.to_string());
        }
    }
    let mut body = serde_json::json!({
        "image": image,
        "env": env,
        "onstart": "exec /usr/local/bin/pp-gpu-node 2>&1",
        // Every stage fetch()s the FULL gguf (whole file mmap'd by
        // from_gguf), regardless of which layers it runs. qwen3:30b-a3b
        // Q4_K_M is ~18 GB; with the ~4 GB CUDA-runtime image that
        // overruns the old 20 GB allotment. 30 GB leaves headroom for
        // the tinygrad kernel cache. Raising disk shrinks the offer pool
        // slightly — acceptable at reliability2>=0.995.
        "disk": 30,
    });
    // A vast.ai-native label tags the whole cluster so it is discoverable
    // later via `list_instances_by_label` (and `vastai show instances`)
    // without us keeping any local state — vast.ai is the registry.
    if let Some(l) = label {
        body["label"] = serde_json::Value::String(l.to_string());
    }

    let resp = client
        .put(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("create_instance request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("create_instance HTTP {status}: {body}"));
    }

    let parsed: CreateResponse = resp
        .json()
        .await
        .map_err(|e| format!("create_instance parse failed: {e}"))?;
    Ok(InstanceInfo {
        contract_id: parsed.new_contract,
    })
}

/// Destroy one vast.ai instance by contract id.
///
/// Returns `Err` if the server responds with a non-success HTTP status, so
/// callers can distinguish a failed teardown from a successful one. The
/// best-effort cleanup wrapper [`destroy_all_instances`] still continues past
/// such errors.
pub async fn destroy_instance(
    client: &Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<(), String> {
    let url = format!("{base_url}/api/v0/instances/{contract_id}/");
    let resp = client
        .delete(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("destroy_instance request failed: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "destroy_instance {contract_id} HTTP {status}: {body}"
        ));
    }
    Ok(())
}

/// Rent one instance per stage. `offer_ids[i]` becomes the host of stage `i`.
///
/// `NUM_STAGES` is `offer_ids.len()`. The creates are issued sequentially in
/// index order so `received_requests()` ordering is deterministic for tests
/// and so we know exactly which contracts to roll back on partial failure.
///
/// If any single create fails, every instance already created in this call
/// is destroyed best-effort (per [`destroy_all_instances`]) before the
/// failure is propagated to the caller. The destroy errors themselves are
/// discarded — they would only mask the original creation failure that the
/// caller actually needs to see.
pub async fn create_pipeline_instances(
    client: &Client,
    base_url: &str,
    api_key: &str,
    offer_ids: &[u64],
    seed_addr: &str,
    seed_relay: Option<&str>,
    image: &str,
    diag_env: Option<&DiagEnv>,
) -> Result<Vec<InstanceInfo>, String> {
    let num_stages = offer_ids.len() as u32;
    let mut created: Vec<InstanceInfo> = Vec::with_capacity(offer_ids.len());

    for (i, &offer_id) in offer_ids.iter().enumerate() {
        let stage = i as u32;
        match create_instance(
            client,
            base_url,
            api_key,
            offer_id,
            stage,
            num_stages,
            seed_addr,
            seed_relay,
            image,
            None,
            None,
            diag_env,
        )
        .await
        {
            Ok(info) => created.push(info),
            Err(e) => {
                let ids: Vec<u64> = created.iter().map(|c| c.contract_id).collect();
                let _ = destroy_all_instances(client, base_url, api_key, &ids).await;
                return Err(format!(
                    "create_pipeline_instances failed at stage {stage}: {e}"
                ));
            }
        }
    }

    Ok(created)
}

/// Destroy every contract in `contract_ids` and return per-id results in the
/// same order. Always attempts every id, even if earlier ones returned `Err`.
pub async fn destroy_all_instances(
    client: &Client,
    base_url: &str,
    api_key: &str,
    contract_ids: &[u64],
) -> Vec<Result<(), String>> {
    let mut results = Vec::with_capacity(contract_ids.len());
    for &id in contract_ids {
        results.push(destroy_instance(client, base_url, api_key, id).await);
    }
    results
}

/// SSH endpoint + identity of a held instance, discovered by label.
#[derive(Debug, Clone)]
pub struct LabeledInstance {
    pub contract_id: u64,
    /// vast.ai SSH proxy host (e.g. `ssh5.vast.ai`); empty if not yet assigned.
    pub ssh_host: String,
    pub ssh_port: u16,
    pub public_ipaddr: String,
    pub actual_status: String,
}

#[derive(Debug, Deserialize)]
struct InstanceListResponse {
    instances: Vec<InstanceListEntry>,
}

#[derive(Debug, Deserialize)]
struct InstanceListEntry {
    id: u64,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    actual_status: Option<String>,
    #[serde(default)]
    ssh_host: Option<String>,
    #[serde(default)]
    ssh_port: Option<u16>,
    #[serde(default)]
    public_ipaddr: Option<String>,
}

/// List every instance on the account tagged with `label`, sorted by
/// contract id. vast.ai is the source of truth for "what's rented" — we
/// keep no local cluster state, so attach/redeploy/teardown all rediscover
/// the cluster through this call. Returns the SSH endpoint per instance so
/// the caller can scp/ssh to redeploy in place.
pub async fn list_instances_by_label(
    client: &Client,
    base_url: &str,
    api_key: &str,
    label: &str,
) -> Result<Vec<LabeledInstance>, String> {
    let url = format!("{base_url}/api/v0/instances/");
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("list_instances request failed: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("list_instances HTTP {status}: {body}"));
    }
    let body: InstanceListResponse = resp
        .json()
        .await
        .map_err(|e| format!("list_instances parse failed: {e}"))?;
    let mut out: Vec<LabeledInstance> = body
        .instances
        .into_iter()
        .filter(|e| e.label.as_deref() == Some(label))
        .map(|e| LabeledInstance {
            contract_id: e.id,
            ssh_host: e.ssh_host.unwrap_or_default(),
            ssh_port: e.ssh_port.unwrap_or(0),
            public_ipaddr: e.public_ipaddr.unwrap_or_default(),
            actual_status: e.actual_status.unwrap_or_else(|| "unknown".to_string()),
        })
        .collect();
    out.sort_by_key(|i| i.contract_id);
    Ok(out)
}

/// Find `num_stages` distinct offers for the same GPU type. Each call to
/// [`find_offer`] excludes every offer id returned by the previous calls,
/// so the result is `num_stages` pairwise-distinct offers.
///
/// If fewer than `num_stages` matching offers exist, the call that runs
/// out propagates the [`find_offer`] error to the caller (no rollback is
/// needed — nothing was created).
pub async fn find_offer_chain(
    client: &Client,
    base_url: &str,
    api_key: &str,
    gpu_name: &str,
    num_stages: u32,
) -> Result<Vec<Offer>, String> {
    let mut chosen: Vec<Offer> = Vec::with_capacity(num_stages as usize);
    for i in 0..num_stages {
        let exclude: Vec<u64> = chosen.iter().map(|o| o.id).collect();
        match find_offer(client, base_url, api_key, gpu_name, &exclude).await {
            Ok(o) => chosen.push(o),
            Err(e) => {
                return Err(format!(
                    "find_offer_chain: offer {}/{} for {gpu_name} not available: {e}",
                    i + 1,
                    num_stages,
                ));
            }
        }
    }
    Ok(chosen)
}

/// Destroy one contract, retrying on transient failures (HTTP 429 rate-limit,
/// 5xx, network blips). Rollback and stage-replacement paths use this so a
/// throttled DELETE does not silently strand a billing instance — the bug that
/// orphaned a Tesla T4 when a 12-node lease rolled back during a 429 storm.
async fn destroy_instance_with_retry(
    client: &Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<(), String> {
    const ATTEMPTS: u32 = 5;
    let mut last = String::new();
    for attempt in 1..=ATTEMPTS {
        match destroy_instance(client, base_url, api_key, contract_id).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = e;
                // Back off proportionally; the endpoint threshold is ~4.5 req/s.
                tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
            }
        }
    }
    Err(format!(
        "destroy {contract_id} failed after {ATTEMPTS} attempts: {last}"
    ))
}

/// Best-effort teardown of every contract created so far, used on the lease's
/// failure paths. Each destroy is retried (see [`destroy_instance_with_retry`])
/// so a 429 storm during rollback cannot leave a billing orphan. Logs but does
/// not propagate errors — the caller is already returning the original failure.
async fn rollback(client: &Client, base_url: &str, api_key: &str, created: &[InstanceInfo]) {
    for info in created {
        if let Err(e) =
            destroy_instance_with_retry(client, base_url, api_key, info.contract_id).await
        {
            eprintln!(
                "lease_chain: WARNING rollback could not destroy {}: {e}",
                info.contract_id
            );
        }
    }
}

/// Find an offer for `stage` (excluding everything in `tried_offer_ids`, which
/// it appends to) and create one instance, retrying with the next-best offer
/// when a create is throttled (429) or the offer was snatched between select
/// and create. Returns the created [`InstanceInfo`], or an error after
/// `MAX_CREATE_ATTEMPTS`. Pulls this stage's pinned identity from
/// `stage_secrets[stage]` so a replacement keeps the same node id.
#[allow(clippy::too_many_arguments)]
async fn provision_stage(
    client: &Client,
    base_url: &str,
    api_key: &str,
    gpu_name: &str,
    stage: u32,
    num_stages: u32,
    seed_addr: &str,
    seed_relay: Option<&str>,
    image: &str,
    label: Option<&str>,
    stage_secrets: Option<&[String]>,
    diag_env: Option<&DiagEnv>,
    tried_offer_ids: &mut Vec<u64>,
    // PROTOTYPE_PREFLIGHT_HF (spec §5.2): host ids already claimed by
    // earlier stages in this chain. Used (and updated) only when the
    // preflight gate is enabled.
    used_host_ids: &mut std::collections::HashSet<u64>,
) -> Result<InstanceInfo, String> {
    const MAX_CREATE_ATTEMPTS: u32 = 5;
    let preflight = prototype_preflight_hf::enabled();
    let mut last_err: Option<String> = None;
    for attempt in 1..=MAX_CREATE_ATTEMPTS {
        let offer = match find_offer(client, base_url, api_key, gpu_name, tried_offer_ids).await {
            Ok(o) => o,
            Err(e) => {
                last_err = Some(format!("find_offer for stage {stage}: {e}"));
                break;
            }
        };
        tried_offer_ids.push(offer.id);

        // PROTOTYPE_PREFLIGHT_HF (spec §5.2): "Candidate chains MUST
        // be filtered such that no two chain slots share the same
        // public network endpoint (e.g. the offer's public IP)." The
        // offer doesn't carry a resolved public IP yet, but `host_id`
        // (the physical machine) is the public-endpoint proxy: two
        // offers on the same host_id share the same NAT'd public IP.
        // Gate is off by default → behavior unchanged.
        if preflight {
            if let Some(h) = offer.host_id {
                if used_host_ids.contains(&h) {
                    eprintln!(
                        "PROTOTYPE_PREFLIGHT_HF: stage {stage} skipping offer {} \
                         on host {h} (already used by an earlier chain slot)",
                        offer.id,
                    );
                    last_err = Some(format!(
                        "preflight rejected offer {} (host {h} already in chain)",
                        offer.id,
                    ));
                    continue;
                }
            }
        }

        match create_instance(
            client,
            base_url,
            api_key,
            offer.id,
            stage,
            num_stages,
            seed_addr,
            seed_relay,
            image,
            label,
            stage_secrets
                .and_then(|ss| ss.get(stage as usize))
                .map(|s| s.as_str()),
            diag_env,
        )
        .await
        {
            Ok(info) => {
                if preflight {
                    if let Some(h) = offer.host_id {
                        used_host_ids.insert(h);
                    }
                }
                return Ok(info);
            }
            Err(e) => {
                eprintln!(
                    "lease_chain: stage {stage} create on offer {} failed (attempt {attempt}/{MAX_CREATE_ATTEMPTS}): {e}",
                    offer.id,
                );
                last_err = Some(e);
                // Try the next-best offer; the loop excludes the already-tried
                // id via `tried_offer_ids`.
            }
        }
    }
    Err(format!(
        "stage {stage} could not be created after {MAX_CREATE_ATTEMPTS} attempts: {}",
        last_err.unwrap_or_default(),
    ))
}

// ─── PROTOTYPE_PREFLIGHT_HF (spec §5.2) ──────────────────────────────
//
// Pre-deploy host-throughput probe scaffolding. Disabled by default —
// when `PP_PREFLIGHT_HF` is unset or "0", host selection behaves as it
// does today (spec §5.2 gate clause).
//
// When enabled:
//   - Candidate chains are filtered so that no two slots share the
//     same `host_id` (public-network-endpoint proxy, spec §5.2).
//   - The active per-host ranged-GET throughput probe is a MAY per
//     spec §5.2 and is currently a no-op stub: we expose the
//     threshold + sample-size knobs so future re-implementation has
//     a stable surface area, but the orchestrator does NOT issue
//     speculative leases just to probe. A future implementation
//     would brief-lease a candidate, ssh-curl the model URL with
//     `--range 0-PP_PREFLIGHT_HF_SAMPLE_MB`, and reject if measured
//     throughput < `PP_PREFLIGHT_HF_MIN_MBPS`.
//
// Removal criterion (spec §5.2): when a per-host quality data layer
// exists outside this code path, delete this module, the
// `used_host_ids` argument on `provision_stage`, the local set
// threaded through `lease_chain`, and the `PP_PREFLIGHT_HF*` env
// vars from any deploy docs.
pub mod prototype_preflight_hf {
    /// Spec §5.2 gate. True when `PP_PREFLIGHT_HF` is set to anything
    /// other than empty, `0`, or `false` (case-insensitive).
    pub fn enabled() -> bool {
        match std::env::var("PP_PREFLIGHT_HF") {
            Ok(v) => {
                let v = v.trim();
                !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
            }
            Err(_) => false,
        }
    }

    /// Minimum acceptable measured throughput in MB/s. Hosts whose
    /// measured throughput is below this MUST be rejected (spec §5.2).
    /// Default 50 MB/s.
    pub fn min_mbps() -> f64 {
        std::env::var("PP_PREFLIGHT_HF_MIN_MBPS")
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(50.0)
    }

    /// Per-host probe sample size in MB. Spec §5.2 says "default
    /// sample size on the order of tens of MB". Default 20 MB.
    pub fn sample_mb() -> u64 {
        std::env::var("PP_PREFLIGHT_HF_SAMPLE_MB")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(20)
    }
}

/// Rent `num_stages` vast.ai instances and wait for each to reach `running`.
/// Yields N running contracts or rolls everything back.
///
/// An N-node heterogeneous lease routinely meets a flaky host, so the helper is
/// resilient by construction:
/// * each stage's create retries with the next-best offer on a 429 / snatched
///   offer (see [`provision_stage`]);
/// * a stage whose instance never reaches `running` (e.g. a host that loads the
///   image, then stops) is destroyed and re-provisioned on a fresh offer — up
///   to `MAX_REPLACE_ATTEMPTS` — rather than aborting the whole lease;
/// * on give-up, every instance created during this call is rolled back with
///   retried destroys (see [`rollback`]) so nothing is left billing.
#[allow(clippy::too_many_arguments)]
pub async fn lease_chain(
    client: &Client,
    base_url: &str,
    api_key: &str,
    gpu_name: &str,
    num_stages: u32,
    seed_addr: &str,
    seed_relay: Option<&str>,
    image: &str,
    label: Option<&str>,
    stage_secrets: Option<&[String]>,
    poll_interval: Duration,
    max_polls: u32,
    diag_env: Option<&DiagEnv>,
) -> Result<Vec<InstanceInfo>, String> {
    let mut tried_offer_ids: Vec<u64> = Vec::new();
    let mut created: Vec<InstanceInfo> = Vec::with_capacity(num_stages as usize);
    // PROTOTYPE_PREFLIGHT_HF (spec §5.2): host ids in use by this
    // chain. Only consulted when the gate is on; the set is owned
    // here so it survives across both provisioning phases.
    let mut used_host_ids: std::collections::HashSet<u64> =
        std::collections::HashSet::new();

    // Phase 1 — provision every stage (find + create, with per-stage retry).
    for stage in 0..num_stages {
        match provision_stage(
            client,
            base_url,
            api_key,
            gpu_name,
            stage,
            num_stages,
            seed_addr,
            seed_relay,
            image,
            label,
            stage_secrets,
            diag_env,
            &mut tried_offer_ids,
            &mut used_host_ids,
        )
        .await
        {
            Ok(info) => created.push(info),
            Err(e) => {
                rollback(client, base_url, api_key, &created).await;
                return Err(format!("lease_chain: {e}"));
            }
        }
    }

    // Phase 2 — wait for each instance to reach `running`. A host that stops
    // after loading the image must not abort the lease: destroy it and
    // re-provision the SAME stage slot (same stage index + pinned identity)
    // on a fresh offer, up to MAX_REPLACE_ATTEMPTS, before giving up.
    const MAX_REPLACE_ATTEMPTS: u32 = 3;
    for stage in 0..num_stages {
        let idx = stage as usize;
        let mut replaced: u32 = 0;
        loop {
            let cid = created[idx].contract_id;
            match wait_for_running(client, base_url, api_key, cid, poll_interval, max_polls).await {
                Ok(_) => break,
                Err(e) => {
                    eprintln!(
                        "lease_chain: stage {stage} contract {cid} did not reach running: {e}"
                    );
                    // Tear down the dead instance (retried, so a 429 can't orphan it).
                    if let Err(de) =
                        destroy_instance_with_retry(client, base_url, api_key, cid).await
                    {
                        eprintln!(
                            "lease_chain: WARNING could not destroy dead contract {cid}: {de}"
                        );
                    }
                    replaced += 1;
                    if replaced > MAX_REPLACE_ATTEMPTS {
                        // Give up on this stage; roll back the survivors (cid is
                        // already destroyed, so exclude it).
                        let survivors: Vec<InstanceInfo> = created
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| *i != idx)
                            .map(|(_, c)| c.clone())
                            .collect();
                        rollback(client, base_url, api_key, &survivors).await;
                        return Err(format!(
                            "lease_chain: stage {stage} never reached running after \
                             {MAX_REPLACE_ATTEMPTS} replacements; last error: {e}"
                        ));
                    }
                    eprintln!(
                        "lease_chain: replacing stage {stage} (replacement {replaced}/{MAX_REPLACE_ATTEMPTS})"
                    );
                    match provision_stage(
                        client,
                        base_url,
                        api_key,
                        gpu_name,
                        stage,
                        num_stages,
                        seed_addr,
                        seed_relay,
                        image,
                        label,
                        stage_secrets,
                        diag_env,
                        &mut tried_offer_ids,
                        &mut used_host_ids,
                    )
                    .await
                    {
                        // Loop re-waits on the replacement instance.
                        Ok(info) => created[idx] = info,
                        Err(pe) => {
                            let survivors: Vec<InstanceInfo> = created
                                .iter()
                                .enumerate()
                                .filter(|(i, _)| *i != idx)
                                .map(|(_, c)| c.clone())
                                .collect();
                            rollback(client, base_url, api_key, &survivors).await;
                            return Err(format!(
                                "lease_chain: stage {stage} replacement could not be provisioned: {pe}"
                            ));
                        }
                    }
                }
            }
        }
    }

    Ok(created)
}
