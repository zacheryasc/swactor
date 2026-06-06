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

use std::io::{IsTerminal, Write};
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

/// Identifier for a rented vast.ai instance.
#[derive(Debug, Clone)]
pub struct InstanceInfo {
    pub contract_id: u64,
}

/// Cluster env forwarded to rented stage containers at create time.
///
/// The orchestrator-side caller of [`lease_chain`] builds this from its own
/// process env and `create_instance` injects it — plus the per-stage
/// `STAGE_INDEX` / `STAGE_COUNT` / `NODE_ROLE=stage` — into each container's env.
/// Currently this carries only the custom iroh relay URL: when the orchestrator
/// runs behind a custom relay, every stage must use the same one to reach the
/// SWIM cluster across the internet. Leaving `iroh_relay_url` `None` skips the
/// injection (the local/default-relay case).
#[derive(Debug, Clone, Default)]
pub struct StageEnv {
    pub iroh_relay_url: Option<String>,
}

impl StageEnv {
    /// Read the cluster env from the current process. `iroh_relay_url` comes
    /// from `ENV_IROH_RELAY_URL`; all-`None` when nothing is set.
    pub fn from_process_env() -> Self {
        fn nonempty(v: &str) -> Option<String> {
            std::env::var(v)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        }
        Self {
            iroh_relay_url: nonempty(crate::relay_config::ENV_IROH_RELAY_URL),
        }
    }

    /// `true` when there is anything worth propagating into stage container env.
    pub fn is_enabled(&self) -> bool {
        self.iroh_relay_url.is_some()
    }
}

/// A vast.ai offer (GPU rental option) returned by [`select_offer_pool`].
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
    /// vast.ai host verification state: `"verified"`, `"unverified"` (never
    /// tested), or `"deverified"` (was verified, then failed vast's checks).
    /// Deverified hosts recurrently fail CDI GPU-device injection at container
    /// start despite a high `reliability2`, so they are dropped by default.
    #[serde(default)]
    pub verification: Option<String>,
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
    // Bytes pulled so far while an image loads. vast reports -1 before the
    // container exists; once it does, this advances as the pull progresses, so
    // it (alongside `status_msg`) is our only signal that a slow node is still
    // making progress rather than stalled.
    #[serde(default)]
    disk_usage: Option<f64>,
}

/// Cost model for ranking offers on *true* lease cost rather than the $/hr
/// figure vast.ai sorts on. vast.ai bills image-pull bandwidth separately from
/// `dph_total`, so a host that is cheap-by-the-hour but gouges on download
/// bandwidth can cost more once a fat image pull is priced in.
///
/// Composable on purpose: today it folds in the one-time image-pull cost
/// (`image_GB * down_$/TB / 1000`). A future inter-stage / usage-bandwidth term
/// keyed off run parameters slots into [`CostModel::effective_price`] without
/// reworking selection.
#[derive(Debug, Clone, Default)]
pub struct CostModel {
    /// Deploy-image size in GB; `None` → image pull is not priced in.
    pub image_gb: Option<f64>,
}

impl CostModel {
    /// Build from the environment (`PP_IMAGE_SIZE_GB`).
    fn from_env() -> Self {
        Self {
            image_gb: env_image_size_gb(),
        }
    }

    /// One-time cost of pulling the deploy image to this offer's host.
    fn pull_cost(&self, o: &Offer) -> f64 {
        self.image_gb
            .map_or(0.0, |gb| gb * o.inet_down_cost_per_tb / 1000.0)
    }

    /// Effective hourly-equivalent price the selection ranks on: the listed
    /// $/hr plus the priced-in image pull. With `image_gb` unset this is just
    /// `dph_total`. (Extension slot: add an inter-stage bandwidth term here.)
    fn effective_price(&self, o: &Offer) -> f64 {
        o.dph_total + self.pull_cost(o)
    }
}

/// Rank a set of offers into the survivor pool: drop the suspiciously-cheap
/// tail *within each GPU model*, then merge and sort ascending by effective
/// price. Cheap-for-its-model has correlated with reliability failures (CDI
/// device-injection faults, hosts that load the image then stop) that the
/// vast.ai reliability score does not capture, so the bottom slice of each
/// model is trimmed.
///
/// The drop is `floor(drop_frac * group_len)` per model, so tiny groups (1–3
/// offers) are kept intact rather than wiped — a model with a single offer
/// keeps it.
fn rank_survivors(offers: Vec<Offer>, cost: &CostModel, drop_frac: f64) -> Vec<Offer> {
    let mut by_model: std::collections::HashMap<String, Vec<Offer>> =
        std::collections::HashMap::new();
    for o in offers {
        by_model.entry(o.gpu_name.clone()).or_default().push(o);
    }
    let price = |o: &Offer| cost.effective_price(o);
    let by_price = |a: &Offer, b: &Offer| {
        price(a)
            .partial_cmp(&price(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    };
    let mut survivors: Vec<Offer> = Vec::new();
    for (_model, mut group) in by_model {
        group.sort_by(&by_price);
        let drop = (drop_frac * group.len() as f64).floor() as usize;
        survivors.extend(group.into_iter().skip(drop));
    }
    survivors.sort_by(&by_price);
    survivors
}

/// The next pool offer to lease: the cheapest survivor (the pool is pre-sorted
/// ascending) that has not already been tried and whose physical host is not
/// already claimed by this lease. `host_id == None` can't be deduped, so such
/// offers are always eligible. Returns `None` once the pool is exhausted.
fn next_eligible_offer<'a>(
    pool: &'a [Offer],
    tried_offer_ids: &[u64],
    used_host_ids: &std::collections::HashSet<u64>,
) -> Option<&'a Offer> {
    pool.iter().find(|o| {
        !tried_offer_ids.contains(&o.id)
            && o.host_id.map_or(true, |h| !used_host_ids.contains(&h))
    })
}

/// The offers [`lease_chain`] would rent on the happy path: the cheapest
/// `num_stages` on distinct hosts, in stage order (stage 0 = cheapest). Mirrors
/// the distinct-host draw in [`next_eligible_offer`] (a `None` host id is never
/// deduped, matching that helper). Actual picks can differ only if a create
/// fails and the chain falls through to the next survivor — so the confirmed
/// cost is the floor, not a ceiling.
fn plan_picks(pool: &[Offer], num_stages: u32) -> Vec<&Offer> {
    let mut picks: Vec<&Offer> = Vec::with_capacity(num_stages as usize);
    let mut used: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for o in pool {
        if picks.len() == num_stages as usize {
            break;
        }
        if let Some(h) = o.host_id {
            if !used.insert(h) {
                continue; // host already claimed by an earlier pick
            }
        }
        picks.push(o);
    }
    picks
}

/// `PP_ASSUME_YES`: skip the interactive lease confirmation (for scripted / CI
/// runs that intend to rent without a human at the keyboard). Truthy = any
/// non-empty value other than `0` / `false` / `no`.
fn assume_yes() -> bool {
    std::env::var("PP_ASSUME_YES")
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            !v.is_empty() && v != "0" && v != "false" && v != "no"
        })
        .unwrap_or(false)
}

/// Print the planned lease + its hourly cost and, when running interactively,
/// require an explicit `y`/`N` before any instance is created.
///
/// This is the cost guardrail: the offer search can legitimately land on an
/// expensive card when the cheap pool is thin, and an unconfirmed lease once put
/// a 3-stage run onto an A100. The prompt is the last gate before money is spent.
///
/// To keep scripted runs and the (HTTP-mocked) test suite unaffected, the
/// confirmation is *skipped* (the lease proceeds) when `PP_ASSUME_YES` is set or
/// when stdin is not a TTY — there is no human to answer in those cases. The cost
/// summary is always logged regardless.
fn confirm_lease(pool: &[Offer], num_stages: u32, cost: &CostModel) -> Result<(), String> {
    let picks = plan_picks(pool, num_stages);
    let total_dph: f64 = picks.iter().map(|o| o.dph_total).sum();
    let total_eff: f64 = picks.iter().map(|o| cost.effective_price(o)).sum();

    eprintln!(
        "pp-orchestrator: lease plan — {num_stages} stage(s), cheapest on distinct hosts:"
    );
    for (i, o) in picks.iter().enumerate() {
        eprintln!(
            "  stage {i}  {:<14} {:>8}  ${:.3}/hr  [{}]  host {}",
            o.gpu_name,
            o.gpu_ram
                .map(|r| format!("{:.0}MB", r))
                .unwrap_or_else(|| "?MB".into()),
            o.dph_total,
            o.geolocation.as_deref().unwrap_or("?"),
            o.host_id
                .map(|h| h.to_string())
                .unwrap_or_else(|| "?".into()),
        );
    }
    if picks.len() < num_stages as usize {
        eprintln!(
            "  WARNING: only {} distinct-host offer(s) available for {num_stages} stage(s) — \
             the lease will likely fail to fill the chain.",
            picks.len(),
        );
    }
    let eff_note = if (total_eff - total_dph).abs() > 1e-6 {
        format!("   (image-pull priced in: ${total_eff:.3}/hr eff)")
    } else {
        String::new()
    };
    eprintln!(
        "  TOTAL  ${total_dph:.3}/hr  (~${:.2}/day){eff_note}",
        total_dph * 24.0,
    );

    if assume_yes() {
        eprintln!("pp-orchestrator: PP_ASSUME_YES set — proceeding without confirmation");
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        eprintln!(
            "pp-orchestrator: stdin is not a TTY — proceeding without interactive confirmation \
             (set PP_ASSUME_YES=1 to silence this)"
        );
        return Ok(());
    }

    eprint!("Proceed with renting these {num_stages} instance(s)? [y/N]: ");
    let _ = std::io::stderr().flush();
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|e| format!("failed to read lease confirmation: {e}"))?;
    let ans = line.trim().to_ascii_lowercase();
    if ans == "y" || ans == "yes" {
        Ok(())
    } else {
        Err("operator declined the lease (cost not confirmed); no instances were created".into())
    }
}

/// Build the ranked survivor pool for a heterogeneous PP lease in a single
/// query. Replaces the old per-stage `find_offer` / `find_offer_chain`.
///
/// Policy: from all rentable offers at/above the VRAM threshold that are
/// reachable and pass the quality gates, drop the cheapest `PP_DROP_CHEAP_FRAC`
/// per GPU model, then return the full list ranked ascending by effective
/// price. The caller leases the N cheapest survivors on distinct physical hosts
/// (see [`next_eligible_offer`]); returning the whole pool lets the lease's
/// resilience layer draw replacements without re-querying.
pub async fn select_offer_pool(
    client: &Client,
    base_url: &str,
    api_key: &str,
    gpu_name: &str,
    num_stages: u32,
) -> Result<Vec<Offer>, String> {
    // Hard gates expressed server-side. reliability2 (PP_MIN_RELIABILITY,
    // default 0.95 — "semi-reliable", NOT near-perfect) and cuda_max_good >=
    // 12.6 (our CUDA-12.6 base image) drop hosts that recurrently fail
    // container init / CDI device injection; num_gpus == 1 keeps us from
    // renting a multi-GPU rig per stage. Network speed can't be probed before
    // renting, so we trust vast.ai's measured inet figures and gate on a
    // configurable minimum (PP_MIN_INET_DOWN_MBPS, default 100; the upload
    // gate is off by default).
    let mut query = serde_json::json!({
        "rentable": {"eq": true},
        "rented": {"eq": false},
        "reliability2": {"gte": env_min_reliability()},
        "cuda_max_good": {"gte": 12.6},
        "direct_port_count": {"gte": 1},
        "num_gpus": {"eq": 1},
        "inet_down": {"gte": env_min_inet_down_mbps()},
        // vast.ai treats `limit` as a SCAN BUDGET (machines examined in the
        // engine's default high-perf-first order), NOT a result cap: a small
        // limit returns *fewer* matches because it never reaches the cheap
        // commodity hosts that rank low. Empirically `limit:512` returned ~184
        // ram>=8000 offers while `limit:5000` returned ~1639 — the missing
        // ~1450 included the cheap 3090/3060 supply, so a small limit alone
        // skews the pool toward datacenter cards. Set high so the survivor pool
        // reflects the whole market.
        "limit": 5000,
    });
    if let Some(up) = env_min_inet_up_mbps() {
        query["inet_up"] = serde_json::json!({"gte": up});
    }
    // vast.ai's `verified` flag means the host passed vast's own datacenter
    // vetting. AND'd with the other gates it discarded ~90% of supply — almost
    // every cheap consumer 3090/3060 is unverified — so it is OFF by default and
    // reliability2 (above) carries the quality floor. Opt back in with
    // PP_REQUIRE_VERIFIED=1 for a vetted-hosts-only pool.
    if env_require_verified() {
        query["verified"] = serde_json::json!({"eq": true});
    }
    // GPU selection is two independent, optional filters — neither is required.
    // A VRAM floor (PP_GPU_MIN_RAM_MB) spans a heterogeneous card set (each PP
    // stage is an independent process exchanging fp16 hidden state, so stages
    // need not share a model — only enough VRAM for their block slice). A model
    // pin (PP_GPU / `gpu_name`) restricts to one model. Unset both → the GPU
    // itself isn't filtered and the quality gates above + cost ranking pick the
    // host.
    if let Some(min_ram) = env_min_gpu_ram_mb() {
        query["gpu_ram"] = serde_json::json!({"gte": min_ram});
    }
    if !gpu_name.is_empty() {
        query["gpu_name"] = serde_json::json!({"eq": gpu_name});
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
        .map_err(|e| format!("select_offer_pool request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("select_offer_pool HTTP {status}: {body}"));
    }

    let body: SearchResponse = resp
        .json()
        .await
        .map_err(|e| format!("select_offer_pool parse failed: {e}"))?;

    // Post-filter in Rust: drop unknown/Chinese geolocations (Docker Hub and
    // iroh relays are unreachable from behind the Great Firewall), any
    // blacklisted host (providers caught gouging on bandwidth), and — by
    // default — `deverified` hosts. vast.ai deverifies a host after it fails
    // vast's own checks; in practice these recurrently fail CDI GPU-device
    // injection at container start ("unresolvable CDI devices …/gpu=0") even
    // though their `reliability2` stays ~0.99, which is why the reliability
    // gate alone does not catch them. `unverified` (never-tested) hosts are
    // kept — they hold the cheap consumer-GPU supply and usually start fine.
    // PP_REQUIRE_VERIFIED already restricts the query to verified-only, in
    // which case this filter is a no-op.
    let blacklist = blacklisted_host_ids();
    let reachable: Vec<Offer> = body
        .offers
        .into_iter()
        .filter(|o| {
            o.geolocation
                .as_deref()
                .map_or(false, |g| !g.to_uppercase().contains("CN"))
        })
        .filter(|o| o.host_id.map_or(true, |h| !blacklist.contains(&h)))
        .filter(|o| o.verification.as_deref() != Some("deverified"))
        .collect();

    let cost = CostModel::from_env();
    let drop_frac = env_drop_cheap_frac();
    let pool = rank_survivors(reachable, &cost, drop_frac);

    if pool.is_empty() {
        return Err(
            "no offers available (after quality/geo/host-blacklist filters and cheap-tail drop)"
                .to_string(),
        );
    }
    // One audit line: how big the survivor pool is and what the cheapest
    // survivor costs once bandwidth is priced in. Per-stage picks are logged in
    // provision_stage as they are leased.
    eprintln!(
        "select_offer_pool: {} survivor(s) for {num_stages} stage(s) after \
         per-model {:.0}% cheap-drop (cheapest ${:.3}/hr eff)",
        pool.len(),
        drop_frac * 100.0,
        cost.effective_price(&pool[0]),
    );
    Ok(pool)
}

/// `PP_GPU_MIN_RAM_MB`: when set to a positive integer, adds a VRAM floor
/// (`gpu_ram >= N` MB) to the offer search, enabling a heterogeneous cluster.
/// Independent of the optional `PP_GPU` model pin; unset / blank / zero → no
/// VRAM filter.
fn env_min_gpu_ram_mb() -> Option<u64> {
    std::env::var("PP_GPU_MIN_RAM_MB")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
}

/// `PP_MIN_INET_DOWN_MBPS`: minimum vast.ai-reported download speed (Mbps) an
/// offer must advertise. Network throughput can't be probed before renting, so
/// the lease trusts vast's measured figure and gates on it. Default 100 (the
/// historical hardcoded floor); 0 disables the gate.
fn env_min_inet_down_mbps() -> f64 {
    std::env::var("PP_MIN_INET_DOWN_MBPS")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|&v| v >= 0.0)
        .unwrap_or(100.0)
}

/// `PP_MIN_RELIABILITY`: minimum vast.ai `reliability2` an offer must carry.
/// The old hardcoded 0.995, combined with the `verified` gate, admitted almost
/// only datacenter rigs (A100/H100) — every cheap consumer 3090/3060 sits at
/// 0.95–0.99 and/or is unverified, so the two gates AND'd together left zero
/// cheap cards and the lease was forced onto an expensive datacenter card.
/// Default 0.95 ("semi-reliable"); clamped to [0, 1]; 0 disables the gate.
fn env_min_reliability() -> f64 {
    std::env::var("PP_MIN_RELIABILITY")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|&v| (0.0..=1.0).contains(&v))
        .unwrap_or(0.95)
}

/// `PP_REQUIRE_VERIFIED`: when truthy (`1`/`true`/`yes`, case-insensitive),
/// restrict the search to vast.ai-verified hosts. Default off — the verified
/// flag AND'd with the other gates excluded nearly all cheap consumer GPUs, so
/// reliability2 carries the quality floor and unvetted hosts are admitted.
fn env_require_verified() -> bool {
    std::env::var("PP_REQUIRE_VERIFIED")
        .ok()
        .map(|s| matches!(s.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

/// `PP_MIN_INET_UP_MBPS`: optional minimum reported upload speed (Mbps).
/// Default unset / 0 → no upload-speed gate.
fn env_min_inet_up_mbps() -> Option<f64> {
    std::env::var("PP_MIN_INET_UP_MBPS")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|&v| v > 0.0)
}

/// `PP_DROP_CHEAP_FRAC`: fraction of the cheapest offers to drop *within each
/// GPU model* before leasing (cheap-for-its-model has correlated with
/// reliability failures). Applied as a floor per model, so tiny groups survive.
/// Default 0.30; clamped to [0, 0.99].
fn env_drop_cheap_frac() -> f64 {
    std::env::var("PP_DROP_CHEAP_FRAC")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .map(|v| v.clamp(0.0, 0.99))
        .unwrap_or(0.30)
}

/// `PP_LEASE_PACE_MS`: delay between successive stage provisions in
/// [`lease_chain`] Phase 1, in milliseconds. Keeps the lease's request rate
/// under the vast.ai endpoint throttle (~4.5 req/s). Default 600ms.
fn lease_pace() -> Duration {
    let ms = std::env::var("PP_LEASE_PACE_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(600);
    Duration::from_millis(ms)
}

/// `PP_MAX_REPLACE_ATTEMPTS`: how many times [`lease_chain`] will destroy a
/// stage that never reached `running` and re-lease a replacement before giving
/// up. Default 3 (the historical hardcoded value). **0 disables replacement**:
/// a stage that fails to come up is reported and the lease fails without
/// re-leasing — the stand-down switch for a slow host where churning on
/// replacements is worse than waiting (tune `max_polls`/poll interval instead).
fn env_max_replace_attempts() -> u32 {
    std::env::var("PP_MAX_REPLACE_ATTEMPTS")
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(3)
}

/// `PP_PULL_STALL_SECS`: how long an instance may sit in a loading state with
/// **no** change to `status_msg` or `disk_usage` before [`wait_for_running`]
/// declares it stalled and returns early. A node whose pull is still advancing
/// rides out to the poll limit and is never killed for merely being slow.
/// Default 180s; 0 disables stall detection (only the poll limit bounds the
/// wait).
fn env_pull_stall_secs() -> u64 {
    std::env::var("PP_PULL_STALL_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(180)
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
    let stall = Duration::from_secs(env_pull_stall_secs());
    let stall_enabled = !stall.is_zero();

    // Progress tracking: a slow-but-advancing node must ride out to the poll
    // limit, never killed for merely being slow. We reset `progress_since`
    // whenever status_msg or disk_usage changes; only when neither has moved
    // for `stall` do we declare the node stalled. `state_since` is just for the
    // elapsed-in-current-state figure in the log.
    let mut state_since = std::time::Instant::now();
    let mut progress_since = std::time::Instant::now();
    let mut last_state: Option<String> = None;
    let mut last_msg: Option<String> = None;
    let mut last_disk: Option<f64> = None;

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

        // Did the node make any progress since the last poll? Either the
        // status message advanced (e.g. "Pulling from ...") or bytes landed
        // on disk.
        let msg = status.status_msg.clone();
        let disk = status.disk_usage;
        if msg != last_msg || disk != last_disk {
            progress_since = std::time::Instant::now();
        }
        if last_state.as_deref() != Some(actual) {
            state_since = std::time::Instant::now();
        }
        last_state = Some(actual.to_string());
        last_msg = msg.clone();
        last_disk = disk;

        let stalled = stall_enabled && progress_since.elapsed() >= stall;
        let in_state = state_since.elapsed().as_secs();
        let msg_disp = match msg.as_deref() {
            Some(m) if !m.is_empty() => format!(" msg=\"{m}\""),
            _ => String::new(),
        };
        let disk_disp = match disk {
            Some(d) if d >= 0.0 => format!(" disk={d:.2}GB"),
            _ => String::new(),
        };
        eprintln!(
            "  contract {contract_id} poll {}/{max_polls}: status={actual} in-state={in_state}s {}{msg_disp}{disk_disp}",
            poll + 1,
            if stalled { "STALLED" } else { "progressing" },
        );

        if let Some(m) = &msg {
            if m.contains("Error") || m.contains("failed") {
                return Err(format!("instance {contract_id} error: {m}"));
            }
        }
        if intended == "stopped" && actual != "running" {
            return Err(format!(
                "instance {contract_id} stopped: {}",
                msg.unwrap_or_default()
            ));
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
                // A genuinely hung load (no status_msg/disk_usage movement for
                // `stall`) fails fast; a still-advancing one keeps waiting up
                // to max_polls.
                if stalled {
                    return Err(format!(
                        "instance {contract_id} stalled in '{actual}' for {}s with no status_msg/disk_usage progress",
                        progress_since.elapsed().as_secs()
                    ));
                }
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
/// the new node knows where to join the SWIM cluster. When `stage_env` carries
/// a custom iroh relay URL, it is injected too so the rented container reaches
/// the cluster across the internet — see [`StageEnv`].
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
    stage_env: Option<&StageEnv>,
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
    // Pin this stage's iroh identity so it survives a restart: re-read from
    // PID 1's env on restart, the stage keeps the same node id and the
    // pipeline name registry stays valid. See
    // pp-worker::stage_secret_from_env.
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
    if let Some(stage_env) = stage_env {
        if let Some(relay_url) = stage_env.iroh_relay_url.as_deref() {
            env[crate::relay_config::ENV_IROH_RELAY_URL] =
                serde_json::Value::String(relay_url.to_string());
        }
    }
    let mut body = serde_json::json!({
        "image": image,
        "env": env,
        // Run the PID-1 supervisor (not the worker directly): it brings up
        // sshd deterministically and keeps the container — and the shell —
        // alive if the worker crashes. No `exec` of the worker: the supervisor
        // owns PID 1 and runs pp-worker as a child.
        "onstart": "/usr/local/bin/pp_entrypoint.sh 2>&1",
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
    stage_env: Option<&StageEnv>,
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
            stage_env,
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
/// keep no local cluster state, so teardown rediscovers the cluster through
/// this call. Returns the SSH endpoint per instance so an operator can
/// scp/ssh to a node for a manual binary/worker swap.
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

/// Lease one instance for `stage` by drawing from the shared ranked `pool`:
/// take the next survivor that is neither already tried (`tried_offer_ids`,
/// which it appends to) nor on a host already claimed by this lease
/// (`used_host_ids`, which it updates on a successful create), then create.
/// Retries with the next eligible survivor when a create is throttled (429) or
/// the offer was snatched between select and create. Returns the created
/// [`InstanceInfo`], or an error once the pool is exhausted or
/// `MAX_CREATE_ATTEMPTS` is hit. Pulls this stage's pinned identity from
/// `stage_secrets[stage]` so a replacement keeps the same node id.
#[allow(clippy::too_many_arguments)]
async fn provision_stage(
    client: &Client,
    base_url: &str,
    api_key: &str,
    pool: &[Offer],
    stage: u32,
    num_stages: u32,
    seed_addr: &str,
    seed_relay: Option<&str>,
    image: &str,
    label: Option<&str>,
    stage_secrets: Option<&[String]>,
    stage_env: Option<&StageEnv>,
    tried_offer_ids: &mut Vec<u64>,
    // Host ids already leased by this chain. Distinct-host selection is
    // unconditional: two offers on the same host_id share the same NAT'd public
    // endpoint, so two stages must never land on one machine.
    used_host_ids: &mut std::collections::HashSet<u64>,
) -> Result<InstanceInfo, String> {
    const MAX_CREATE_ATTEMPTS: u32 = 5;
    let cost = CostModel::from_env();
    let mut last_err: Option<String> = None;
    for attempt in 1..=MAX_CREATE_ATTEMPTS {
        let offer = match next_eligible_offer(pool, tried_offer_ids, used_host_ids) {
            Some(o) => o.clone(),
            None => {
                // Genuine exhaustion (every survivor is tried or on a used
                // host), not a rate signal — let the caller roll back.
                last_err = Some(format!(
                    "pool exhausted for stage {stage} (no untried offer on an unused host)"
                ));
                break;
            }
        };
        tried_offer_ids.push(offer.id);
        // One line per stage so a heterogeneous lease is auditable: which
        // physical card the stage landed on, its $/hr, and the effective cost
        // it was ranked on (with bandwidth priced in when PP_IMAGE_SIZE_GB set).
        eprintln!(
            "lease_chain: stage {stage} → offer {} — {} {} @ ${:.3}/hr [{}] host {} eff ${:.3}/hr",
            offer.id,
            offer.gpu_name,
            offer
                .gpu_ram
                .map(|r| format!("{:.0}MB", r))
                .unwrap_or_else(|| "?MB".into()),
            offer.dph_total,
            offer.geolocation.as_deref().unwrap_or("?"),
            offer
                .host_id
                .map(|h| h.to_string())
                .unwrap_or_else(|| "?".into()),
            cost.effective_price(&offer),
        );

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
            stage_env,
        )
        .await
        {
            Ok(info) => {
                if let Some(h) = offer.host_id {
                    used_host_ids.insert(h);
                }
                return Ok(info);
            }
            Err(e) => {
                eprintln!(
                    "lease_chain: stage {stage} create on offer {} failed (attempt {attempt}/{MAX_CREATE_ATTEMPTS}): {e}",
                    offer.id,
                );
                // Back off before drawing the next survivor. A 429 means the
                // endpoint is throttling (threshold ~4.5 req/s) and needs a
                // longer pause; a snatched offer (no_such_ask) just needs the
                // next candidate.
                let is_429 = e.contains("429") || e.contains("Too Many Requests");
                last_err = Some(e);
                if attempt < MAX_CREATE_ATTEMPTS {
                    let backoff = if is_429 {
                        Duration::from_millis(2000 * attempt as u64)
                    } else {
                        Duration::from_millis(400)
                    };
                    tokio::time::sleep(backoff).await;
                }
                // Next iteration draws the next eligible survivor; the tried id
                // is already excluded.
            }
        }
    }
    Err(format!(
        "stage {stage} could not be created after {MAX_CREATE_ATTEMPTS} attempts: {}",
        last_err.unwrap_or_default(),
    ))
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
    stage_env: Option<&StageEnv>,
) -> Result<Vec<InstanceInfo>, String> {
    // One query builds the whole ranked survivor pool up front, instead of a
    // search per stage — fewer requests (less 429 pressure) and one consistent
    // candidate set that both provisioning phases draw from. Both phases share
    // the cursor state below so replacements never reuse an offer or a host.
    let pool = select_offer_pool(client, base_url, api_key, gpu_name, num_stages)
        .await
        .map_err(|e| format!("lease_chain: {e}"))?;

    // Cost guardrail: show what we're about to rent and (interactively) require
    // a y/N before spending money. Runs before any create_instance, so a decline
    // is a clean abort with nothing leased. Skipped for non-TTY / PP_ASSUME_YES.
    confirm_lease(&pool, num_stages, &CostModel::from_env())?;

    let mut tried_offer_ids: Vec<u64> = Vec::new();
    let mut created: Vec<InstanceInfo> = Vec::with_capacity(num_stages as usize);
    // Host ids leased by this chain, owned here so it survives across both
    // provisioning phases. Distinct-host selection is unconditional.
    let mut used_host_ids: std::collections::HashSet<u64> =
        std::collections::HashSet::new();

    // Phase 1 — provision every stage (draw from the pool + create, with
    // per-stage retry on a snatched/throttled create).
    for stage in 0..num_stages {
        match provision_stage(
            client,
            base_url,
            api_key,
            &pool,
            stage,
            num_stages,
            seed_addr,
            seed_relay,
            image,
            label,
            stage_secrets,
            stage_env,
            &mut tried_offer_ids,
            &mut used_host_ids,
        )
        .await
        {
            Ok(info) => {
                created.push(info);
            }
            Err(e) => {
                rollback(client, base_url, api_key, &created).await;
                return Err(format!("lease_chain: {e}"));
            }
        }
        // Pace successive stages. Each provision is a search + a create; firing
        // 2*num_stages requests in a tight burst trips the endpoint's ~4.5 req/s
        // throttle even when no individual create fails. Sleeping between stages
        // keeps the happy-path lease under the threshold. Tunable via
        // PP_LEASE_PACE_MS (default 600ms); the last stage needn't wait.
        if stage + 1 < num_stages {
            tokio::time::sleep(lease_pace()).await;
        }
    }

    // Phase 2 — wait for each instance to reach `running`. A host that stops
    // after loading the image must not abort the lease: destroy it and
    // re-provision the SAME stage slot (same stage index + pinned identity)
    // on a fresh offer, up to PP_MAX_REPLACE_ATTEMPTS, before giving up.
    // PP_MAX_REPLACE_ATTEMPTS=0 disables replacement entirely (a failed stage
    // fails the lease without churning on a slow host).
    let max_replace_attempts = env_max_replace_attempts();
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
                    if replaced > max_replace_attempts {
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
                             {max_replace_attempts} replacement(s); last error: {e}"
                        ));
                    }
                    eprintln!(
                        "lease_chain: replacing stage {stage} (replacement {replaced}/{max_replace_attempts})"
                    );
                    match provision_stage(
                        client,
                        base_url,
                        api_key,
                        &pool,
                        stage,
                        num_stages,
                        seed_addr,
                        seed_relay,
                        image,
                        label,
                        stage_secrets,
                        stage_env,
                        &mut tried_offer_ids,
                        &mut used_host_ids,
                    )
                    .await
                    {
                        // Loop re-waits on the replacement instance.
                        Ok(info) => {
                            created[idx] = info;
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn offer(id: u64, gpu: &str, dph: f64, host: u64) -> Offer {
        Offer {
            id,
            gpu_name: gpu.to_string(),
            dph_total: dph,
            gpu_ram: Some(24576.0),
            geolocation: Some("US".to_string()),
            inet_down_cost_per_tb: 0.0,
            inet_up_cost_per_tb: 0.0,
            host_id: Some(host),
            verification: Some("verified".to_string()),
        }
    }

    #[test]
    fn cheap_tail_dropped_per_model_then_ranked_by_price() {
        // Model A has 5 offers → drop floor(0.3*5)=1 cheapest. Model B has 3 →
        // drop floor(0.3*3)=0, so the tiny group survives intact.
        let mut offers = Vec::new();
        for (i, p) in [0.10, 0.11, 0.12, 0.13, 0.14].iter().enumerate() {
            offers.push(offer(100 + i as u64, "A", *p, 100 + i as u64));
        }
        for (i, p) in [0.20, 0.21, 0.22].iter().enumerate() {
            offers.push(offer(200 + i as u64, "B", *p, 200 + i as u64));
        }
        let pool = rank_survivors(offers, &CostModel::default(), 0.30);

        let ids: Vec<u64> = pool.iter().map(|o| o.id).collect();
        assert_eq!(pool.len(), 7, "5 A (drop 1) + 3 B (drop 0) survivors");
        assert!(!ids.contains(&100), "the single cheapest A offer is dropped");
        assert!(
            ids.contains(&200),
            "model B's cheapest survives — its group is too small to drop any",
        );
        let prices: Vec<f64> = pool.iter().map(|o| o.dph_total).collect();
        assert!(
            prices.windows(2).all(|w| w[0] <= w[1]),
            "merged pool must be sorted ascending by price: {prices:?}",
        );
    }

    #[test]
    fn bandwidth_cost_reorders_a_cheap_per_hour_but_gouging_host_below_a_free_one() {
        // X is cheaper per hour but charges $40/TB download; Y is pricier per
        // hour but has free bandwidth. A 20GB image pull (=$0.80) flips the order.
        let x = Offer {
            inet_down_cost_per_tb: 40.0,
            ..offer(1, "Z", 0.10, 1)
        };
        let y = offer(2, "Z", 0.12, 2);
        let priced = CostModel {
            image_gb: Some(20.0),
        };

        let with_bw = rank_survivors(vec![x.clone(), y.clone()], &priced, 0.0);
        assert_eq!(
            with_bw[0].id, 2,
            "free-bandwidth Y ranks first once the pull is priced in",
        );

        let without_bw = rank_survivors(vec![x, y], &CostModel::default(), 0.0);
        assert_eq!(
            without_bw[0].id, 1,
            "on $/hr alone the cheaper-per-hour X ranks first",
        );
    }

    #[test]
    fn distinct_host_draw_never_repeats_a_host_and_skips_cheaper_same_host_offers() {
        // Pool ascending by price; offers 1 and 2 share host 10. Simulate the
        // lease's draw loop: pick, mark id tried + host used, repeat.
        let pool = vec![
            offer(1, "A", 0.10, 10),
            offer(2, "A", 0.11, 10),
            offer(3, "A", 0.12, 20),
            offer(4, "A", 0.13, 30),
        ];
        let mut tried: Vec<u64> = Vec::new();
        let mut used: HashSet<u64> = HashSet::new();
        let mut leased_hosts: Vec<u64> = Vec::new();
        for _ in 0..3 {
            let o = next_eligible_offer(&pool, &tried, &used).expect("three distinct hosts exist");
            tried.push(o.id);
            used.insert(o.host_id.unwrap());
            leased_hosts.push(o.host_id.unwrap());
        }
        let mut distinct = leased_hosts.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            3,
            "the three leased hosts must be distinct, got {leased_hosts:?}",
        );
        assert!(
            !tried.contains(&2),
            "the cheaper second offer on already-used host 10 must never be leased",
        );
    }

    #[test]
    fn distinct_host_draw_exhausts_when_every_remaining_offer_shares_a_used_host() {
        // Both offers sit on host 10, so only one distinct host is leasable.
        let pool = vec![offer(1, "A", 0.10, 10), offer(2, "A", 0.11, 10)];
        let mut tried: Vec<u64> = Vec::new();
        let mut used: HashSet<u64> = HashSet::new();
        let first = next_eligible_offer(&pool, &tried, &used).expect("first draw succeeds");
        tried.push(first.id);
        used.insert(first.host_id.unwrap());
        assert!(
            next_eligible_offer(&pool, &tried, &used).is_none(),
            "no second distinct host is available",
        );
    }
}
