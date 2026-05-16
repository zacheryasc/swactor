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

/// A vast.ai offer (GPU rental option) returned by [`find_offer`].
#[derive(Debug, Clone, Deserialize)]
pub struct Offer {
    pub id: u64,
    pub gpu_name: String,
    pub dph_total: f64,
    #[serde(default)]
    pub geolocation: Option<String>,
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
    let query = serde_json::json!({
        "gpu_name": {"eq": gpu_name},
        "rentable": {"eq": true},
        "rented": {"eq": false},
        "reliability2": {"gte": 0.99},
        "cuda_max_good": {"gte": 12.0},
        "verified": {"eq": true},
        "direct_port_count": {"gte": 1},
        "inet_down": {"gte": 100.0},
    });
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

    let candidates: Vec<Offer> = filtered
        .into_iter()
        .filter(|o| !exclude_ids.contains(&o.id))
        .collect();

    candidates
        .into_iter()
        .min_by(|a, b| a.dph_total.partial_cmp(&b.dph_total).unwrap())
        .ok_or_else(|| "no offers available (after geo/exclusion filter)".to_string())
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
        let resp = client
            .get(&url)
            .header("Authorization", format!("Bearer {api_key}"))
            .send()
            .await
            .map_err(|e| format!("wait_for_running request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("wait_for_running HTTP {status}: {body}"));
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
/// the new node knows where to join the SWIM cluster.
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
    let body = serde_json::json!({
        "image": image,
        "env": env,
        "onstart": "exec /usr/local/bin/pp-gpu-node 2>&1",
        "disk": 20,
    });

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
