//! vast.ai REST API client for the smoke test orchestrator.
//!
//! Functions: find_offer, create_instance, wait_for_running, destroy_instance.
//! All functions accept a `base_url` parameter so tests can point at a mock server.

use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Offer {
    pub id: u64,
    pub gpu_name: String,
    pub dph_total: f64,
    #[serde(default)]
    pub geolocation: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InstanceInfo {
    pub contract_id: u64,
}

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

/// Create a vast.ai instance from an offer, passing SEED_ADDR and SEED_RELAY in the env.
pub async fn create_instance(
    client: &Client,
    base_url: &str,
    api_key: &str,
    offer_id: u64,
    seed_addr: &str,
    seed_relay: Option<&str>,
    image: &str,
) -> Result<InstanceInfo, String> {
    let url = format!("{base_url}/api/v0/asks/{offer_id}/");
    let mut env = serde_json::json!({ "SEED_ADDR": seed_addr });
    if let Some(relay) = seed_relay {
        env["SEED_RELAY"] = serde_json::Value::String(relay.to_string());
    }
    let body = serde_json::json!({
        "image": image,
        "env": env,
        "onstart": "exec /usr/local/bin/gpu-node 2>&1",
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
        eprintln!("  poll {}/{}: status={actual}", poll + 1, max_polls);

        // Check for error in status_msg (host-side failures like OCI errors)
        if let Some(msg) = &status.status_msg {
            if msg.contains("Error") || msg.contains("failed") {
                return Err(format!("instance error: {msg}"));
            }
        }

        // Check if intended_status has gone to stopped (instance gave up)
        if intended == "stopped" && actual != "running" {
            let msg = status.status_msg.unwrap_or_default();
            return Err(format!("instance stopped: {msg}"));
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
                return Err(format!("instance reached terminal status: {actual}"));
            }
            _ => {
                tokio::time::sleep(poll_interval).await;
            }
        }
    }

    Err("instance did not reach running within poll limit".to_string())
}

/// Request instance logs and return the download URL.
/// Logs take a few seconds to become available after this call.
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

/// Fetch instance logs from S3 URL. Returns the log text.
pub async fn fetch_logs(client: &Client, log_url: &str) -> Result<String, String> {
    // Wait for the log to become available
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let resp = client
        .get(log_url)
        .send()
        .await
        .map_err(|e| format!("fetch_logs failed: {e}"))?;
    resp.text()
        .await
        .map_err(|e| format!("fetch_logs read failed: {e}"))
}

/// Destroy a vast.ai instance.
pub async fn destroy_instance(
    client: &Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<(), String> {
    let url = format!("{base_url}/api/v0/instances/{contract_id}/");
    client
        .delete(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("destroy_instance request failed: {e}"))?;

    Ok(())
}
