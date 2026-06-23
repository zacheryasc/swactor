use crate::filters::reachable_offers;
use crate::pricing::{CostModel, rank_survivors};
use crate::types::{Offer, SearchResponse, SelectionPolicy};

/// Historical env-backed offer search wrapper.
pub async fn select_offer_pool(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    gpu_name: &str,
    target_count: u32,
) -> Result<Vec<Offer>, String> {
    let mut policy = SelectionPolicy::from_env();
    if !gpu_name.is_empty() {
        policy.gpu_name = Some(gpu_name.to_string());
    }
    select_offer_pool_with_policy(client, base_url, api_key, &policy, target_count).await
}

/// Search vast.ai offers, apply quality filters, and rank survivors.
pub async fn select_offer_pool_with_policy(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    policy: &SelectionPolicy,
    target_count: u32,
) -> Result<Vec<Offer>, String> {
    // Hard gates expressed server-side. Network speed cannot be probed before
    // renting, so this trusts vast.ai's measured inet figures.
    let mut query = serde_json::json!({
        "rentable": {"eq": true},
        "rented": {"eq": false},
        "reliability2": {"gte": policy.min_reliability},
        "cuda_max_good": {"gte": 12.6},
        "direct_port_count": {"gte": 1},
        "num_gpus": {"eq": 1},
        "inet_down": {"gte": policy.min_down_mbps},
        // vast.ai treats `limit` as a scan budget, not a simple result cap.
        "limit": 5000,
    });
    if let Some(up) = policy.min_up_mbps {
        query["inet_up"] = serde_json::json!({"gte": up});
    }
    if policy.require_verified {
        query["verified"] = serde_json::json!({"eq": true});
    }
    if let Some(min_ram) = policy.min_gpu_ram_mb {
        query["gpu_ram"] = serde_json::json!({"gte": min_ram});
    }
    if let Some(gpu_name) = policy.gpu_name.as_deref().filter(|s| !s.is_empty()) {
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

    let reachable = reachable_offers(body.offers, policy);
    let cost = CostModel::from_policy(policy);
    let pool = rank_survivors(reachable, &cost, policy.drop_cheap_frac);

    if pool.is_empty() {
        return Err(
            "no offers available (after quality/geo/host-blacklist filters and cheap-tail drop)"
                .to_string(),
        );
    }
    eprintln!(
        "select_offer_pool: {} survivor(s) for {target_count} instance(s) after \
         per-model {:.0}% cheap-drop (cheapest ${:.3}/hr eff)",
        pool.len(),
        policy.drop_cheap_frac * 100.0,
        cost.effective_price(&pool[0]),
    );
    Ok(pool)
}
