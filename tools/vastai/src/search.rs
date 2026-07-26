use crate::filters::reachable_offers;
use crate::pricing::{CostModel, rank_survivors};
use crate::types::{Offer, SearchResponse, SelectionPolicy};
use std::collections::HashSet;

/// Choose the first batch of offers from one ranked pool, preferring different
/// hosts whenever the filtered pool can satisfy that.
pub fn plan_distinct_host_first_wave(
    pool: &[Offer],
    target_count: u32,
    blacklisted_hosts: &[u64],
    failed_hosts: &[u64],
) -> Vec<Offer> {
    let target = target_count as usize;
    let blocked = blacklisted_hosts
        .iter()
        .chain(failed_hosts.iter())
        .copied()
        .collect::<HashSet<_>>();
    let mut selected = Vec::with_capacity(target);
    let mut selected_ids = HashSet::new();
    let mut selected_hosts = HashSet::new();

    for offer in pool.iter().filter(|offer| {
        offer
            .host_id
            .is_none_or(|host_id| !blocked.contains(&host_id))
    }) {
        if selected.len() == target {
            break;
        }
        if let Some(host_id) = offer.host_id {
            if !selected_hosts.insert(host_id) {
                continue;
            }
        }
        selected_ids.insert(offer.id);
        selected.push(offer.clone());
    }

    if selected.len() < target {
        for offer in pool.iter().filter(|offer| {
            offer
                .host_id
                .is_none_or(|host_id| !blocked.contains(&host_id))
        }) {
            if selected.len() == target {
                break;
            }
            if selected_ids.insert(offer.id) {
                selected.push(offer.clone());
            }
        }
    }

    selected
}

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
    if let Some(min_compute_cap) = policy.min_compute_cap {
        query["compute_cap"] = serde_json::json!({"gte": min_compute_cap});
    }
    if let Some(max_dph_total) = policy.max_dph_total {
        query["dph_total"] = serde_json::json!({"lte": max_dph_total});
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
        let cap = policy
            .max_dph_total
            .map_or_else(|| "uncapped".to_owned(), |max| format!("max ${max:.3}/hr"));
        return Err(format!(
            "no offers available ({cap}, after quality/geo/host-blacklist filters and cheap-tail drop)"
        ));
    }
    let cap = policy
        .max_dph_total
        .map_or_else(|| "uncapped".to_owned(), |max| format!("max ${max:.3}/hr"));
    eprintln!(
        "select_offer_pool: {} survivor(s) for {target_count} instance(s) after \
         per-model {:.0}% cheap-drop ({cap}, cheapest ${:.3}/hr eff)",
        pool.len(),
        policy.drop_cheap_frac * 100.0,
        cost.effective_price(&pool[0]),
    );
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(id: u64, host_id: Option<u64>) -> Offer {
        Offer {
            id,
            gpu_name: "RTX 4090".to_owned(),
            dph_total: id as f64 / 100.0,
            gpu_ram: Some(24_000.0),
            compute_cap: 890,
            geolocation: Some("US".to_owned()),
            inet_down_cost_per_tb: 0.0,
            inet_up_cost_per_tb: 0.0,
            host_id,
            verification: Some("verified".to_owned()),
        }
    }

    #[test]
    fn first_wave_plan_prefers_distinct_hosts_and_preserves_blacklists() {
        let pool = vec![
            offer(1, Some(10)),
            offer(2, Some(10)),
            offer(3, Some(20)),
            offer(4, Some(30)),
            offer(5, Some(40)),
        ];

        let plan = plan_distinct_host_first_wave(&pool, 3, &[30], &[]);

        assert_eq!(
            plan.iter().map(|offer| offer.id).collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
        assert_eq!(
            plan.iter()
                .filter_map(|offer| offer.host_id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
        assert!(
            plan.iter().all(|offer| offer.host_id != Some(30)),
            "operator blacklist must remain authoritative"
        );
    }

    #[test]
    fn first_wave_plan_excludes_failed_hosts_from_replacements() {
        let pool = vec![
            offer(1, Some(10)),
            offer(2, Some(20)),
            offer(3, Some(30)),
            offer(4, Some(20)),
        ];

        let plan = plan_distinct_host_first_wave(&pool, 2, &[], &[20]);

        assert_eq!(
            plan.iter().map(|offer| offer.id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(plan.iter().all(|offer| offer.host_id != Some(20)));
    }
}
