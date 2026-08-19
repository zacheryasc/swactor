use crate::filters::reachable_offers;
use crate::pricing::{CostModel, rank_survivors};
use crate::types::{Offer, SearchResponse, SelectionPolicy};
use std::collections::HashSet;
/// Operator-entered filters for marketplace browsing.
///
/// Unlike [`SelectionPolicy`], these criteria carry no automatic-provisioning
/// defaults: an unset field does not constrain the listing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OfferBrowseCriteria {
    pub gpu_name_contains: Option<String>,
    pub min_gpu_ram_mb: Option<u64>,
    pub min_compute_cap: Option<u64>,
    pub min_reliability: Option<f64>,
    pub require_verified: bool,
    pub min_down_mbps: Option<f64>,
    pub min_up_mbps: Option<f64>,
    pub max_dph_total: Option<f64>,
    pub blacklist_hosts: Vec<u64>,
}

/// Lists selectable marketplace offers using only operator-entered filters.
pub async fn browse_offers(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    criteria: &OfferBrowseCriteria,
) -> Result<Vec<Offer>, String> {
    // These are technical eligibility gates, not provisioning preferences.
    let mut query = serde_json::json!({
        "rentable": {"eq": true},
        "rented": {"eq": false},
        "cuda_max_good": {"gte": 12.6},
        "direct_port_count": {"gte": 1},
        "num_gpus": {"eq": 1},
        "limit": 5000,
    });
    if let Some(value) = criteria.min_reliability {
        query["reliability2"] = serde_json::json!({"gte": value});
    }
    if criteria.require_verified {
        query["verified"] = serde_json::json!({"eq": true});
    }
    if let Some(value) = criteria.min_gpu_ram_mb {
        query["gpu_ram"] = serde_json::json!({"gte": value});
    }
    if let Some(value) = criteria.min_compute_cap {
        query["compute_cap"] = serde_json::json!({"gte": value});
    }
    if let Some(value) = criteria.min_down_mbps {
        query["inet_down"] = serde_json::json!({"gte": value});
    }
    if let Some(value) = criteria.min_up_mbps {
        query["inet_up"] = serde_json::json!({"gte": value});
    }
    if let Some(value) = criteria.max_dph_total {
        query["dph_total"] = serde_json::json!({"lte": value});
    }

    let blocked = criteria
        .blacklist_hosts
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let gpu_query = criteria
        .gpu_name_contains
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty());
    let mut offers = fetch_offers(client, base_url, api_key, &query, "browse_offers")
        .await?
        .into_iter()
        .filter(|offer| {
            offer
                .host_id
                .is_none_or(|host_id| !blocked.contains(&host_id))
        })
        .filter(|offer| {
            gpu_query.is_none_or(|query| contains_ascii_case_insensitive(&offer.gpu_name, query))
        })
        .collect::<Vec<_>>();
    offers.sort_by(|left, right| {
        left.dph_total
            .total_cmp(&right.dph_total)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(offers)
}

fn contains_ascii_case_insensitive(value: &str, query: &str) -> bool {
    query.is_empty()
        || value
            .as_bytes()
            .windows(query.len())
            .any(|candidate| candidate.eq_ignore_ascii_case(query.as_bytes()))
}

async fn fetch_offers(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    query: &serde_json::Value,
    operation: &str,
) -> Result<Vec<Offer>, String> {
    let url = format!(
        "{base_url}/api/v0/bundles/?q={}",
        urlencoding::encode(&query.to_string())
    );
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|error| format!("{operation} request failed: {error}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("{operation} HTTP {status}: {body}"));
    }

    resp.json::<SearchResponse>()
        .await
        .map(|body| body.offers)
        .map_err(|error| format!("{operation} parse failed: {error}"))
}

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

    let offers = fetch_offers(client, base_url, api_key, &query, "select_offer_pool").await?;
    let reachable = reachable_offers(offers, policy);
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
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
            reliability2: Some(0.99),
            inet_down: Some(500.0),
            inet_up: Some(250.0),
        }
    }

    #[test]
    fn gpu_browse_text_matches_ascii_substrings_case_insensitively() {
        assert!(contains_ascii_case_insensitive("RTX 4090", "4090"));
        assert!(contains_ascii_case_insensitive(
            "NVIDIA GeForce RTX 4090",
            "rtx 4090"
        ));
        assert!(contains_ascii_case_insensitive("A100 SXM4", "a100"));
        assert!(!contains_ascii_case_insensitive("RTX 4080", "4090"));
    }

    #[tokio::test]
    async fn browsing_matches_partial_models_without_provisioning_filters() {
        let server = MockServer::start().await;
        let mut matching = offer(1, Some(10));
        matching.geolocation = Some("CN".to_owned());
        matching.verification = Some("deverified".to_owned());
        matching.reliability2 = Some(0.1);
        matching.inet_down = Some(1.0);
        let mut other = offer(2, Some(20));
        other.gpu_name = "RTX 4080".to_owned();
        Mock::given(method("GET"))
            .and(path("/api/v0/bundles/"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"offers": [matching, other]})),
            )
            .mount(&server)
            .await;

        let offers = browse_offers(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            &OfferBrowseCriteria {
                gpu_name_contains: Some("4090".to_owned()),
                ..OfferBrowseCriteria::default()
            },
        )
        .await
        .expect("browse offers");

        assert_eq!(offers.iter().map(|offer| offer.id).collect::<Vec<_>>(), [1]);
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
