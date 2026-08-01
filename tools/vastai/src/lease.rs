use std::collections::{BTreeMap, HashSet};
use std::io::{IsTerminal, Write};
use std::time::Duration;

use crate::config::{ENV_ASSUME_YES, truthy_env};
use crate::monitor::wait_for_running_with_policy;
use crate::pricing::CostModel;
use crate::provision::create_instance;
use crate::search::{plan_distinct_host_first_wave, select_offer_pool_with_policy};
use crate::teardown::{destroy_instance_with_retry, rollback};
use crate::types::{
    CreateInstanceRequest, InstanceInfo, Offer, ProvisionRequest, ProvisionedFleet,
    ProvisionedInstance,
};

/// Print the planned lease + hourly cost and, on TTY, require y/N confirmation.
pub fn confirm_lease(pool: &[Offer], num_instances: u32, cost: &CostModel) -> Result<(), String> {
    let picks = plan_distinct_host_first_wave(pool, num_instances, &[], &[]);
    let total_dph: f64 = picks.iter().map(|o| o.dph_total).sum();
    let total_eff: f64 = picks.iter().map(|o| cost.effective_price(o)).sum();

    eprintln!("vastai: lease plan — {num_instances} instance(s), cheapest on distinct hosts:");
    for (i, o) in picks.iter().enumerate() {
        eprintln!(
            "  node {i}  {:<14} {:>8}  ${:.3}/hr  [{}]  host {}",
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
    if picks.len() < num_instances as usize {
        eprintln!(
            "  WARNING: only {} distinct-host offer(s) available for {num_instances} instance(s)",
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

    if truthy_env(ENV_ASSUME_YES) {
        eprintln!("vastai: {ENV_ASSUME_YES} set — proceeding without confirmation");
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        eprintln!(
            "vastai: stdin is not a TTY — proceeding without interactive confirmation \
             (set {ENV_ASSUME_YES}=1 to silence this)"
        );
        return Ok(());
    }

    eprint!("Proceed with renting these {num_instances} instance(s)? [y/N]: ");
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

fn next_eligible_offer<'a>(
    pool: &'a [Offer],
    tried_offer_ids: &[u64],
    used_host_ids: &HashSet<u64>,
    failed_host_ids: &HashSet<u64>,
    preferred_offer_id: Option<u64>,
) -> Option<&'a Offer> {
    if let Some(offer_id) = preferred_offer_id {
        if let Some(offer) = pool.iter().find(|o| o.id == offer_id) {
            if !tried_offer_ids.contains(&offer.id)
                && offer.host_id.is_none_or(|h| !failed_host_ids.contains(&h))
            {
                return Some(offer);
            }
        }
    }

    pool.iter().find(|o| {
        !tried_offer_ids.contains(&o.id)
            && o.host_id
                .is_none_or(|h| !used_host_ids.contains(&h) && !failed_host_ids.contains(&h))
    })
}

fn env_for_index(req: &ProvisionRequest, index: u32) -> BTreeMap<String, String> {
    let mut env = req.env.clone();
    if let Some(extra) = req.per_instance_env.get(index as usize) {
        env.extend(extra.clone());
    }
    env
}

async fn provision_one(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    req: &ProvisionRequest,
    pool: &[Offer],
    index: u32,
    tried_offer_ids: &mut Vec<u64>,
    used_host_ids: &mut HashSet<u64>,
    failed_host_ids: &mut HashSet<u64>,
    preferred_offer_id: Option<u64>,
) -> Result<ProvisionedInstance, String> {
    let mut attempt = 1_u64;
    loop {
        let offer = match next_eligible_offer(
            pool,
            tried_offer_ids,
            used_host_ids,
            failed_host_ids,
            preferred_offer_id.filter(|offer_id| !tried_offer_ids.contains(offer_id)),
        ) {
            Some(o) => o.clone(),
            None => {
                return Err(format!(
                    "pool exhausted for index {index} (no untried offer outside failed hosts)"
                ));
            }
        };
        tried_offer_ids.push(offer.id);
        let cost = CostModel::from_policy(&req.selection);
        eprintln!(
            "lease_chain: index {index} → offer {} — {} {} @ ${:.3}/hr [{}] host {} eff ${:.3}/hr",
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

        let create = CreateInstanceRequest {
            offer_id: offer.id,
            image: req.image.clone(),
            disk_gb: req.disk_gb,
            label: req.label.clone(),
            env: env_for_index(req, index),
            onstart: req.onstart.clone(),
        };
        match create_instance(client, base_url, api_key, &create).await {
            Ok(info) => {
                if let Some(h) = offer.host_id {
                    used_host_ids.insert(h);
                }
                return Ok(ProvisionedInstance {
                    index,
                    contract_id: info.contract_id,
                    offer_id: offer.id,
                    host_id: offer.host_id,
                    gpu_name: offer.gpu_name,
                    gpu_ram: offer.gpu_ram,
                    dph_total: offer.dph_total,
                });
            }
            Err(e) => {
                eprintln!(
                    "lease_chain: index {index} create on offer {} failed (attempt {attempt}): {e}",
                    offer.id,
                );
                let is_429 = e.contains("429") || e.contains("Too Many Requests");
                let backoff = if is_429 {
                    std::cmp::min(
                        Duration::from_millis(2_000_u64.saturating_mul(attempt)),
                        Duration::from_secs(30),
                    )
                } else {
                    Duration::from_millis(400)
                };
                tokio::time::sleep(backoff).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

fn as_instance_infos(instances: &[ProvisionedInstance]) -> Vec<InstanceInfo> {
    instances
        .iter()
        .map(|i| InstanceInfo {
            contract_id: i.contract_id,
        })
        .collect()
}

/// Rent, monitor, replace, and roll back an N-instance fleet.
pub async fn provision_fleet(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    req: ProvisionRequest,
) -> Result<ProvisionedFleet, String> {
    let pool = select_offer_pool_with_policy(client, base_url, api_key, &req.selection, req.count)
        .await
        .map_err(|e| format!("lease_chain: {e}"))?;

    if req.confirm_lease {
        confirm_lease(&pool, req.count, &CostModel::from_policy(&req.selection))?;
    }

    let first_wave =
        plan_distinct_host_first_wave(&pool, req.count, &req.selection.blacklist_hosts, &[]);
    let mut tried_offer_ids = Vec::new();
    let mut created: Vec<ProvisionedInstance> = Vec::with_capacity(req.count as usize);
    let mut used_host_ids = HashSet::new();
    let mut failed_host_ids = HashSet::new();
    for index in 0..req.count {
        match provision_one(
            client,
            base_url,
            api_key,
            &req,
            &pool,
            index,
            &mut tried_offer_ids,
            &mut used_host_ids,
            &mut failed_host_ids,
            req.preferred_offer_id
                .filter(|_| req.count == 1)
                .or_else(|| first_wave.get(index as usize).map(|offer| offer.id)),
        )
        .await
        {
            Ok(info) => created.push(info),
            Err(e) => {
                rollback(client, base_url, api_key, &as_instance_infos(&created)).await;
                return Err(format!("lease_chain: {e}"));
            }
        }
        if index + 1 < req.count {
            tokio::time::sleep(req.lifecycle.lease_pace).await;
        }
    }

    for index in 0..req.count {
        let idx = index as usize;
        loop {
            let cid = created[idx].contract_id;
            match wait_for_running_with_policy(client, base_url, api_key, cid, &req.lifecycle).await
            {
                Ok(_) => break,
                Err(e) => {
                    if let Some(host_id) = created[idx].host_id {
                        failed_host_ids.insert(host_id);
                    }
                    eprintln!(
                        "lease_chain: index {index} contract {cid} did not reach running: {e}"
                    );
                    if let Err(de) =
                        destroy_instance_with_retry(client, base_url, api_key, cid).await
                    {
                        eprintln!(
                            "lease_chain: WARNING could not destroy dead contract {cid}: {de}"
                        );
                    }
                    eprintln!("lease_chain: replacing index {index}");
                    match provision_one(
                        client,
                        base_url,
                        api_key,
                        &req,
                        &pool,
                        index,
                        &mut tried_offer_ids,
                        &mut used_host_ids,
                        &mut failed_host_ids,
                        None,
                    )
                    .await
                    {
                        Ok(info) => created[idx] = info,
                        Err(pe) => {
                            let survivors: Vec<InstanceInfo> = created
                                .iter()
                                .enumerate()
                                .filter(|(i, _)| *i != idx)
                                .map(|(_, c)| InstanceInfo {
                                    contract_id: c.contract_id,
                                })
                                .collect();
                            rollback(client, base_url, api_key, &survivors).await;
                            return Err(format!(
                                "lease_chain: index {index} replacement could not be provisioned: {pe}"
                            ));
                        }
                    }
                }
            }
        }
    }

    Ok(ProvisionedFleet {
        label: req.label,
        instances: created,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::types::{LifecyclePolicy, SelectionPolicy};

    fn offer(id: u64, host_id: u64) -> serde_json::Value {
        json!({
            "id": id,
            "gpu_name": "RTX 4090",
            "dph_total": id as f64 / 100.0,
            "gpu_ram": 24_000.0,
            "compute_cap": 890,
            "geolocation": "US",
            "internet_down_cost_per_tb": 0.0,
            "internet_up_cost_per_tb": 0.0,
            "host_id": host_id,
            "verification": "verified"
        })
    }

    fn request(count: u32) -> ProvisionRequest {
        ProvisionRequest {
            count,
            image: "registry.example/myelin-worker:latest".to_owned(),
            label: Some("lease-test".to_owned()),
            disk_gb: 80,
            env: BTreeMap::new(),
            per_instance_env: Vec::new(),
            preferred_offer_id: None,
            onstart: None,
            selection: SelectionPolicy {
                drop_cheap_frac: 0.0,
                ..SelectionPolicy::default()
            },
            lifecycle: LifecyclePolicy {
                lease_pace: Duration::ZERO,
                poll_interval: Duration::from_millis(1),
                state_timeout: Duration::from_millis(5),
            },
            confirm_lease: false,
        }
    }

    #[tokio::test]
    async fn replacement_excludes_failed_host_from_shared_offer_pool() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/bundles/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "offers": [
                    offer(1, 10),
                    offer(2, 20),
                    offer(3, 10),
                    offer(4, 30)
                ]
            })))
            .mount(&server)
            .await;
        for (offer_id, contract_id) in [(1, 101), (2, 102), (4, 104)] {
            Mock::given(method("PUT"))
                .and(path(format!("/api/v0/asks/{offer_id}/")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "new_contract": contract_id
                })))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/101/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "instances": {
                    "actual_status": "error",
                    "intended_status": "running",
                    "status_msg": "container failed before runtime readiness"
                }
            })))
            .mount(&server)
            .await;
        for contract_id in [102, 104] {
            Mock::given(method("GET"))
                .and(path(format!("/api/v0/instances/{contract_id}/")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "instances": {
                        "actual_status": "running",
                        "intended_status": "running",
                        "public_ipaddr": "127.0.0.1",
                        "ssh_port": 22
                    }
                })))
                .mount(&server)
                .await;
        }
        Mock::given(method("DELETE"))
            .and(path("/api/v0/instances/101/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let fleet = provision_fleet(&reqwest::Client::new(), &server.uri(), "secret", request(2))
            .await
            .expect("replacement should use non-failed host");

        assert_eq!(
            fleet
                .instances
                .iter()
                .map(|instance| (instance.index, instance.offer_id, instance.host_id))
                .collect::<Vec<_>>(),
            vec![(0, 4, Some(30)), (1, 2, Some(20))]
        );
        let requests = server.received_requests().await.expect("recorded requests");
        assert!(
            requests
                .iter()
                .any(|request| request.url.path() == "/api/v0/asks/4/"),
            "replacement should rent an offer from a non-failed host"
        );
        assert!(
            !requests
                .iter()
                .any(|request| request.url.path() == "/api/v0/asks/3/"),
            "replacement must skip untried offers on the failed host"
        );
    }
}
