use std::collections::{BTreeMap, HashSet};
use std::io::{IsTerminal, Write};
use std::time::Duration;

use crate::config::{ENV_ASSUME_YES, truthy_env};
use crate::monitor::wait_for_running_with_policy;
use crate::pricing::{CostModel, plan_picks};
use crate::provision::create_instance;
use crate::search::select_offer_pool_with_policy;
use crate::teardown::{destroy_instance_with_retry, rollback};
use crate::types::{
    CreateInstanceRequest, InstanceInfo, Offer, ProvisionRequest, ProvisionedFleet,
    ProvisionedInstance,
};

/// Print the planned lease + hourly cost and, on TTY, require y/N confirmation.
pub fn confirm_lease(pool: &[Offer], num_instances: u32, cost: &CostModel) -> Result<(), String> {
    let picks = plan_picks(pool, num_instances);
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
) -> Option<&'a Offer> {
    pool.iter().find(|o| {
        !tried_offer_ids.contains(&o.id) && o.host_id.map_or(true, |h| !used_host_ids.contains(&h))
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
) -> Result<ProvisionedInstance, String> {
    let mut attempt = 1_u64;
    loop {
        let offer = match next_eligible_offer(pool, tried_offer_ids, used_host_ids) {
            Some(o) => o.clone(),
            None => {
                return Err(format!(
                    "pool exhausted for index {index} (no untried offer on an unused host)"
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

    let mut tried_offer_ids = Vec::new();
    let mut created: Vec<ProvisionedInstance> = Vec::with_capacity(req.count as usize);
    let mut used_host_ids = HashSet::new();

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
