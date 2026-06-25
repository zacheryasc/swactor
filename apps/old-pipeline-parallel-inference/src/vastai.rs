//! Pipeline-specific compatibility wrapper over the peripheral `swactor-vastai`
//! utility crate.
//!
//! The reusable vast.ai logic now lives in `tools/vastai`. This module keeps the
//! old pipeline app API by translating stage-specific env (`STAGE`,
//! `NUM_STAGES`, `SEED_ADDR`, `PP_STAGE_SECRET`, etc.) into the generic
//! `CreateInstanceRequest` / `ProvisionRequest` structs.

use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::Client;

pub use swactor_vastai::{
    CostModel, CreateInstanceRequest, FleetState, InstanceInfo, LabeledInstance, LifecyclePolicy,
    Offer, ProvisionRequest, ProvisionedFleet, ProvisionedInstance, RunningInstance,
    SelectionPolicy, destroy_all_instances, destroy_instance, destroy_instance_with_retry,
    fetch_logs, list_instances_by_label, request_logs, select_offer_pool, wait_for_running,
};

const DEFAULT_ONSTART: &str = "/usr/local/bin/pp_entrypoint.sh 2>&1";
const DEFAULT_DISK_GB: u32 = 30;

/// Env-var bundle the orchestrator forwards into every rented stage container.
#[derive(Debug, Clone, Default)]
pub struct StageEnv {
    /// Custom iroh relay URL stages dial to reach the orchestrator across the internet.
    pub iroh_relay_url: Option<String>,
    /// Orchestrator's deploy SSH public key, injected as `SSH_PUBLIC_KEY`.
    pub ssh_public_key: Option<String>,
}

impl StageEnv {
    /// Read the relay URL + deploy SSH public key from the current process env.
    pub fn from_process_env() -> Self {
        fn nonempty(v: &str) -> Option<String> {
            std::env::var(v)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        }
        Self {
            iroh_relay_url: nonempty(crate::relay_config::ENV_IROH_RELAY_URL),
            ssh_public_key: nonempty("PP_DEPLOY_PUBKEY"),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.iroh_relay_url.is_some() || self.ssh_public_key.is_some()
    }
}

fn base_env(
    seed_addr: &str,
    seed_relay: Option<&str>,
    stage_env: Option<&StageEnv>,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert("SEED_ADDR".to_string(), seed_addr.to_string());
    if let Some(relay) = seed_relay {
        env.insert("SEED_RELAY".to_string(), relay.to_string());
    }
    for var in ["PP_WORKER_STUB", "PYTHON", "MODEL", "CUDA", "MAX_TOKENS"] {
        if let Ok(v) = std::env::var(var) {
            if !v.trim().is_empty() {
                env.insert(var.to_string(), v);
            }
        }
    }
    if let Some(stage_env) = stage_env {
        if let Some(relay_url) = stage_env.iroh_relay_url.as_deref() {
            env.insert(
                crate::relay_config::ENV_IROH_RELAY_URL.to_string(),
                relay_url.to_string(),
            );
        }
        if let Some(pubkey) = stage_env.ssh_public_key.as_deref() {
            env.insert("SSH_PUBLIC_KEY".to_string(), pubkey.to_string());
        }
    }
    env
}

fn stage_overlay(
    stage: u32,
    num_stages: u32,
    stage_secret: Option<&str>,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert("STAGE".to_string(), stage.to_string());
    env.insert("NUM_STAGES".to_string(), num_stages.to_string());
    if let Some(secret) = stage_secret {
        env.insert("PP_STAGE_SECRET".to_string(), secret.to_string());
    }
    env
}

/// Create one vast.ai instance from `offer_id`, passing pipeline stage env vars.
#[allow(clippy::too_many_arguments)]
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
    let mut env = base_env(seed_addr, seed_relay, stage_env);
    env.extend(stage_overlay(stage, num_stages, stage_secret));
    let req = CreateInstanceRequest {
        offer_id,
        image: image.to_string(),
        disk_gb: DEFAULT_DISK_GB,
        label: label.map(ToString::to_string),
        env,
        onstart: Some(DEFAULT_ONSTART.to_string()),
    };
    swactor_vastai::create_instance(client, base_url, api_key, &req).await
}

/// Rent one instance per pipeline stage, rolling back on partial create failure.
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
    let mut created = Vec::with_capacity(offer_ids.len());
    for (i, &offer_id) in offer_ids.iter().enumerate() {
        let stage = i as u32;
        match create_instance(
            client, base_url, api_key, offer_id, stage, num_stages, seed_addr, seed_relay, image,
            None, None, stage_env,
        )
        .await
        {
            Ok(info) => created.push(info),
            Err(e) => {
                let ids: Vec<u64> = created
                    .iter()
                    .map(|c: &InstanceInfo| c.contract_id)
                    .collect();
                let _ = destroy_all_instances(client, base_url, api_key, &ids).await;
                return Err(format!(
                    "create_pipeline_instances failed at stage {stage}: {e}"
                ));
            }
        }
    }
    Ok(created)
}

/// Rent `num_stages` vast.ai instances and wait for each to reach `running`.
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
    let mut selection = SelectionPolicy::from_env();
    if !gpu_name.is_empty() {
        selection.gpu_name = Some(gpu_name.to_string());
    }
    let lifecycle = LifecyclePolicy::from_env(poll_interval, max_polls);
    let per_instance_env = (0..num_stages)
        .map(|stage| {
            stage_overlay(
                stage,
                num_stages,
                stage_secrets
                    .and_then(|ss| ss.get(stage as usize))
                    .map(|s| s.as_str()),
            )
        })
        .collect();
    let req = ProvisionRequest {
        count: num_stages,
        image: image.to_string(),
        label: label.map(ToString::to_string),
        disk_gb: DEFAULT_DISK_GB,
        env: base_env(seed_addr, seed_relay, stage_env),
        per_instance_env,
        onstart: Some(DEFAULT_ONSTART.to_string()),
        selection,
        lifecycle,
        confirm_lease: true,
    };
    let fleet = swactor_vastai::provision_fleet(client, base_url, api_key, req).await?;
    Ok(fleet
        .instances
        .into_iter()
        .map(|i| InstanceInfo {
            contract_id: i.contract_id,
        })
        .collect())
}
