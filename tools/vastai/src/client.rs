use std::time::Duration;

pub const VASTAI_BASE_URL_ENV: &str = "VASTAI_BASE_URL";
const DEFAULT_VASTAI_BASE_URL: &str = "https://cloud.vast.ai";

use crate::search::OfferBrowseCriteria;

use crate::types::{
    CreateInstanceRequest, InstanceInfo, LabeledInstance, LifecyclePolicy, Offer,
    ProviderInstanceStatus, ProvisionRequest, ProvisionedFleet, RunningInstance,
};

/// Small convenience wrapper around a reqwest client + vast.ai endpoint.
#[derive(Clone)]
pub struct VastClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl VastClient {
    pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);

    /// Build a client for the configured Vast.ai API endpoint.
    ///
    /// `VASTAI_BASE_URL` supports operator proxies and production-shaped local
    /// fixtures. Empty values retain the public Vast.ai endpoint.
    pub fn new(api_key: impl Into<String>) -> Self {
        let base_url = std::env::var(VASTAI_BASE_URL_ENV)
            .ok()
            .filter(|url| !url.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_VASTAI_BASE_URL.to_owned());
        Self::with_base_url(base_url, api_key)
    }

    pub fn with_base_url(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Self::REQUEST_TIMEOUT)
            .build()
            .expect("valid Vast.ai HTTP client");
        Self {
            http,
            base_url: base_url.into(),
            api_key: api_key.into(),
        }
    }

    pub async fn create_instance(
        &self,
        req: &CreateInstanceRequest,
    ) -> Result<InstanceInfo, String> {
        crate::provision::create_instance(&self.http, &self.base_url, &self.api_key, req).await
    }

    pub async fn destroy_instance(&self, contract_id: u64) -> Result<(), String> {
        crate::teardown::destroy_instance(&self.http, &self.base_url, &self.api_key, contract_id)
            .await
    }

    pub async fn destroy_instance_with_retry(&self, contract_id: u64) -> Result<(), String> {
        crate::teardown::destroy_instance_with_retry(
            &self.http,
            &self.base_url,
            &self.api_key,
            contract_id,
        )
        .await
    }

    pub async fn search_offers(
        &self,
        policy: &crate::types::SelectionPolicy,
        target_count: u32,
    ) -> Result<Vec<Offer>, String> {
        crate::search::select_offer_pool_with_policy(
            &self.http,
            &self.base_url,
            &self.api_key,
            policy,
            target_count,
        )
        .await
    }

    pub async fn browse_offers(
        &self,
        criteria: &OfferBrowseCriteria,
    ) -> Result<Vec<Offer>, String> {
        crate::search::browse_offers(&self.http, &self.base_url, &self.api_key, criteria).await
    }

    pub async fn provision(&self, req: ProvisionRequest) -> Result<ProvisionedFleet, String> {
        crate::lease::provision_fleet(&self.http, &self.base_url, &self.api_key, req).await
    }

    pub async fn instance_status(
        &self,
        contract_id: u64,
    ) -> Result<ProviderInstanceStatus, String> {
        crate::monitor::fetch_instance_status(
            &self.http,
            &self.base_url,
            &self.api_key,
            contract_id,
        )
        .await
    }
    pub async fn wait_for_running(
        &self,
        contract_id: u64,
        policy: &LifecyclePolicy,
    ) -> Result<RunningInstance, String> {
        crate::monitor::wait_for_running_with_policy(
            &self.http,
            &self.base_url,
            &self.api_key,
            contract_id,
            policy,
        )
        .await
    }

    pub async fn wait_for_ssh_endpoint(
        &self,
        contract_id: u64,
        label: &str,
        policy: &LifecyclePolicy,
    ) -> Result<RunningInstance, String> {
        crate::monitor::wait_for_ssh_endpoint_with_policy(
            &self.http,
            &self.base_url,
            &self.api_key,
            contract_id,
            label,
            policy,
        )
        .await
    }

    pub async fn list_by_label(&self, label: &str) -> Result<Vec<LabeledInstance>, String> {
        crate::teardown::list_instances_by_label(&self.http, &self.base_url, &self.api_key, label)
            .await
    }
}
