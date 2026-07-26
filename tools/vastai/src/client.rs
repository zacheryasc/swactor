use std::time::Duration;

use crate::types::{
    LabeledInstance, LifecyclePolicy, Offer, ProvisionRequest, ProvisionedFleet, RunningInstance,
};

/// Small convenience wrapper around a reqwest client + vast.ai endpoint.
#[derive(Clone)]
pub struct VastClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl VastClient {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url("https://cloud.vast.ai", api_key)
    }

    pub fn with_base_url(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(45))
            .build()
            .expect("valid Vast.ai HTTP client");
        Self {
            http,
            base_url: base_url.into(),
            api_key: api_key.into(),
        }
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
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

    pub async fn provision(&self, req: ProvisionRequest) -> Result<ProvisionedFleet, String> {
        crate::lease::provision_fleet(&self.http, &self.base_url, &self.api_key, req).await
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

    pub async fn list_by_label(&self, label: &str) -> Result<Vec<LabeledInstance>, String> {
        crate::teardown::list_instances_by_label(&self.http, &self.base_url, &self.api_key, label)
            .await
    }
}
