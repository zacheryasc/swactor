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
    streaming_http: reqwest::Client,
    base_url: String,
    api_key: String,
    observer: crate::observation::ProviderObserver,
    execution_deadline: Option<std::time::Instant>,
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
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Self::REQUEST_TIMEOUT)
            .build()
            .expect("valid Vast.ai HTTP client");
        let streaming_http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("valid Vast.ai streaming HTTP client");
        Self {
            http,
            streaming_http,
            base_url: base_url.into(),
            api_key: api_key.into(),
            observer: crate::observation::ProviderObserver::default(),
            execution_deadline: None,
        }
    }

    /// The application may contain all execution I/O under one inherited owner.
    /// Dedicated cleanup methods retain their separate reserved deadline.
    pub fn with_execution_deadline(mut self, deadline: Option<std::time::Instant>) -> Self {
        self.execution_deadline = deadline;
        self
    }

    pub(crate) async fn execute<F: Future>(
        &self,
        operation: &str,
        future: F,
    ) -> Result<F::Output, String> {
        let Some(deadline) = self.execution_deadline else {
            return Ok(future.await);
        };
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "Vast.ai execution owner deadline: pending {operation}"
            ));
        }
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
            .await
            .map_err(|_| format!("Vast.ai execution owner deadline: pending {operation}"))
    }

    pub async fn create_instance(
        &self,
        req: &CreateInstanceRequest,
    ) -> Result<InstanceInfo, crate::CreateInstanceError> {
        self.execute(
            "create response and exact contract accounting",
            crate::provision::create_instance(&self.http, &self.base_url, &self.api_key, req),
        )
        .await
        .map_err(crate::CreateInstanceError::Ambiguous)?
    }

    pub async fn destroy_instance(&self, contract_id: u64) -> Result<(), String> {
        self.execute(
            "contract destruction",
            crate::teardown::destroy_instance(
                &self.http,
                &self.base_url,
                &self.api_key,
                contract_id,
            ),
        )
        .await?
    }

    pub async fn destroy_instance_with_retry(&self, contract_id: u64) -> Result<(), String> {
        self.execute(
            "contract destruction retries",
            crate::teardown::destroy_instance_with_retry(
                &self.http,
                &self.base_url,
                &self.api_key,
                contract_id,
            ),
        )
        .await?
    }

    pub async fn search_offers(
        &self,
        policy: &crate::types::SelectionPolicy,
        target_count: u32,
    ) -> Result<Vec<Offer>, String> {
        self.execute(
            "eligible offer search",
            crate::search::select_offer_pool_with_policy(
                &self.http,
                &self.base_url,
                &self.api_key,
                policy,
                target_count,
            ),
        )
        .await?
    }

    pub async fn browse_offers(
        &self,
        criteria: &OfferBrowseCriteria,
    ) -> Result<Vec<Offer>, String> {
        self.execute(
            "exact offer validation",
            crate::search::browse_offers(&self.http, &self.base_url, &self.api_key, criteria),
        )
        .await?
    }

    pub async fn account_ssh_keys(&self) -> Result<String, String> {
        self.execute("account SSH key discovery", async {
            self.http
                .get(format!("{}/api/v0/ssh/", self.base_url))
                .bearer_auth(&self.api_key)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|error| {
                    format!("list account SSH keys: {}", error.without_url())
                        .replace(&self.api_key, "[REDACTED]")
                })?
                .text()
                .await
                .map_err(|error| {
                    format!("read account SSH keys: {}", error.without_url())
                        .replace(&self.api_key, "[REDACTED]")
                })
        })
        .await?
    }

    pub async fn register_account_ssh_key(&self, public_key: &str) -> Result<(), String> {
        self.execute("account SSH key registration", async {
            self.http
                .post(format!("{}/api/v0/ssh/", self.base_url))
                .bearer_auth(&self.api_key)
                .json(&serde_json::json!({"ssh_key": public_key}))
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|error| {
                    format!("register account SSH key: {}", error.without_url())
                        .replace(&self.api_key, "[REDACTED]")
                })?;
            Ok(())
        })
        .await?
    }
    pub async fn request_logs(&self, contract_id: u64) -> Result<String, String> {
        self.execute(
            "provider log request",
            crate::logs::request_logs(&self.http, &self.base_url, &self.api_key, contract_id),
        )
        .await?
    }

    pub async fn fetch_logs(&self, log_url: &str) -> Result<String, String> {
        self.execute(
            "provider log response",
            crate::logs::fetch_logs(&self.http, log_url),
        )
        .await?
    }

    pub async fn open_log_stream(
        &self,
        log_url: &str,
    ) -> Result<crate::logs::VastLogStream, String> {
        self.execute(
            "provider log stream connection",
            crate::logs::open_log_stream(&self.streaming_http, log_url),
        )
        .await?
    }

    pub async fn provision(&self, req: ProvisionRequest) -> Result<ProvisionedFleet, String> {
        self.execute(
            "fleet provisioning",
            crate::lease::provision_fleet(&self.http, &self.base_url, &self.api_key, req),
        )
        .await?
    }

    pub async fn instance_status(
        &self,
        contract_id: u64,
    ) -> Result<ProviderInstanceStatus, String> {
        self.execute(
            "exact contract status",
            self.observer
                .status(&self.http, &self.base_url, &self.api_key, contract_id),
        )
        .await?
    }
    pub async fn wait_for_running(
        &self,
        contract_id: u64,
        policy: &LifecyclePolicy,
    ) -> Result<RunningInstance, String> {
        self.execute(
            "running provider state",
            crate::monitor::wait_for_running_with_observer(
                &self.http,
                &self.base_url,
                &self.api_key,
                &self.observer,
                contract_id,
                policy,
            ),
        )
        .await?
    }

    pub async fn wait_for_ssh_endpoint(
        &self,
        contract_id: u64,
        labels: &std::collections::BTreeSet<String>,
        policy: &LifecyclePolicy,
    ) -> Result<RunningInstance, String> {
        self.execute(
            "usable SSH endpoint",
            crate::monitor::wait_for_ssh_endpoint_with_observer(
                &self.http,
                &self.base_url,
                &self.api_key,
                &self.observer,
                contract_id,
                labels,
                policy,
            ),
        )
        .await?
    }

    pub async fn list_by_label(&self, label: &str) -> Result<Vec<LabeledInstance>, String> {
        let labels = std::collections::BTreeSet::from([label.to_owned()]);
        self.owned_census(
            &labels,
            &std::collections::BTreeSet::new(),
            std::time::Instant::now(),
            Self::REQUEST_TIMEOUT * 4 + Duration::from_secs(180),
        )
        .await
        .map(|census| census.instances)
    }

    pub async fn owned_census(
        &self,
        labels: &std::collections::BTreeSet<String>,
        known_ids: &std::collections::BTreeSet<u64>,
        fresh_after: std::time::Instant,
        deadline: Duration,
    ) -> Result<crate::OwnedCensus, String> {
        self.execute(
            "fresh exact owned provider census",
            self.observer.census(
                &self.http,
                &self.base_url,
                &self.api_key,
                labels,
                known_ids,
                fresh_after,
                std::time::Instant::now() + deadline,
            ),
        )
        .await?
    }

    pub async fn cleanup_owned(
        &self,
        labels: &std::collections::BTreeSet<String>,
        known_ids: &std::collections::BTreeSet<u64>,
        deadline: Duration,
    ) -> Result<crate::OwnedCensus, String> {
        crate::teardown::cleanup_owned_with_observer(
            &self.http,
            &self.base_url,
            &self.api_key,
            &self.observer,
            labels,
            known_ids,
            deadline,
        )
        .await
    }

    /// Cleanup with unresolved create reservations and durable discovery capture.
    pub async fn cleanup_owned_reconciled(
        &self,
        labels: &std::collections::BTreeSet<String>,
        known_ids: &std::collections::BTreeSet<u64>,
        unresolved_labels: &std::collections::BTreeSet<String>,
        deadline: Duration,
        record_discovery: &mut (
                 dyn FnMut(&std::collections::BTreeMap<u64, String>) -> Result<(), String> + Send
             ),
    ) -> Result<crate::OwnedCensus, crate::teardown::CleanupFailure> {
        crate::teardown::cleanup_owned_reconciled_with_observer(
            &self.http,
            &self.base_url,
            &self.api_key,
            &self.observer,
            labels,
            known_ids,
            unresolved_labels,
            deadline,
            record_discovery,
        )
        .await
    }

    pub async fn cleanup_owned_labels(
        &self,
        labels: &std::collections::BTreeSet<String>,
        deadline: std::time::Duration,
    ) -> Result<(), String> {
        self.cleanup_owned(labels, &std::collections::BTreeSet::new(), deadline)
            .await
            .map(|_| ())
    }
}
