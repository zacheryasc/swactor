//! Fresh, rate-aware scoped account discovery and exact-contract observations.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::Mutex;

use crate::types::{
    InstanceListResponse, InstanceResponse, LabeledInstance, ProviderInstanceStatus,
};

#[derive(Clone, Debug, Default, Serialize)]
pub struct ProviderObservationCounters {
    pub request_count: u64,
    pub successful_request_count: u64,
    pub rate_limited_count: u64,
    pub retry_count: u64,
    pub coalesced_count: u64,
    pub observation_gap_count: u64,
    pub request_micros: u64,
    pub rate_wait_micros: u64,
    pub response_bytes: u64,
    pub elapsed_micros: u64,
}

/// An exact owned-set observation, never a persisted account-wide listing.
/// Freshness is the request start, not the time a cached result was delivered.
#[derive(Clone, Debug)]
pub struct OwnedCensus {
    pub instances: Vec<LabeledInstance>,
    pub contract_labels: BTreeMap<u64, String>,
    /// Cumulative owned identities seen during cleanup, including now-absent contracts.
    pub discovered_contract_labels: BTreeMap<u64, String>,
    pub missing_contract_ids: BTreeSet<u64>,
    pub generation: u64,
    pub request_started: Instant,
    pub observed_at: Instant,
    pub counters: ProviderObservationCounters,
}

#[derive(Clone)]
pub(crate) struct ProviderObserver {
    account: Arc<Mutex<ObservationState>>,
    contracts: Arc<Mutex<BTreeMap<u64, Arc<Mutex<ContractObservationState>>>>>,
}

struct ObservationState {
    epoch: Instant,
    next_allowed: Instant,
    counters: ProviderObservationCounters,
    generation: u64,
    last: Option<(BTreeSet<String>, BTreeSet<u64>, OwnedCensus)>,
}

struct ContractObservationState {
    next_allowed: Instant,
    last: Option<(Instant, Result<ProviderInstanceStatus, String>)>,
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

impl Default for ProviderObserver {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            account: Arc::new(Mutex::new(ObservationState {
                epoch: now,
                next_allowed: now,
                counters: ProviderObservationCounters::default(),
                generation: 0,
                last: None,
            })),
            contracts: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl ProviderObserver {
    /// Coalesce overlapping readiness/monitor requests for one exact contract.
    /// This is not a case-boundary absence proof: those always use `census`.
    pub(crate) async fn status(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        api_key: &str,
        contract_id: u64,
    ) -> Result<ProviderInstanceStatus, String> {
        let requested = Instant::now();
        let timeout_at = tokio::time::Instant::now()
            + crate::VastClient::REQUEST_TIMEOUT * 4
            + Duration::from_secs(180);
        tokio::time::timeout_at(timeout_at, async {
            let contract = self.contracts.lock().await.entry(contract_id).or_insert_with(|| {
                Arc::new(Mutex::new(ContractObservationState {
                    next_allowed: requested,
                    last: None,
                }))
            }).clone();
            let mut state = contract.lock().await;
            if let Some((completed, result)) = &state.last
                && *completed >= requested
            {
                return result.clone();
            }
            let result = async {
                for attempt in 0..4 {
                    if state.next_allowed > Instant::now() {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(state.next_allowed)).await;
                    }
                    let response = client
                        .get(format!("{base_url}/api/v0/instances/{contract_id}/"))
                        .bearer_auth(api_key)
                        .send()
                        .await
                        .map_err(|error| format!("fetch_instance_status request failed: {}", error.without_url()))?;
                    let status = response.status();
                    let header_retry_after = response.headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok());
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        && let Some(seconds) = header_retry_after
                    {
                        // Headers already constrain the next request, even if
                        // cancellation interrupts the subsequent body read.
                        state.next_allowed = Instant::now().checked_add(Duration::from_secs(seconds))
                            .ok_or_else(|| "provider status Retry-After overflow".to_owned())?;
                    }
                    let body = response.bytes().await
                        .map_err(|error| format!("fetch_instance_status response failed: {}", error.without_url()))?;
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        if header_retry_after.is_none() {
                            let seconds = serde_json::from_slice::<serde_json::Value>(&body).ok()
                                .and_then(|value| value.get("retry_after").and_then(|value| value.as_u64()))
                                .unwrap_or(20);
                            state.next_allowed = Instant::now().checked_add(Duration::from_secs(seconds))
                                .ok_or_else(|| "provider status Retry-After overflow".to_owned())?;
                        }
                        if attempt < 3 {
                            continue;
                        }
                    }
                    if !status.is_success() {
                        let detail = String::from_utf8_lossy(&body).replace(api_key, "[REDACTED]");
                        let detail = detail.chars().take(80).collect::<String>();
                        return Err(if status == reqwest::StatusCode::NOT_FOUND {
                            format!("instance {contract_id} not found while fetching provider status: {detail}")
                        } else {
                            format!("fetch_instance_status HTTP {status}: {detail}")
                        });
                    }
                    let wrapper: InstanceResponse = serde_json::from_slice(&body)
                        .map_err(|error| format!("fetch_instance_status parse failed: {error}").replace(api_key, "[REDACTED]"))?;
                    let mut status = ProviderInstanceStatus::from(wrapper.instances);
                    if !api_key.is_empty() {
                        for text in [&mut status.actual_status, &mut status.intended_status]
                            .into_iter().chain(status.status_msg.iter_mut())
                        {
                            if text.contains(api_key) {
                                *text = text.replace(api_key, "[REDACTED]");
                            }
                        }
                    }
                    return Ok(status);
                }
                unreachable!("provider status attempts return a result")
            }.await;
            state.last = Some((Instant::now(), result.clone()));
            result
        }).await.map_err(|_| format!("provider status deadline: exact contract {contract_id} pending"))?
    }

    pub(crate) async fn census(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        api_key: &str,
        labels: &BTreeSet<String>,
        known_ids: &BTreeSet<u64>,
        fresh_after: Instant,
        deadline: Instant,
    ) -> Result<OwnedCensus, String> {
        let timeout_at = tokio::time::Instant::from_std(deadline);
        let mut state = tokio::time::timeout_at(timeout_at, self.account.lock())
            .await
            .map_err(|_| {
                "provider census deadline: waiting for shared account observer".to_owned()
            })?;
        if let Some((previous_labels, previous_ids, census)) = &state.last
            && previous_labels == labels
            && previous_ids == known_ids
            && census.request_started >= fresh_after
        {
            let mut census = census.clone();
            state.counters.coalesced_count += 1;
            state.counters.elapsed_micros = micros(state.epoch.elapsed());
            census.counters = state.counters.clone();
            return Ok(census);
        }
        let url = format!("{base_url}/api/v0/instances/");
        for attempt in 0..4 {
            let wait_started = Instant::now();
            if state.next_allowed > wait_started {
                let next_allowed = state.next_allowed;
                let wait = tokio::time::timeout_at(
                    timeout_at,
                    tokio::time::sleep_until(tokio::time::Instant::from_std(next_allowed)),
                )
                .await;
                state.counters.rate_wait_micros += micros(wait_started.elapsed());
                state.counters.elapsed_micros = micros(state.epoch.elapsed());
                if wait.is_err() {
                    state.counters.observation_gap_count += 1;
                    return Err(format!(
                        "provider census capacity deadline: Retry-After exceeds remaining budget; counters={:?}",
                        state.counters
                    ));
                }
            }
            if Instant::now() >= deadline {
                state.counters.observation_gap_count += 1;
                return Err(
                    "provider census deadline: fresh exact owned-set listing pending".to_owned(),
                );
            }
            let request_started = Instant::now();
            state.counters.request_count += 1;
            let response = tokio::time::timeout_at(timeout_at, async {
                let response =
                    client
                        .get(&url)
                        .bearer_auth(api_key)
                        .send()
                        .await
                        .map_err(|error| {
                            format!("provider census request failed: {}", error.without_url())
                        })?;
                let status = response.status();
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok());
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    state.counters.rate_limited_count += 1;
                    if let Some(seconds) = retry_after {
                        state.next_allowed = Instant::now()
                            .checked_add(Duration::from_secs(seconds))
                            .ok_or_else(|| "provider census Retry-After overflow".to_owned())?;
                    }
                }
                let body = response.bytes().await.map_err(|error| {
                    format!("provider census response failed: {}", error.without_url())
                })?;
                Ok::<_, String>((status, retry_after, body))
            })
            .await;
            state.counters.request_micros += micros(request_started.elapsed());
            state.counters.elapsed_micros = micros(state.epoch.elapsed());
            let (status, header_retry_after, body) = match response {
                Ok(Ok(response)) => response,
                failure => {
                    state.counters.observation_gap_count += 1;
                    let reason = match failure {
                        Ok(Err(error)) => error.replace(api_key, "[REDACTED]"),
                        _ => "provider census request deadline: fresh exact owned-set listing pending".to_owned(),
                    };
                    return Err(format!("{reason}; counters={:?}", state.counters));
                }
            };
            state.counters.response_bytes += body.len() as u64;
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if header_retry_after.is_none() {
                    let seconds = serde_json::from_slice::<serde_json::Value>(&body)
                        .ok()
                        .and_then(|value| value.get("retry_after").and_then(|value| value.as_u64()))
                        .unwrap_or(20);
                    state.next_allowed =
                        Instant::now()
                            .checked_add(Duration::from_secs(seconds))
                            .ok_or_else(|| "provider census Retry-After overflow".to_owned())?;
                }
                if attempt < 3 {
                    state.counters.retry_count += 1;
                    continue;
                }
            }
            if !status.is_success() {
                state.counters.observation_gap_count += 1;
                return Err(format!(
                    "provider census HTTP {status}; counters={:?}",
                    state.counters
                ));
            }
            let listing: InstanceListResponse = serde_json::from_slice(&body).map_err(|error| {
                state.counters.observation_gap_count += 1;
                format!("provider census parse failed: {error}").replace(api_key, "[REDACTED]")
            })?;
            let mut instances = Vec::new();
            let mut contract_labels = BTreeMap::new();
            let mut missing_contract_ids = known_ids.clone();
            for instance in listing.instances {
                if !known_ids.contains(&instance.id)
                    && !instance
                        .label
                        .as_ref()
                        .is_some_and(|label| labels.contains(label))
                {
                    continue;
                }
                missing_contract_ids.remove(&instance.id);
                if let Some(label) = instance
                    .label
                    .as_ref()
                    .filter(|label| labels.contains(*label))
                {
                    contract_labels.insert(instance.id, label.clone());
                }
                instances.push(instance.into());
            }
            instances.sort_by_key(|instance: &LabeledInstance| instance.contract_id);
            state.generation += 1;
            state.counters.successful_request_count += 1;
            let census = OwnedCensus {
                instances,
                discovered_contract_labels: contract_labels.clone(),
                contract_labels,
                missing_contract_ids,
                generation: state.generation,
                request_started,
                observed_at: Instant::now(),
                counters: state.counters.clone(),
            };
            state.last = Some((labels.clone(), known_ids.clone(), census.clone()));
            return Ok(census);
        }
        unreachable!("provider listing attempts return a result")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn malformed_census_never_exposes_echoed_credentials_or_proves_absence() {
        let server = MockServer::start().await;
        let key = "census-credential-marker-never-persist";
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instances": [{"id": key, "label": "owned"}]
            })))
            .mount(&server)
            .await;
        let error = crate::VastClient::with_base_url(server.uri(), key)
            .owned_census(
                &BTreeSet::from(["owned".to_owned()]),
                &BTreeSet::from([901]),
                Instant::now(),
                Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert!(error.contains("parse failed"));
        assert!(!error.contains(key));
    }

    #[tokio::test]
    async fn inherited_execution_deadline_contains_label_retries_but_not_reserved_cleanup() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"instances": []})),
            )
            .mount(&server)
            .await;
        let client = crate::VastClient::with_base_url(server.uri(), "secret")
            .with_execution_deadline(Some(Instant::now() + Duration::from_millis(100)));
        let worker = client.clone();
        let pending = tokio::task::spawn_blocking(move || {
            crate::BlockingVastClient::new(worker)
                .unwrap()
                .list_by_label_with_retry("owned", 10, Duration::from_secs(60))
        });
        let error = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.contains("execution owner deadline"));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        let labels = BTreeSet::from(["owned".to_owned()]);
        let known = BTreeSet::new();
        assert!(
            client
                .owned_census(&labels, &known, Instant::now(), Duration::from_secs(60))
                .await
                .unwrap_err()
                .contains("execution owner deadline")
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        let absence = client
            .cleanup_owned(&labels, &known, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(absence.instances.is_empty());
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn overlapping_exact_status_observers_share_only_the_inflight_result() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/73/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(20))
                    .set_body_json(serde_json::json!({
                        "instances": {"actual_status": "loading", "intended_status": "running"}
                    })),
            )
            .mount(&server)
            .await;
        let client = crate::VastClient::with_base_url(server.uri(), "secret");
        let clone = client.clone();
        let (first, second) = tokio::join!(client.instance_status(73), clone.instance_status(73));
        assert_eq!(first.unwrap().actual_status, "loading");
        assert_eq!(second.unwrap().actual_status, "loading");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/73/"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        assert!(
            client
                .instance_status(73)
                .await
                .unwrap_err()
                .contains("not found")
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn header_retry_after_survives_cancelled_response_body() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        for account_census in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(AtomicUsize::new(0));
            let received = Arc::clone(&requests);
            let server = tokio::spawn(async move {
                let mut connections = Vec::new();
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0; 1024];
                    while !request.ends_with(b"\r\n\r\n") {
                        let count = stream.read(&mut buffer).await.unwrap();
                        if count == 0 {
                            break;
                        }
                        request.extend_from_slice(&buffer[..count]);
                    }
                    received.fetch_add(1, Ordering::SeqCst);
                    stream.write_all(
                        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 60\r\nContent-Length: 100\r\nConnection: close\r\n\r\n",
                    ).await.unwrap();
                    // Retain the socket but withhold the declared body.
                    connections.push(stream);
                }
            });
            let client = crate::VastClient::with_base_url(url, "secret");
            let labels = BTreeSet::from(["owned".to_owned()]);
            let known = BTreeSet::new();
            let observe = || async {
                if account_census {
                    client
                        .owned_census(&labels, &known, Instant::now(), Duration::from_secs(2))
                        .await
                        .map(|_| ())
                } else {
                    client.instance_status(73).await.map(|_| ())
                }
            };
            let first = tokio::time::timeout(Duration::from_millis(150), observe()).await;
            let second = tokio::time::timeout(Duration::from_millis(150), observe()).await;
            let request_count = requests.load(Ordering::SeqCst);
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
            assert!(first.is_err() && second.is_err());
            assert_eq!(
                request_count, 1,
                "cancelled body lost cooldown; census={account_census}"
            );
        }
    }

    #[tokio::test]
    async fn cancelled_status_wait_preserves_shared_retry_after_without_blocking_other_contracts() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/73/"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "60"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/74/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instances": {"actual_status": "running", "intended_status": "running"}
            })))
            .mount(&server)
            .await;
        let client = crate::VastClient::with_base_url(server.uri(), "secret");
        let clone = client.clone();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), client.instance_status(73))
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), clone.instance_status(73))
                .await
                .is_err()
        );
        let healthy = tokio::time::timeout(Duration::from_secs(1), client.instance_status(74))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(healthy.actual_status, "running");
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/instances/73/")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn shared_scope_coalesces_without_serving_pre_boundary_census() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instances": [{"id": 1, "label": "owned"}, {"id": 9, "label": "unrelated"}]
            })))
            .mount(&server)
            .await;
        let client = crate::VastClient::with_base_url(server.uri(), "secret");
        let clone = client.clone();
        let labels = BTreeSet::from(["owned".to_owned()]);
        let known = BTreeSet::from([1, 2]);
        let boundary = Instant::now();
        let (first, second) = tokio::join!(
            client.owned_census(&labels, &known, boundary, Duration::from_secs(2)),
            clone.owned_census(&labels, &known, boundary, Duration::from_secs(2)),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(
            first
                .instances
                .iter()
                .map(|instance| instance.contract_id)
                .collect::<Vec<_>>(),
            [1]
        );
        assert_eq!(first.missing_contract_ids, BTreeSet::from([2]));
        assert_eq!(first.generation, second.generation);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        let next_boundary = Instant::now();
        let fresh = client
            .owned_census(&labels, &known, next_boundary, Duration::from_secs(2))
            .await
            .unwrap();
        assert!(fresh.request_started >= next_boundary);
        assert!(fresh.generation > first.generation);
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn provider_retry_after_cannot_be_shortened_to_fit_freshness_budget() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "60")
                    .set_body_json(serde_json::json!({"retry_after": 60})),
            )
            .mount(&server)
            .await;
        let client = crate::VastClient::with_base_url(server.uri(), "secret");
        let error = client
            .owned_census(
                &BTreeSet::from(["owned".to_owned()]),
                &BTreeSet::new(),
                Instant::now(),
                Duration::from_millis(100),
            )
            .await
            .unwrap_err();
        assert!(error.contains("capacity deadline"), "{error}");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}
