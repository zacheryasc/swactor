use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use reqwest::StatusCode;

use crate::OwnedCensus;
use crate::observation::ProviderObserver;
use crate::types::{InstanceInfo, LabeledInstance};

const DESTROY_RETRY_ATTEMPTS: u64 = 10;

/// Destroy one vast.ai instance by contract id.
pub async fn destroy_instance(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<(), String> {
    let url = format!("{base_url}/api/v0/instances/{contract_id}/");
    let resp = client
        .delete(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|error| format!("destroy_instance request failed: {}", error.without_url()))?;
    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(());
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(
            format!("destroy_instance {contract_id} HTTP {status}: {body}")
                .replace(api_key, "[REDACTED]"),
        );
    }
    Ok(())
}

/// Destroy every contract and return per-id results in the same order.
pub async fn destroy_all_instances(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_ids: &[u64],
) -> Vec<Result<(), String>> {
    let mut results = Vec::with_capacity(contract_ids.len());
    for &id in contract_ids {
        results.push(destroy_instance(client, base_url, api_key, id).await);
    }
    results
}

/// Destroy one contract, retrying transient failures so rollback does not strand billing instances.
pub async fn destroy_instance_with_retry(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<(), String> {
    destroy_instance_with_retry_policy(
        client,
        base_url,
        api_key,
        contract_id,
        DESTROY_RETRY_ATTEMPTS,
        Duration::from_millis(500),
        Duration::from_secs(30),
    )
    .await
}

async fn destroy_instance_with_retry_policy(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
    max_attempts: u64,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Result<(), String> {
    let max_attempts = max_attempts.max(1);
    let mut attempt = 1_u64;
    loop {
        match destroy_instance(client, base_url, api_key, contract_id).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt >= max_attempts => {
                return Err(format!(
                    "destroy_instance {contract_id} failed after {attempt} attempts: {error}"
                ));
            }
            Err(_) => {
                let backoff =
                    std::cmp::min(initial_backoff.saturating_mul(attempt as u32), max_backoff);
                tokio::time::sleep(backoff).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

pub(crate) async fn rollback(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    created: &[InstanceInfo],
) {
    for info in created {
        if let Err(e) =
            destroy_instance_with_retry(client, base_url, api_key, info.contract_id).await
        {
            eprintln!(
                "lease_chain: WARNING rollback could not destroy {}: {e}",
                info.contract_id
            );
        }
    }
}

/// List every instance on the account tagged with `label`, sorted by contract id.
pub async fn list_instances_by_label(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    label: &str,
) -> Result<Vec<LabeledInstance>, String> {
    ProviderObserver::default()
        .census(
            client,
            base_url,
            api_key,
            &BTreeSet::from([label.to_owned()]),
            &BTreeSet::new(),
            Instant::now(),
            Instant::now() + crate::VastClient::REQUEST_TIMEOUT * 4 + Duration::from_secs(180),
        )
        .await
        .map(|census| census.instances)
}

/// Discover and destroy only owned labels, then prove their typed absence.
/// The deadline includes discovery, concurrent deletion, retries, and polling.
#[cfg(test)]
pub(crate) async fn cleanup_owned_instances(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    labels: &std::collections::BTreeSet<String>,
    deadline: Duration,
) -> Result<(), String> {
    cleanup_owned_with_observer(
        client,
        base_url,
        api_key,
        &ProviderObserver::default(),
        labels,
        &BTreeSet::new(),
        deadline,
    )
    .await
    .map(|_| ())
}

enum CleanupEvent {
    Census(Result<OwnedCensus, String>),
    Deleted(u64, Result<(), String>),
}

#[derive(Debug)]
pub struct CleanupFailure {
    pub detail: String,
    /// Query failures and ambiguous accounting remain failures after spend stops.
    pub accounting_failed: bool,
    pub absence: Option<OwnedCensus>,
}

/// One owner for discovery, deletion and typed absence. Pending deletes do not
/// prevent further discovery or completion of independent healthy deletions.
pub(crate) async fn cleanup_owned_with_observer(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    observer: &ProviderObserver,
    labels: &BTreeSet<String>,
    known_ids: &BTreeSet<u64>,
    duration: Duration,
) -> Result<OwnedCensus, String> {
    cleanup_owned_reconciled_with_observer(
        client,
        base_url,
        api_key,
        observer,
        labels,
        known_ids,
        &BTreeSet::new(),
        duration,
        &mut |_| Ok(()),
    )
    .await
    .map_err(|error| error.detail)
}

pub(crate) async fn cleanup_owned_reconciled_with_observer(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    observer: &ProviderObserver,
    labels: &BTreeSet<String>,
    known_ids: &BTreeSet<u64>,
    unresolved_labels: &BTreeSet<String>,
    duration: Duration,
    record_discovery: &mut (dyn FnMut(&BTreeMap<u64, String>) -> Result<(), String> + Send),
) -> Result<OwnedCensus, CleanupFailure> {
    if !unresolved_labels.is_subset(labels) {
        return Err(CleanupFailure {
            detail: "unresolved create labels exceed exact cleanup ownership".to_owned(),
            accounting_failed: true,
            absence: None,
        });
    }
    let mut unresolved_labels = unresolved_labels.clone();
    let mut accounting_errors = BTreeSet::new();
    let deadline = Instant::now() + duration;
    let mut known_ids = known_ids.clone();
    // Known exact identities can be destroyed while account discovery is rate-limited.
    let mut visible = known_ids.clone();
    let mut deleting = BTreeSet::new();
    let mut retry_at = BTreeMap::new();
    let mut tasks = tokio::task::JoinSet::new();
    let mut listing = false;
    let mut next_discovery = Instant::now();
    let mut last_error = None;
    let mut last_census = None;
    let mut discovered_contract_labels = BTreeMap::new();
    let result = loop {
        if Instant::now() >= deadline {
            break Err(CleanupFailure {
                detail: format!(
                    "owned VastAI contracts lack an absence proof within cleanup deadline; known={known_ids:?}; unresolved={unresolved_labels:?}; discovered={discovered_contract_labels:?}; pending_deletes={deleting:?}; accounting_errors={accounting_errors:?}; last_error={last_error:?}; last_census={last_census:?}; durable cleanup recovery remains required"
                ),
                accounting_failed: !accounting_errors.is_empty(),
                absence: None,
            });
        }
        for &contract in &visible {
            if deleting.len() >= 8 {
                break;
            }
            if deleting.contains(&contract)
                || retry_at
                    .get(&contract)
                    .is_some_and(|next| *next > Instant::now())
            {
                continue;
            }
            deleting.insert(contract);
            let client = client.clone();
            let base_url = base_url.to_owned();
            let api_key = api_key.to_owned();
            tasks.spawn(async move {
                CleanupEvent::Deleted(
                    contract,
                    destroy_instance(&client, &base_url, &api_key, contract).await,
                )
            });
        }
        if !listing && next_discovery <= Instant::now() {
            listing = true;
            let observer = observer.clone();
            let client = client.clone();
            let base_url = base_url.to_owned();
            let api_key = api_key.to_owned();
            let labels = labels.clone();
            let known_ids = known_ids.clone();
            let fresh_after = Instant::now();
            tasks.spawn(async move {
                CleanupEvent::Census(
                    observer
                        .census(
                            &client,
                            &base_url,
                            &api_key,
                            &labels,
                            &known_ids,
                            fresh_after,
                            deadline,
                        )
                        .await,
                )
            });
        }
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_discovery)), if !listing => {}
            event = tasks.join_next(), if !tasks.is_empty() => {
                match event {
                    Some(Ok(CleanupEvent::Census(result))) => {
                        listing = false;
                        // Fallback reconciliation is only for unchanged provider state.
                        // A completed deletion advances this immediately below.
                        next_discovery = Instant::now() + Duration::from_secs(2);
                        match result {
                            Ok(mut census) => {
                                discovered_contract_labels.extend(census.contract_labels.clone());
                                visible = census.instances.iter().map(|instance| instance.contract_id).collect();
                                known_ids.extend(&visible);
                                for label in census.contract_labels.values() {
                                    unresolved_labels.remove(label);
                                }
                                let mut by_label = BTreeMap::<&str, BTreeSet<u64>>::new();
                                for (&contract, label) in &discovered_contract_labels {
                                    by_label.entry(label.as_str()).or_default().insert(contract);
                                }
                                if by_label.values().any(|contracts| contracts.len() > 1)
                                    || census.instances.len() != visible.len()
                                {
                                    accounting_errors.insert("duplicate owned provider identities or labels".to_owned());
                                }
                                // A crash after deletion must not lose acceptance accounting.
                                // Persistence failure poisons success, never suppresses deletion.
                                if let Err(error) = record_discovery(&discovered_contract_labels) {
                                    accounting_errors.insert(error.replace(api_key, "[REDACTED]"));
                                }
                                if visible.is_empty() && unresolved_labels.is_empty()
                                    && known_ids.is_subset(&census.missing_contract_ids)
                                {
                                    census.discovered_contract_labels = discovered_contract_labels;
                                    if !accounting_errors.is_empty() {
                                        break Err(CleanupFailure {
                                            detail: format!("owned spending stopped but cleanup accounting failed: {accounting_errors:?}"),
                                            accounting_failed: true,
                                            absence: Some(census),
                                        });
                                    }
                                    break Ok(census);
                                }
                                last_census = Some((census.generation, census.counters));
                            }
                            Err(error) => {
                                accounting_errors.insert(error.replace(api_key, "[REDACTED]"));
                                last_error = Some(error.replace(api_key, "[REDACTED]"));
                            }
                        }
                    }
                    Some(Ok(CleanupEvent::Deleted(contract, result))) => {
                        deleting.remove(&contract);
                        visible.remove(&contract);
                        retry_at.insert(contract, Instant::now() + Duration::from_millis(500));
                        if let Err(error) = result {
                            let error = error.replace(api_key, "[REDACTED]");
                            accounting_errors.insert(error.clone());
                            last_error = Some(error);
                        }
                        next_discovery = Instant::now();
                    }
                    Some(Err(error)) => {
                        last_error = Some(format!("owned cleanup task failed: {error}"));
                        accounting_errors.insert("owned cleanup task failed".to_owned());
                    }
                    None => {}
                }
            }
        }
    };
    // Cancellation drops active reqwest futures; unlike detached blocking work,
    // every HTTP task is joined before returning to the durable cleanup owner.
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn destroy_rejection_never_returns_echoed_credentials() {
        let server = MockServer::start().await;
        let key = "destroy-credential-marker-never-persist";
        Mock::given(method("DELETE"))
            .and(path("/api/v0/instances/901/"))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_string(format!("permission denied for bearer {key}")),
            )
            .mount(&server)
            .await;
        let error = destroy_instance(&reqwest::Client::new(), &server.uri(), key, 901)
            .await
            .unwrap_err();
        assert!(error.contains("403"));
        assert!(error.contains("permission denied"));
        assert!(!error.contains(key));
    }

    async fn scripted_owned_account(
        initial: BTreeMap<u64, &'static str>,
        late: Option<(u64, &'static str)>,
        fail_first_query: bool,
    ) -> (
        MockServer,
        std::sync::Arc<parking_lot::Mutex<BTreeMap<u64, &'static str>>>,
    ) {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let server = MockServer::start().await;
        let live = Arc::new(parking_lot::Mutex::new(initial));
        let queries = Arc::new(AtomicUsize::new(0));
        let account = live.clone();
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(move |_: &wiremock::Request| {
                let query = queries.fetch_add(1, Ordering::SeqCst);
                if fail_first_query && query == 0 {
                    return ResponseTemplate::new(503);
                }
                let mut account = account.lock();
                let instances = account
                    .iter()
                    .map(|(id, label)| serde_json::json!({"id": id, "label": label}))
                    .collect::<Vec<_>>();
                if query == 0 {
                    if let Some((id, label)) = late {
                        account.insert(id, label);
                    }
                }
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"instances": instances}))
            })
            .mount(&server)
            .await;
        let account = live.clone();
        Mock::given(method("DELETE"))
            .respond_with(move |request: &wiremock::Request| {
                let id = request
                    .url
                    .path()
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                assert_ne!(id, 9, "unrelated account resource was deleted");
                account.lock().remove(&id);
                ResponseTemplate::new(204)
            })
            .mount(&server)
            .await;
        (server, live)
    }

    #[tokio::test]
    async fn unresolved_create_waits_past_initial_empty_census() {
        let (server, live) = scripted_owned_account(
            BTreeMap::from([(9, "unrelated")]),
            Some((1, "owned")),
            false,
        )
        .await;
        let labels = BTreeSet::from(["owned".to_owned()]);
        let mut durable = BTreeMap::new();
        let census = cleanup_owned_reconciled_with_observer(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            &ProviderObserver::default(),
            &labels,
            &BTreeSet::new(),
            &labels,
            Duration::from_secs(10),
            &mut |discovered| {
                durable.extend(discovered.clone());
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(durable, BTreeMap::from([(1, "owned".to_owned())]));
        assert_eq!(census.missing_contract_ids, BTreeSet::from([1]));
        assert_eq!(*live.lock(), BTreeMap::from([(9, "unrelated")]));
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| { matches!(request.method.as_str(), "GET" | "DELETE") })
        );
    }

    #[tokio::test]
    async fn known_contract_with_changed_label_is_still_destroyed() {
        let (server, live) = scripted_owned_account(
            BTreeMap::from([(1, "changed-label"), (9, "unrelated")]),
            None,
            false,
        )
        .await;
        let census = cleanup_owned_with_observer(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            &ProviderObserver::default(),
            &BTreeSet::from(["owned".to_owned()]),
            &BTreeSet::from([1]),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(census.missing_contract_ids, BTreeSet::from([1]));
        assert_eq!(*live.lock(), BTreeMap::from([(9, "unrelated")]));
    }

    #[tokio::test]
    async fn duplicate_labels_stop_spending_but_fail_accounting() {
        let (server, live) = scripted_owned_account(
            BTreeMap::from([(1, "owned"), (2, "owned"), (9, "unrelated")]),
            None,
            false,
        )
        .await;
        let labels = BTreeSet::from(["owned".to_owned()]);
        let mut durable = BTreeMap::new();
        let error = cleanup_owned_reconciled_with_observer(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            &ProviderObserver::default(),
            &labels,
            &BTreeSet::new(),
            &labels,
            Duration::from_secs(10),
            &mut |discovered| {
                durable.extend(discovered.clone());
                Ok(())
            },
        )
        .await
        .unwrap_err();
        assert!(error.accounting_failed);
        assert_eq!(
            error.absence.unwrap().missing_contract_ids,
            BTreeSet::from([1, 2])
        );
        assert_eq!(
            durable.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2])
        );
        assert_eq!(*live.lock(), BTreeMap::from([(9, "unrelated")]));
    }

    #[tokio::test]
    async fn failed_provider_query_stops_spending_without_successful_accounting() {
        let (server, live) =
            scripted_owned_account(BTreeMap::from([(1, "owned"), (9, "unrelated")]), None, true)
                .await;
        let error = cleanup_owned_reconciled_with_observer(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            &ProviderObserver::default(),
            &BTreeSet::from(["owned".to_owned()]),
            &BTreeSet::from([1]),
            &BTreeSet::new(),
            Duration::from_secs(10),
            &mut |_| Ok(()),
        )
        .await
        .unwrap_err();
        assert!(error.accounting_failed);
        assert_eq!(
            error.absence.unwrap().missing_contract_ids,
            BTreeSet::from([1])
        );
        assert_eq!(*live.lock(), BTreeMap::from([(9, "unrelated")]));
    }

    #[tokio::test]
    async fn failed_delete_stops_spending_without_successful_accounting() {
        let (server, live) = scripted_owned_account(
            BTreeMap::from([(1, "owned"), (9, "unrelated")]),
            None,
            false,
        )
        .await;
        let key = "cleanup-delete-credential-marker";
        let account = live.clone();
        Mock::given(method("DELETE"))
            .and(path("/api/v0/instances/1/"))
            .respond_with(move |_: &wiremock::Request| {
                account.lock().remove(&1);
                ResponseTemplate::new(503).set_body_string(format!("lost delete reply {key}"))
            })
            .with_priority(1)
            .mount(&server)
            .await;
        let error = cleanup_owned_reconciled_with_observer(
            &reqwest::Client::new(),
            &server.uri(),
            key,
            &ProviderObserver::default(),
            &BTreeSet::from(["owned".to_owned()]),
            &BTreeSet::new(),
            &BTreeSet::new(),
            Duration::from_secs(10),
            &mut |_| Ok(()),
        )
        .await
        .unwrap_err();
        assert!(error.accounting_failed);
        assert!(!error.detail.contains(key));
        assert_eq!(
            error.absence.unwrap().missing_contract_ids,
            BTreeSet::from([1])
        );
        assert_eq!(*live.lock(), BTreeMap::from([(9, "unrelated")]));
    }

    #[tokio::test]
    async fn provider_redirect_cannot_escape_scripted_endpoint() {
        let server = MockServer::start().await;
        let target = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Location", format!("{}/api/v0/instances/", target.uri())),
            )
            .mount(&server)
            .await;
        let result = crate::VastClient::with_base_url(server.uri(), "secret")
            .owned_census(
                &BTreeSet::from(["owned".to_owned()]),
                &BTreeSet::new(),
                Instant::now(),
                Duration::from_secs(2),
            )
            .await;
        assert!(result.is_err());
        assert!(target.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn owned_cleanup_discovers_late_contracts_and_preserves_unrelated_instances() {
        use parking_lot::Mutex;
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::Arc;
        let server = MockServer::start().await;
        let live = Arc::new(Mutex::new(BTreeMap::from([
            (1_u64, "owned-1"),
            (9, "unrelated"),
        ])));
        let account = live.clone();
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(move |_: &wiremock::Request| {
                let instances = account
                    .lock()
                    .iter()
                    .map(|(id, label)| serde_json::json!({"id": id, "label": label}))
                    .collect::<Vec<_>>();
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"instances": instances}))
            })
            .mount(&server)
            .await;
        let account = live.clone();
        Mock::given(method("DELETE"))
            .respond_with(move |request: &wiremock::Request| {
                let id = request
                    .url
                    .path()
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                let mut account = account.lock();
                assert_ne!(id, 9, "cleanup touched unrelated account state");
                account.remove(&id);
                if id == 1 {
                    account.insert(2, "owned-2");
                }
                ResponseTemplate::new(204)
            })
            .mount(&server)
            .await;
        cleanup_owned_instances(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            &BTreeSet::from(["owned-1".to_owned(), "owned-2".to_owned()]),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(*live.lock(), BTreeMap::from([(9, "unrelated")]));
    }

    #[tokio::test]
    async fn owned_cleanup_deadline_covers_parallel_deletions() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instances": (1..=5).map(|id| serde_json::json!({"id": id, "label": "owned"}))
                    .collect::<Vec<_>>()
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(10)))
            .mount(&server)
            .await;
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            cleanup_owned_instances(
                &reqwest::Client::new(),
                &server.uri(),
                "secret",
                &std::collections::BTreeSet::from(["owned".to_owned()]),
                Duration::from_secs(1),
            ),
        )
        .await
        .expect("cleanup deadline did not bound pending provider requests");
        assert!(result.unwrap_err().contains("lack an absence proof"));
        let deletes = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|request| request.method.as_str() == "DELETE")
            .map(|request| request.url.path().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            deletes,
            (1..=5)
                .map(|id| format!("/api/v0/instances/{id}/"))
                .collect()
        );
    }

    #[tokio::test]
    async fn destroy_missing_contract_is_success() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/api/v0/instances/123/"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&server)
            .await;

        destroy_instance(&reqwest::Client::new(), &server.uri(), "secret", 123)
            .await
            .expect("destroy should be idempotent when the contract is already gone");
    }

    #[tokio::test]
    async fn destroy_authenticates_without_exposing_credentials_in_urls() {
        let server = MockServer::start().await;
        let credential = "teardown-secret-marker";
        Mock::given(method("DELETE"))
            .and(path("/api/v0/instances/123/"))
            .and(wiremock::matchers::header(
                "Authorization",
                format!("Bearer {credential}"),
            ))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        destroy_instance(&reqwest::Client::new(), &server.uri(), credential, 123)
            .await
            .unwrap();
        let requests = server.received_requests().await.unwrap();
        assert!(requests[0].url.query().is_none());

        let error = destroy_instance(
            &reqwest::Client::new(),
            "http://127.0.0.1:0",
            credential,
            123,
        )
        .await
        .unwrap_err();
        assert!(!error.contains(credential), "{error}");
    }

    #[tokio::test]
    async fn destroy_retry_returns_last_error_after_policy_exhausted() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/api/v0/instances/123/"))
            .respond_with(ResponseTemplate::new(500).set_body_string("try later"))
            .mount(&server)
            .await;

        let error = destroy_instance_with_retry_policy(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            123,
            3,
            Duration::from_millis(1),
            Duration::from_millis(1),
        )
        .await
        .expect_err("persistent destroy failure should not retry forever");

        assert!(
            error.contains("failed after 3 attempts"),
            "error should report retry exhaustion: {error}"
        );
        assert!(
            error.contains("HTTP 500"),
            "error should preserve provider failure details: {error}"
        );
    }
}
