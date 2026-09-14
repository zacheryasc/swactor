use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::observation::ProviderObserver;
use crate::types::{LabeledInstance, LifecyclePolicy, ProviderInstanceStatus, RunningInstance};

pub async fn fetch_instance_status(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<ProviderInstanceStatus, String> {
    ProviderObserver::default()
        .status(client, base_url, api_key, contract_id)
        .await
}

fn provider_terminal_error(
    contract_id: u64,
    actual: &str,
    intended: &str,
    msg: Option<&str>,
) -> Option<String> {
    if let Some(m) = msg {
        let lower = m.to_ascii_lowercase();
        if lower.contains("error") || lower.contains("failed") {
            return Some(format!("instance {contract_id} error: {m}"));
        }
    }
    if intended == "stopped" && actual != "running" {
        return Some(format!(
            "instance {contract_id} stopped: {}",
            msg.unwrap_or_default()
        ));
    }
    match actual {
        "exited" | "error" | "stopped" => Some(format!(
            "instance {contract_id} reached terminal status: {actual}"
        )),
        _ => None,
    }
}

fn labeled_endpoint(instance: &LabeledInstance) -> Option<RunningInstance> {
    let host = if instance.ssh_host.trim().is_empty() {
        instance.public_ipaddr.trim()
    } else {
        instance.ssh_host.trim()
    };
    if host.is_empty() || instance.ssh_port == 0 {
        return None;
    }
    Some(RunningInstance {
        ip: host.to_owned(),
        port: instance.ssh_port,
    })
}

fn missing_instance(error: &str) -> bool {
    error.contains("not found while fetching provider status")
}

fn parse_failed(error: &str) -> bool {
    error.contains("parse failed")
}

/// Historical env-backed polling wrapper.
pub async fn wait_for_running(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
    poll_interval: std::time::Duration,
) -> Result<RunningInstance, String> {
    let policy = LifecyclePolicy::from_env(poll_interval);
    wait_for_running_with_policy(client, base_url, api_key, contract_id, &policy).await
}

/// Poll vast.ai until an instance reaches `running`, or fail on terminal provider state.
pub async fn wait_for_running_with_policy(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
    policy: &LifecyclePolicy,
) -> Result<RunningInstance, String> {
    wait_for_running_with_observer(
        client,
        base_url,
        api_key,
        &ProviderObserver::default(),
        contract_id,
        policy,
    )
    .await
}

pub(crate) async fn wait_for_running_with_observer(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    observer: &ProviderObserver,
    contract_id: u64,
    policy: &LifecyclePolicy,
) -> Result<RunningInstance, String> {
    let mut state_since = std::time::Instant::now();
    let mut last_state: Option<String> = None;
    let mut running_without_endpoint_since = None;
    let mut poll = 0_u64;

    loop {
        poll += 1;
        let status = match observer
            .status(client, base_url, api_key, contract_id)
            .await
        {
            Ok(status) => status,
            Err(error) if missing_instance(&error) => {
                return Err(error.replace(
                    "while fetching provider status",
                    "while waiting for running",
                ));
            }
            Err(error) if parse_failed(&error) => return Err(error),
            Err(error) => {
                eprintln!("  contract {contract_id} poll {poll}: {error} (retrying)");
                tokio::time::sleep(policy.poll_interval).await;
                continue;
            }
        };

        let actual = status.actual_status.as_str();
        let intended = status.intended_status.as_str();
        if last_state.as_deref() != Some(actual) {
            state_since = std::time::Instant::now();
        }
        last_state = Some(actual.to_owned());
        let in_state = state_since.elapsed().as_secs();
        let msg_disp = match status.status_msg.as_deref() {
            Some(m) if !m.is_empty() => format!(" msg=\"{m}\""),
            _ => String::new(),
        };
        let disk_disp = match status.disk_usage {
            Some(d) if d >= 0.0 => format!(" disk={d:.2}GB"),
            _ => String::new(),
        };
        eprintln!(
            "  contract {contract_id} poll {poll}: status={actual} in-state={in_state}s{msg_disp}{disk_disp}",
        );

        if let Some(error) =
            provider_terminal_error(contract_id, actual, intended, status.status_msg.as_deref())
        {
            return Err(error);
        }
        if actual == "running" {
            if let Some(endpoint) = status.ssh_endpoint() {
                return Ok(endpoint);
            }
            let since = running_without_endpoint_since
                .get_or_insert_with(std::time::Instant::now)
                .elapsed();
            if !policy.state_timeout.is_zero() && since >= policy.state_timeout {
                return Err(format!(
                    "instance {contract_id} running without usable SSH endpoint for {}s",
                    policy.state_timeout.as_secs()
                ));
            }
        } else {
            running_without_endpoint_since = None;
        }

        tokio::time::sleep(policy.poll_interval).await;
    }
}

/// Poll provider data until a usable SSH endpoint exists, without requiring
/// provider `running` status first.
pub async fn wait_for_ssh_endpoint_with_policy(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
    label: &str,
    policy: &LifecyclePolicy,
) -> Result<RunningInstance, String> {
    wait_for_ssh_endpoint_with_observer(
        client,
        base_url,
        api_key,
        &ProviderObserver::default(),
        contract_id,
        &BTreeSet::from([label.to_owned()]),
        policy,
    )
    .await
}

pub(crate) async fn wait_for_ssh_endpoint_with_observer(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    observer: &ProviderObserver,
    contract_id: u64,
    labels: &BTreeSet<String>,
    policy: &LifecyclePolicy,
) -> Result<RunningInstance, String> {
    let mut running_without_endpoint_since = None;
    let mut poll = 0_u64;

    loop {
        poll += 1;
        // Either provider shape may publish SSH first. A rate-limited account
        // listing must not hide a usable exact endpoint, nor may stale exact
        // status hide an already usable provider proxy endpoint.
        let status = observer.status(client, base_url, api_key, contract_id);
        let boundary = Instant::now();
        let known = BTreeSet::new();
        let census = observer.census(
            client,
            base_url,
            api_key,
            labels,
            &known,
            boundary,
            boundary + crate::VastClient::REQUEST_TIMEOUT * 4 + Duration::from_secs(180),
        );
        tokio::pin!(status, census);
        let (status, census) = tokio::select! {
            status = &mut status => {
                if let Ok(status) = &status
                    && let Some(endpoint) = status.ssh_endpoint()
                {
                    return Ok(endpoint);
                }
                (status, census.await)
            }
            census = &mut census => {
                if let Ok(census) = &census
                    && let Some(endpoint) = census.instances.iter()
                        .find(|instance| instance.contract_id == contract_id)
                        .and_then(labeled_endpoint)
                {
                    return Ok(endpoint);
                }
                (status.await, census)
            }
        };
        match census {
            Ok(census) => {
                if let Some(instance) = census
                    .instances
                    .iter()
                    .find(|instance| instance.contract_id == contract_id)
                {
                    if let Some(endpoint) = labeled_endpoint(instance) {
                        eprintln!(
                            "  contract {contract_id} endpoint poll {poll}: endpoint discovered from label status={}",
                            instance.actual_status
                        );
                        return Ok(endpoint);
                    }
                    eprintln!(
                        "  contract {contract_id} endpoint poll {poll}: label status={} endpoint missing",
                        instance.actual_status
                    );
                }
            }
            Err(error) => {
                eprintln!(
                    "  contract {contract_id} endpoint poll {poll}: list-by-label error: {error} (retrying)"
                );
            }
        }

        let status = match status {
            Ok(status) => status,
            Err(error) if missing_instance(&error) => return Err(error),
            Err(error) if parse_failed(&error) => return Err(error),
            Err(error) => {
                eprintln!("  contract {contract_id} endpoint poll {poll}: {error} (retrying)");
                tokio::time::sleep(policy.poll_interval).await;
                continue;
            }
        };

        let actual = status.actual_status.as_str();
        let intended = status.intended_status.as_str();
        let msg = status.status_msg.as_deref();
        if let Some(endpoint) = status.ssh_endpoint() {
            eprintln!(
                "  contract {contract_id} endpoint poll {poll}: endpoint discovered from provider status={actual}",
            );
            return Ok(endpoint);
        }
        if let Some(error) = provider_terminal_error(contract_id, actual, intended, msg) {
            return Err(error);
        }
        if actual == "running" {
            let since = running_without_endpoint_since
                .get_or_insert_with(std::time::Instant::now)
                .elapsed();
            if !policy.state_timeout.is_zero() && since >= policy.state_timeout {
                return Err(format!(
                    "instance {contract_id} running without usable SSH endpoint for {}s",
                    policy.state_timeout.as_secs()
                ));
            }
        } else {
            running_without_endpoint_since = None;
        }

        let msg_disp = match msg {
            Some(m) if !m.is_empty() => format!(" msg=\"{m}\""),
            _ => String::new(),
        };
        eprintln!(
            "  contract {contract_id} endpoint poll {poll}: status={actual} endpoint missing{msg_disp}; waiting"
        );
        tokio::time::sleep(policy.poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn provider_status_redacts_echoed_credentials_before_truncation() {
        let server = MockServer::start().await;
        let key = "status-credential-marker-never-persist";
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/901/"))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_string(format!("{}rejected key {key}", "x".repeat(55))),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/902/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "instances": {
                    "actual_status": "error",
                    "intended_status": "running",
                    "status_msg": format!("authorization failed for {key}")
                }
            })))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let error = fetch_instance_status(&client, &server.uri(), key, 901)
            .await
            .unwrap_err();
        assert!(error.contains("403"));
        assert!(error.contains("rejected key"));
        assert!(!error.contains("status-"));
        let status = fetch_instance_status(&client, &server.uri(), key, 902)
            .await
            .unwrap();
        assert_eq!(status.actual_status, "error");
        assert!(
            status
                .status_msg
                .as_ref()
                .unwrap()
                .contains("authorization failed")
        );
        assert!(!status.status_msg.as_ref().unwrap().contains(key));
    }

    #[tokio::test]
    async fn loading_state_remains_slow_progress_before_terminal_evidence() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/123/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "instances": {
                    "actual_status": "loading",
                    "intended_status": "running",
                    "status_msg": "pulling image layers"
                }
            })))
            .mount(&server)
            .await;

        let policy = LifecyclePolicy {
            poll_interval: Duration::from_millis(1),
            state_timeout: Duration::from_millis(1),
            ..LifecyclePolicy::default()
        };

        let still_pending = tokio::time::timeout(
            Duration::from_millis(10),
            wait_for_running_with_policy(
                &reqwest::Client::new(),
                &server.uri(),
                "secret",
                123,
                &policy,
            ),
        )
        .await;

        assert!(
            still_pending.is_err(),
            "loading by itself should remain slow progress instead of replaceable failure"
        );
    }

    #[tokio::test]
    async fn endpoint_discovery_uses_label_endpoint_before_running_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "instances": [{
                    "id": 789,
                    "label": "node-789",
                    "actual_status": "loading",
                    "ssh_host": "ssh5.vast.ai",
                    "ssh_port": 22017,
                    "public_ipaddr": ""
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/789/"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "60"))
            .mount(&server)
            .await;

        let policy = LifecyclePolicy {
            poll_interval: Duration::from_secs(60),
            state_timeout: Duration::from_secs(300),
            ..LifecyclePolicy::default()
        };

        let endpoint = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_ssh_endpoint_with_policy(
                &reqwest::Client::new(),
                &server.uri(),
                "secret",
                789,
                "node-789",
                &policy,
            ),
        )
        .await
        .expect("usable proxy SSH must not wait for rate-limited exact status")
        .expect("known endpoint should start bootstrap observation before running status");

        assert_eq!(
            endpoint,
            RunningInstance {
                ip: "ssh5.vast.ai".to_owned(),
                port: 22017,
            }
        );
    }

    #[tokio::test]
    async fn usable_exact_endpoint_cancels_listing_wait_without_resetting_provider_cooldown() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "60"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/790/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(50))
                    .set_body_json(json!({
                        "instances": {"actual_status": "loading", "intended_status": "running",
                            "public_ipaddr": "127.0.0.1", "ssh_port": 22018}
                    })),
            )
            .mount(&server)
            .await;
        let client = crate::VastClient::with_base_url(server.uri(), "secret");
        let labels = BTreeSet::from(["node-790".to_owned()]);
        let endpoint = tokio::time::timeout(
            Duration::from_secs(1),
            client.wait_for_ssh_endpoint(790, &labels, &LifecyclePolicy::default()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            endpoint,
            RunningInstance {
                ip: "127.0.0.1".to_owned(),
                port: 22018
            }
        );
        let error = client
            .owned_census(
                &labels,
                &BTreeSet::new(),
                Instant::now(),
                Duration::from_millis(100),
            )
            .await
            .unwrap_err();
        assert!(error.contains("capacity deadline"));
        assert_eq!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| request.url.path() == "/api/v0/instances/")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn endpoint_missing_after_running_grace_fails() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "instances": [{
                    "id": 900,
                    "label": "node-900",
                    "actual_status": "running",
                    "ssh_host": "",
                    "ssh_port": 0,
                    "public_ipaddr": ""
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/900/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "instances": {
                    "actual_status": "running",
                    "intended_status": "running"
                }
            })))
            .mount(&server)
            .await;

        let policy = LifecyclePolicy {
            poll_interval: Duration::from_millis(1),
            state_timeout: Duration::from_millis(1),
            ..LifecyclePolicy::default()
        };

        let error = wait_for_ssh_endpoint_with_policy(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            900,
            "node-900",
            &policy,
        )
        .await
        .expect_err("running without endpoint past grace is terminal");

        assert!(
            error.contains("running without usable SSH endpoint"),
            "error should identify endpoint classification: {error}"
        );
    }
    #[tokio::test]
    async fn missing_instance_returns_error_instead_of_polling_forever() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/456/"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "success": false,
                "error": "no_such_instance",
                "msg": "Instance 456 not found."
            })))
            .mount(&server)
            .await;

        let policy = LifecyclePolicy {
            poll_interval: Duration::from_secs(60),
            state_timeout: Duration::from_secs(300),
            ..LifecyclePolicy::default()
        };

        let error = wait_for_running_with_policy(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            456,
            &policy,
        )
        .await
        .expect_err("missing instance should fail immediately");

        assert!(
            error.contains("not found while waiting for running"),
            "error should name missing provider instance: {error}"
        );
    }
}
