use std::time::Duration;

use reqwest::StatusCode;

use crate::types::{InstanceInfo, InstanceListResponse, LabeledInstance};

const DESTROY_RETRY_ATTEMPTS: u64 = 10;

/// Destroy one vast.ai instance by contract id.
pub async fn destroy_instance(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<(), String> {
    let url = format!(
        "{base_url}/api/v0/instances/{contract_id}/?api_key={}",
        urlencoding::encode(api_key)
    );
    let resp = client
        .delete(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("destroy_instance request failed: {e}"))?;
    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(());
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "destroy_instance {contract_id} HTTP {status}: {body}"
        ));
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
    let url = format!("{base_url}/api/v0/instances/");
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("list_instances request failed: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("list_instances HTTP {status}: {body}"));
    }
    let body: InstanceListResponse = resp
        .json()
        .await
        .map_err(|e| format!("list_instances parse failed: {e}"))?;
    let mut out: Vec<LabeledInstance> = body
        .instances
        .into_iter()
        .filter(|e| e.label.as_deref() == Some(label))
        .map(Into::into)
        .collect();
    out.sort_by_key(|i| i.contract_id);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
