use crate::types::{InstanceResponse, LifecyclePolicy, RunningInstance};

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
    let url = format!("{base_url}/api/v0/instances/{contract_id}/");
    let mut state_since = std::time::Instant::now();
    let mut last_state: Option<String> = None;

    let mut poll = 0_u64;
    loop {
        poll += 1;
        let resp = match client
            .get(&url)
            .header("Authorization", format!("Bearer {api_key}"))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                eprintln!("  contract {contract_id} poll {poll}: request error: {e} (retrying)");
                tokio::time::sleep(policy.poll_interval).await;
                continue;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            eprintln!(
                "  contract {contract_id} poll {poll}: HTTP {status} (retrying): {}",
                body.chars().take(80).collect::<String>(),
            );
            tokio::time::sleep(policy.poll_interval).await;
            continue;
        }

        let wrapper: InstanceResponse = resp
            .json()
            .await
            .map_err(|e| format!("wait_for_running parse failed: {e}"))?;
        let status = wrapper.instances;

        let actual = status.actual_status.as_deref().unwrap_or("unknown");
        let intended = status.intended_status.as_deref().unwrap_or("unknown");

        let msg = status.status_msg.clone();
        let disk = status.disk_usage;

        if last_state.as_deref() != Some(actual) {
            state_since = std::time::Instant::now();
        }
        last_state = Some(actual.to_string());
        let in_state = state_since.elapsed().as_secs();
        let msg_disp = match msg.as_deref() {
            Some(m) if !m.is_empty() => format!(" msg=\"{m}\""),
            _ => String::new(),
        };
        let disk_disp = match disk {
            Some(d) if d >= 0.0 => format!(" disk={d:.2}GB"),
            _ => String::new(),
        };
        eprintln!(
            "  contract {contract_id} poll {poll}: status={actual} in-state={in_state}s{msg_disp}{disk_disp}",
        );

        if let Some(m) = &msg {
            if m.contains("Error") || m.contains("failed") {
                return Err(format!("instance {contract_id} error: {m}"));
            }
        }
        if intended == "stopped" && actual != "running" {
            return Err(format!(
                "instance {contract_id} stopped: {}",
                msg.unwrap_or_default()
            ));
        }
        if !policy.state_timeout.is_zero() && state_since.elapsed() >= policy.state_timeout {
            return Err(format!(
                "instance {contract_id} stuck in status {actual} for {}s",
                policy.state_timeout.as_secs()
            ));
        }

        match actual {
            "running" => {
                let ip = status
                    .public_ipaddr
                    .unwrap_or_else(|| "unknown".to_string());
                let port = status.ssh_port.unwrap_or(0);
                return Ok(RunningInstance { ip, port });
            }
            "exited" | "error" => {
                return Err(format!(
                    "instance {contract_id} reached terminal status: {actual}"
                ));
            }
            _ => {
                tokio::time::sleep(policy.poll_interval).await;
            }
        }
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
    async fn stuck_loading_state_returns_error_instead_of_polling_forever() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/instances/123/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "instances": {
                    "actual_status": "loading",
                    "intended_status": "running",
                    "status_msg": "afad30e59d72: Already exists"
                }
            })))
            .mount(&server)
            .await;

        let policy = LifecyclePolicy {
            poll_interval: Duration::from_millis(1),
            state_timeout: Duration::from_millis(5),
            ..LifecyclePolicy::default()
        };

        let error = wait_for_running_with_policy(
            &reqwest::Client::new(),
            &server.uri(),
            "secret",
            123,
            &policy,
        )
        .await
        .expect_err("stuck loading should be replaceable");

        assert!(
            error.contains("stuck in status loading"),
            "error should name stuck provider state: {error}"
        );
    }
}
