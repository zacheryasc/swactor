use serde_json::Value;

use crate::types::{CreateInstanceRequest, CreateResponse, InstanceInfo};

/// Create one vast.ai instance from a fully generic payload.
pub async fn create_instance(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    req: &CreateInstanceRequest,
) -> Result<InstanceInfo, String> {
    let url = format!("{base_url}/api/v0/asks/{}/", req.offer_id);
    let env = req
        .env
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect::<serde_json::Map<String, Value>>();

    let mut body = serde_json::json!({
        "image": req.image,
        "env": env,
        "disk": req.disk_gb,
    });
    if let Some(onstart) = req.onstart.as_deref() {
        body["onstart"] = Value::String(onstart.to_string());
    }
    if let Some(label) = req.label.as_deref() {
        body["label"] = Value::String(label.to_string());
    }

    let resp = client
        .put(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("create_instance request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("create_instance HTTP {status}: {body}"));
    }

    let parsed: CreateResponse = resp
        .json()
        .await
        .map_err(|e| format!("create_instance parse failed: {e}"))?;
    Ok(InstanceInfo {
        contract_id: parsed.new_contract,
    })
}
