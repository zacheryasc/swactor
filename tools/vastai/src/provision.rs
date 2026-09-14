use serde_json::Value;

use crate::types::{CreateInstanceRequest, CreateResponse, InstanceInfo};

/// A lost reply or a generic HTTP failure may follow provider acceptance.
/// Only an explicit, understood rejection can settle a consumed create slot.
#[derive(Debug)]
pub enum CreateInstanceError {
    DefinitiveRejection(String),
    Ambiguous(String),
}

impl CreateInstanceError {
    pub fn is_definitive_rejection(&self) -> bool {
        matches!(self, Self::DefinitiveRejection(_))
    }
}

impl std::fmt::Display for CreateInstanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DefinitiveRejection(message) | Self::Ambiguous(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for CreateInstanceError {}

/// Create one vast.ai instance from a fully generic payload.
pub async fn create_instance(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    req: &CreateInstanceRequest,
) -> Result<InstanceInfo, CreateInstanceError> {
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

    // A failed response can follow provider acceptance. One invocation sends
    // exactly one create request; ownership reconciliation, not retries, must
    // account for any contract whose response was lost.
    let resp = client
        .put(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .json(&body)
        .send()
        .await
        .map_err(|error| {
            CreateInstanceError::Ambiguous(
                format!("create_instance request failed: {}", error.without_url())
                    .replace(api_key, "[REDACTED]"),
            )
        })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let message =
            format!("create_instance HTTP {status}: {text}").replace(api_key, "[REDACTED]");
        let vanished_offer = status == reqwest::StatusCode::BAD_REQUEST
            && serde_json::from_str::<Value>(&text)
                .ok()
                .is_some_and(|value| {
                    value.get("error").and_then(Value::as_str) == Some("no_such_ask")
                });
        return Err(if vanished_offer {
            CreateInstanceError::DefinitiveRejection(message)
        } else {
            CreateInstanceError::Ambiguous(message)
        });
    }
    let parsed: CreateResponse = resp.json().await.map_err(|error| {
        CreateInstanceError::Ambiguous(
            format!("create_instance parse failed: {}", error.without_url())
                .replace(api_key, "[REDACTED]"),
        )
    })?;
    Ok(InstanceInfo {
        contract_id: parsed.new_contract,
    })
}
