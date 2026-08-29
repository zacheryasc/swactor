pub struct VastLogStream {
    response: reqwest::Response,
}

impl VastLogStream {
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.response
            .chunk()
            .await
            .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
            .map_err(|error| format!("fetch_logs read failed: {error}"))
    }
}

pub async fn open_log_stream(
    client: &reqwest::Client,
    log_url: &str,
) -> Result<VastLogStream, String> {
    let response = client
        .get(log_url)
        .send()
        .await
        .map_err(|error| format!("fetch_logs failed: {error}"))?
        .error_for_status()
        .map_err(|error| format!("fetch_logs status failed: {error}"))?;
    Ok(VastLogStream { response })
}

pub async fn request_logs(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<String, String> {
    let url = format!("{base_url}/api/v0/instances/request_logs/{contract_id}/");
    let resp = client
        .put(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("request_logs failed: {e}"))?;
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("request_logs parse failed: {e}"))?;
    body["result_url"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "no result_url in log response".to_string())
}

pub async fn fetch_logs(client: &reqwest::Client, log_url: &str) -> Result<String, String> {
    let resp = client
        .get(log_url)
        .send()
        .await
        .map_err(|e| format!("fetch_logs failed: {e}"))?;
    resp.text()
        .await
        .map_err(|e| format!("fetch_logs read failed: {e}"))
}
