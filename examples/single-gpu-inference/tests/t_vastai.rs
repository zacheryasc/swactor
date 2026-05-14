//! T-vastai: vast.ai API client tests with mocked HTTP responses.
//!
//! Validates the orchestration layer that rents and manages GPU instances.
//! All tests hit a local wiremock server — no real API calls, no VAST_API_KEY needed.

use reqwest::Client;
use single_gpu_inference::vastai;
use std::time::Duration;
use wiremock::matchers::{header, method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ─── find_offer ───────────────────────────────────────────────────────────

/// Three offers at different prices — the client must pick the cheapest one.
#[tokio::test]
async fn find_offer_picks_cheapest_from_multiple_offers() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex("/api/v0/bundles.*"))
        .and(header("Authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "offers": [
                { "id": 101, "gpu_name": "RTX 3090", "dph_total": 0.30, "geolocation": "US" },
                { "id": 102, "gpu_name": "RTX 3090", "dph_total": 0.15, "geolocation": "US" },
                { "id": 103, "gpu_name": "RTX 3090", "dph_total": 0.25, "geolocation": "US" },
            ]
        })))
        .mount(&server)
        .await;

    let client = Client::new();
    let offer = vastai::find_offer(&client, &server.uri(), "test-key", "RTX 3090", &[])
        .await
        .expect("should find an offer");

    assert_eq!(offer.id, 102, "must pick the cheapest offer (id=102, $0.15/hr)");
    assert!((offer.dph_total - 0.15).abs() < f64::EPSILON);
}

/// Empty offer list — the client must return an error, not panic.
#[tokio::test]
async fn find_offer_returns_error_on_empty_list() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex("/api/v0/bundles.*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "offers": []
        })))
        .mount(&server)
        .await;

    let client = Client::new();
    let result = vastai::find_offer(&client, &server.uri(), "test-key", "RTX 3090", &[]).await;

    assert!(result.is_err(), "empty offer list must produce an error");
    assert!(
        result.unwrap_err().contains("no offers"),
        "error should mention no offers"
    );
}

// ─── create_instance ──────────────────────────────────────────────────────

/// Successful creation — parse the new_contract ID and verify SEED_ADDR is in the request.
#[tokio::test]
async fn create_instance_parses_contract_and_sends_seed_addr() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path_regex("/api/v0/asks/102/"))
        .and(header("Authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "new_contract": 9999,
            "success": true
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = Client::new();
    let info = vastai::create_instance(
        &client,
        &server.uri(),
        "test-key",
        102,
        "iroh://node-abc123",
        None,
        "swactor-gpu:latest",
    )
    .await
    .expect("create should succeed");

    assert_eq!(info.contract_id, 9999);
}

/// Verify the request body contains SEED_ADDR by inspecting the recorded request.
#[tokio::test]
async fn create_instance_includes_seed_addr_in_env_payload() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path_regex("/api/v0/asks/55/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "new_contract": 7777,
        })))
        .mount(&server)
        .await;

    let client = Client::new();
    vastai::create_instance(
        &client,
        &server.uri(),
        "test-key",
        55,
        "iroh://seed-address-xyz",
        None,
        "swactor-gpu:latest",
    )
    .await
    .expect("create should succeed");

    // Inspect the recorded request to verify the body contains SEED_ADDR.
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(
        body["env"]["SEED_ADDR"],
        "iroh://seed-address-xyz",
        "request body must contain SEED_ADDR in the env payload"
    );
}

// ─── wait_for_running ─────────────────────────────────────────────────────

/// Polling sequence: loading → loading → running. Must extract IP and port.
#[tokio::test]
async fn wait_for_running_handles_loading_then_running_sequence() {
    let server = MockServer::start().await;

    // First two polls: loading
    Mock::given(method("GET"))
        .and(path_regex("/api/v0/instances/9999/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": { "actual_status": "loading" }
        })))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;

    // Third poll: running with IP and port
    Mock::given(method("GET"))
        .and(path_regex("/api/v0/instances/9999/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": {
                "actual_status": "running",
                "public_ipaddr": "203.0.113.42",
                "ssh_port": 31337
            }
        })))
        .mount(&server)
        .await;

    let client = Client::new();
    let running = vastai::wait_for_running(
        &client,
        &server.uri(),
        "test-key",
        9999,
        Duration::from_millis(10), // fast polling for tests
        10,
    )
    .await
    .expect("should eventually reach running");

    assert_eq!(running.ip, "203.0.113.42");
    assert_eq!(running.port, 31337);
}

/// Terminal status (exited) — must return error immediately without further polling.
#[tokio::test]
async fn wait_for_running_returns_error_on_exited_status() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex("/api/v0/instances/1234/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": { "actual_status": "exited" }
        })))
        .expect(1) // Must only be called once — no further polling after terminal status.
        .mount(&server)
        .await;

    let client = Client::new();
    let result = vastai::wait_for_running(
        &client,
        &server.uri(),
        "test-key",
        1234,
        Duration::from_millis(10),
        10,
    )
    .await;

    assert!(result.is_err(), "terminal status must produce an error");
    assert!(
        result.unwrap_err().contains("terminal status"),
        "error should mention terminal status"
    );
}

// ─── destroy_instance ─────────────────────────────────────────────────────

/// Verify the correct DELETE request is sent for instance teardown.
#[tokio::test]
async fn destroy_instance_sends_correct_delete_request() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path_regex("/api/v0/instances/9999/"))
        .and(header("Authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = Client::new();
    vastai::destroy_instance(&client, &server.uri(), "test-key", 9999)
        .await
        .expect("destroy should succeed");
}
