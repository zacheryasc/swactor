//! T-vastai: pipeline-parallel vast.ai client tests.
//!
//! Every test runs against a local wiremock server — no real vast.ai traffic,
//! no `VAST_API_KEY` required. Covers TEST_SPEC §7 verbatim.
//!
//! Reused single-instance behaviors (offer search, status polling) are
//! validated in `single-gpu-inference/tests/t_vastai.rs` and not duplicated.

use pipeline_parallel_inference::vastai;
use reqwest::Client;
use wiremock::matchers::{header, method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SEED_ADDR: &str = "iroh://pp-seed-abc123";
const IMAGE: &str = "swactor-pp-gpu:latest";
const API_KEY: &str = "test-key";

/// Two consecutive `create_instance` calls send `STAGE=0` and `STAGE=1`
/// (one per stage), distinctly.
#[tokio::test]
async fn create_two_instances_sends_distinct_stage_env_vars() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path_regex("/api/v0/asks/101/"))
        .and(header("Authorization", format!("Bearer {API_KEY}").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "new_contract": 9001
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex("/api/v0/asks/102/"))
        .and(header("Authorization", format!("Bearer {API_KEY}").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "new_contract": 9002
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = Client::new();
    let infos = vastai::create_pipeline_instances(
        &client,
        &server.uri(),
        API_KEY,
        &[101, 102],
        SEED_ADDR,
        None,
        IMAGE,
    )
    .await
    .expect("creation must succeed");

    assert_eq!(infos.len(), 2);
    assert_eq!(infos[0].contract_id, 9001);
    assert_eq!(infos[1].contract_id, 9002);

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 2, "exactly one PUT per offer");

    // Pair each request body with the offer id from its URL so the assertion
    // is order-independent (sequential ordering is implementation detail).
    let mut by_offer: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    for req in &reqs {
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        let path = req.url.path().to_string();
        by_offer.insert(path, body);
    }

    let body_101 = by_offer.get("/api/v0/asks/101/").expect("PUT to offer 101");
    let body_102 = by_offer.get("/api/v0/asks/102/").expect("PUT to offer 102");
    assert_eq!(body_101["env"]["STAGE"], "0", "offer 101 -> stage 0");
    assert_eq!(body_102["env"]["STAGE"], "1", "offer 102 -> stage 1");
    assert_ne!(
        body_101["env"]["STAGE"], body_102["env"]["STAGE"],
        "stage env vars must be distinct between the two creates"
    );
}

/// Both creation calls carry the same `SEED_ADDR` in their env payload.
#[tokio::test]
async fn create_two_instances_both_receive_same_seed_addr() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path_regex(r"/api/v0/asks/(201|202)/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "new_contract": 9999
        })))
        .expect(2)
        .mount(&server)
        .await;

    let client = Client::new();
    vastai::create_pipeline_instances(
        &client,
        &server.uri(),
        API_KEY,
        &[201, 202],
        SEED_ADDR,
        None,
        IMAGE,
    )
    .await
    .expect("creation must succeed");

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 2);
    for req in &reqs {
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(
            body["env"]["SEED_ADDR"], SEED_ADDR,
            "both creates must carry the same SEED_ADDR"
        );
    }
}

/// If the second `create_instance` fails, the first instance is destroyed
/// before the error is returned.
#[tokio::test]
async fn failure_to_create_second_instance_triggers_destroy_of_first() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path_regex("/api/v0/asks/301/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "new_contract": 8001
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path_regex("/api/v0/asks/302/"))
        .respond_with(ResponseTemplate::new(500).set_body_string("kaboom"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path_regex("/api/v0/instances/8001/"))
        .and(header("Authorization", format!("Bearer {API_KEY}").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"success": true})))
        .expect(1)
        .mount(&server)
        .await;

    let client = Client::new();
    let result = vastai::create_pipeline_instances(
        &client,
        &server.uri(),
        API_KEY,
        &[301, 302],
        SEED_ADDR,
        None,
        IMAGE,
    )
    .await;

    assert!(
        result.is_err(),
        "partial-success creation must surface an error"
    );
    // The mock's .expect(1) on the DELETE confirms the first instance was
    // destroyed exactly once before we exited. server drop verifies it.
}

/// Best-effort cleanup over a list of contract ids issues one DELETE per id.
#[tokio::test]
async fn destroy_two_instances_sends_two_delete_requests() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path_regex("/api/v0/instances/4001/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"success": true})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path_regex("/api/v0/instances/4002/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"success": true})))
        .expect(1)
        .mount(&server)
        .await;

    let client = Client::new();
    let results =
        vastai::destroy_all_instances(&client, &server.uri(), API_KEY, &[4001, 4002]).await;

    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok(), "first destroy should succeed");
    assert!(results[1].is_ok(), "second destroy should succeed");
}

/// A failure on one DELETE does not prevent the attempt on the other.
#[tokio::test]
async fn destroy_continues_when_one_delete_fails() {
    let server = MockServer::start().await;

    // First id fails server-side.
    Mock::given(method("DELETE"))
        .and(path_regex("/api/v0/instances/5001/"))
        .respond_with(ResponseTemplate::new(500).set_body_string("nope"))
        .expect(1)
        .mount(&server)
        .await;
    // Second id must still be attempted and must succeed.
    Mock::given(method("DELETE"))
        .and(path_regex("/api/v0/instances/5002/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"success": true})))
        .expect(1)
        .mount(&server)
        .await;

    let client = Client::new();
    let results =
        vastai::destroy_all_instances(&client, &server.uri(), API_KEY, &[5001, 5002]).await;

    assert_eq!(results.len(), 2);
    assert!(
        results[0].is_err(),
        "the failing DELETE should surface as Err"
    );
    assert!(
        results[1].is_ok(),
        "second DELETE must have been attempted and succeeded"
    );
}
