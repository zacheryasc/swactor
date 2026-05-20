//! T-vastai: pipeline-parallel vast.ai client tests.
//!
//! Every test runs against a local wiremock server — no real vast.ai traffic,
//! no `VAST_API_KEY` required. Covers TEST_SPEC §10 (and the N=2 baseline
//! that subsumes the original §7).
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
        None,
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
        None,
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
        None,
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

/// When a DiagEnv with a collector URL is supplied, every create_instance
/// PUT must carry the orchestrator-side SWACTOR_DIAG_* vars plus the
/// auto-derived per-stage NODE_ROLE/STAGE_INDEX/STAGE_COUNT. This is the
/// guarantee the --vastai path relies on to make rented containers ship
/// into the same diagnostics bundle as the orchestrator. Without it the
/// collector only sees the orchestrator's events and the post-processor's
/// "first peer to go dead" answer is unanchored — exactly the situation
/// VASTAI_STATUS.md describes for the failing N>=3 runs.
#[tokio::test]
async fn create_instances_forward_diag_env_to_each_stage_container() {
    let server = MockServer::start().await;

    for i in 0..3u64 {
        Mock::given(method("PUT"))
            .and(path_regex(format!("^/api/v0/asks/{}/$", 500 + i).as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "new_contract": 9500 + i,
                })),
            )
            .expect(1)
            .mount(&server)
            .await;
    }

    let client = Client::new();
    let diag = vastai::DiagEnv {
        collector_url: Some("https://collector.example:9080".into()),
        run_id: Some("vastai-run-42".into()),
        udp_echo: Some("collector.example:9081".into()),
        iroh_relay_url: None,
    };
    vastai::create_pipeline_instances(
        &client,
        &server.uri(),
        API_KEY,
        &[500, 501, 502],
        SEED_ADDR,
        None,
        IMAGE,
        Some(&diag),
    )
    .await
    .expect("create_pipeline_instances must succeed");

    let mut envs_by_stage: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    for req in server.received_requests().await.unwrap() {
        if req.method.as_ref() != "PUT" {
            continue;
        }
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        let stage = body["env"]["STAGE"].as_str().unwrap().to_string();
        envs_by_stage.insert(stage, body["env"].clone());
    }

    assert_eq!(envs_by_stage.len(), 3);
    for i in 0..3u32 {
        let env = envs_by_stage
            .get(&i.to_string())
            .unwrap_or_else(|| panic!("missing stage {i}"));
        assert_eq!(env["SWACTOR_DIAG_COLLECTOR_URL"], "https://collector.example:9080");
        assert_eq!(env["SWACTOR_DIAG_RUN_ID"], "vastai-run-42");
        assert_eq!(env["SWACTOR_DIAG_UDP_ECHO"], "collector.example:9081");
        assert_eq!(env["SWACTOR_DIAG_NODE_ROLE"], "stage");
        assert_eq!(env["SWACTOR_DIAG_STAGE_INDEX"], i.to_string());
        assert_eq!(env["SWACTOR_DIAG_STAGE_COUNT"], "3");
    }
}

/// Conversely: without a DiagEnv (None), no SWACTOR_DIAG_* keys appear in
/// the create_instance payload — confirming the forwarding is fully opt-in
/// and doesn't leak the orchestrator's local diagnostics setup into runs
/// that didn't ask for it.
#[tokio::test]
async fn create_instances_omit_diag_env_when_not_supplied() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path_regex("^/api/v0/asks/(550|551)/$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "new_contract": 9700
        })))
        .expect(2)
        .mount(&server)
        .await;

    let client = Client::new();
    vastai::create_pipeline_instances(
        &client,
        &server.uri(),
        API_KEY,
        &[550, 551],
        SEED_ADDR,
        None,
        IMAGE,
        None,
    )
    .await
    .expect("create_pipeline_instances must succeed");

    for req in server.received_requests().await.unwrap() {
        if req.method.as_ref() != "PUT" {
            continue;
        }
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        for key in [
            "SWACTOR_DIAG_COLLECTOR_URL",
            "SWACTOR_DIAG_RUN_ID",
            "SWACTOR_DIAG_UDP_ECHO",
            "SWACTOR_DIAG_NODE_ROLE",
            "SWACTOR_DIAG_STAGE_INDEX",
            "SWACTOR_DIAG_STAGE_COUNT",
        ] {
            assert!(
                body["env"].get(key).is_none(),
                "env must not carry {key} when no DiagEnv supplied; got {:?}",
                body["env"],
            );
        }
    }
}

// ─── §10 N-stage parameterised tests (N ∈ {3, 5}) ─────────────────────

/// Mount N PUT mocks (offer ids `base..base+N`) that each return a
/// distinct contract id `(contract_base + i)`. Each is `.expect(1)` so a
/// missing or extra call fails on server drop.
async fn mount_n_creates_ok(server: &MockServer, base_offer: u64, base_contract: u64, n: u32) {
    for i in 0..n {
        let offer_id = base_offer + i as u64;
        let contract_id = base_contract + i as u64;
        Mock::given(method("PUT"))
            .and(path_regex(format!("^/api/v0/asks/{offer_id}/$").as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "new_contract": contract_id
                })),
            )
            .expect(1)
            .mount(server)
            .await;
    }
}

/// Run `create_pipeline_instances` for `num_stages` distinct offers and
/// return the parsed env payload of each PUT request, keyed by offer id.
/// All §10 "create N instances" tests share this scaffolding so each one
/// stays focused on the specific property it asserts.
async fn run_create_n_and_collect_envs(
    base_offer: u64,
    num_stages: u32,
) -> std::collections::HashMap<u64, serde_json::Value> {
    let server = MockServer::start().await;
    mount_n_creates_ok(&server, base_offer, 9000, num_stages).await;

    let client = Client::new();
    let offer_ids: Vec<u64> = (0..num_stages).map(|i| base_offer + i as u64).collect();
    vastai::create_pipeline_instances(
        &client,
        &server.uri(),
        API_KEY,
        &offer_ids,
        SEED_ADDR,
        None,
        IMAGE,
        None,
    )
    .await
    .expect("create_pipeline_instances must succeed");

    let mut by_offer = std::collections::HashMap::new();
    for req in server.received_requests().await.unwrap() {
        if req.method.as_ref() != "PUT" {
            continue;
        }
        // path is "/api/v0/asks/<id>/" — strip prefix and trailing slash.
        let id: u64 = req
            .url
            .path()
            .trim_start_matches("/api/v0/asks/")
            .trim_end_matches('/')
            .parse()
            .expect("offer id in path");
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        by_offer.insert(id, body["env"].clone());
    }
    assert_eq!(
        by_offer.len(),
        num_stages as usize,
        "exactly one PUT per stage",
    );
    by_offer
}

async fn assert_distinct_stages(base_offer: u64, num_stages: u32) {
    let envs = run_create_n_and_collect_envs(base_offer, num_stages).await;
    let mut stages: Vec<String> = (0..num_stages)
        .map(|i| envs[&(base_offer + i as u64)]["STAGE"].as_str().unwrap().to_string())
        .collect();
    stages.sort();
    let expected: Vec<String> = (0..num_stages).map(|i| i.to_string()).collect();
    assert_eq!(
        stages, expected,
        "every stage index 0..{num_stages} must appear exactly once",
    );
}

#[tokio::test]
async fn create_three_instances_sends_distinct_stage_env_vars() {
    assert_distinct_stages(600, 3).await;
}

#[tokio::test]
async fn create_five_instances_sends_distinct_stage_env_vars() {
    assert_distinct_stages(700, 5).await;
}

async fn assert_same_seed_addr(base_offer: u64, num_stages: u32) {
    let envs = run_create_n_and_collect_envs(base_offer, num_stages).await;
    for i in 0..num_stages {
        assert_eq!(
            envs[&(base_offer + i as u64)]["SEED_ADDR"], SEED_ADDR,
            "stage {i} must carry the shared SEED_ADDR",
        );
    }
}

#[tokio::test]
async fn create_three_instances_all_receive_same_seed_addr() {
    assert_same_seed_addr(610, 3).await;
}

#[tokio::test]
async fn create_five_instances_all_receive_same_seed_addr() {
    assert_same_seed_addr(710, 5).await;
}

async fn assert_same_num_stages(base_offer: u64, num_stages: u32) {
    let envs = run_create_n_and_collect_envs(base_offer, num_stages).await;
    for i in 0..num_stages {
        assert_eq!(
            envs[&(base_offer + i as u64)]["NUM_STAGES"],
            num_stages.to_string(),
            "stage {i} must carry NUM_STAGES={num_stages}",
        );
    }
}

#[tokio::test]
async fn create_two_instances_all_receive_same_num_stages_env() {
    assert_same_num_stages(620, 2).await;
}

#[tokio::test]
async fn create_three_instances_all_receive_same_num_stages_env() {
    assert_same_num_stages(630, 3).await;
}

#[tokio::test]
async fn create_five_instances_all_receive_same_num_stages_env() {
    assert_same_num_stages(720, 5).await;
}

/// Generalisation of the existing two-instance rollback: at N=5, the
/// fourth create fails and the three already-created contracts must each
/// be destroyed before the error returns.
#[tokio::test]
async fn failure_to_create_kth_instance_triggers_destroy_of_prior_at_n_5() {
    let server = MockServer::start().await;

    // First three offers create successfully.
    for i in 0..3u64 {
        Mock::given(method("PUT"))
            .and(path_regex(format!("^/api/v0/asks/{}/$", 800 + i).as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "new_contract": 7000 + i,
                })),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    // Fourth offer fails.
    Mock::given(method("PUT"))
        .and(path_regex("^/api/v0/asks/803/$"))
        .respond_with(ResponseTemplate::new(500).set_body_string("nope"))
        .expect(1)
        .mount(&server)
        .await;
    // Each prior contract must be destroyed exactly once.
    for i in 0..3u64 {
        Mock::given(method("DELETE"))
            .and(path_regex(format!("^/api/v0/instances/{}/$", 7000 + i).as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"success": true})),
            )
            .expect(1)
            .mount(&server)
            .await;
    }

    let client = Client::new();
    let result = vastai::create_pipeline_instances(
        &client,
        &server.uri(),
        API_KEY,
        &[800, 801, 802, 803, 804],
        SEED_ADDR,
        None,
        IMAGE,
        None,
    )
    .await;
    assert!(result.is_err(), "partial-success creation must surface an error");
}

/// `find_offer_chain` filters returned offers against every previously
/// chosen id, so even when the upstream catalog repeats, the N chosen
/// offers are pairwise distinct.
#[tokio::test]
async fn find_offer_excludes_all_prior_offer_ids() {
    let server = MockServer::start().await;
    // Catalog of 5 offers in ascending price; find_offer returns the
    // cheapest not in `exclude_ids`, so the chain picks 100, 101, 102, 103.
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/bundles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "offers": [
                {"id": 100, "gpu_name": "RTX 4090", "dph_total": 0.10, "geolocation": "US"},
                {"id": 101, "gpu_name": "RTX 4090", "dph_total": 0.11, "geolocation": "US"},
                {"id": 102, "gpu_name": "RTX 4090", "dph_total": 0.12, "geolocation": "US"},
                {"id": 103, "gpu_name": "RTX 4090", "dph_total": 0.13, "geolocation": "US"},
                {"id": 104, "gpu_name": "RTX 4090", "dph_total": 0.14, "geolocation": "US"},
            ]
        })))
        .mount(&server)
        .await;

    let client = Client::new();
    let chosen = vastai::find_offer_chain(&client, &server.uri(), API_KEY, "RTX 4090", 4)
        .await
        .expect("4 distinct offers exist in the catalog");
    let ids: Vec<u64> = chosen.iter().map(|o| o.id).collect();
    assert_eq!(ids, vec![100, 101, 102, 103]);
    let mut unique: Vec<u64> = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 4, "all chosen offer ids must be distinct");
}

/// If the catalog has fewer than `num_stages` matching offers, the chain
/// must surface an error — it cannot silently rent fewer than requested.
#[tokio::test]
async fn find_offer_returns_error_when_fewer_than_n_offers_available() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/bundles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "offers": [
                {"id": 200, "gpu_name": "RTX 4090", "dph_total": 0.10, "geolocation": "US"},
                {"id": 201, "gpu_name": "RTX 4090", "dph_total": 0.11, "geolocation": "US"},
            ]
        })))
        .mount(&server)
        .await;

    let client = Client::new();
    let result = vastai::find_offer_chain(&client, &server.uri(), API_KEY, "RTX 4090", 4).await;
    assert!(
        result.is_err(),
        "chain must error when fewer than N offers exist",
    );
}

/// `destroy_all_instances` issues exactly N DELETEs (one per id) at N=5.
#[tokio::test]
async fn destroy_five_instances_sends_five_delete_requests() {
    let server = MockServer::start().await;
    let ids: Vec<u64> = (5100..5105).collect();
    for &id in &ids {
        Mock::given(method("DELETE"))
            .and(path_regex(format!("^/api/v0/instances/{id}/$").as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"success": true})),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    let client = Client::new();
    let results = vastai::destroy_all_instances(&client, &server.uri(), API_KEY, &ids).await;
    assert_eq!(results.len(), 5);
    for (i, r) in results.iter().enumerate() {
        assert!(r.is_ok(), "destroy {} should succeed: {:?}", ids[i], r);
    }
}
