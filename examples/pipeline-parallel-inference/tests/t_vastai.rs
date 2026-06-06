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

/// A custom iroh relay URL in the [`StageEnv`] is injected into every rented
/// stage's container env, so all stages reach the SWIM cluster through the same
/// relay across the internet.
#[tokio::test]
async fn create_instances_forward_iroh_relay_to_each_stage_container() {
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
    let stage_env = vastai::StageEnv {
        iroh_relay_url: Some("https://relay.example:4443".into()),
    };
    vastai::create_pipeline_instances(
        &client,
        &server.uri(),
        API_KEY,
        &[500, 501, 502],
        SEED_ADDR,
        None,
        IMAGE,
        Some(&stage_env),
    )
    .await
    .expect("create_pipeline_instances must succeed");

    let relay_key = pipeline_parallel_inference::relay_config::ENV_IROH_RELAY_URL;
    let mut stages_seen = 0;
    for req in server.received_requests().await.unwrap() {
        if req.method.as_ref() != "PUT" {
            continue;
        }
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["env"][relay_key], "https://relay.example:4443");
        stages_seen += 1;
    }
    assert_eq!(stages_seen, 3);
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

/// `select_offer_pool` drops the suspiciously-cheap slice of each GPU model
/// (cheap-for-its-model has correlated with reliability failures) and returns
/// the rest ranked ascending by effective price. With a single-model catalog of
/// 5, the default 30% drop removes the one cheapest offer.
#[tokio::test]
async fn select_offer_pool_drops_cheap_tail_and_ranks_ascending() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/bundles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "offers": [
                {"id": 100, "gpu_name": "RTX 4090", "dph_total": 0.10, "geolocation": "US", "host_id": 100},
                {"id": 101, "gpu_name": "RTX 4090", "dph_total": 0.11, "geolocation": "US", "host_id": 101},
                {"id": 102, "gpu_name": "RTX 4090", "dph_total": 0.12, "geolocation": "US", "host_id": 102},
                {"id": 103, "gpu_name": "RTX 4090", "dph_total": 0.13, "geolocation": "US", "host_id": 103},
                {"id": 104, "gpu_name": "RTX 4090", "dph_total": 0.14, "geolocation": "US", "host_id": 104},
            ]
        })))
        .mount(&server)
        .await;

    let client = Client::new();
    let pool = vastai::select_offer_pool(&client, &server.uri(), API_KEY, "RTX 4090", 4)
        .await
        .expect("catalog has survivors after the cheap-tail drop");

    let ids: Vec<u64> = pool.iter().map(|o| o.id).collect();
    assert_eq!(pool.len(), 4, "5 offers, default 30% drop removes floor(1.5)=1");
    assert!(
        !ids.contains(&100),
        "the single cheapest offer is dropped as the suspicious tail, got {ids:?}",
    );
    let prices: Vec<f64> = pool.iter().map(|o| o.dph_total).collect();
    assert!(
        prices.windows(2).all(|w| w[0] <= w[1]),
        "survivors must be ranked ascending by price: {prices:?}",
    );
}

/// When no offer passes the filters, `select_offer_pool` surfaces an error
/// rather than handing the lease an empty pool it can't draw from.
#[tokio::test]
async fn select_offer_pool_errors_on_empty_catalog() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/bundles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "offers": [] })))
        .mount(&server)
        .await;

    let client = Client::new();
    let result = vastai::select_offer_pool(&client, &server.uri(), API_KEY, "RTX 4090", 4).await;
    assert!(result.is_err(), "an empty catalog must surface an error");
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

/// A node that loads slowly — staying in a non-running state across several
/// polls while its pull keeps advancing (status_msg + disk_usage move) — must
/// be waited out, not declared dead. Once it flips to `running`,
/// `wait_for_running` returns its endpoint. This is the core guarantee that a
/// slow-but-healthy host is never killed for being slow.
#[tokio::test]
async fn wait_for_running_waits_out_a_slowly_progressing_node() {
    let server = MockServer::start().await;
    let inst = "/api/v0/instances/7001/";

    fn loading(disk: f64, msg: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": {
                "actual_status": "loading",
                "intended_status": "running",
                "status_msg": msg,
                "disk_usage": disk,
            }
        }))
    }

    // Mount running first, then the loading frames in reverse: wiremock checks
    // the most-recently-mounted matching mock first, so with up_to_n_times(1)
    // each, the GET sequence is loading→loading→loading→running.
    Mock::given(method("GET"))
        .and(path_regex(format!("^{inst}$").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": {
                "actual_status": "running",
                "intended_status": "running",
                "public_ipaddr": "203.0.113.7",
                "ssh_port": 2222,
            }
        })))
        .mount(&server)
        .await;
    for (disk, msg) in [(3.0, "Pulling fs layer 3/3"), (2.0, "Pulling fs layer 2/3"), (1.0, "Pulling from registry")] {
        Mock::given(method("GET"))
            .and(path_regex(format!("^{inst}$").as_str()))
            .respond_with(loading(disk, msg))
            .up_to_n_times(1)
            .mount(&server)
            .await;
    }

    let client = Client::new();
    let running = vastai::wait_for_running(
        &client,
        &server.uri(),
        API_KEY,
        7001,
        std::time::Duration::from_millis(10),
        20,
    )
    .await
    .expect("a slowly-progressing node must be waited out, not declared dead");

    assert_eq!(running.ip, "203.0.113.7");
    assert_eq!(running.port, 2222);
}

/// A node whose load genuinely hangs — `status_msg` and `disk_usage` frozen
/// across every poll — is declared stalled and surfaced as an error (so the
/// lease can stop fast rather than waiting out the full poll budget). Guarded
/// by PP_PULL_STALL_SECS. The progressing-node test above is immune because its
/// pull advances every poll, so the two can run concurrently.
#[tokio::test]
async fn wait_for_running_flags_a_frozen_node_as_stalled() {
    let server = MockServer::start().await;
    let inst = "/api/v0/instances/7002/";

    Mock::given(method("GET"))
        .and(path_regex(format!("^{inst}$").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": {
                "actual_status": "loading",
                "intended_status": "running",
                "status_msg": "Pulling from registry",
                "disk_usage": 1.0,
            }
        })))
        .mount(&server)
        .await;

    // SAFETY: single-threaded effect on a process-global; only this file's
    // wait_for_running tests read PP_PULL_STALL_SECS and the sibling test is
    // immune (it keeps making progress), so a transient overlap can't flip it.
    unsafe { std::env::set_var("PP_PULL_STALL_SECS", "1") };
    let result = vastai::wait_for_running(
        &Client::new(),
        &server.uri(),
        API_KEY,
        7002,
        std::time::Duration::from_millis(250),
        40,
    )
    .await;
    unsafe { std::env::remove_var("PP_PULL_STALL_SECS") };

    let err = result.expect_err("a frozen node must be flagged, not waited out forever");
    assert!(
        err.contains("stalled"),
        "stall error should name the condition, got: {err}"
    );
}

/// With PP_MAX_REPLACE_ATTEMPTS=0, a stage that never reaches `running` must
/// fail the lease WITHOUT re-leasing a replacement — the stand-down switch for
/// a slow host. We assert exactly one create (`PUT /asks`) is issued: no
/// replacement create follows the failed wait.
#[tokio::test]
async fn lease_chain_does_not_replace_when_max_replace_attempts_is_zero() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/bundles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "offers": [
                {"id": 700, "gpu_name": "RTX 4090", "dph_total": 0.20, "geolocation": "US", "host_id": 700},
            ]
        })))
        .mount(&server)
        .await;
    // Exactly one create allowed — the assertion is that no replacement create
    // is attempted after the wait fails.
    Mock::given(method("PUT"))
        .and(path_regex(r"^/api/v0/asks/700/$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "new_contract": 8001
        })))
        .expect(1)
        .mount(&server)
        .await;
    // The instance never reaches running: wait_for_running fails on the poll limit.
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/instances/8001/$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": {"actual_status": "loading", "intended_status": "running"}
        })))
        .mount(&server)
        .await;
    // Rollback destroys the dead instance.
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/api/v0/instances/8001/$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"success": true})))
        .mount(&server)
        .await;

    // SAFETY: only this file's lease_chain test reads PP_MAX_REPLACE_ATTEMPTS.
    unsafe { std::env::set_var("PP_MAX_REPLACE_ATTEMPTS", "0") };
    let result = vastai::lease_chain(
        &Client::new(),
        &server.uri(),
        API_KEY,
        "RTX 4090",
        1,
        SEED_ADDR,
        None,
        IMAGE,
        None,
        None,
        std::time::Duration::from_millis(10),
        2,
        None,
    )
    .await;
    unsafe { std::env::remove_var("PP_MAX_REPLACE_ATTEMPTS") };

    assert!(
        result.is_err(),
        "a stage that never runs must fail the lease when replacement is disabled"
    );
    // Only the create endpoint lives under /api/v0/asks/ (bundles + instances
    // are elsewhere), so a path filter is enough to count creates.
    let creates = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().starts_with("/api/v0/asks/"))
        .count();
    assert_eq!(
        creates, 1,
        "PP_MAX_REPLACE_ATTEMPTS=0 must not re-lease: expected exactly one create, got {creates}"
    );
}
