//! T-orchestrator: TEST_SPEC §9 — the seed spawn chain, the vast.ai
//! lease chain (mocked HTTP), and the convergence-wait helper.
//!
//! None of these tests touch real iroh, real vast.ai, or the
//! `pp-worker` binary. The spawn chain is exercised with a `sh -c`
//! "fake child" that prints `PP_GPU_NODE_ADDR ...` and then sleeps; the
//! vast.ai chain uses `wiremock`; the convergence-wait helper is a
//! pure function fed a closure.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pipeline_parallel_inference::orchestrator::{
    await_convergence, spawn_chain, ConvergeError, SpawnChainError, StageSpawnCtx,
};
use pipeline_parallel_inference::vastai;

use reqwest::Client;
use wiremock::matchers::{header, method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ─── helpers for spawn_chain tests ────────────────────────────────────

/// A unique tempdir per test, so parallel tests do not collide and
/// teardown is automatic on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("pp-t-orch-{tag}-{pid}-{nanos}"));
        std::fs::create_dir_all(&path).expect("create tempdir");
        Self { path }
    }
    fn child(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Build a `sh -c` command for a fake child that
///
/// 1. Records its OS pid to `pids_file` (newline-terminated).
/// 2. Prints a deterministic `PP_GPU_NODE_ADDR <fake_hex> <fake_direct>`
///    line so [`spawn_chain`] can parse it.
/// 3. Sleeps for `sleep_secs` so it remains alive until killed.
///
/// `fake_hex` / `fake_direct` are derived from `ctx.stage` so each
/// stage's announcement is unique and predictable from the test side.
fn fake_child_command(
    ctx: &StageSpawnCtx,
    pids_file: &PathBuf,
    sleep_secs: u32,
) -> Command {
    let hex = fake_hex(ctx.stage);
    let direct = fake_direct(ctx.stage);
    let script = format!(
        "echo $$ >> {pids}; echo 'PP_GPU_NODE_ADDR {hex} {direct}'; exec sleep {sleep_secs}",
        pids = shell_escape(pids_file),
    );
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(script);
    cmd
}

fn shell_escape(p: &PathBuf) -> String {
    // The tempdir paths are tag-pid-nanos — no shell-special characters
    // expected. Wrap in single quotes anyway so a stray dot or dash never
    // turns into a glob.
    format!("'{}'", p.display())
}

fn fake_hex(stage: u32) -> String {
    // 64 hex chars total, with the stage index baked into the last byte
    // so each is distinguishable but every entry is structurally valid
    // for downstream parsers that expect 32-byte hex.
    let prefix: String = std::iter::repeat('a').take(62).collect();
    format!("{prefix}{stage:02x}")
}

fn fake_direct(stage: u32) -> String {
    format!("127.0.0.1:{}", 40000 + stage)
}

/// Read every PID line out of `pids_file` (one per line).
fn read_pids(pids_file: &PathBuf) -> Vec<u32> {
    let contents = std::fs::read_to_string(pids_file).unwrap_or_default();
    contents
        .lines()
        .filter_map(|l| l.trim().parse::<u32>().ok())
        .collect()
}

/// Returns true if `/proc/<pid>` exists, i.e. the kernel still has an
/// entry for that pid. After a child has been `wait()`ed on, the entry
/// disappears almost immediately.
fn pid_is_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

// ─── §9.1 spawn chain ─────────────────────────────────────────────────

#[test]
fn spawn_chain_propagates_each_stage_peer_direct_to_successor() {
    let tmp = TempDir::new("propagates");
    let pids_file = tmp.child("pids");
    let captured: Arc<Mutex<Vec<StageSpawnCtx>>> = Arc::new(Mutex::new(Vec::new()));

    let cap = captured.clone();
    let pf = pids_file.clone();
    let guard = spawn_chain(4, Duration::from_secs(5), move |ctx| {
        cap.lock().unwrap().push(ctx.clone());
        fake_child_command(&ctx, &pf, 30)
    })
    .expect("spawn_chain must succeed");

    let calls = captured.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert!(calls[0].peer.is_none(), "stage 0 must have no PEER_DIRECT");
    for i in 1..calls.len() {
        let peer = calls[i].peer.as_ref().unwrap_or_else(|| {
            panic!("stage {i} must have peer set from stage {}", i - 1)
        });
        assert_eq!(
            peer.hex,
            fake_hex((i - 1) as u32),
            "stage {i} PEER_NODE_ID must match stage {}'s announcement",
            i - 1,
        );
        assert_eq!(
            peer.direct,
            fake_direct((i - 1) as u32),
            "stage {i} PEER_DIRECT must match stage {}'s announcement",
            i - 1,
        );
    }
    drop(guard); // tears down the sleep children
}

#[test]
fn spawn_chain_reads_addr_announcement_in_order() {
    let tmp = TempDir::new("reads-in-order");
    let pids_file = tmp.child("pids");

    let pf = pids_file.clone();
    let guard = spawn_chain(3, Duration::from_secs(5), move |ctx| {
        fake_child_command(&ctx, &pf, 30)
    })
    .expect("spawn_chain must succeed");

    let stages = guard.stages();
    assert_eq!(stages.len(), 3);
    for (i, spawned) in stages.iter().enumerate() {
        assert_eq!(spawned.stage, i as u32, "stage ordering");
        assert_eq!(
            spawned.addr.hex,
            fake_hex(i as u32),
            "stage {i} announcement",
        );
        assert_eq!(spawned.addr.direct, fake_direct(i as u32));
    }
}

#[test]
fn spawn_chain_kills_already_spawned_on_failure() {
    let tmp = TempDir::new("kill-on-spawn-fail");
    let pids_file = tmp.child("pids");

    let pf = pids_file.clone();
    let result = spawn_chain(4, Duration::from_secs(5), move |ctx| {
        if ctx.stage == 2 {
            // Stage 2's binary does not exist; cmd.spawn() will fail.
            return Command::new("/this/path/intentionally/does/not/exist");
        }
        fake_child_command(&ctx, &pf, 60)
    });

    match result {
        Err(SpawnChainError::Spawn { stage: 2, .. }) => {}
        other => panic!("expected Spawn error for stage 2, got {other:?}"),
    }

    // Stages 0 and 1 were spawned and then dropped by the guard on
    // error. Drop ran kill+wait synchronously, so the pids should no
    // longer have /proc entries. Give the kernel a moment to reap.
    std::thread::sleep(Duration::from_millis(100));
    let pids = read_pids(&pids_file);
    assert_eq!(pids.len(), 2, "stages 0 and 1 should have started");
    for pid in pids {
        assert!(
            !pid_is_alive(pid),
            "child pid {pid} should have been killed after spawn_chain failure",
        );
    }
}

#[test]
fn spawn_chain_kills_already_spawned_on_addr_timeout() {
    let tmp = TempDir::new("kill-on-timeout");
    let pids_file = tmp.child("pids");

    let pf = pids_file.clone();
    let result = spawn_chain(3, Duration::from_millis(400), move |ctx| {
        if ctx.stage == 1 {
            // Stage 1 starts but never announces PP_GPU_NODE_ADDR.
            // Record its pid so we can verify the kill happened, then
            // sleep so we exceed the timeout.
            let script = format!(
                "echo $$ >> {pids}; exec sleep 30",
                pids = shell_escape(&pf),
            );
            let mut cmd = Command::new("sh");
            cmd.arg("-c").arg(script);
            return cmd;
        }
        fake_child_command(&ctx, &pf, 30)
    });

    match result {
        Err(SpawnChainError::AddressTimeout { stage: 1, .. }) => {}
        other => panic!("expected AddressTimeout for stage 1, got {other:?}"),
    }

    std::thread::sleep(Duration::from_millis(100));
    let pids = read_pids(&pids_file);
    assert_eq!(
        pids.len(),
        2,
        "stages 0 and 1 should each have written a pid",
    );
    for pid in pids {
        assert!(
            !pid_is_alive(pid),
            "child pid {pid} should have been killed after addr timeout",
        );
    }
}

#[test]
fn spawn_chain_supports_num_stages_two_through_eight() {
    for n in 2u32..=8 {
        let tag = format!("n-{n}");
        let tmp = TempDir::new(&tag);
        let pids_file = tmp.child("pids");
        let pf = pids_file.clone();
        let guard = spawn_chain(n, Duration::from_secs(5), move |ctx| {
            fake_child_command(&ctx, &pf, 30)
        })
        .unwrap_or_else(|e| panic!("spawn_chain N={n} must succeed: {e}"));

        assert_eq!(guard.len(), n as usize, "all {n} stages must be spawned");
        for (i, s) in guard.stages().iter().enumerate() {
            assert_eq!(s.stage, i as u32);
            assert_eq!(s.addr.hex, fake_hex(i as u32));
        }
        drop(guard);
    }
}

// ─── §9.2 vast.ai lease chain ─────────────────────────────────────────

const SEED_ADDR: &str = "iroh://pp-seed-abc123";
const IMAGE: &str = "swactor-pp-gpu:latest";
const API_KEY: &str = "lease-test-key";

/// Build a JSON `offers` body with `count` synthetic offers, each cheaper
/// than the next (and on its own host) so the lease's price-sort and
/// distinct-host pick are deterministic.
fn offers_json(count: u32) -> serde_json::Value {
    let offers: Vec<_> = (0..count)
        .map(|i| {
            serde_json::json!({
                "id": 1000 + i,
                "gpu_name": "RTX 4090",
                "dph_total": 0.10 + (i as f64) * 0.01,
                "geolocation": "US",
                "host_id": 1000 + i,
            })
        })
        .collect();
    serde_json::json!({ "offers": offers })
}

/// Mount the `/api/v0/bundles/?q=...` GET so the lease's single
/// `select_offer_pool` query gets the same fixed candidate list. The lease
/// then picks the cheapest survivors on distinct hosts.
async fn mount_offers(server: &MockServer, count: u32) {
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/bundles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(offers_json(count)))
        .mount(server)
        .await;
}

/// Mount per-offer create endpoints that succeed and return distinct
/// contract ids `(9000 + offer_id_offset, …)`, expecting exactly one create
/// per offer. Use for tests where the catalog size equals the stage count, so
/// every offer is leased.
async fn mount_creates_ok(server: &MockServer, num_stages: u32) {
    for i in 0..num_stages {
        let offer_id = 1000 + i;
        let contract_id = 9000 + i;
        Mock::given(method("PUT"))
            .and(path_regex(format!("^/api/v0/asks/{offer_id}/$").as_str()))
            .and(header("Authorization", format!("Bearer {API_KEY}").as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "new_contract": contract_id,
                })),
            )
            .expect(1)
            .mount(server)
            .await;
    }
}

/// Mount creates for the whole `catalog` (offers 1000..1000+catalog → contracts
/// 9000+i), without a per-offer call-count expectation: only the leased
/// survivors are actually created, and which ones is up to the selection policy
/// (cheapest survivor on an unused host after the per-model cheap-drop).
async fn mount_creates_for_catalog(server: &MockServer, catalog: u32) {
    for i in 0..catalog {
        let offer_id = 1000 + i;
        let contract_id = 9000 + i;
        Mock::given(method("PUT"))
            .and(path_regex(format!("^/api/v0/asks/{offer_id}/$").as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "new_contract": contract_id,
                })),
            )
            .mount(server)
            .await;
    }
}

/// Mount `running` status for every catalog contract (9000..9000+catalog),
/// without a call-count expectation (only leased contracts are polled).
async fn mount_status_running_for_catalog(server: &MockServer, catalog: u32) {
    for i in 0..catalog {
        let id = 9000 + i;
        Mock::given(method("GET"))
            .and(path_regex(format!("^/api/v0/instances/{id}/$").as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "instances": {
                        "actual_status": "running",
                        "intended_status": "running",
                        "public_ipaddr": "203.0.113.10",
                        "ssh_port": 22,
                    }
                })),
            )
            .mount(server)
            .await;
    }
}

/// Mount per-contract `running` status responses so `wait_for_running`
/// resolves in one poll for each. `expected` is enforced so the test
/// would fail if a contract was skipped.
async fn mount_status_running(server: &MockServer, contract_ids: &[u64]) {
    for &id in contract_ids {
        Mock::given(method("GET"))
            .and(path_regex(format!("^/api/v0/instances/{id}/$").as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "instances": {
                        "actual_status": "running",
                        "intended_status": "running",
                        "public_ipaddr": "203.0.113.10",
                        "ssh_port": 22,
                    }
                })),
            )
            .expect(1..)
            .mount(server)
            .await;
    }
}

#[tokio::test]
async fn lease_chain_finds_n_distinct_offers() {
    let server = MockServer::start().await;
    mount_offers(&server, 6).await;
    mount_creates_for_catalog(&server, 6).await;
    mount_status_running_for_catalog(&server, 6).await;

    let client = Client::new();
    let infos = vastai::lease_chain(
        &client,
        &server.uri(),
        API_KEY,
        "RTX 4090",
        4,
        SEED_ADDR,
        None,
        IMAGE,
        None,
        None,
        Duration::from_millis(10),
        3,
        None,
        None,
    )
    .await
    .expect("lease_chain must succeed when N distinct offers exist");

    // Contract: N pairwise-distinct instances, each drawn from the catalog.
    // Which offer a given stage lands on is policy (cheapest survivor on an
    // unused host after the per-model cheap-drop), so we assert distinctness +
    // membership rather than a fixed id order.
    let ids: Vec<u64> = infos.iter().map(|i| i.contract_id).collect();
    assert_eq!(ids.len(), 4, "must lease N=4 instances");
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 4, "the 4 leased contracts must be distinct");
    assert!(
        ids.iter().all(|c| (9000..9006).contains(c)),
        "every leased contract must come from the mounted catalog, got {ids:?}",
    );
}

#[tokio::test]
async fn lease_chain_creates_n_instances_with_distinct_stage_env() {
    let server = MockServer::start().await;
    // Exactly N offers, all creatable, so the median-priced selection never
    // has to fall back to an unmounted offer — keeping the create count at one
    // PUT per stage regardless of which offer each stage picks.
    mount_offers(&server, 3).await;
    mount_creates_ok(&server, 3).await;
    mount_status_running(&server, &[9000, 9001, 9002]).await;

    let client = Client::new();
    vastai::lease_chain(
        &client,
        &server.uri(),
        API_KEY,
        "RTX 4090",
        3,
        SEED_ADDR,
        None,
        IMAGE,
        None,
        None,
        Duration::from_millis(10),
        3,
        None,
        None,
    )
    .await
    .expect("lease_chain must succeed");

    let reqs = server.received_requests().await.unwrap();
    let puts: Vec<_> = reqs
        .iter()
        .filter(|r| r.method.as_ref() == "PUT")
        .collect();
    assert_eq!(puts.len(), 3, "exactly one create (PUT) per stage");

    // Contract: the three creates collectively cover STAGE 0/1/2 exactly once
    // each, and every one carries NUM_STAGES=3. We assert STAGE as a set
    // rather than tying it to a specific offer id, since which offer hosts a
    // given stage is up to the (median-priced) selector.
    let mut stages: Vec<String> = Vec::new();
    for r in &puts {
        let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(
            body["env"]["NUM_STAGES"], "3",
            "every create must carry NUM_STAGES=3",
        );
        stages.push(
            body["env"]["STAGE"]
                .as_str()
                .expect("STAGE env must be a string")
                .to_string(),
        );
    }
    stages.sort();
    assert_eq!(
        stages,
        vec!["0", "1", "2"],
        "the creates must cover STAGE 0,1,2 exactly once each",
    );
}

#[tokio::test]
async fn lease_chain_rolls_back_on_partial_creation() {
    let server = MockServer::start().await;
    mount_offers(&server, 3).await;
    // Stages 0 and 1 create OK; stage 2 fails.
    for i in 0..2u32 {
        Mock::given(method("PUT"))
            .and(path_regex(format!("^/api/v0/asks/{}/$", 1000 + i).as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "new_contract": 9000 + i,
                })),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("PUT"))
        .and(path_regex("^/api/v0/asks/1002/$"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .expect(1)
        .mount(&server)
        .await;
    // Rollback should DELETE 9000 and 9001.
    for &id in &[9000u64, 9001] {
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
    let result = vastai::lease_chain(
        &client,
        &server.uri(),
        API_KEY,
        "RTX 4090",
        3,
        SEED_ADDR,
        None,
        IMAGE,
        None,
        None,
        Duration::from_millis(10),
        3,
        None,
        None,
    )
    .await;

    assert!(result.is_err(), "partial create must surface an error");
    // Drop of server verifies the DELETE expectations.
}

#[tokio::test]
async fn lease_chain_waits_for_running_per_contract() {
    let server = MockServer::start().await;
    mount_offers(&server, 4).await;
    mount_creates_for_catalog(&server, 4).await;
    mount_status_running_for_catalog(&server, 4).await;

    let client = Client::new();
    let infos = vastai::lease_chain(
        &client,
        &server.uri(),
        API_KEY,
        "RTX 4090",
        3,
        SEED_ADDR,
        None,
        IMAGE,
        None,
        None,
        Duration::from_millis(10),
        3,
        None,
        None,
    )
    .await
    .expect("lease_chain must succeed");

    // wait_for_running must poll every leased contract at least once. Use the
    // contracts actually returned rather than fixed ids, since which survivors
    // get picked is up to the selection policy.
    let reqs = server.received_requests().await.unwrap();
    for info in &infos {
        let cid = info.contract_id;
        let polls = reqs
            .iter()
            .filter(|r| {
                r.method.as_ref() == "GET"
                    && r.url.path() == format!("/api/v0/instances/{cid}/")
            })
            .count();
        assert!(
            polls >= 1,
            "wait_for_running must poll leased contract {cid} at least once",
        );
    }
}

/// A host that loads the image then stops (never reaching `running`) must not
/// sink the whole lease: the stage it was filling is destroyed and
/// re-provisioned on a fresh offer, and `lease_chain` still returns N distinct
/// running contracts — none of them the dead one. (This is the real failure
/// that aborted a 12-node lease: one instance reported "stopped: Successfully
/// loaded <image>".)
#[tokio::test]
async fn lease_chain_replaces_a_stage_that_stops_before_running() {
    let server = MockServer::start().await;
    // Four offers (1000..1003) at ascending price. The default 30% per-model
    // drop removes the cheapest (1000); the lease then takes the cheapest
    // survivors on distinct hosts — 1001 then 1002 — and the replacement for
    // the stopped stage draws the next survivor, 1003.
    mount_offers(&server, 4).await;
    for (offer, contract) in [(1002u32, 9002u64), (1001, 9001), (1003, 9003)] {
        Mock::given(method("PUT"))
            .and(path_regex(format!("^/api/v0/asks/{offer}/$").as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "new_contract": contract })),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    // 9002 stops after loading the image (the failure we are guarding against).
    Mock::given(method("GET"))
        .and(path_regex("^/api/v0/instances/9002/$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": {
                "actual_status": "created",
                "intended_status": "stopped",
                "status_msg": "Successfully loaded zacheryasc/swactor-pp-gpu:latest",
            }
        })))
        .mount(&server)
        .await;
    // The replacement (9003) and the healthy stage 1 (9001) both come up.
    mount_status_running(&server, &[9003, 9001]).await;
    // The dead instance must be torn down so it stops billing.
    Mock::given(method("DELETE"))
        .and(path_regex("^/api/v0/instances/9002/$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "success": true })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = Client::new();
    let infos = vastai::lease_chain(
        &client,
        &server.uri(),
        API_KEY,
        "RTX 4090",
        2,
        SEED_ADDR,
        None,
        IMAGE,
        None,
        None,
        Duration::from_millis(10),
        3,
        None,
        None,
    )
    .await
    .expect("lease_chain must recover by replacing the stopped stage");

    let ids: Vec<u64> = infos.iter().map(|i| i.contract_id).collect();
    assert_eq!(ids.len(), 2, "lease must still yield N=2 running instances");
    assert!(
        !ids.contains(&9002),
        "the stopped instance must not appear in the lease, got {ids:?}",
    );
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 2, "leased contracts must be distinct");
    // The DELETE + create expectations are verified on server drop.
}

// ─── §9.3 convergence wait ────────────────────────────────────────────

#[test]
fn await_convergence_returns_when_all_stages_alive() {
    let mut polls = 0usize;
    let res = await_convergence(
        3, // 3 stages
        Duration::from_secs(2),
        Duration::from_millis(1),
        || {
            polls += 1;
            // 0, 1, 2 on the first three polls; reaches 3 on the fourth.
            (polls - 1).min(3)
        },
    );
    assert!(res.is_ok(), "should converge once alive >= 3, got {res:?}");
    assert!(polls >= 4, "should have polled until threshold met");
}

#[test]
fn await_convergence_returns_error_after_timeout() {
    let res = await_convergence(
        5, // expects 5 alive peers
        Duration::from_millis(150),
        Duration::from_millis(10),
        || 3, // never converges
    );
    match res {
        Err(ConvergeError::Timeout {
            expected,
            last_seen,
            ..
        }) => {
            assert_eq!(expected, 5);
            assert_eq!(last_seen, 3);
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
}
