//! Integration test for the diagnostics `HttpSink` + spool (S3).
//!
//! Drives a real `Aggregator<HttpSink>` against an in-process collector
//! and asserts the three behaviors S3 is meant to provide:
//!
//! 1. Live delivery — events emitted while the collector is up land
//!    on its on-disk record store as `events-{seq}.json`, the boot
//!    record arrives automatically on aggregator construction, and the
//!    sink records a `clock_sample` Custom event for every successful
//!    POST.
//! 2. Spool fallback — when the collector goes away, emitted events
//!    accumulate on disk under `{spool_dir}/{run_id}/` instead of
//!    being lost.
//! 3. Drain on recovery — when the collector comes back at the same
//!    address, the sink replays everything from the spool to the new
//!    collector and the spool ends empty.

#![cfg(feature = "collector")]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use distribution::diagnostics::collector::{CollectorState, bind, serve};
use distribution::diagnostics::{
    Aggregator, Event, HttpSink, Identity, Role, SinkConfig, SnapshotTrigger,
};
use distribution::types::NodeId;
use tokio::net::TcpSocket;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_sink_delivers_boot_and_events_to_a_live_collector() {
    let env = TestEnv::new("happy");
    let collector = Collector::start(&env, SocketAddr::from(([127, 0, 0, 1], 0))).await;

    let identity = env.identity();
    let node_id_hex = identity.node_id_hex.clone();
    let sink = HttpSink::new(env.sink_config(collector.addr())).expect("sink");
    let aggregator = Aggregator::new(identity, sink);

    for i in 0..3 {
        aggregator.emit(Event::MessageSent {
            peer: NodeId([0x11; 32]),
            kind: "ping".into(),
            size: 32 + i,
        });
    }

    // boot + at least one events batch.
    wait_for_delivered(aggregator.sink().handle(), 2, Duration::from_secs(3)).await;

    let collector_node_dir = collector.node_dir(&env.run_id, &node_id_hex);
    let files = list_files(&collector_node_dir);
    assert!(
        files.iter().any(|f| f.starts_with("boot-")),
        "expected boot file under {collector_node_dir:?}, saw {files:?}"
    );
    assert!(
        files.iter().any(|f| f.starts_with("events-")),
        "expected events file under {collector_node_dir:?}, saw {files:?}"
    );

    // Drive a snapshot through the same path.
    aggregator.snapshot(SnapshotTrigger::Periodic);
    wait_until(Duration::from_secs(3), || {
        list_files(&collector_node_dir)
            .iter()
            .any(|f| f.starts_with("snapshot-"))
    })
    .await;

    // The next outbound batch should carry the clock_sample.
    aggregator.emit(Event::MessageSent {
        peer: NodeId([0x11; 32]),
        kind: "ping".into(),
        size: 99,
    });
    wait_until(Duration::from_secs(3), || {
        any_event_with_clock_sample(&collector_node_dir)
    })
    .await;

    aggregator.sink().handle().shutdown().await;
    collector.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spool_fills_when_collector_is_down_then_drains_after_recovery() {
    let env = TestEnv::new("spool");
    // Reserve a stable address before any collector boots so we can
    // come back to the same port after a downtime window.
    let initial_listener = bind_reusable(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = initial_listener.local_addr().expect("local_addr");
    let collector = Collector::start_on(&env, initial_listener).await;

    let identity = env.identity();
    let node_id_hex = identity.node_id_hex.clone();
    let sink = HttpSink::new(env.sink_config(addr)).expect("sink");
    let handle = sink.handle();
    let aggregator = Aggregator::new(identity, sink);

    // Phase 1 — collector is up. Boot + a couple of events get through.
    for i in 0..2 {
        aggregator.emit(Event::MessageSent {
            peer: NodeId([0x22; 32]),
            kind: "ping".into(),
            size: 32 + i,
        });
    }
    wait_for_delivered(handle.clone(), 2, Duration::from_secs(3)).await;
    let delivered_before_outage = handle.delivered_count();

    // Phase 2 — collector goes down.
    collector.shutdown().await;

    // Emit a burst while the collector is gone. The drainer should
    // spool them.
    for i in 0..5 {
        aggregator.emit(Event::MessageSent {
            peer: NodeId([0x22; 32]),
            kind: "ping".into(),
            size: 100 + i,
        });
    }

    let spool_run_dir = handle.spool_run_dir();
    wait_until(Duration::from_secs(5), || {
        // batch_interval is short — the drainer flushes on the next
        // tick after we emit, fails, and writes a spool file.
        list_files(&spool_run_dir).iter().any(|f| f.ends_with(".bin"))
    })
    .await;
    let spool_files_at_outage = list_files(&spool_run_dir);
    assert!(
        spool_files_at_outage.iter().any(|f| f.ends_with(".bin")),
        "spool should have at least one .bin file while collector is down; saw {spool_files_at_outage:?}"
    );

    // Phase 3 — collector comes back at the *same* address. The
    // drainer should notice on its next tick and replay the spool.
    let recovered_listener = bind_reusable(addr).await.expect("rebind");
    let collector = Collector::start_on(&env, recovered_listener).await;
    let collector_node_dir = collector.node_dir(&env.run_id, &node_id_hex);

    wait_until(Duration::from_secs(10), || {
        // The spool should be empty (every entry POSTed and removed)
        // AND we should have more deliveries than before the outage.
        let spool_empty = list_files(&spool_run_dir)
            .iter()
            .filter(|f| f.ends_with(".bin"))
            .count()
            == 0;
        let delivered_increased = handle.delivered_count() > delivered_before_outage;
        spool_empty && delivered_increased
    })
    .await;

    // Sanity: the post-outage events did reach the collector. The
    // collector's seq counter is in-memory and resets on restart, so
    // assertions on filename count would be brittle — instead read
    // every events JSON on disk and check that the size=100..105
    // events emitted during the outage are present.
    let recovered_sizes = collected_message_sizes(&collector_node_dir);
    let post_outage_present = (100..105).all(|s| recovered_sizes.contains(&s));
    assert!(
        post_outage_present,
        "expected post-outage events (sizes 100..105) to land on collector after drain; \
         saw sizes {recovered_sizes:?}"
    );

    handle.shutdown().await;
    collector.shutdown().await;
}

// ---------------------------------------------------------------------
// Test plumbing
// ---------------------------------------------------------------------

struct TestEnv {
    _tmpdir: TempDir,
    spool_dir: PathBuf,
    collector_root: PathBuf,
    run_id: String,
    node_id_hex: String,
}

impl TestEnv {
    fn new(label: &str) -> Self {
        let tmpdir = TempDir::new(label);
        let root = tmpdir.path().to_path_buf();
        let spool_dir = root.join("spool");
        let collector_root = root.join("collector");
        std::fs::create_dir_all(&spool_dir).unwrap();
        std::fs::create_dir_all(&collector_root).unwrap();
        let run_id = format!("run-{label}");
        let node_id_hex = "a".repeat(64);
        Self {
            _tmpdir: tmpdir,
            spool_dir,
            collector_root,
            run_id,
            node_id_hex,
        }
    }

    fn identity(&self) -> Identity {
        // The hex is all-`a`, which decodes to a deterministic NodeId.
        let mut bytes = [0u8; 32];
        bytes.fill(0xaa);
        Identity::new(NodeId(bytes), Role::stage(), self.run_id.clone()).with_stage(0, 1)
    }

    fn sink_config(&self, addr: SocketAddr) -> SinkConfig {
        SinkConfig::new(
            format!("http://{addr}"),
            self.run_id.clone(),
            self.node_id_hex.clone(),
            &self.spool_dir,
        )
        .with_batch_interval(Duration::from_millis(80))
        .with_retry_initial(Duration::from_millis(80))
        .with_retry_max(Duration::from_millis(500))
        .with_request_timeout(Duration::from_millis(800))
    }
}

struct Collector {
    addr: SocketAddr,
    root: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

impl Collector {
    async fn start(env: &TestEnv, bind_to: SocketAddr) -> Self {
        let listener = bind(bind_to).await.expect("bind");
        Self::start_on(env, listener).await
    }

    async fn start_on(env: &TestEnv, listener: tokio::net::TcpListener) -> Self {
        let addr = listener.local_addr().expect("addr");
        let state = Arc::new(CollectorState::new(&env.collector_root));
        let task = tokio::spawn(async move {
            let _ = serve(listener, state).await;
        });
        // Tiny pause so the spawned task gets to accept().
        tokio::time::sleep(Duration::from_millis(50)).await;
        Self {
            addr,
            root: env.collector_root.clone(),
            task,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn node_dir(&self, run_id: &str, node_id_hex: &str) -> PathBuf {
        self.root.join(run_id).join(node_id_hex)
    }

    async fn shutdown(self) {
        self.task.abort();
        let _ = self.task.await;
        // Give the OS a moment to release the listener so the next
        // bind on the same port doesn't race.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn bind_reusable(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(128)
}

async fn wait_for_delivered(
    handle: distribution::diagnostics::SinkHandle,
    target: u64,
    budget: Duration,
) {
    let deadline = Instant::now() + budget;
    loop {
        if handle.delivered_count() >= target {
            return;
        }
        if Instant::now() > deadline {
            panic!(
                "delivered_count never reached {target} within {budget:?}; saw {} at deadline",
                handle.delivered_count()
            );
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

async fn wait_until<F: Fn() -> bool>(budget: Duration, predicate: F) {
    let deadline = Instant::now() + budget;
    loop {
        if predicate() {
            return;
        }
        if Instant::now() > deadline {
            panic!("predicate never became true within {budget:?}");
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

fn list_files(dir: &Path) -> Vec<String> {
    match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Collect the `size` field of every `MessageSent` record from every
/// `events-*.json` file under `dir`. Used to verify that specific user
/// events landed (size is the easiest disambiguator in the test setup).
fn collected_message_sizes(dir: &Path) -> Vec<u32> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in rd.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("events-") || !name.ends_with(".json") {
            continue;
        }
        let Ok(bytes) = std::fs::read(entry.path()) else {
            continue;
        };
        let Ok(v): Result<serde_json::Value, _> = serde_json::from_slice(&bytes) else {
            continue;
        };
        let Some(arr) = v.as_array() else { continue };
        for record in arr {
            if record.get("type").and_then(|t| t.as_str()) != Some("MessageSent") {
                continue;
            }
            if let Some(sz) = record.get("size").and_then(|s| s.as_u64()) {
                out.push(sz as u32);
            }
        }
    }
    out
}

/// Look through every events-*.json file in `dir` and return true if
/// any of them contains a record with `kind == "clock_sample"`.
fn any_event_with_clock_sample(dir: &Path) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in rd.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("events-") || !name.ends_with(".json") {
            continue;
        }
        let Ok(bytes) = std::fs::read(entry.path()) else {
            continue;
        };
        let Ok(v): Result<serde_json::Value, _> = serde_json::from_slice(&bytes) else {
            continue;
        };
        let Some(arr) = v.as_array() else {
            continue;
        };
        for record in arr {
            if record.get("type").and_then(|t| t.as_str()) == Some("Custom")
                && record.get("kind").and_then(|k| k.as_str()) == Some("clock_sample")
            {
                return true;
            }
        }
    }
    false
}

// ---------- tempdir helper (avoids tempfile dep) ----------

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let pid = std::process::id();
        let nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "swactor-diag-aggregator-test-{pid}-{label}-{nano}-{n}"
        ));
        std::fs::create_dir_all(&path).expect("create tempdir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
