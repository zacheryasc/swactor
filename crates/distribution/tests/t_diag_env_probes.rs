#![cfg(feature = "collector")]
//! Tier-3 environment scrape end-to-end
//! (`DIAGNOSTICS_PLAN.md` T3.3 + T3.4 + T3.5).
//!
//! Three contracts under test:
//!
//! 1. **UDP probe round-trip.** The collector spawns a UDP echo on an
//!    ephemeral port; a `ProbeScheduler` configured to hit that port
//!    refreshes once and records a success outcome, an RTT, and a
//!    matching `ProbeSent` + `ProbeReceived` event pair on the
//!    aggregator's sink.
//!
//! 2. **Probe failure modes.** A scheduler pointed at an unbound UDP
//!    address times out (or hits ICMP unreachable). The tier-3 probe
//!    state distinguishes that from success — `last_outcome` is not
//!    `"ok"`, `last_error` is populated, and the attempt counter still
//!    increments.
//!
//! 3. **Vastai context + process stats fill the snapshot body.** When
//!    both introspectors are installed every snapshot carries both
//!    blocks; an empty environment yields an empty `env_vars` vec
//!    rather than an `Option::None` body.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use distribution::diagnostics::{
    Aggregator, Event, Identity, InMemorySink, ProbeScheduler, ProcessStats, Role,
    SnapshotTrigger, VastaiContext, probes::kinds,
};
use distribution::diagnostics::collector::spawn_udp_echo;
use distribution::types::NodeId;

fn build_aggregator(run_id: &str) -> (Arc<InMemorySink>, Arc<Aggregator<Arc<InMemorySink>>>) {
    let sink = Arc::new(InMemorySink::new());
    let identity = Identity::new(NodeId([0xab; 32]), Role::stage(), run_id);
    let aggregator = Arc::new(Aggregator::new(identity, sink.clone()));
    (sink, aggregator)
}

#[tokio::test]
async fn probe_to_collector_udp_echo_succeeds_and_lands_in_snapshot() {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let echo = spawn_udp_echo(bind).await.expect("spawn udp echo");
    let echo_addr = echo.local_addr;

    let (sink, agg) = build_aggregator("run-tier3-probe");
    let scheduler = Arc::new(ProbeScheduler::new());
    scheduler.set_timeout(Duration::from_millis(500));
    let emitter: distribution::diagnostics::sink::DynEmitter = agg.clone();
    scheduler.set_emitter(emitter);
    scheduler.add_target("collector-echo", echo_addr.to_string(), kinds::UDP_ECHO);
    agg.set_probe_introspector(scheduler.clone());

    // Run probe synchronously off the tokio runtime so the blocking
    // recv doesn't starve the executor.
    let s = scheduler.clone();
    tokio::task::spawn_blocking(move || s.refresh_now())
        .await
        .expect("blocking probe");

    let snap = agg.snapshot(SnapshotTrigger::OnDemand);
    let probes = snap.body.probes.expect("probe state present");
    assert_eq!(probes.probes.len(), 1);
    let p = &probes.probes[0];
    assert_eq!(p.target, "collector-echo");
    assert_eq!(p.kind, kinds::UDP_ECHO);
    assert_eq!(p.last_outcome, "ok", "probe must succeed: {:?}", p);
    assert!(p.last_rtt_ms.is_some());
    assert!(p.resolved_addr.is_some());
    assert_eq!(p.attempts, 1);
    assert_eq!(p.successes, 1);
    assert!(probes.scraped_at_ms > 0);

    let records = sink.records();
    let sent = records.iter().any(|r| matches!(&r.event, Event::ProbeSent { kind, .. } if kind == kinds::UDP_ECHO));
    let received = records
        .iter()
        .any(|r| matches!(&r.event, Event::ProbeReceived { outcome, rtt_ms, .. } if outcome == "ok" && rtt_ms.is_some()));
    assert!(sent, "ProbeSent event missing");
    assert!(received, "ProbeReceived event missing");

    drop(echo);
}

#[tokio::test]
async fn probe_to_unbound_target_records_failure() {
    // Reserve and release a port to get an address that is almost
    // certainly not listening.
    let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let dead_addr = s.local_addr().unwrap();
    drop(s);

    let (_sink, agg) = build_aggregator("run-tier3-probe-fail");
    let scheduler = Arc::new(ProbeScheduler::new());
    scheduler.set_timeout(Duration::from_millis(50));
    let emitter: distribution::diagnostics::sink::DynEmitter = agg.clone();
    scheduler.set_emitter(emitter);
    scheduler.add_target("nobody-home", dead_addr.to_string(), kinds::UDP_ECHO);
    agg.set_probe_introspector(scheduler.clone());

    let s2 = scheduler.clone();
    tokio::task::spawn_blocking(move || s2.refresh_now())
        .await
        .expect("blocking probe");

    let snap = agg.snapshot(SnapshotTrigger::OnDemand);
    let probes = snap.body.probes.expect("probe state present");
    let p = &probes.probes[0];
    assert_ne!(p.last_outcome, "ok");
    assert!(p.last_error.is_some(), "failure must record an error string");
    assert_eq!(p.attempts, 1);
    assert_eq!(p.successes, 0);
}

#[tokio::test]
async fn vastai_and_process_blocks_appear_in_snapshot_body() {
    let (_sink, agg) = build_aggregator("run-tier3-env");
    // Pass an explicit env map so the test does not depend on the
    // ambient process env.
    let env = std::collections::BTreeMap::from([
        ("VAST_CONTAINER_LABEL".to_string(), "green-3".to_string()),
        ("CONTAINER_ID".to_string(), "test-container".to_string()),
        ("VAST_BANDWIDTH_MBPS".to_string(), "1000".to_string()),
    ]);
    let vastai: Arc<dyn distribution::diagnostics::VastaiIntrospector> =
        Arc::new(VastaiContext::capture_from(env));
    agg.set_vastai_introspector(vastai);
    let proc: Arc<dyn distribution::diagnostics::ProcessIntrospector> =
        Arc::new(ProcessStats::new());
    agg.set_process_introspector(proc);

    let snap = agg.snapshot(SnapshotTrigger::OnDemand);
    let vast = snap.body.vastai.expect("vastai block present");
    assert_eq!(vast.container_id.as_deref(), Some("test-container"));
    let env_keys: Vec<&str> = vast.env_vars.iter().map(|(k, _)| k.as_str()).collect();
    assert!(env_keys.contains(&"VAST_CONTAINER_LABEL"));
    assert!(env_keys.contains(&"VAST_BANDWIDTH_MBPS"));
    assert!(vast.captured_at_ms > 0);

    let ps = snap.body.process.expect("process block present");
    assert!(ps.captured_at_ms > 0);
    // Tokio is available because we're inside `#[tokio::test]`.
    let tokio_stats = ps.tokio.expect("tokio stats inside tokio test");
    assert!(!tokio_stats.flavor.is_empty());
    #[cfg(target_os = "linux")]
    {
        assert!(ps.rss_bytes.is_some(), "Linux must read VmRSS");
        assert!(ps.open_fd_count.is_some(), "Linux must count fds");
    }
}

#[tokio::test]
async fn snapshot_omits_tier3_env_blocks_when_no_introspector_installed() {
    let (_sink, agg) = build_aggregator("run-tier3-no-env");
    let snap = agg.snapshot(SnapshotTrigger::OnDemand);
    assert!(snap.body.probes.is_none());
    assert!(snap.body.vastai.is_none());
    assert!(snap.body.process.is_none());
}
