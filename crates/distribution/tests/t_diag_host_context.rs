//! Host-context scrape end-to-end (`DIAGNOSTICS_PLAN.md` T3.1 + T3.2).
//!
//! Two contracts under test:
//!
//! 1. The `HostIntrospect`, when installed on an `Aggregator`, fills
//!    out the snapshot's `body.host` field. On Linux that means a
//!    populated `network` block with at least the loopback interface
//!    and the resolv.conf nameserver list; on non-Linux it means an
//!    `Some(_)` tier-3 block with `network == None`.
//!
//! 2. Capability gaps degrade gracefully — conntrack must report
//!    `None` rather than panic when the file is unreadable, and the
//!    introspector emits a one-time `Error` event so the bundle
//!    reader knows the gap exists. Skipped when the test host
//!    actually grants the capability (rare in CI; common on bare
//!    metal).
//!
//! Plus a smoke test of the DNS path: registered hostnames appear in
//! the capture, and an obviously-unresolvable target produces an error
//! string rather than blocking the scrape. Skipped if the runtime
//! shows no DNS resolver in `/etc/resolv.conf` (offline).

use std::sync::Arc;

use distribution::diagnostics::{
    Aggregator, HostIntrospect, Identity, InMemorySink, Role, SnapshotTrigger,
};
use distribution::types::NodeId;

fn build_aggregator(
    run_id: &str,
) -> (
    Arc<InMemorySink>,
    Arc<Aggregator<Arc<InMemorySink>>>,
    Arc<HostIntrospect>,
) {
    let sink = Arc::new(InMemorySink::new());
    let identity = Identity::new(NodeId([0xab; 32]), Role::stage(), run_id);
    let aggregator = Arc::new(Aggregator::new(identity, sink.clone()));
    let host = Arc::new(HostIntrospect::new());
    aggregator.set_host_introspector(host.clone());
    (sink, aggregator, host)
}

#[test]
fn snapshot_includes_host_block_after_refresh() {
    let (_sink, agg, host) = build_aggregator("run-tier3-host");
    host.refresh_now();
    let snap = agg.snapshot(SnapshotTrigger::OnDemand);
    let body = snap.body.host.expect("host block present after install");
    assert!(body.scraped_at_ms > 0);

    #[cfg(target_os = "linux")]
    {
        let net = body
            .network
            .expect("Linux refresh populates the network block");
        let lo = net
            .interfaces
            .iter()
            .find(|i| i.name == "lo")
            .expect("loopback always visible on Linux");
        assert!(lo.up, "loopback should be up");
        // resolv.conf MAY be empty in sandboxed CI; do not require
        // entries, only require the field to be present in the shape.
        let _ = &net.resolv_conf_nameservers;
        // ipv6_enabled is best-effort but the field name is published.
        let _ = net.ipv6_enabled;
        // udp_sockets and default_routes can be empty in a clean
        // container; the contract is "shape exists," not "data populated."
        let _ = net.udp_sockets;
        let _ = net.default_routes;
        assert!(net.refreshed_at_ms > 0);
    }

    #[cfg(not(target_os = "linux"))]
    {
        // Non-Linux: shape promised, network slot None.
        assert!(body.network.is_none());
    }
}

#[test]
fn host_block_omitted_when_no_introspector_installed() {
    let sink = Arc::new(InMemorySink::new());
    let identity = Identity::new(NodeId([0x01; 32]), Role::stage(), "run-no-host");
    let aggregator = Aggregator::new(identity, sink.clone());
    let snap = aggregator.snapshot(SnapshotTrigger::OnDemand);
    assert!(snap.body.host.is_none(), "no introspector => no host block");
}

#[test]
fn dns_target_resolution_produces_an_answer_or_a_recorded_error() {
    let (_sink, agg, host) = build_aggregator("run-tier3-dns");
    // RFC 6761 reserves `.invalid` for guaranteed-non-resolvable
    // names. The scrape must come back with an error string rather
    // than blocking, panicking, or silently dropping the entry.
    host.add_dns_target("nonexistent-host.invalid");
    host.refresh_now();

    let snap = agg.snapshot(SnapshotTrigger::OnDemand);
    let body = snap.body.host.expect("host block present");
    let entry = body
        .dns
        .iter()
        .find(|d| d.hostname.contains("invalid"))
        .expect("dns target appears in capture");
    assert!(entry.a_records.is_empty());
    assert!(entry.aaaa_records.is_empty());
    assert!(
        entry.error.is_some(),
        "unresolvable target must record an error",
    );
    assert!(entry.resolved_at_ms > 0);
}

#[test]
fn dns_target_resolves_localhost_when_resolver_available() {
    // `localhost` is required by every sensible host to resolve
    // without going to DNS — the test asserts that the resolver path
    // round-trips at all. If even localhost cannot resolve we skip
    // the test (sandbox-without-nsswitch is a real failure mode).
    use std::net::ToSocketAddrs;
    let resolves = "localhost:0".to_socket_addrs().map(|i| i.count()).unwrap_or(0) > 0;
    if !resolves {
        eprintln!("skip: localhost does not resolve in this sandbox");
        return;
    }
    let (_sink, agg, host) = build_aggregator("run-tier3-dns-ok");
    host.add_dns_target("localhost");
    host.refresh_now();

    let snap = agg.snapshot(SnapshotTrigger::OnDemand);
    let body = snap.body.host.expect("host block present");
    let entry = body
        .dns
        .iter()
        .find(|d| d.hostname == "localhost")
        .expect("localhost in dns block");
    assert!(entry.error.is_none(), "localhost must resolve cleanly: {:?}", entry.error);
    let total = entry.a_records.len() + entry.aaaa_records.len();
    assert!(total > 0, "localhost should resolve to at least one address");
    assert!(entry.resolved_at_ms > 0);
}

#[cfg(target_os = "linux")]
#[test]
fn conntrack_missing_capability_records_one_time_error_event() {
    use distribution::diagnostics::sink::EventEmitter;
    use distribution::diagnostics::Event;

    // The conntrack-gap event only fires when the read fails. CI
    // sandboxes almost always lack the capability; on a host that
    // *does* expose nf_conntrack_count, the read will succeed and the
    // event won't fire — skip the assertion in that case so the test
    // is honest about what it can verify.
    let probe = std::fs::read_to_string("/proc/sys/net/netfilter/nf_conntrack_count").ok();
    if probe.is_some() {
        eprintln!("skip: this host can read nf_conntrack_count; cannot exercise gap path");
        return;
    }

    let (sink, agg, host) = build_aggregator("run-tier3-conntrack");
    // The introspector emits one-shot Error events via the emitter
    // it's been given. The aggregator implements EventEmitter, so we
    // hand it through directly.
    let emitter: Arc<dyn EventEmitter + Send + Sync + 'static> = agg.clone();
    host.set_emitter(emitter);

    host.refresh_now();
    host.refresh_now(); // second refresh must NOT emit a second event

    let _ = agg.snapshot(SnapshotTrigger::OnDemand);

    let records = sink.records();
    let host_errors: Vec<_> = records
        .iter()
        .filter(|r| {
            matches!(
                &r.event,
                Event::Error { component, .. } if component == "host_introspect"
            )
        })
        .collect();
    assert_eq!(
        host_errors.len(),
        1,
        "exactly one conntrack-gap event expected (saw {}): records={:#?}",
        host_errors.len(),
        records,
    );

    // The capture's conntrack_count must be None in this case.
    let snap = agg.snapshot(SnapshotTrigger::OnDemand);
    let body = snap.body.host.expect("host block present");
    let net = body.network.expect("network slot present on Linux");
    assert!(
        net.conntrack_count.is_none(),
        "conntrack must be None when the gap path fired",
    );
}
