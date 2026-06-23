//! QAD validation harness — docean relay + this device only (no vast.ai).
//!
//! Brings up a single iroh node on this machine homed to the custom relay
//! (`SWACTOR_IROH_RELAY_URL`) via the exact `IrohDriver` path the cluster
//! uses, and reports what it discovers. The point is to prove the relay
//! fix: with QUIC Address Discovery (QAD) now served by the relay and the
//! client trusting its cert, this NAT'd node should learn its **public
//! reflexive address** from the relay — the mechanism that was dead when
//! the relay ran `quic: None`.
//!
//! PASS signal: `home relay` connects AND a non-private (public) address
//! appears in `direct_addresses()`. With `RUST_LOG=iroh=debug` the iroh
//! net_report QAD probe to the relay's :7842 is visible too.
//!
//! Run:
//!   SWACTOR_IROH_RELAY_URL=http://146.190.110.128:7843/ \
//!   RUST_LOG=iroh=debug \
//!   cargo run --example qad_validate

use std::net::IpAddr;
use std::time::{Duration, Instant};

use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh_driver::IrohDriverConfig;

use pipeline_parallel_inference::cluster::ClusterNode;
use pipeline_parallel_inference::iroh_transport::ACTOR_ALPN;
use pipeline_parallel_inference::messages::inference_codec_registry;
use pipeline_parallel_inference::relay_config::relay_mode_from_env;

/// A non-loopback, non-private, non-link-local address is one this host
/// could only know about via the relay (QAD) or a port-mapping — i.e. its
/// public-facing reflexive address.
fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !v4.is_loopback() && !v4.is_private() && !v4.is_link_local() && !v4.is_unspecified()
        }
        IpAddr::V6(v6) => !v6.is_loopback() && !v6.is_unspecified(),
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let relay_mode = relay_mode_from_env();
    eprintln!("qad_validate: relay_mode = {relay_mode:?}");

    let node = DistributedNodeConfig {
        swim: SwimConfig::default(),
        cache_capacity: 100,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    };

    let mut cluster = ClusterNode::new(
        IrohDriverConfig {
            secret_key: None,
            relay_mode,
            node: node.clone(),
            peer_auth: None,
            additional_alpns: vec![ACTOR_ALPN.to_vec()],
        },
        node,
        inference_codec_registry(),
        |_| {},
    )
    .expect("failed to create cluster node");

    let my_hex: String = cluster
        .node_id()
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    eprintln!("qad_validate: node_id = {my_hex}");

    // Poll for ~30s, letting iroh's net_report run its QAD probe against the
    // relay and populate discovered addresses.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_print = Instant::now() - Duration::from_secs(10);
    let mut saw_public = false;
    let mut saw_relay = false;
    while Instant::now() < deadline {
        cluster.pump_once();
        if last_print.elapsed() >= Duration::from_secs(3) {
            let relay = cluster.driver.home_relay_url().map(|u| u.to_string());
            let addrs = cluster.driver.direct_addresses();
            let publics: Vec<String> = addrs
                .iter()
                .filter(|a| is_public(a.ip()))
                .map(|a| a.to_string())
                .collect();
            saw_relay |= relay.is_some();
            saw_public |= !publics.is_empty();
            eprintln!(
                "  t+{:>2}s home_relay={} | direct_addrs={:?} | public/reflexive={:?}",
                (30 - deadline.saturating_duration_since(Instant::now()).as_secs()),
                relay.as_deref().unwrap_or("(none yet)"),
                addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                publics,
            );
            last_print = Instant::now();
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    eprintln!("\n=== QAD validation result ===");
    eprintln!("home relay connected:        {saw_relay}");
    eprintln!("public/reflexive addr found: {saw_public}");
    if saw_relay && saw_public {
        eprintln!(
            "RESULT: PASS — node reached the relay and learned a public address (QAD working)."
        );
    } else if saw_relay {
        eprintln!(
            "RESULT: PARTIAL — relay connected but no public address discovered \
             (QAD may not have completed; check RUST_LOG=iroh=debug for the net_report probe)."
        );
    } else {
        eprintln!("RESULT: FAIL — never connected to the relay.");
    }

    cluster.driver.shutdown();
}
