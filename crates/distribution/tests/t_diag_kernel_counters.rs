//! Spec §11 (kernel network counters, gap 11).
//!
//! Tier-3 host scrape carries UDP-layer counters from `/proc/net/snmp`
//! and per-interface byte/packet/drop/error counters from
//! `/proc/net/dev`. All counters are best-effort `Option`s: absent on
//! non-Linux, absent when the file can't be read, never silently zero.
//! The post-processor highlights any node whose UDP-drop or
//! interface-drop deltas are non-zero across the run window.

#![cfg(feature = "collector")]

use std::fs;

use distribution::diagnostics::identity::Identity;
use distribution::diagnostics::postproc::{render_summary, Bundle};
use distribution::diagnostics::snapshot::{
    Snapshot, SnapshotBody, SnapshotTrigger, Tier3DnsResolution, Tier3HostNetwork, Tier3HostState,
    Tier3Interface, Tier3InterfaceCounters, Tier3UdpKernelStats,
};
use distribution::diagnostics::Role;
use distribution::types::NodeId;

#[test]
fn snapshot_carries_kernel_counters_as_options_and_roundtrips() {
    let host = Tier3HostState {
        network: Some(Tier3HostNetwork {
            interfaces: vec![Tier3Interface {
                name: "eth0".into(),
                addresses: vec!["10.0.0.1".into()],
                mtu: Some(1500),
                up: true,
                counters: Some(Tier3InterfaceCounters {
                    rx_bytes: 1000,
                    rx_packets: 10,
                    rx_errors: 0,
                    rx_dropped: 0,
                    tx_bytes: 2000,
                    tx_packets: 20,
                    tx_errors: 0,
                    tx_dropped: 0,
                }),
            }],
            udp_kernel_stats: Some(Tier3UdpKernelStats {
                in_datagrams: Some(50),
                no_ports: Some(0),
                in_errors: Some(0),
                out_datagrams: Some(100),
                rcvbuf_errors: Some(0),
                sndbuf_errors: None,
            }),
            refreshed_at_ms: 1234,
            ..Tier3HostNetwork::default()
        }),
        dns: Vec::<Tier3DnsResolution>::new(),
        scraped_at_ms: 1234,
    };
    let s = serde_json::to_string(&host).unwrap();
    let back: Tier3HostState = serde_json::from_str(&s).unwrap();
    let net = back.network.expect("network present");
    let udp = net.udp_kernel_stats.expect("udp_kernel_stats present");
    assert_eq!(udp.in_datagrams, Some(50));
    assert_eq!(udp.sndbuf_errors, None, "missing fields must remain absent, never zero");
    let iface = &net.interfaces[0];
    let counters = iface.counters.as_ref().expect("counters present");
    assert_eq!(counters.rx_bytes, 1000);
    assert_eq!(counters.tx_packets, 20);
}

#[test]
fn old_snapshot_without_kernel_counters_still_parses() {
    // Spec §1: additive evolution. An old bundle (no `udp_kernel_stats`
    // / no `counters` per interface) must still parse cleanly through
    // the new schema.
    let json = serde_json::json!({
        "network": {
            "interfaces": [
                { "name": "lo", "addresses": [], "up": true }
            ],
            "refreshed_at_ms": 7
        },
        "dns": [],
        "scraped_at_ms": 7
    });
    let parsed: Tier3HostState = serde_json::from_value(json).unwrap();
    let net = parsed.network.expect("network present");
    assert!(net.udp_kernel_stats.is_none(), "old bundle: udp counters absent");
    assert!(net.interfaces[0].counters.is_none(), "old bundle: iface counters absent");
}

#[test]
fn postproc_surfaces_nodes_with_rising_drop_counters() {
    // Build a tiny in-memory bundle with two snapshots; the second one
    // shows a non-zero delta for udp.no_ports and for eth0.rx_dropped.
    let tmp = tempdir();
    let path = write_bundle_with_two_snapshots(tmp.path());
    let bundle = Bundle::parse_path(&path).expect("parse bundle");
    let md = render_summary(&bundle);
    assert!(
        md.contains("## Kernel network drops"),
        "summary must include the kernel-drops section; got:\n{md}",
    );
    assert!(
        md.contains("udp.no_ports +5"),
        "summary must call out the udp.no_ports delta (+5); got:\n{md}",
    );
    assert!(
        md.contains("eth0.rx_dropped +12"),
        "summary must call out the interface drop delta; got:\n{md}",
    );
}

#[test]
fn postproc_says_nothing_when_drops_stayed_at_zero() {
    let tmp = tempdir();
    let path = write_bundle_with_clean_counters(tmp.path());
    let bundle = Bundle::parse_path(&path).expect("parse bundle");
    let md = render_summary(&bundle);
    assert!(
        md.contains("No non-zero UDP/interface drop deltas observed."),
        "summary must say drops were clean; got:\n{md}",
    );
}

fn write_bundle_with_two_snapshots(dir: &std::path::Path) -> std::path::PathBuf {
    let node_hex = "11".repeat(32);
    let id = Identity::new(node_id_from_hex(&node_hex), Role::stage(), "run-counters");

    let snap0 = make_snapshot(&id, 1000, 0, 100, 0);
    let snap1 = make_snapshot(&id, 2000, 5, 200, 12);

    write_bundle(
        dir,
        "run-counters",
        &node_hex,
        "stage-0",
        &[snap0, snap1],
    )
}

fn write_bundle_with_clean_counters(dir: &std::path::Path) -> std::path::PathBuf {
    let node_hex = "22".repeat(32);
    let id = Identity::new(node_id_from_hex(&node_hex), Role::stage(), "run-clean");
    let snap0 = make_snapshot(&id, 1000, 0, 100, 0);
    let snap1 = make_snapshot(&id, 2000, 0, 200, 0);
    write_bundle(dir, "run-clean", &node_hex, "stage-0", &[snap0, snap1])
}

fn make_snapshot(
    id: &Identity,
    wall_ms: u64,
    no_ports: u64,
    in_datagrams: u64,
    rx_dropped: u64,
) -> Snapshot {
    Snapshot {
        identity: id.clone(),
        run_id: id.run_id.clone(),
        snapshot_id: format!("snap-{wall_ms}"),
        wall_ms,
        monotonic_seq: wall_ms,
        trigger: SnapshotTrigger::Periodic,
        body: SnapshotBody {
            host: Some(Tier3HostState {
                network: Some(Tier3HostNetwork {
                    interfaces: vec![Tier3Interface {
                        name: "eth0".into(),
                        addresses: Vec::new(),
                        mtu: None,
                        up: true,
                        counters: Some(Tier3InterfaceCounters {
                            rx_dropped,
                            ..Tier3InterfaceCounters::default()
                        }),
                    }],
                    udp_kernel_stats: Some(Tier3UdpKernelStats {
                        no_ports: Some(no_ports),
                        in_datagrams: Some(in_datagrams),
                        out_datagrams: Some(0),
                        in_errors: Some(0),
                        rcvbuf_errors: Some(0),
                        sndbuf_errors: Some(0),
                    }),
                    refreshed_at_ms: wall_ms,
                    ..Tier3HostNetwork::default()
                }),
                dns: Vec::new(),
                scraped_at_ms: wall_ms,
            }),
            ..SnapshotBody::default()
        },
    }
}

fn write_bundle(
    dir: &std::path::Path,
    run_id: &str,
    node_hex: &str,
    label: &str,
    snapshots: &[Snapshot],
) -> std::path::PathBuf {
    let tarball = dir.join(format!("{run_id}.tar.gz"));
    let f = fs::File::create(&tarball).unwrap();
    let gz = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    let mut tar = tar::Builder::new(gz);

    let manifest = serde_json::json!({
        "run_id": run_id,
        "run_start_collector_ms": 1,
        "run_end_collector_ms": 9000,
        "finalize_received": true,
        "nodes": [
            { "node_id_hex": node_hex, "label": label, "role": "stage", "stage_index": 0, "boot_recorded": true, "event_batches": 0, "snapshots": snapshots.len() as u64, "finalize_recorded": true },
        ],
    });
    append_bytes(
        &mut tar,
        &format!("{run_id}/MANIFEST.json"),
        &serde_json::to_vec_pretty(&manifest).unwrap(),
    );
    let boot = serde_json::json!({
        "node_id_hex": node_hex,
        "node_id_short": &node_hex[..8],
        "role": "stage",
        "stage_index": 0,
        "stage_count": 1,
        "run_id": run_id,
        "process_start_unix_ms": 1,
        "boot_sequence": 0,
    });
    append_bytes(
        &mut tar,
        &format!("{run_id}/{label}/boot.json"),
        &serde_json::to_vec_pretty(&boot).unwrap(),
    );
    for (i, snap) in snapshots.iter().enumerate() {
        let body = serde_json::to_vec_pretty(snap).unwrap();
        append_bytes(
            &mut tar,
            &format!("{run_id}/{label}/snapshots/snapshot-{:06}.json", i + 1),
            &body,
        );
    }

    tar.finish().unwrap();
    tarball
}

fn append_bytes(
    tar: &mut tar::Builder<flate2::write::GzEncoder<fs::File>>,
    dst: &str,
    bytes: &[u8],
) {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_entry_type(tar::EntryType::Regular);
    header.set_cksum();
    tar.append_data(&mut header, dst, bytes).unwrap();
}

fn node_id_from_hex(hex: &str) -> NodeId {
    let mut out = [0u8; 32];
    for (i, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        let hi = match pair[0] {
            b'0'..=b'9' => pair[0] - b'0',
            b'a'..=b'f' => pair[0] - b'a' + 10,
            _ => 0,
        };
        let lo = match pair[1] {
            b'0'..=b'9' => pair[1] - b'0',
            b'a'..=b'f' => pair[1] - b'a' + 10,
            _ => 0,
        };
        out[i] = (hi << 4) | lo;
    }
    NodeId(out)
}

struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn tempdir() -> TempDir {
    let mut path = std::env::temp_dir();
    let n: u32 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.as_nanos() as u32) ^ std::process::id())
        .unwrap_or(0);
    path.push(format!("swactor-counters-{n:x}"));
    fs::create_dir_all(&path).unwrap();
    TempDir { path }
}
