//! Spec §6 (iroh API version sanity, gap 6).
//!
//! The bundle's iroh version string is sourced from `Cargo.lock`, not
//! hardcoded. The `iroh_api_missing` event payload and every tier-2
//! transport snapshot carry the same string. The runtime `api_gaps`
//! list is computed from per-peer field population, so bumping iroh to
//! a version that exposes a previously-derived field causes the
//! corresponding gap to disappear with no other code change.

use distribution::diagnostics::IROH_VERSION;
use distribution::diagnostics::snapshot::{Tier2IrohState, Tier2Peer};

#[test]
fn iroh_version_constant_matches_workspace_lockfile() {
    // Read the workspace Cargo.lock and extract the iroh version, then
    // compare to the IROH_VERSION constant the build script emitted.
    let lockfile = std::fs::read_to_string(workspace_lockfile_path())
        .expect("workspace Cargo.lock must be readable from tests");
    let lock_version = extract_iroh_version(&lockfile)
        .expect("Cargo.lock must contain an iroh package entry");
    assert_eq!(
        IROH_VERSION, lock_version,
        "diagnostics::IROH_VERSION ({IROH_VERSION}) disagrees with Cargo.lock ({lock_version}) \
         — gap 6 acceptance requires bundle versions to match what was linked",
    );
}

#[test]
fn api_gaps_drop_a_field_once_a_peer_populates_it_natively() {
    // No peers scraped: every candidate is a gap.
    let bare = Tier2IrohState::compute_api_gaps(&[]);
    assert!(
        bare.iter().any(|g| g.contains("conn_type")),
        "with zero peers we have no native evidence; conn_type must remain a gap, got {bare:?}",
    );
    assert!(
        bare.iter().any(|g| g.contains("latency_ms")),
        "with zero peers we have no native evidence; latency_ms must remain a gap, got {bare:?}",
    );

    // One peer carries a native conn_type and a native latency_ms.
    // These specific candidates must drop out without changing any
    // other code.
    let native = Tier2Peer {
        peer_node_id_hex: "aa".repeat(32),
        conn_type: Some(distribution::diagnostics::ConnType::Direct),
        conn_type_source: Some("iroh".to_string()),
        latency_ms: Some(42),
        last_used_ms: None,
        last_received_ms: None,
        direct_addresses: Vec::new(),
        relay_urls: Vec::new(),
        addr_sources: None,
    };
    let gaps = Tier2IrohState::compute_api_gaps(&[native]);
    assert!(
        !gaps.iter().any(|g| g.contains("conn_type")),
        "a peer with conn_type_source=iroh must drop conn_type from api_gaps; got {gaps:?}",
    );
    assert!(
        !gaps.iter().any(|g| g.contains("latency_ms")),
        "a peer with latency_ms populated must drop latency_ms from api_gaps; got {gaps:?}",
    );
    assert!(
        gaps.iter().any(|g| g.contains("last_used_ms")),
        "fields still derived/None should keep their gap entry; got {gaps:?}",
    );
}

#[test]
fn derived_conn_type_does_not_satisfy_native_population() {
    let derived = Tier2Peer {
        peer_node_id_hex: "bb".repeat(32),
        conn_type: Some(distribution::diagnostics::ConnType::Relay),
        conn_type_source: Some("derived".to_string()),
        latency_ms: None,
        last_used_ms: None,
        last_received_ms: None,
        direct_addresses: Vec::new(),
        relay_urls: Vec::new(),
        addr_sources: None,
    };
    let gaps = Tier2IrohState::compute_api_gaps(&[derived]);
    assert!(
        gaps.iter().any(|g| g.contains("conn_type")),
        "a peer whose conn_type was derived (not native) must still show conn_type in api_gaps; \
         got {gaps:?}",
    );
}

fn workspace_lockfile_path() -> std::path::PathBuf {
    // CARGO_MANIFEST_DIR is the test crate's root; walk up to find
    // Cargo.lock the same way the build script does.
    let mut dir: std::path::PathBuf = env!("CARGO_MANIFEST_DIR").into();
    loop {
        let candidate = dir.join("Cargo.lock");
        if candidate.is_file() {
            return candidate;
        }
        if !dir.pop() {
            panic!("could not locate workspace Cargo.lock walking up from CARGO_MANIFEST_DIR");
        }
    }
}

fn extract_iroh_version(lockfile: &str) -> Option<String> {
    let mut lines = lockfile.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "name = \"iroh\"" {
            continue;
        }
        for next in lines.by_ref() {
            let t = next.trim();
            if t.starts_with("[[package]]") {
                return None;
            }
            if let Some(rest) = t.strip_prefix("version = \"") {
                if let Some(end) = rest.find('"') {
                    return Some(rest[..end].to_string());
                }
            }
        }
    }
    None
}
