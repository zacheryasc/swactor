//! Spec §2 (relay-session tunnel state, gap 2) and §3 (per-transition
//! relay events, gap 3).
//!
//! After this work every snapshot a node emits carries an explicit
//! answer to "is my tunnel to my relay healthy right now," separate
//! from "do my peer connections through that tunnel work." When the
//! transport library does not expose enough state to populate the
//! field natively, the snapshot says so explicitly via the
//! `status_source` discriminator, and the field name appears in
//! `Tier2IrohState::api_gaps` so the bundle reader is never left
//! guessing whether `unknown` means "tunnel is unknown" vs "we
//! couldn't ask."
//!
//! For §3: every relay-state flip produces an event on the event
//! stream. `RelaySessionStateChanged` is the authoritative source for
//! "did the tunnel flap" — a grep for the variant across the bundle
//! tells you which nodes flapped and when.

use distribution::diagnostics::event::Event;
use distribution::diagnostics::snapshot::{Tier2IrohState, Tier2Peer, Tier2RelaySession};

#[test]
fn relay_session_carries_status_and_status_source_discriminator() {
    // Bundle-reader contract from spec §2: every snapshot must carry
    // an explicit (status, status_source) pair so absent is
    // distinguishable from "we couldn't ask."
    let unknown = Tier2RelaySession {
        relay_url: None,
        status: "unknown".to_string(),
        status_source: "derived".to_string(),
        status_changed_at_ms: None,
        status_entered_at_ms: Some(100),
        last_send_at_ms: None,
        last_recv_at_ms: None,
        tx_bytes_total: None,
        rx_bytes_total: None,
    };
    let json = serde_json::to_value(&unknown).unwrap();
    assert_eq!(json["status"], "unknown");
    assert_eq!(json["status_source"], "derived");
    let back: Tier2RelaySession = serde_json::from_value(json).unwrap();
    assert_eq!(back.status, "unknown");
    assert_eq!(back.status_source, "derived");
}

#[test]
fn relay_tunnel_status_is_an_api_gap_until_iroh_populates_it_natively() {
    // §2 cross-references §6: when the tunnel status is derived (not
    // reported), its canonical name must appear in `api_gaps` so the
    // bundle reader knows the value is synthesized.
    let derived = Tier2RelaySession {
        relay_url: Some("https://relay.example/".into()),
        status: "connected".into(),
        status_source: "derived".into(),
        ..Tier2RelaySession::default()
    };
    let gaps = Tier2IrohState::compute_api_gaps_full(&[], Some(&derived));
    assert!(
        gaps.iter().any(|g| g == "RelayTunnel.status"),
        "derived status must keep RelayTunnel.status in the gap list; got {gaps:?}",
    );

    let native = Tier2RelaySession {
        relay_url: Some("https://relay.example/".into()),
        status: "connected".into(),
        status_source: "iroh".into(),
        ..Tier2RelaySession::default()
    };
    let gaps_native = Tier2IrohState::compute_api_gaps_full(&[], Some(&native));
    assert!(
        !gaps_native.iter().any(|g| g == "RelayTunnel.status"),
        "natively-sourced status must drop RelayTunnel.status from gaps; got {gaps_native:?}",
    );
}

#[test]
fn computed_gaps_combine_peer_and_relay_candidates() {
    // §1 cross-cut: a bundle reader sees one gap list per snapshot
    // covering both per-peer and per-relay-tunnel candidates.
    let no_data = Tier2IrohState::compute_api_gaps_full(&[], None);
    assert!(
        no_data.iter().any(|g| g.contains("RemoteInfo.")),
        "with no peer evidence we must list peer-side gaps; got {no_data:?}",
    );
    assert!(
        no_data.iter().any(|g| g.contains("RelayTunnel.")),
        "with no relay evidence we must list relay-side gaps; got {no_data:?}",
    );
}

#[test]
fn relay_session_state_changed_event_round_trips_through_serde() {
    // §3 acceptance: the event must be greppable in the bundle. That
    // means it must round-trip through serde with its discriminator
    // intact.
    let ev = Event::RelaySessionStateChanged {
        relay_url: Some("https://relay.example/".into()),
        from_status: "connecting".into(),
        to_status: "connected".into(),
        reason: Some("watcher-update".into()),
    };
    let json = serde_json::to_value(&ev).unwrap();
    assert_eq!(json["type"], "RelaySessionStateChanged");
    assert_eq!(json["from_status"], "connecting");
    assert_eq!(json["to_status"], "connected");
    let back: Event = serde_json::from_value(json).unwrap();
    match back {
        Event::RelaySessionStateChanged { from_status, to_status, .. } => {
            assert_eq!(from_status, "connecting");
            assert_eq!(to_status, "connected");
        }
        _ => panic!("expected RelaySessionStateChanged"),
    }
}

#[test]
fn old_snapshot_without_relay_session_still_parses() {
    // Spec §1 (additive evolution): a Tier2IrohState built by old
    // code that knew nothing about `relay_session` parses cleanly
    // through the new struct.
    let json = serde_json::json!({
        "peers": [],
        "metrics": [],
        "connection_cache": [],
        "api_gaps": [],
        "scraped_at_ms": 1
    });
    let parsed: Tier2IrohState = serde_json::from_value(json).unwrap();
    assert!(parsed.relay_session.is_none(), "old bundle: relay_session absent");
}

#[test]
fn peer_gap_logic_unchanged_for_existing_callers() {
    // Sanity: the new compute_api_gaps_full default-callsite (with
    // relay=None) must still surface conn_type when no peer
    // populates it natively. Catches accidental regressions in the
    // shared candidate-list logic that the §6 work depends on.
    let derived_peer = Tier2Peer {
        peer_node_id_hex: "dd".repeat(32),
        conn_type: Some(distribution::diagnostics::ConnType::Direct),
        conn_type_source: Some("derived".into()),
        latency_ms: None,
        last_used_ms: None,
        last_received_ms: None,
        direct_addresses: Vec::new(),
        relay_urls: Vec::new(),
        addr_sources: None,
    };
    let gaps = Tier2IrohState::compute_api_gaps(&[derived_peer]);
    assert!(gaps.iter().any(|g| g == "RemoteInfo.conn_type"));
}
