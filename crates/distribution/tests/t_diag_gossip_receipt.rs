//! Spec §10 (gossip-receipt event, gap 10).
//!
//! The bundle's authoritative source for "did node X ever hear about
//! name Y from peer Z" is the typed `GossipReceived` event. The
//! existing coarse `MessageReceived` counter stays for backward
//! compatibility but is not the source of truth.
//!
//! Acceptance: in any run where one node fails to learn about
//! another node's registered name, the bundle distinguishes
//! unambiguously whether the gossip was never received vs received
//! and ignored. With `GossipReceived` present, the former is
//! readable from the receiver's event stream (no events with
//! payload_kind = "name_registry" from that source) vs the latter
//! (events present, but no corresponding registry entry in the
//! receiver's `Tier2Registry`).

use distribution::diagnostics::Event;
use distribution::diagnostics::sink::InMemorySink;
use distribution::diagnostics::{Aggregator, Identity, Role};
use distribution::swim::node::SwimNode;
use distribution::swim::probe::SwimConfig;
use distribution::types::{MemberState, NodeId, NodeRecord};

#[test]
fn gossip_received_round_trips_through_serde_with_a_typed_discriminator() {
    let ev = Event::GossipReceived {
        source_peer: NodeId([0x33; 32]),
        payload_kind: "swim_piggyback".into(),
        payload_bytes: 256,
        item_count: 7,
    };
    let json = serde_json::to_value(&ev).unwrap();
    assert_eq!(json["type"], "GossipReceived");
    assert_eq!(json["payload_kind"], "swim_piggyback");
    assert_eq!(json["payload_bytes"], 256);
    assert_eq!(json["item_count"], 7);
    let back: Event = serde_json::from_value(json).unwrap();
    match back {
        Event::GossipReceived { payload_kind, item_count, .. } => {
            assert_eq!(payload_kind, "swim_piggyback");
            assert_eq!(item_count, 7);
        }
        _ => panic!("expected GossipReceived"),
    }
}

#[test]
fn swim_piggyback_apply_emits_gossip_received_with_correct_source_and_item_count() {
    // Spec §10 acceptance contract: the typed event must fire when a
    // node receives a SWIM piggyback. The source_peer must match
    // whichever node sent the piggyback; item_count must match the
    // number of membership updates packed inside.
    use distribution::swim::dissemination::{membership_update, DisseminationQueue};

    let my_id = NodeId([0xaa; 32]);
    let peer_id = NodeId([0xbb; 32]);
    let other_id = NodeId([0xcc; 32]);

    let mut node = SwimNode::new(my_id, SwimConfig::default());

    // Wire a diagnostics aggregator so we can observe what SWIM emits.
    let id = Identity::new(my_id, Role::stage(), "run-gossip");
    let agg = std::sync::Arc::new(Aggregator::new(id, InMemorySink::new()));
    let emitter: distribution::diagnostics::sink::DynEmitter = agg.clone()
        as std::sync::Arc<dyn distribution::diagnostics::sink::EventEmitter + Send + Sync>;
    node.set_diagnostics(emitter);

    // Pack two membership updates into a piggyback as a real sender
    // would, then deliver it via a ping from `peer_id`.
    let mut queue = DisseminationQueue::new(4);
    queue.enqueue(membership_update(other_id, MemberState::Alive, 0), 4);
    queue.enqueue(membership_update(peer_id, MemberState::Alive, 0), 4);
    let piggyback = queue.pack_piggyback(8);
    let _ = node.handle_ping(peer_id, 1, &piggyback);

    let records = agg.sink().records();
    let gossip: Vec<_> = records
        .iter()
        .filter_map(|r| match &r.event {
            Event::GossipReceived {
                source_peer,
                payload_kind,
                payload_bytes,
                item_count,
            } => Some((*source_peer, payload_kind.clone(), *payload_bytes, *item_count)),
            _ => None,
        })
        .collect();
    assert_eq!(
        gossip.len(),
        1,
        "exactly one GossipReceived per piggyback; got {gossip:?}",
    );
    let (src, kind, bytes, items) = &gossip[0];
    assert_eq!(*src, peer_id, "source must be the SWIM sender (the from arg)");
    assert_eq!(kind, "swim_piggyback");
    assert!(*bytes > 0, "payload_bytes must reflect actual piggyback size");
    assert!(*items >= 1, "item_count must include the packed updates");
}

#[test]
fn empty_piggyback_does_not_fabricate_a_gossip_event() {
    // §10 honesty: an empty piggyback is not a content receipt.
    // Spec talks about "payload through the gossip layer" — empty
    // bytes are not a payload. The bundle reader looking at
    // GossipReceived counts must see actual gossip, not heartbeat
    // ping noise.
    let my_id = NodeId([0x11; 32]);
    let peer_id = NodeId([0x22; 32]);
    let mut node = SwimNode::new(my_id, SwimConfig::default());
    let id = Identity::new(my_id, Role::stage(), "run-empty");
    let agg = std::sync::Arc::new(Aggregator::new(id, InMemorySink::new()));
    let emitter: distribution::diagnostics::sink::DynEmitter = agg.clone()
        as std::sync::Arc<dyn distribution::diagnostics::sink::EventEmitter + Send + Sync>;
    node.set_diagnostics(emitter);

    let _ = node.handle_ping(peer_id, 1, &[]);

    let gossip_count = agg
        .sink()
        .records()
        .iter()
        .filter(|r| matches!(r.event, Event::GossipReceived { .. }))
        .count();
    assert_eq!(
        gossip_count, 0,
        "empty piggyback must not emit GossipReceived",
    );
    // Suppress unused-import warning when the assertion above is the
    // only NodeRecord-related use in this test.
    let _ = NodeRecord {
        node_id: peer_id,
        state: MemberState::Alive,
        incarnation: 0,
    };
}
