//! Layer 1 — §10 message-handler emission set.
//!
//! Each handler's externally-visible effect set is fixed by SWIM_ACTOR_SPEC §10:
//! which `MembershipChanged` notifications fire, and which updates are enqueued
//! for dissemination (the "every accepted change → re-gossip" edge, §7 inv. 5).
//! These pin that emission set per handler, including the two calls-outs the
//! plan names: the §10.9 `JoinResponse` asymmetry (it seeds but does NOT
//! re-gossip the bulk snapshot) and the §10.2 learn-sender notification.
//!
//! Dissemination has no public getter, so it is observed exactly as the wire
//! does — by draining the queue onto an outgoing message's `piggyback` and
//! decoding it. The notification stream is observed as the handler's returned
//! `MembershipChanged` actions (the actor's sole membership output, §6.3).

use std::time::Instant;

use distribution::messages::MembershipUpdate;
use distribution::swim::dissemination::{membership_update, DisseminationQueue};
use distribution::swim::node::{NodeAction, SwimNode};
use distribution::swim::probe::SwimConfig;
use distribution::types::{MemberState, NodeId, NodeRecord};

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

fn fresh_node(self_byte: u8) -> SwimNode {
    SwimNode::new(node(self_byte), SwimConfig::default(), Instant::now())
}

/// The `(node_id, state, incarnation)` of every `MembershipChanged` in `actions`.
fn membership_changes(actions: &[NodeAction]) -> Vec<(NodeId, MemberState, u64)> {
    actions
        .iter()
        .filter_map(|a| match a {
            NodeAction::MembershipChanged { node_id, state, incarnation } => {
                Some((*node_id, *state, *incarnation))
            }
            _ => None,
        })
        .collect()
}

/// A piggyback frame carrying a single update about `target`.
fn piggyback_about(target: NodeId, state: MemberState, incarnation: u64) -> Vec<u8> {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(membership_update(target, state, incarnation), 5);
    q.pack_piggyback(10)
}

/// Drain whatever the node currently has queued for dissemination by triggering
/// one outgoing message and decoding its piggyback. `prober` is a throwaway
/// peer id (learning it has no dissemination effect, §10.2), so the decoded
/// updates are exactly what the handler-under-test enqueued.
fn drain_dissemination(swim: &mut SwimNode, prober: NodeId) -> Vec<MembershipUpdate> {
    for a in swim.handle_ping(prober, 9999, &[]) {
        if let NodeAction::SendAck { piggyback, .. } = a {
            return DisseminationQueue::unpack_piggyback(&piggyback);
        }
    }
    Vec::new()
}

// ─── §10.0 — gossip ingestion ────────────────────────────────────────────────

#[test]
fn accepted_gossip_emits_one_change_and_re_gossips_it() {
    // §10.0 + §7 inv. 5: an accepted merge emits exactly one MembershipChanged
    // and re-enqueues that update for dissemination — the infection edge.
    let mut swim = fresh_node(0);
    let actions = swim.handle_ping(node(1), 1, &piggyback_about(node(2), MemberState::Suspect, 3));

    let changes = membership_changes(&actions);
    assert!(
        changes.contains(&(node(2), MemberState::Suspect, 3)),
        "an accepted gossip merge must notify via MembershipChanged, got {changes:?}"
    );

    // The very ack this handler emits must carry the merged update onward.
    let ack_pb = actions.iter().find_map(|a| match a {
        NodeAction::SendAck { piggyback, .. } => Some(piggyback.clone()),
        _ => None,
    });
    let onward = DisseminationQueue::unpack_piggyback(&ack_pb.expect("Ping must produce an Ack"));
    assert!(
        onward.iter().any(|u| u.node_id == node(2) && u.state == MemberState::Suspect && u.incarnation == 3),
        "every accepted change must be re-gossiped (§7 inv. 5)"
    );
}

#[test]
fn dominated_gossip_emits_no_change() {
    // §10.0: gossip the merge rejects (stale) produces no notification and
    // nothing new to disseminate — it is a pure no-op at the boundary.
    let mut swim = fresh_node(0);
    swim.handle_ping(node(1), 1, &piggyback_about(node(2), MemberState::Dead, 5));
    // A stale, dominated update about node 2.
    let actions = swim.handle_ping(node(1), 2, &piggyback_about(node(2), MemberState::Alive, 1));
    assert!(
        membership_changes(&actions).is_empty(),
        "a dominated (stale) gossip update must emit no MembershipChanged"
    );
}

// ─── §10.2 — Ping learns its sender ──────────────────────────────────────────

#[test]
fn ping_acks_the_sender() {
    // §10.2: a Ping is always answered with an Ack addressed back to the sender.
    let mut swim = fresh_node(0);
    let actions = swim.handle_ping(node(1), 42, &[]);
    let ack_to = actions.iter().find_map(|a| match a {
        NodeAction::SendAck { to, sequence, .. } => Some((*to, *sequence)),
        _ => None,
    });
    assert_eq!(ack_to, Some((node(1), 42)), "Ping must produce exactly one Ack to the sender");
}

#[test]
fn ping_that_learns_a_new_sender_notifies_via_membership_changed() {
    // §10.2 (the flagged learn-sender notification): with `MembershipChanged`
    // now the sole membership channel, a Ping that first learns its sender must
    // route the event through it — consistent with Join and gossip.
    //
    // EXPECTED RED against the transcribed source, which emitted only a removed
    // diagnostic here; the actor rewrite routes it through MembershipChanged.
    let mut swim = fresh_node(0);
    let actions = swim.handle_ping(node(1), 1, &[]);
    let changes = membership_changes(&actions);
    assert!(
        changes.contains(&(node(1), MemberState::Alive, 0)),
        "learning a previously-unknown sender must notify via MembershipChanged, got {changes:?}"
    );
}

#[test]
fn ping_from_known_sender_is_not_a_membership_change() {
    // §10.2: re-pinging from an already-tracked Alive sender is not a change —
    // `apply(from, Alive, 0)` does not downgrade or duplicate it.
    let mut swim = fresh_node(0);
    swim.handle_ping(node(1), 1, &[]); // first sighting
    let actions = swim.handle_ping(node(1), 2, &[]); // already known
    assert!(
        membership_changes(&actions).is_empty(),
        "a Ping from a known Alive sender must emit no MembershipChanged"
    );
}

// ─── §10.8 — JoinRequest (we are the seed) ───────────────────────────────────

#[test]
fn join_request_admits_joiner_and_replies_with_self_in_the_roster() {
    // §10.8: a new joiner is admitted (one MembershipChanged + enqueued for
    // dissemination) and answered with a JoinResponse whose roster includes the
    // responder itself.
    let mut seed = fresh_node(0);
    let actions = seed.handle_join_request(node(1));

    assert_eq!(
        membership_changes(&actions),
        vec![(node(1), MemberState::Alive, 0)],
        "admitting a new joiner must fire exactly one MembershipChanged"
    );

    let roster = actions.iter().find_map(|a| match a {
        NodeAction::SendJoinResponse { to, members } if *to == node(1) => Some(members.clone()),
        _ => None,
    });
    let roster = roster.expect("JoinRequest must produce a JoinResponse to the joiner");
    assert!(
        roster.iter().any(|r| r.node_id == node(0)),
        "§10.8: the JoinResponse roster must include the responder (self)"
    );

    // §7 inv. 5: the admitted joiner is enqueued for dissemination.
    let drained = drain_dissemination(&mut seed, node(7));
    assert!(
        drained.iter().any(|u| u.node_id == node(1) && u.state == MemberState::Alive),
        "JoinRequest must enqueue the new member for gossip"
    );
}

#[test]
fn re_join_of_known_member_emits_no_change_but_still_responds() {
    // §10.8: a JoinRequest from an already-known member is not a membership
    // change, but the seed still answers with a fresh roster.
    let mut seed = fresh_node(0);
    seed.handle_join_request(node(1));
    let actions = seed.handle_join_request(node(1));
    assert!(
        membership_changes(&actions).is_empty(),
        "re-join of a known member must emit no MembershipChanged"
    );
    assert!(
        actions.iter().any(|a| matches!(a, NodeAction::SendJoinResponse { to, .. } if *to == node(1))),
        "a JoinRequest must always be answered with a JoinResponse"
    );
}

// ─── §10.9 — JoinResponse (we are the joiner) — the asymmetry ─────────────────

#[test]
fn join_response_seeds_the_roster_with_one_change_per_record() {
    // §10.9: the joiner fires one MembershipChanged per newly-learned record.
    let mut joiner = fresh_node(1);
    let actions = joiner.handle_join_response(vec![
        NodeRecord { node_id: node(2), state: MemberState::Alive, incarnation: 0 },
        NodeRecord { node_id: node(3), state: MemberState::Alive, incarnation: 0 },
    ]);
    let mut changed: Vec<NodeId> = membership_changes(&actions).into_iter().map(|(id, ..)| id).collect();
    changed.sort();
    assert_eq!(changed, vec![node(2), node(3)], "one MembershipChanged per newly-seeded record");
}

#[test]
fn join_response_does_not_re_gossip_the_bulk_snapshot() {
    // §10.9 asymmetry (the lone exception to §7 inv. 5): a JoinResponse seeds
    // the member list and fires notifications, but must NOT enqueue the bulk
    // snapshot for dissemination — re-gossiping the whole roster would be a
    // burst, and the members are confirmed by subsequent probing.
    let mut joiner = fresh_node(1);
    joiner.handle_join_response(vec![
        NodeRecord { node_id: node(2), state: MemberState::Alive, incarnation: 0 },
        NodeRecord { node_id: node(3), state: MemberState::Alive, incarnation: 0 },
        NodeRecord { node_id: node(4), state: MemberState::Dead, incarnation: 2 },
    ]);
    let drained = drain_dissemination(&mut joiner, node(9));
    assert!(
        drained.is_empty(),
        "§10.9: JoinResponse must not re-gossip the bulk snapshot, but queued {drained:?}"
    );
}

// ─── §10.10 — Leave ──────────────────────────────────────────────────────────

#[test]
fn leave_enqueues_self_dead_without_immediate_notification() {
    // §10.10: Leave gossips self as Dead at the current incarnation and fires no
    // immediate MembershipChanged — the Dead self-record propagates on later
    // piggybacks.
    let mut swim = fresh_node(0);
    let actions = swim.leave();
    assert!(
        membership_changes(&actions).is_empty(),
        "Leave must produce no immediate MembershipChanged"
    );
    let drained = drain_dissemination(&mut swim, node(9));
    assert!(
        drained.iter().any(|u| u.node_id == node(0) && u.state == MemberState::Dead),
        "Leave must enqueue self as Dead for subsequent piggybacks"
    );
}

// ─── §10.4 / §10.3 / §10.5 — the relay path ──────────────────────────────────

#[test]
fn ping_req_relays_a_ping_to_the_target() {
    // §10.4: as the relay, a PingReq forwards a Ping to the named target with
    // the same sequence (its `from` is self on the wire).
    let mut relay = fresh_node(0);
    let actions = relay.handle_ping_req(node(1), node(2), 77, &[]);
    let forwarded = actions.iter().find_map(|a| match a {
        NodeAction::SendPing { to, sequence, .. } => Some((*to, *sequence)),
        _ => None,
    });
    assert_eq!(
        forwarded,
        Some((node(2), 77)),
        "a PingReq must forward a Ping to the target carrying the same sequence"
    );
}

#[test]
fn relay_forwards_indirect_ack_to_requester_on_target_ack() {
    // §10.3: once the relayed target acks (matching the pending relay's
    // target+sequence), the relay sends an IndirectAck back to the original
    // requester — and only to it.
    let mut relay = fresh_node(0);
    relay.handle_ping_req(node(1), node(2), 88, &[]); // requester = 1, target = 2
    let actions = relay.handle_ack(node(2), 88, &[]); // the target answers
    let fwd = actions.iter().find_map(|a| match a {
        NodeAction::ForwardAck { to, target, sequence, .. } => Some((*to, *target, *sequence)),
        _ => None,
    });
    assert_eq!(
        fwd,
        Some((node(1), node(2), 88)),
        "a matching target Ack must be relayed home to the requester as an IndirectAck"
    );
}

#[test]
fn unmatched_ack_does_not_forward_an_indirect_ack() {
    // §10.3: an Ack that matches no pending relay produces no IndirectAck.
    let mut relay = fresh_node(0);
    relay.handle_ping_req(node(1), node(2), 88, &[]);
    let actions = relay.handle_ack(node(2), 999, &[]); // wrong sequence
    assert!(
        !actions.iter().any(|a| matches!(a, NodeAction::ForwardAck { .. })),
        "an Ack with no matching pending relay must not be forwarded"
    );
}

#[test]
fn indirect_ack_applies_its_piggyback_gossip() {
    // §10.5: as the original prober, an IndirectAck's piggyback is merged like
    // any other gossip — the indirectly-probed peer's news rides home on it.
    let mut prober = fresh_node(0);
    let actions = prober.handle_indirect_ack(node(2), 5, &piggyback_about(node(3), MemberState::Suspect, 4));
    assert!(
        membership_changes(&actions).contains(&(node(3), MemberState::Suspect, 4)),
        "an IndirectAck must apply the gossip carried in its piggyback"
    );
}
