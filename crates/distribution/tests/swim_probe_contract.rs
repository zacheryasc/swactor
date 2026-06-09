//! Layer 1 — §8 lifecycle / §9 probe cycle / §10.6 emissions / §12 Lifeguard floor.
//!
//! These pin the guarded-command contracts of the probe state machine, expressed
//! as the spec's behavioral invariants rather than the shape of today's `match`:
//!
//!   * §9.5 — **one `probe_timeout` bounds both phases**: the direct phase ends
//!     it at `probe_timeout`, and the indirect phase ends it at *another*
//!     `probe_timeout` — the same budget, applied twice;
//!   * §9.5 — the **still-Suspect guard** honors a mid-window refute: a node that
//!     becomes Alive again before its suspicion timer expires is NOT declared
//!     Dead, and its timer is dropped;
//!   * §10.6 — a genuinely unanswered probe ends `Suspect`, then `Dead` after the
//!     suspicion window, emitting `MembershipChanged` and re-gossiping each;
//!   * §12 — the effective suspicion timeout is `max(static, dynamic)`, so
//!     Lifeguard can only *lengthen* the window, never shorten it below the floor.

use std::time::{Duration, Instant};

use distribution::swim::dissemination::DisseminationQueue;
use distribution::swim::lifeguard::LifeguardConfig;
use distribution::swim::member_list::MemberList;
use distribution::swim::node::{NodeAction, SwimNode};
use distribution::swim::probe::{SwimAction, SwimConfig, SwimEvent, SwimProbe};
use distribution::types::{MemberState, NodeId};

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

/// Synthetic per-step clock granularity (SWIM is wall-clock driven).
const TICK: Duration = Duration::from_millis(10);

fn ticks(n: u64) -> Duration {
    TICK * n as u32
}

/// Step the probe once, advancing the synthetic clock by one `TICK` first.
fn tick_once(probe: &mut SwimProbe, members: &mut MemberList, now: &mut Instant) -> Vec<SwimAction> {
    *now += TICK;
    probe.step(*now, SwimEvent::Tick, members)
}

fn tick_n(probe: &mut SwimProbe, members: &mut MemberList, now: &mut Instant, n: u64) -> Vec<SwimAction> {
    let mut all = Vec::new();
    for _ in 0..n {
        all.extend(tick_once(probe, members, now));
    }
    all
}

fn has_ping_req(actions: &[SwimAction]) -> bool {
    actions.iter().any(|a| matches!(a, SwimAction::SendPingReq { .. }))
}

fn has_suspect(actions: &[SwimAction]) -> bool {
    actions.iter().any(|a| matches!(a, SwimAction::Suspect(_)))
}

fn ping_target(actions: &[SwimAction]) -> Option<NodeId> {
    actions.iter().find_map(|a| match a {
        SwimAction::SendPing { to, .. } => Some(*to),
        _ => None,
    })
}

// ─── §9.6 / §10.7 — SendFailed is a reactive probe trigger ────────────────────

#[test]
fn send_failed_reactively_probes_a_known_live_peer_but_ignores_unknown_and_dead() {
    // §9.6: a failed send to a peer is evidence that peer may be gone, so — while
    // Idle — it triggers an *immediate directed probe* of that peer (the reactive
    // trigger of §10.7), but ONLY when the target is a known, non-Dead member.
    // An unknown peer (nothing to probe) and a peer already Dead are both no-ops.
    let config = SwimConfig {
        probe_interval: ticks(1000), // park periodic probing far away so the only
        probe_timeout: ticks(3),     // SendPing we can observe is the reactive one
        indirect_probes: 0,
        suspicion_timeout: ticks(1000),
        dead_reprobe_interval: ticks(0),
        ..SwimConfig::default()
    };
    let now = Instant::now();
    let mut probe = SwimProbe::new(config, now);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), MemberState::Alive, 0); // known & live
    members.apply(node(2), MemberState::Dead, 4); // known & dead

    // Unknown peer: a SendFailed about a node we have never heard of probes nobody.
    let unknown = probe.step(now, SwimEvent::SendFailed { to: node(9) }, &mut members);
    assert!(ping_target(&unknown).is_none(), "a SendFailed for an unknown peer must not start a probe");

    // Dead peer: we have already given up on it — no reactive probe.
    let dead = probe.step(now, SwimEvent::SendFailed { to: node(2) }, &mut members);
    assert!(ping_target(&dead).is_none(), "a SendFailed for a Dead peer must not start a probe");

    // Known live peer, Idle: the failure reactively probes exactly that peer.
    let live = probe.step(now, SwimEvent::SendFailed { to: node(1) }, &mut members);
    assert_eq!(
        ping_target(&live),
        Some(node(1)),
        "a SendFailed for a known live peer must reactively probe that peer"
    );
}

// ─── §9.5 — one probe_timeout bounds BOTH the direct and indirect phase ──────

#[test]
fn the_same_probe_timeout_bounds_both_phases() {
    // §9.5: a probe sent at T fans out indirect probes at exactly T+probe_timeout
    // (direct phase), and — if still unanswered — suspects the target at exactly
    // T+2·probe_timeout (indirect phase). The two phases share one budget.
    let config = SwimConfig {
        probe_interval: ticks(5),
        probe_timeout: ticks(3),
        indirect_probes: 2,
        suspicion_timeout: ticks(1000), // irrelevant here
        dead_reprobe_interval: ticks(0),
        ..SwimConfig::default()
    };
    let mut now = Instant::now();
    let mut probe = SwimProbe::new(config, now);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), MemberState::Alive, 0);
    members.apply(node(2), MemberState::Alive, 0);
    members.apply(node(3), MemberState::Alive, 0);

    // Tick 5 fires the probe (a SendPing).
    let fired = tick_n(&mut probe, &mut members, &mut now, 5);
    assert!(
        fired.iter().any(|a| matches!(a, SwimAction::SendPing { .. })),
        "a probe must fire at the probe interval"
    );

    // Two ticks later: still inside the direct budget — no indirect fanout yet.
    let early = tick_n(&mut probe, &mut members, &mut now, 2);
    assert!(!has_ping_req(&early), "indirect probes must not fire before probe_timeout elapses");

    // The third tick hits probe_timeout exactly — the direct phase ends here.
    let at_direct_timeout = tick_once(&mut probe, &mut members, &mut now);
    assert!(has_ping_req(&at_direct_timeout), "the direct phase must end at exactly probe_timeout");
    assert!(!has_suspect(&at_direct_timeout), "the target is not suspected yet — the indirect phase just began");

    // The indirect phase has its OWN probe_timeout: two more ticks, no suspicion.
    let early = tick_n(&mut probe, &mut members, &mut now, 2);
    assert!(!has_suspect(&early), "suspicion must not fire before the indirect probe_timeout elapses");

    // The third tick hits the second probe_timeout — now the target is suspected.
    let at_indirect_timeout = tick_once(&mut probe, &mut members, &mut now);
    assert!(
        has_suspect(&at_indirect_timeout),
        "the indirect phase must end at another probe_timeout, suspecting the target"
    );
}

// ─── §9.5 — the still-Suspect guard honors a mid-window refute ───────────────

#[test]
fn mid_window_refute_cancels_the_pending_death() {
    // §9.5: a refutation (Alive at a higher incarnation) merged in before the
    // suspicion timer expires clears the Suspect state; the still-Suspect guard
    // must honor that and drop the timer WITHOUT declaring the node Dead.
    let config = SwimConfig {
        probe_interval: ticks(5),
        probe_timeout: ticks(3),
        indirect_probes: 0,
        suspicion_timeout: ticks(10),
        dead_reprobe_interval: ticks(0),
        ..SwimConfig::default()
    };
    let mut now = Instant::now();
    let mut probe = SwimProbe::new(config, now);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), MemberState::Alive, 0);

    // Drive a genuine timeout to produce the Suspect action, and apply it like
    // the handler would (§10.6): node 1 is now Suspect with a running timer.
    tick_n(&mut probe, &mut members, &mut now, 5); // ping
    tick_n(&mut probe, &mut members, &mut now, 3); // direct timeout → indirect phase
    let suspected = tick_n(&mut probe, &mut members, &mut now, 3); // indirect timeout → Suspect
    assert!(has_suspect(&suspected), "precondition: the unanswered probe must produce Suspect");
    assert!(members.suspect(node(1)), "the handler applies the Suspect transition");

    // Mid-window: node 1 refutes — a higher-incarnation Alive merges in.
    members.apply(node(1), MemberState::Alive, 1);
    assert_eq!(members.get(&node(1)).unwrap().state, MemberState::Alive);

    // Tick well past the suspicion timeout. Because node 1 is no longer Suspect,
    // the guard must never declare it Dead.
    let actions = tick_n(&mut probe, &mut members, &mut now, 30);
    assert!(
        !actions.iter().any(|a| matches!(a, SwimAction::DeclareDead(_))),
        "a refuted node must not be declared Dead — the still-Suspect guard honors the refute"
    );
    assert_eq!(members.get(&node(1)).unwrap().state, MemberState::Alive);
}

// ─── §10.6 — a genuinely silent peer ends Suspect, then Dead (with effects) ──

#[test]
fn unanswered_probe_drives_member_through_suspect_then_dead_with_notifications() {
    // §9 + §10.6: the litmus from the behavioral spec — a genuinely unanswered
    // probe ends Suspect, then Dead after the suspicion window. Each transition
    // fires a MembershipChanged and is re-gossiped.
    let config = SwimConfig {
        probe_interval: ticks(5),
        probe_timeout: ticks(3),
        indirect_probes: 0,
        suspicion_timeout: ticks(10),
        dead_reprobe_interval: ticks(0),
        ..SwimConfig::default()
    };
    let mut now = Instant::now();
    let mut swim = SwimNode::new(node(0), config, now);
    swim.handle_join_request(node(1)); // node 1 is a member; it will never answer

    let mut notifications: Vec<(NodeId, MemberState, u64)> = Vec::new();
    for _ in 0..40 {
        now += TICK;
        for a in &swim.tick(now) {
            if let NodeAction::MembershipChanged { node_id, state, incarnation } = a {
                notifications.push((*node_id, *state, *incarnation));
            }
        }
    }

    let suspect_at = notifications.iter().position(|(id, s, _)| *id == node(1) && *s == MemberState::Suspect);
    let dead_at = notifications.iter().position(|(id, s, _)| *id == node(1) && *s == MemberState::Dead);
    assert!(suspect_at.is_some(), "a silent peer must be notified Suspect, got {notifications:?}");
    assert!(dead_at.is_some(), "a silent peer must then be notified Dead, got {notifications:?}");
    assert!(suspect_at < dead_at, "Suspect must precede Dead in the notification stream");

    // The settled membership view agrees: node 1 is Dead, no longer alive.
    assert!(
        swim.members().all_members().iter().any(|e| e.node_id == node(1) && e.state == MemberState::Dead),
        "the settled view must show node 1 as Dead"
    );
    assert_eq!(swim.members().alive_count(), 0, "a Dead peer is not counted alive");

    // §10.6/§7 inv.5: the Dead transition was enqueued for dissemination — it
    // rides the next outgoing message. Drain the queue via a throwaway probe,
    // which is topology-independent: in a 1-peer cluster no probe fires once the
    // only member is Dead. (This learns node 9, so it runs after the view check.)
    let onward = match swim.handle_ping(node(9), 1, &[]).into_iter().find_map(|a| match a {
        NodeAction::SendAck { piggyback, .. } => Some(piggyback),
        _ => None,
    }) {
        Some(pb) => DisseminationQueue::unpack_piggyback(&pb),
        None => Vec::new(),
    };
    assert!(
        onward.iter().any(|u| u.node_id == node(1) && u.state == MemberState::Dead),
        "the Dead transition must be re-gossiped, but the queue held {onward:?}"
    );
}

// ─── §12 — Lifeguard can only lengthen the suspicion window, never shorten ───

/// Count ticks from "node 1 is freshly Suspect" until `DeclareDead` fires.
fn ticks_until_dead(config: SwimConfig) -> u64 {
    let mut now = Instant::now();
    let mut probe = SwimProbe::new(config, now);
    let mut members = MemberList::new(node(0));
    members.apply(node(1), MemberState::Alive, 0);
    tick_n(&mut probe, &mut members, &mut now, 5);
    tick_n(&mut probe, &mut members, &mut now, 3);
    let suspected = tick_n(&mut probe, &mut members, &mut now, 3);
    for a in &suspected {
        if let SwimAction::Suspect(id) = a {
            members.suspect(*id);
        }
    }
    let mut count = 0u64;
    loop {
        count += 1;
        now += TICK;
        let actions = probe.step(now, SwimEvent::Tick, &mut members);
        if actions.iter().any(|a| matches!(a, SwimAction::DeclareDead(_))) {
            return count;
        }
        if count >= 10_000 {
            return count; // guard against a hang under misconfiguration
        }
    }
}

#[test]
fn lifeguard_never_shortens_below_the_static_floor() {
    // §12: the effective suspicion timeout is `max(config.suspicion_timeout,
    // dynamic_…)`. When the adaptive value is SMALLER than the static floor, the
    // floor wins — a healthy small cluster must not die faster than the static
    // window. Same static floor on both sides; only the Lifeguard band differs.
    let base = SwimConfig {
        probe_interval: ticks(5),
        probe_timeout: ticks(3),
        indirect_probes: 0,
        suspicion_timeout: ticks(10), // the static floor
        dead_reprobe_interval: ticks(0),
        ..SwimConfig::default()
    };
    let static_ticks = ticks_until_dead(SwimConfig { lifeguard: None, ..base.clone() });
    let floored_ticks = ticks_until_dead(SwimConfig {
        // A deliberately tiny adaptive band — its dynamic timeout is far below
        // the 10-tick static floor, so the floor must dominate.
        lifeguard: Some(LifeguardConfig {
            base_suspicion_timeout: ticks(1),
            min_suspicion_timeout: ticks(1),
            max_suspicion_timeout: ticks(2),
            ..LifeguardConfig::default()
        }),
        ..base
    });
    assert_eq!(
        floored_ticks, static_ticks,
        "Lifeguard with a sub-floor band ({floored_ticks}) must not shorten the static window ({static_ticks})"
    );
}
