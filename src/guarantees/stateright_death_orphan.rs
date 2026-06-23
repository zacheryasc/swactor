//! Stateright model-checking of death notifications (G6) and orphan cleanup (G7).
//!
//! Two focused models keep the state space tractable:
//!
//! 1. **MonitorModel** (G6): 3 actors with monitor/demonitor/kill. Proves
//!    notification exactness, no-notification-for-alive, demonitor suppression.
//!
//! 2. **OrphanModel** (G7): 4 actors with parent-child/kill/orphan-cleanup.
//!    Proves unsupervised children stop, supervised survive, cascading cleanup.

use super::model_checker::{Model, Property};

// ═══════════════════════════════════════════════════════════════════════════
// G6: Monitor Model
// ═══════════════════════════════════════════════════════════════════════════

const MON_N: usize = 3;
const MON_PAIRS: usize = MON_N * (MON_N - 1); // 6

fn mon_pair(i: usize, j: usize) -> usize {
    debug_assert!(i < MON_N && j < MON_N && i != j);
    if j < i {
        i * (MON_N - 1) + j
    } else {
        i * (MON_N - 1) + j - 1
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct MonitorState {
    alive: [bool; MON_N],
    /// Active monitor from i to j.
    active: [bool; MON_PAIRS],
    /// Demonitored while target was still alive (the meaningful demonitor case).
    deactivated_while_alive: [bool; MON_PAIRS],
    /// Notification count (capped at 2 to detect duplicates).
    notif: [u8; MON_PAIRS],
}

impl MonitorState {
    fn init() -> Self {
        Self {
            alive: [true; MON_N],
            active: [false; MON_PAIRS],
            deactivated_while_alive: [false; MON_PAIRS],
            notif: [0; MON_PAIRS],
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum MonAction {
    Monitor(usize, usize),
    Demonitor(usize, usize),
    Kill(usize),
}

#[derive(Clone)]
struct MonitorModel;

impl Model for MonitorModel {
    type State = MonitorState;
    type Action = MonAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![MonitorState::init()]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        for i in 0..MON_N {
            if !s.alive[i] {
                continue;
            }

            for j in 0..MON_N {
                if i == j {
                    continue;
                }
                let idx = mon_pair(i, j);

                if s.alive[j] && !s.active[idx] {
                    actions.push(MonAction::Monitor(i, j));
                }
                if s.active[idx] {
                    actions.push(MonAction::Demonitor(i, j));
                }
            }

            actions.push(MonAction::Kill(i));
        }
    }

    fn next_state(&self, s: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        match action {
            MonAction::Monitor(w, t) => {
                let idx = mon_pair(w, t);
                n.active[idx] = true;
                n.deactivated_while_alive[idx] = false;
            }
            MonAction::Demonitor(w, t) => {
                let idx = mon_pair(w, t);
                n.active[idx] = false;
                if n.alive[t] {
                    n.deactivated_while_alive[idx] = true;
                }
            }
            MonAction::Kill(t) => {
                if !n.alive[t] {
                    return None;
                }
                n.alive[t] = false;
                for w in 0..MON_N {
                    if w == t {
                        continue;
                    }
                    let idx = mon_pair(w, t);
                    if n.alive[w] && n.active[idx] && n.notif[idx] < 2 {
                        n.notif[idx] += 1;
                    }
                }
            }
        }
        if n == *s { None } else { Some(n) }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "G6a: exactly one notification per active monitor on dead target",
                |_, s| {
                    for w in 0..MON_N {
                        if !s.alive[w] {
                            continue;
                        }
                        for t in 0..MON_N {
                            if w == t {
                                continue;
                            }
                            let idx = mon_pair(w, t);
                            if s.active[idx] && !s.alive[t] && s.notif[idx] != 1 {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            Property::<Self>::always("G6b: no notification for alive targets", |_, s| {
                for w in 0..MON_N {
                    for t in 0..MON_N {
                        if w == t {
                            continue;
                        }
                        if s.alive[t] && s.notif[mon_pair(w, t)] > 0 {
                            return false;
                        }
                    }
                }
                true
            }),
            Property::<Self>::always(
                "G6c: demonitor before death suppresses notification",
                |_, s| {
                    for w in 0..MON_N {
                        for t in 0..MON_N {
                            if w == t {
                                continue;
                            }
                            let idx = mon_pair(w, t);
                            // If demonitored while target was alive, no notification should exist
                            if s.deactivated_while_alive[idx] && s.notif[idx] > 0 {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // Liveness
            Property::<Self>::sometimes("L1: monitor fires", |_, s| s.notif.iter().any(|&c| c > 0)),
            Property::<Self>::sometimes("L2: demonitor suppression reachable", |_, s| {
                (0..MON_N).any(|w| {
                    (0..MON_N).any(|t| {
                        w != t && {
                            let idx = mon_pair(w, t);
                            s.deactivated_while_alive[idx] && !s.alive[t] && s.notif[idx] == 0
                        }
                    })
                })
            }),
            Property::<Self>::sometimes("L3: multiple monitors on same target", |_, s| {
                (0..MON_N).any(|t| {
                    let watchers: usize = (0..MON_N)
                        .filter(|&w| w != t && s.notif[mon_pair(w, t)] > 0)
                        .count();
                    watchers >= 2
                })
            }),
        ]
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// G7: Orphan Model
// ═══════════════════════════════════════════════════════════════════════════

/// 4 actors: enough for parent → child → grandchild chains (3 deep) plus
/// a sibling to test supervised vs unsupervised.
const ORP_N: usize = 4;
const ORP_PAIRS: usize = ORP_N * (ORP_N - 1); // 12

fn orp_pair(i: usize, j: usize) -> usize {
    debug_assert!(i < ORP_N && j < ORP_N && i != j);
    if j < i {
        i * (ORP_N - 1) + j
    } else {
        i * (ORP_N - 1) + j - 1
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct OrphanState {
    alive: [bool; ORP_N],
    /// Unsupervised parent→child.
    parent_unsup: [bool; ORP_PAIRS],
    /// Supervised parent→child.
    parent_sup: [bool; ORP_PAIRS],
    /// Orphan cleanup propagated for actor i.
    orphan_cleaned: [bool; ORP_N],
    /// Whether actor was killed by OrphanCleanup (not by explicit Kill).
    orphan_killed: [bool; ORP_N],
    /// How many parent-child links have been set (cap to limit state space).
    link_count: u8,
}

/// Max parent-child links to prevent state explosion.
const MAX_LINKS: u8 = 3;

impl OrphanState {
    fn init() -> Self {
        Self {
            alive: [true; ORP_N],
            parent_unsup: [false; ORP_PAIRS],
            parent_sup: [false; ORP_PAIRS],
            orphan_cleaned: [false; ORP_N],
            orphan_killed: [false; ORP_N],
            link_count: 0,
        }
    }

    fn has_parent(&self, c: usize) -> bool {
        (0..ORP_N).any(|p| {
            p != c && {
                let idx = orp_pair(p, c);
                self.parent_unsup[idx] || self.parent_sup[idx]
            }
        })
    }

    fn has_children(&self, p: usize) -> bool {
        (0..ORP_N).any(|c| {
            c != p && {
                let idx = orp_pair(p, c);
                self.parent_unsup[idx] || self.parent_sup[idx]
            }
        })
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum OrpAction {
    SetParentUnsup(usize, usize),
    SetParentSup(usize, usize),
    Kill(usize),
    OrphanCleanup(usize),
}

#[derive(Clone)]
struct OrphanModel;

impl Model for OrphanModel {
    type State = OrphanState;
    type Action = OrpAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![OrphanState::init()]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // SetParent actions only if under link cap
        if s.link_count < MAX_LINKS {
            for p in 0..ORP_N {
                if !s.alive[p] {
                    continue;
                }
                for c in 0..ORP_N {
                    if p == c || !s.alive[c] {
                        continue;
                    }
                    if s.has_parent(c) {
                        continue;
                    }
                    // Prevent cycles: c must not be an ancestor of p
                    if is_ancestor(s, c, p) {
                        continue;
                    }
                    actions.push(OrpAction::SetParentUnsup(p, c));
                    actions.push(OrpAction::SetParentSup(p, c));
                }
            }
        }

        for i in 0..ORP_N {
            if s.alive[i] {
                actions.push(OrpAction::Kill(i));
            }
            if !s.alive[i] && !s.orphan_cleaned[i] && s.has_children(i) {
                actions.push(OrpAction::OrphanCleanup(i));
            }
        }
    }

    fn next_state(&self, s: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        match action {
            OrpAction::SetParentUnsup(p, c) => {
                n.parent_unsup[orp_pair(p, c)] = true;
                n.link_count += 1;
            }
            OrpAction::SetParentSup(p, c) => {
                n.parent_sup[orp_pair(p, c)] = true;
                n.link_count += 1;
            }
            OrpAction::Kill(i) => {
                if !n.alive[i] {
                    return None;
                }
                n.alive[i] = false;
            }
            OrpAction::OrphanCleanup(dead_parent) => {
                n.orphan_cleaned[dead_parent] = true;
                // Kill unsupervised children
                for c in 0..ORP_N {
                    if c == dead_parent {
                        continue;
                    }
                    if n.parent_unsup[orp_pair(dead_parent, c)] && n.alive[c] {
                        n.alive[c] = false;
                        n.orphan_killed[c] = true;
                    }
                }
            }
        }
        if n == *s { None } else { Some(n) }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // G7a: After orphan cleanup, all unsupervised children are dead
            Property::<Self>::always("G7a: orphan cleanup stops unsupervised children", |_, s| {
                for p in 0..ORP_N {
                    if !s.alive[p] && s.orphan_cleaned[p] {
                        for c in 0..ORP_N {
                            if c == p {
                                continue;
                            }
                            if s.parent_unsup[orp_pair(p, c)] && s.alive[c] {
                                return false;
                            }
                        }
                    }
                }
                true
            }),
            // G7b: OrphanCleanup never kills supervised-only children.
            // If orphan_killed[c] is true, there must be a parent p with an
            // unsupervised link (parent_unsup[p→c]) that was orphan-cleaned.
            // A supervised-only child has no parent_unsup link, so orphan_killed
            // being true for it would violate this property.
            Property::<Self>::always("G7b: supervised children survive orphan cleanup", |_, s| {
                for c in 0..ORP_N {
                    if s.orphan_killed[c] {
                        // There must exist a dead, cleaned parent with unsup link to c
                        let has_unsup_cleaned_parent = (0..ORP_N).any(|p| {
                            p != c && s.parent_unsup[orp_pair(p, c)] && s.orphan_cleaned[p]
                        });
                        if !has_unsup_cleaned_parent {
                            return false;
                        }
                    }
                }
                true
            }),
            // G7c: Cascading — if orphan-cleaned parent's child also died and was
            // orphan-cleaned, its unsupervised children are dead too
            Property::<Self>::always("G7c: cascading orphan cleanup", |_, s| {
                for p in 0..ORP_N {
                    if s.orphan_cleaned[p] {
                        for c in 0..ORP_N {
                            if c == p {
                                continue;
                            }
                            if s.parent_unsup[orp_pair(p, c)] && !s.alive[c] && s.orphan_cleaned[c]
                            {
                                for gc in 0..ORP_N {
                                    if gc == c {
                                        continue;
                                    }
                                    if s.parent_unsup[orp_pair(c, gc)] && s.alive[gc] {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                }
                true
            }),
            // Liveness
            Property::<Self>::sometimes("L1: orphan cleanup triggers", |_, s| {
                s.orphan_cleaned.iter().any(|&c| c)
            }),
            Property::<Self>::sometimes("L2: cascading cleanup reachable", |_, s| {
                // Parent cleaned → child died → child cleaned
                (0..ORP_N).any(|p| {
                    s.orphan_cleaned[p]
                        && (0..ORP_N).any(|c| {
                            c != p
                                && s.parent_unsup[orp_pair(p, c)]
                                && !s.alive[c]
                                && s.orphan_cleaned[c]
                        })
                })
            }),
            Property::<Self>::sometimes("L3: supervised child survives cleanup", |_, s| {
                (0..ORP_N).any(|p| {
                    s.orphan_cleaned[p]
                        && (0..ORP_N).any(|c| c != p && s.parent_sup[orp_pair(p, c)] && s.alive[c])
                })
            }),
        ]
    }
}

/// Check if `ancestor` is an ancestor of `descendant` via parent links.
fn is_ancestor(s: &OrphanState, ancestor: usize, descendant: usize) -> bool {
    // Walk up from descendant
    let mut current = descendant;
    for _ in 0..ORP_N {
        let parent = (0..ORP_N).find(|&p| {
            p != current && {
                let idx = orp_pair(p, current);
                s.parent_unsup[idx] || s.parent_sup[idx]
            }
        });
        match parent {
            Some(p) if p == ancestor => return true,
            Some(p) => current = p,
            None => return false,
        }
    }
    false
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[test]
#[ignore] // Exhaustive proof — run deliberately with `cargo test -- --ignored`
fn g6_monitor_model_check() {
    let result = MonitorModel.checker().spawn_dfs().join();
    let unique = result.unique_state_count();
    let depth = result.max_depth();
    println!(
        "Stateright G6 (Monitor): {} unique states, max depth {}",
        unique, depth
    );
    result.assert_properties();
    assert!(unique > 100, "Too few states ({unique})");
}

#[test]
#[ignore] // Exhaustive proof — run deliberately with `cargo test -- --ignored`
fn g7_orphan_model_check() {
    let result = OrphanModel.checker().spawn_dfs().join();
    let unique = result.unique_state_count();
    let depth = result.max_depth();
    println!(
        "Stateright G7 (Orphan): {} unique states, max depth {}",
        unique, depth
    );
    result.assert_properties();
    assert!(unique > 100, "Too few states ({unique})");
}
