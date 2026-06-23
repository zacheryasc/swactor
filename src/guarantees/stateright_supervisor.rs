//! Stateright model-checking of supervisor restart decisions (G8).
//!
//! Exhaustively explores all interleavings of child deaths and supervisor
//! restart responses across bounded parameter spaces. Uses production
//! decision functions (`RestartPolicy::should_restart`, `compute_restart_set`)
//! from `std/supervisor.rs`.
//!
//! The model tracks each death event's restart set in state, enabling
//! per-transition property verification:
//!
//! - **G8a**: `OneForOne` restarts only the dead child (if policy permits).
//! - **G8b**: `OneForAll` restarts all children (if policy permits).
//! - **G8c**: `RestForOne` restarts dead child + successors (if policy permits).
//! - **G8d**: `Temporary` dead child triggers no restart.
//! - **G8e**: `Transient` dead child + Normal death → no restart.
//! - **G8f**: `Transient` dead child + Panicked death → restart (if not meltdown).
//! - **G8g**: Meltdown stops supervisor when total_restarts > max_restarts.
//!
//! Liveness canaries prove non-vacuity: restarts occur, meltdowns are
//! reachable, and each strategy is exercised.

use super::model_checker::{Model, Property};
use crate::actor::StopReason;
use crate::std::supervisor::compute_restart_set;
use crate::std::{RestartPolicy, SupervisorStrategy};

// ── Bounded constants ────────────────────────────────────────────────────────

/// Number of supervised children. 4 exercises all strategies meaningfully
/// (RestForOne needs at least 3 to distinguish "rest" from "all").
const NUM_CHILDREN: usize = 4;

/// Maximum restarts before meltdown. Kept small (3) to make meltdown
/// reachable without state explosion.
const MAX_RESTARTS: u8 = 3;

/// Maximum deaths to process. Bounds the exploration depth.
const MAX_DEATHS: u8 = 4;

// ── State ────────────────────────────────────────────────────────────────────

/// Simplified death reason matching `StopReason` variants relevant to restart.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum DeathReason {
    Normal,
    Panicked,
}

impl DeathReason {
    fn to_stop_reason(self) -> StopReason {
        match self {
            DeathReason::Normal => StopReason::Normal,
            DeathReason::Panicked => StopReason::Panicked,
        }
    }
}

/// Per-child state within the supervisor.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ChildState {
    alive: bool,
    policy: RestartPolicy,
    /// How many times this child has been restarted.
    restart_count: u8,
}

/// Record of the most recent death event's outcome. Stored in state so
/// that `always` properties can verify per-transition correctness.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LastEvent {
    dead_idx: usize,
    reason: DeathReason,
    dead_policy: RestartPolicy,
    /// Which children were restarted by this event.
    restarted: [bool; NUM_CHILDREN],
    /// Whether this event triggered meltdown.
    triggered_meltdown: bool,
}

/// Supervisor state machine for Stateright exploration.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SupervisorState {
    strategy: SupervisorStrategy,
    children: [ChildState; NUM_CHILDREN],
    total_restarts: u8,
    melted_down: bool,
    /// How many death events have been processed (bounds exploration).
    deaths_processed: u8,
    /// The last death event's outcome, for per-transition property checks.
    last_event: Option<LastEvent>,
}

// ── Actions ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum SupAction {
    /// A child dies with the given reason. The supervisor immediately
    /// processes the death: checks policy, computes restart set, executes.
    ChildDies(usize, DeathReason),
}

// ── Model ────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct SupervisorModel;

impl SupervisorModel {
    /// Generate all initial states: every combination of strategy × per-child policy.
    fn all_init_states() -> Vec<SupervisorState> {
        let strategies = [
            SupervisorStrategy::OneForOne,
            SupervisorStrategy::OneForAll,
            SupervisorStrategy::RestForOne,
        ];
        let policies = [
            RestartPolicy::Permanent,
            RestartPolicy::Transient,
            RestartPolicy::Temporary,
        ];

        let mut states = Vec::new();

        for &strategy in &strategies {
            // Enumerate all 3^NUM_CHILDREN policy assignments
            for combo in 0..3u32.pow(NUM_CHILDREN as u32) {
                let mut children: [ChildState; NUM_CHILDREN] =
                    std::array::from_fn(|_| ChildState {
                        alive: true,
                        policy: RestartPolicy::Permanent,
                        restart_count: 0,
                    });

                let mut c = combo;
                for child in children.iter_mut() {
                    child.policy = policies[(c % 3) as usize];
                    c /= 3;
                }

                states.push(SupervisorState {
                    strategy,
                    children,
                    total_restarts: 0,
                    melted_down: false,
                    deaths_processed: 0,
                    last_event: None,
                });
            }
        }

        states
    }
}

impl Model for SupervisorModel {
    type State = SupervisorState;
    type Action = SupAction;

    fn init_states(&self) -> Vec<Self::State> {
        Self::all_init_states()
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if s.melted_down || s.deaths_processed >= MAX_DEATHS {
            return;
        }

        for idx in 0..NUM_CHILDREN {
            if s.children[idx].alive {
                actions.push(SupAction::ChildDies(idx, DeathReason::Normal));
                actions.push(SupAction::ChildDies(idx, DeathReason::Panicked));
            }
        }
    }

    fn next_state(&self, s: &Self::State, action: Self::Action) -> Option<Self::State> {
        let SupAction::ChildDies(dead_idx, reason) = action;

        if !s.children[dead_idx].alive {
            return None;
        }

        let mut next = s.clone();
        next.deaths_processed += 1;

        let dead_policy = next.children[dead_idx].policy;

        // Mark dead
        next.children[dead_idx].alive = false;

        let mut event = LastEvent {
            dead_idx,
            reason,
            dead_policy,
            restarted: [false; NUM_CHILDREN],
            triggered_meltdown: false,
        };

        // Use production should_restart on the dead child's policy
        let stop_reason = reason.to_stop_reason();
        if !dead_policy.should_restart(stop_reason) {
            next.last_event = Some(event);
            return if next == *s { None } else { Some(next) };
        }

        // Meltdown check
        let new_total = next.total_restarts + 1;
        if new_total > MAX_RESTARTS {
            next.melted_down = true;
            event.triggered_meltdown = true;
            next.last_event = Some(event);
            return Some(next);
        }
        next.total_restarts = new_total;

        // Use production compute_restart_set
        let restart_indices = compute_restart_set(next.strategy, dead_idx, NUM_CHILDREN);

        for &idx in &restart_indices {
            if idx < NUM_CHILDREN {
                next.children[idx].alive = true;
                next.children[idx].restart_count =
                    next.children[idx].restart_count.saturating_add(1);
                event.restarted[idx] = true;
            }
        }

        next.last_event = Some(event);
        if next == *s { None } else { Some(next) }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // ── G8a: OneForOne restarts only the dead child ─────────────
            Property::<Self>::always("G8a: OneForOne restarts only dead child", |_, s| {
                if s.strategy != SupervisorStrategy::OneForOne {
                    return true;
                }
                let Some(ev) = &s.last_event else { return true };
                if !ev.restarted.iter().any(|&r| r) {
                    return true; // no restart (policy denied or meltdown)
                }
                // Only the dead child should be in the restart set
                for (i, &restarted) in ev.restarted.iter().enumerate() {
                    if i == ev.dead_idx {
                        if !restarted {
                            return false;
                        }
                    } else if restarted {
                        return false;
                    }
                }
                true
            }),
            // ── G8b: OneForAll restarts all children ────────────────────
            Property::<Self>::always("G8b: OneForAll restarts all children", |_, s| {
                if s.strategy != SupervisorStrategy::OneForAll {
                    return true;
                }
                let Some(ev) = &s.last_event else { return true };
                if !ev.restarted.iter().any(|&r| r) {
                    return true;
                }
                // All children must be in the restart set
                ev.restarted.iter().all(|&r| r)
            }),
            // ── G8c: RestForOne restarts dead child + successors ────────
            Property::<Self>::always("G8c: RestForOne restarts dead + successors only", |_, s| {
                if s.strategy != SupervisorStrategy::RestForOne {
                    return true;
                }
                let Some(ev) = &s.last_event else { return true };
                if !ev.restarted.iter().any(|&r| r) {
                    return true;
                }
                // Children before dead_idx must NOT be restarted
                for i in 0..ev.dead_idx {
                    if ev.restarted[i] {
                        return false;
                    }
                }
                // Dead child + all after must be restarted
                for i in ev.dead_idx..NUM_CHILDREN {
                    if !ev.restarted[i] {
                        return false;
                    }
                }
                true
            }),
            // ── G8d: Temporary dead child triggers no restart ───────────
            // When the child that DIES has Temporary policy, should_restart
            // returns false and no children are restarted at all.
            Property::<Self>::always("G8d: Temporary dead child triggers no restart", |_, s| {
                let Some(ev) = &s.last_event else { return true };
                if ev.dead_policy != RestartPolicy::Temporary {
                    return true;
                }
                ev.restarted.iter().all(|&r| !r)
            }),
            // ── G8e: Transient + Normal death → no restart ──────────────
            Property::<Self>::always("G8e: Transient + Normal triggers no restart", |_, s| {
                let Some(ev) = &s.last_event else { return true };
                if ev.dead_policy != RestartPolicy::Transient || ev.reason != DeathReason::Normal {
                    return true;
                }
                ev.restarted.iter().all(|&r| !r)
            }),
            // ── G8f: Transient + Panicked → restart (unless meltdown) ──
            Property::<Self>::always(
                "G8f: Transient + Panicked triggers restart unless meltdown",
                |_, s| {
                    let Some(ev) = &s.last_event else { return true };
                    if ev.dead_policy != RestartPolicy::Transient
                        || ev.reason != DeathReason::Panicked
                        || ev.triggered_meltdown
                    {
                        return true;
                    }
                    // A restart should have happened
                    ev.restarted.iter().any(|&r| r)
                },
            ),
            // ── G8g: Meltdown bounds total restarts ─────────────────────
            Property::<Self>::always("G8g: meltdown when total_restarts exceeds max", |_, s| {
                if s.melted_down {
                    true // no further actions (enforced by empty actions)
                } else {
                    s.total_restarts <= MAX_RESTARTS
                }
            }),
            // ── Liveness Canaries ───────────────────────────────────────
            Property::<Self>::sometimes("L1: a restart occurs", |_, s| {
                s.children.iter().any(|c| c.restart_count > 0)
            }),
            Property::<Self>::sometimes("L2: meltdown is reachable", |_, s| s.melted_down),
            Property::<Self>::sometimes("L3: OneForOne exercised with restart", |_, s| {
                s.strategy == SupervisorStrategy::OneForOne
                    && s.children.iter().any(|c| c.restart_count > 0)
            }),
            Property::<Self>::sometimes("L4: OneForAll exercised with restart", |_, s| {
                s.strategy == SupervisorStrategy::OneForAll
                    && s.children.iter().any(|c| c.restart_count > 0)
            }),
            Property::<Self>::sometimes("L5: RestForOne exercised with restart", |_, s| {
                s.strategy == SupervisorStrategy::RestForOne
                    && s.children.iter().any(|c| c.restart_count > 0)
            }),
        ]
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[test]
#[ignore] // Exhaustive proof — run deliberately with `cargo test -- --ignored`
fn g8_supervisor_restart_model_check() {
    let result = SupervisorModel.checker().spawn_dfs().join();
    let unique = result.unique_state_count();
    let depth = result.max_depth();
    println!(
        "Stateright G8 (Supervisor Restart): {} unique states, max depth {}",
        unique, depth,
    );
    result.assert_properties();
    assert!(
        unique > 100,
        "Model explored too few states ({unique}); bounds may be too tight",
    );
}
