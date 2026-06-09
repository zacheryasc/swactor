//! Pipeline topology helpers.
//!
//! The chain is a linear sequence of `NUM_STAGES` stages. Each stage knows
//! only its own `STAGE` index and `NUM_STAGES` from env — neighbour names are
//! derived locally, never read from a central topology config (SPEC §3).
//!
//! Names registered on the SWIM registry:
//!
//! * `pp-entry`        — chain entry point (stage 0).
//! * `pp-exit`         — chain exit point (last stage).
//! * `pp-stage-{i}`    — every stage's per-index handle.
//!
//! Stage 0 and the last stage register two names that resolve to the same
//! `ActorAddress`: the per-index handle plus the entry/exit alias.

use crate::cluster::ClusterNode;
use swactor::actor::ActorAddress;

/// SWIM name for the chain entry. The orchestrator resolves this to submit a
/// request; stage 0 registers it.
pub const ENTRY_NAME: &str = "pp-entry";

/// SWIM name for the chain exit. The last stage registers it; stage 1 (in the
/// two-stage MVP) uses its own address here so the orchestrator can resolve
/// the response sink if it ever needs to.
pub const EXIT_NAME: &str = "pp-exit";

/// The per-index name for a stage, e.g. `pp-stage-0`.
pub fn stage_name(stage: u32) -> String {
    format!("pp-stage-{stage}")
}

/// First stage in the chain owns the prompt-side entry point.
pub fn is_first_stage(stage: u32) -> bool {
    stage == 0
}

/// Last stage in the chain owns the sampler and detokenizer.
pub fn is_last_stage(stage: u32, num_stages: u32) -> bool {
    // `num_stages == 0` is degenerate; treat any stage as terminal so callers
    // do not try to compute a next neighbour.
    num_stages == 0 || stage + 1 >= num_stages
}

/// Name of the downstream neighbour (where `StageActivation`s go), or `None`
/// if this is the last stage.
pub fn next_stage_name(stage: u32, num_stages: u32) -> Option<String> {
    if is_last_stage(stage, num_stages) {
        None
    } else {
        Some(stage_name(stage + 1))
    }
}

/// Name of the upstream neighbour (where `NextToken`s flow back), or `None`
/// if this is the first stage.
pub fn prev_stage_name(stage: u32) -> Option<String> {
    if is_first_stage(stage) {
        None
    } else {
        Some(stage_name(stage - 1))
    }
}

/// Register every name this stage is responsible for, all pointing at the
/// same `addr`. Returns the list of names registered, mostly for log lines
/// and tests.
///
/// * Stage 0     → `pp-entry`, `pp-stage-0`.
/// * Last stage  → `pp-exit`,  `pp-stage-{stage}`.
/// * Middle      → `pp-stage-{stage}` only.
///
/// In the two-stage MVP, stage 1 is the last stage, so it registers
/// `pp-exit` and `pp-stage-1`.
pub fn register_stage_names(
    node: &ClusterNode,
    stage: u32,
    num_stages: u32,
    addr: ActorAddress,
) -> Vec<String> {
    let mut names = Vec::new();

    let idx_name = stage_name(stage);
    node.register_name(&idx_name, addr);
    names.push(idx_name);

    if is_first_stage(stage) {
        node.register_name(ENTRY_NAME, addr);
        names.push(ENTRY_NAME.into());
    }
    if is_last_stage(stage, num_stages) {
        node.register_name(EXIT_NAME, addr);
        names.push(EXIT_NAME.into());
    }

    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_name_uses_pp_prefix() {
        assert_eq!(stage_name(0), "pp-stage-0");
        assert_eq!(stage_name(7), "pp-stage-7");
    }

    #[test]
    fn first_stage_has_no_prev() {
        assert_eq!(prev_stage_name(0), None);
    }

    #[test]
    fn middle_stage_has_both_neighbours() {
        assert_eq!(next_stage_name(1, 4).as_deref(), Some("pp-stage-2"));
        assert_eq!(prev_stage_name(1).as_deref(), Some("pp-stage-0"));
    }
}
