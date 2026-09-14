//! Failure-case reduction for persisted regressions.

use std::collections::BTreeSet;

use crate::budget::Budget;
use crate::ir::{
    ActionOp, BehaviorCase, ExpectedOutcome, FailureInjection, ProcessProgram, TopologyFamily,
};

struct CleanupScope {
    live_nodes: BTreeSet<u64>,
    read_only_paths: BTreeSet<String>,
    owned_paths: BTreeSet<String>,
    exact_live_nodes: bool,
}

impl CleanupScope {
    fn new(case: &BehaviorCase, exact_live_nodes: bool) -> Self {
        Self {
            live_nodes: case.live_nodes.clone(),
            read_only_paths: case.read_only_fixture_paths.clone(),
            owned_paths: case.owned_paths(),
            exact_live_nodes,
        }
    }

    fn admits(&self, candidate: &BehaviorCase) -> bool {
        candidate.read_only_fixture_paths == self.read_only_paths
            && (if self.exact_live_nodes {
                candidate.live_nodes == self.live_nodes
            } else {
                candidate.live_nodes.is_subset(&self.live_nodes)
            })
            && candidate.owned_paths().is_subset(&self.owned_paths)
    }
}

pub fn shrink_failure(
    case: BehaviorCase,
    mut still_fails: impl FnMut(&BehaviorCase) -> bool,
) -> BehaviorCase {
    match try_shrink_failure(case, |candidate| {
        Ok::<bool, std::convert::Infallible>(still_fails(candidate))
    }) {
        Ok(case) => case,
        Err(never) => match never {},
    }
}

pub fn try_shrink_failure<E>(
    case: BehaviorCase,
    still_fails: impl FnMut(&BehaviorCase) -> Result<bool, E>,
) -> Result<BehaviorCase, E> {
    shrink_checked(case, still_fails, || Ok(()), true)
}

/// Diagnosis stays on the accepted fixture; minimizing the live topology
/// would require a different baseline and is not an admissible candidate.
pub fn try_shrink_failure_budgeted(
    case: BehaviorCase,
    budget: &Budget,
    still_fails: impl FnMut(&BehaviorCase) -> Result<bool, String>,
) -> Result<BehaviorCase, String> {
    shrink_checked(
        case,
        still_fails,
        || budget.check("shrink candidate generation and oracle"),
        false,
    )
}

fn shrink_checked<E>(
    mut case: BehaviorCase,
    mut still_fails: impl FnMut(&BehaviorCase) -> Result<bool, E>,
    mut check: impl FnMut() -> Result<(), E>,
    reduce_live_nodes: bool,
) -> Result<BehaviorCase, E> {
    let cleanup_scope = CleanupScope::new(&case, !reduce_live_nodes);
    check()?;
    if !matches!(case.failure, FailureInjection::None) {
        let mut candidate = case.clone();
        candidate.failure = FailureInjection::None;
        if cleanup_scope.admits(&candidate)
            && candidate.validate().is_ok()
            && still_fails(&candidate)?
        {
            case = candidate;
        }
    }
    for route_index in (0..case.routes.len()).rev() {
        check()?;
        let mut candidate = case.clone();
        let paths = candidate
            .routes
            .remove(route_index)
            .edges
            .into_iter()
            .map(|edge| edge.path)
            .collect::<BTreeSet<_>>();
        if candidate
            .processes
            .iter()
            .flat_map(|process| &process.actions)
            .any(|action| {
                paths.contains(action.operation.path())
                    && matches!(
                        action.operation,
                        ActionOp::GatedStreamRead { .. } | ActionOp::GatedStreamWrite { .. }
                    )
            })
        {
            continue;
        }
        for process in &mut candidate.processes {
            process
                .actions
                .retain(|action| !paths.contains(action.operation.path()));
        }
        candidate
            .processes
            .retain(|process| !process.actions.is_empty());
        let remaining = candidate
            .processes
            .iter()
            .map(|process| process.id.clone())
            .collect::<BTreeSet<_>>();
        for process in &mut candidate.processes {
            process
                .depends_on
                .retain(|dependency| remaining.contains(dependency));
        }
        if candidate.routes.is_empty() {
            candidate.topology = TopologyFamily::Fixed;
            candidate
                .scenarios
                .remove(&crate::ir::CoverageScenario::ConcurrentStartup);
            candidate
                .scenarios
                .remove(&crate::ir::CoverageScenario::RingCompletion);
        }
        if structural_candidate_is_admissible(&cleanup_scope, &case, &candidate)
            && still_fails(&candidate)?
        {
            case = candidate;
        }
    }
    // Same-path mutations can be mutual prerequisites across processes.
    // Try removing their namespace work together before individual deletions,
    // rather than weakening the guard or getting stuck on an unrelated cycle.
    // This adds at most one candidate per distinct, execution-resolved path.
    let paths = case
        .processes
        .iter()
        .flat_map(|process| {
            process.actions.iter().flat_map(move |action| {
                action
                    .operation
                    .paths()
                    .into_iter()
                    .flatten()
                    .map(move |path| prerequisite_path(process, path).into_owned())
            })
        })
        .collect::<BTreeSet<_>>();
    for path in paths {
        check()?;
        let mut candidate = case.clone();
        let mut removed = false;
        for (process_index, process) in candidate.processes.iter_mut().enumerate() {
            let original = &case.processes[process_index];
            process.actions.retain(|action| {
                let keep = !action
                    .operation
                    .paths()
                    .into_iter()
                    .flatten()
                    .any(|other| prerequisite_path(original, other).as_ref() == path.as_str());
                removed |= !keep;
                keep
            });
        }
        if !removed {
            continue;
        }
        candidate
            .processes
            .retain(|process| !process.actions.is_empty());
        let remaining = candidate
            .processes
            .iter()
            .map(|process| process.id.clone())
            .collect::<BTreeSet<_>>();
        for process in &mut candidate.processes {
            process
                .depends_on
                .retain(|dependency| remaining.contains(dependency));
        }
        if structural_candidate_is_admissible(&cleanup_scope, &case, &candidate)
            && still_fails(&candidate)?
        {
            case = candidate;
        }
    }
    let mut index = case.processes.len();
    while index > 0 && case.processes.len() > 1 {
        check()?;
        index -= 1;
        let mut candidate = case.clone();
        let removed = candidate.processes.remove(index).id;
        for process in &mut candidate.processes {
            process
                .depends_on
                .retain(|dependency| dependency != &removed);
        }
        if structural_candidate_is_admissible(&cleanup_scope, &case, &candidate)
            && still_fails(&candidate)?
        {
            case = candidate;
        }
    }
    for process_index in 0..case.processes.len() {
        let mut action_index = case.processes[process_index].actions.len();
        while action_index > 0 && case.processes[process_index].actions.len() > 1 {
            check()?;
            action_index -= 1;
            let mut candidate = case.clone();
            candidate.processes[process_index]
                .actions
                .remove(action_index);
            if structural_candidate_is_admissible(&cleanup_scope, &case, &candidate)
                && still_fails(&candidate)?
            {
                case = candidate;
            }
        }
    }
    for process_index in 0..case.processes.len() {
        for dependency_index in (0..case.processes[process_index].depends_on.len()).rev() {
            check()?;
            let mut candidate = case.clone();
            candidate.processes[process_index]
                .depends_on
                .remove(dependency_index);
            if structural_candidate_is_admissible(&cleanup_scope, &case, &candidate)
                && still_fails(&candidate)?
            {
                case = candidate;
            }
        }
    }
    for process_index in 0..case.processes.len() {
        for action_index in 0..case.processes[process_index].actions.len() {
            check()?;
            let stream = match &case.processes[process_index].actions[action_index].operation {
                ActionOp::StreamWrite { path, chunks, .. } if chunks.len() > 1 => {
                    Some((path.clone(), chunks.concat()))
                }
                _ => None,
            };
            let Some((path, payload)) = stream else {
                continue;
            };
            if case.routes.iter().any(|route| {
                route.edges.len() > 1 && route.edges.iter().any(|edge| edge.path == path)
            }) {
                continue;
            }
            let mut candidate = case.clone();
            if let ActionOp::StreamWrite { chunks, .. } =
                &mut candidate.processes[process_index].actions[action_index].operation
            {
                *chunks = vec![payload];
            }
            if cleanup_scope.admits(&candidate)
                && candidate.validate().is_ok()
                && still_fails(&candidate)?
            {
                case = candidate;
            }
        }
    }
    for process_index in 0..case.processes.len() {
        for action_index in 0..case.processes[process_index].actions.len() {
            loop {
                check()?;
                let Some(candidate) = shrink_payload_candidate(&case, process_index, action_index)
                else {
                    break;
                };
                if cleanup_scope.admits(&candidate)
                    && candidate.validate().is_ok()
                    && still_fails(&candidate)?
                {
                    case = candidate;
                } else {
                    break;
                }
            }
        }
    }
    check()?;
    if !reduce_live_nodes {
        return Ok(case);
    }
    let mut used_nodes = case
        .processes
        .iter()
        .map(|process| process.logical_node_id)
        .chain(
            case.routes
                .iter()
                .flat_map(|route| &route.edges)
                .flat_map(|edge| [edge.source, edge.destination]),
        )
        .collect::<BTreeSet<_>>();
    if used_nodes.len() == 1
        && let Some(spare) = case
            .live_nodes
            .iter()
            .copied()
            .find(|node| !used_nodes.contains(node))
    {
        used_nodes.insert(spare);
    }
    check()?;
    if used_nodes != case.live_nodes {
        let mut candidate = case.clone();
        candidate.live_nodes = used_nodes;
        if cleanup_scope.admits(&candidate)
            && candidate.validate().is_ok()
            && still_fails(&candidate)?
        {
            case = candidate;
        }
    }
    check()?;
    Ok(case)
}

/// A typed abort can match even when deleting its setup makes the error correct.
/// Preserve the original namespace prerequisites instead of asking that failed
/// replay to prove its own expected-success contract. This deliberately keeps
/// all potentially relevant mutations, not just a guessed winning publication.
fn structural_candidate_is_admissible(
    cleanup_scope: &CleanupScope,
    original: &BehaviorCase,
    candidate: &BehaviorCase,
) -> bool {
    if !cleanup_scope.admits(candidate) || candidate.validate().is_err() {
        return false;
    }
    let retained = original
        .processes
        .iter()
        .map(|process| {
            let mut actions = candidate
                .processes
                .iter()
                .find(|remaining| remaining.id == process.id)
                .into_iter()
                .flat_map(|remaining| &remaining.actions)
                .peekable();
            let retained = process
                .actions
                .iter()
                .map(|action| {
                    if actions.peek().is_some_and(|next| *next == action) {
                        actions.next();
                        true
                    } else {
                        false
                    }
                })
                .collect::<Vec<_>>();
            debug_assert!(
                actions.next().is_none(),
                "structural candidates only delete actions"
            );
            retained
        })
        .collect::<Vec<_>>();
    for (process_index, process) in original.processes.iter().enumerate() {
        for (action_index, action) in process.actions.iter().enumerate() {
            if retained[process_index][action_index] {
                continue;
            }
            for path in action.operation.paths().into_iter().flatten() {
                // Successful local probes and waits also establish a prefix
                // on which a later success relies. Across processes, retain
                // mutations and synchronization, not unrelated pure readers.
                let affects_peers = action.operation.mutating_paths().contains(&Some(path))
                    || matches!(
                        action.operation,
                        ActionOp::AwaitEntry { .. } | ActionOp::WaitForQuiescent { .. }
                    );
                let path = prerequisite_path(process, path);
                for (consumer_index, consumer) in original.processes.iter().enumerate() {
                    for (step, required) in consumer.actions.iter().enumerate() {
                        if !retained[consumer_index][step]
                            || (process_index == consumer_index && step <= action_index)
                            || (process_index != consumer_index && !affects_peers)
                            || !matches!(
                                required.expected,
                                ExpectedOutcome::Ok
                                    | ExpectedOutcome::Linearized { successes: 1.., .. }
                            )
                        {
                            continue;
                        }
                        if required
                            .operation
                            .paths()
                            .into_iter()
                            .flatten()
                            .any(|required| prerequisite_path(consumer, required) == path)
                        {
                            return false;
                        }
                    }
                }
            }
        }
    }
    // A retained producer is insufficient if removing a dependency (or an
    // intermediate process) lets its consumer run before publication. Keep
    // transitive ordering, while permitting removal of redundant direct edges.
    candidate.processes.iter().all(|process| {
        let before = dependency_ancestors(original, process);
        let after = dependency_ancestors(candidate, process);
        candidate.processes.iter().all(|ancestor| {
            !before.contains(ancestor.id.as_str()) || after.contains(ancestor.id.as_str())
        })
    })
}

fn prerequisite_path<'a>(process: &ProcessProgram, path: &'a str) -> std::borrow::Cow<'a, str> {
    if let Some(suffix) = path.strip_prefix("/runs/self")
        && (suffix.is_empty() || suffix.starts_with('/'))
    {
        format!("/runs/{}{suffix}", process.access.execution_id).into()
    } else {
        path.into()
    }
}

fn dependency_ancestors<'a>(case: &'a BehaviorCase, process: &ProcessProgram) -> BTreeSet<&'a str> {
    let mut ancestors = BTreeSet::new();
    let mut pending = vec![process.id.as_str()];
    while let Some(id) = pending.pop() {
        if let Some(program) = case.processes.iter().find(|program| program.id == id) {
            for dependency in &program.depends_on {
                if ancestors.insert(dependency.as_str()) {
                    pending.push(dependency);
                }
            }
        }
    }
    ancestors
}

fn shrink_payload_candidate(
    case: &BehaviorCase,
    process_index: usize,
    action_index: usize,
) -> Option<BehaviorCase> {
    let operation = &case
        .processes
        .get(process_index)?
        .actions
        .get(action_index)?
        .operation;
    // A multi-edge relay's payload is coupled to downstream transformations.
    // Keep that behavioral graph intact rather than manufacturing a mismatch
    // by changing only one edge's writer/reader expectations.
    if case.routes.iter().any(|route| {
        route.edges.len() > 1 && route.edges.iter().any(|edge| edge.path == operation.path())
    }) {
        return None;
    }
    let (stream, payload) = match operation {
        ActionOp::PublishBlob { bytes, .. } => {
            (false, std::borrow::Cow::Borrowed(bytes.as_slice()))
        }
        ActionOp::DescriptorWrite { bytes, length, .. } if *length == Some(bytes.len() as u64) => {
            (false, std::borrow::Cow::Borrowed(bytes.as_slice()))
        }
        ActionOp::StreamWrite { chunks, .. } => (true, std::borrow::Cow::Owned(chunks.concat())),
        _ => return None,
    };
    if payload.is_empty() {
        return None;
    }
    let path = prerequisite_path(&case.processes[process_index], operation.path());
    let reduced = &payload[..payload.len() / 2];
    let mut candidate = case.clone();
    for (peer_index, process) in candidate.processes.iter_mut().enumerate() {
        for (step, action) in process.actions.iter_mut().enumerate() {
            if peer_index == process_index && step == action_index {
                continue;
            }
            let original_process = &case.processes[peer_index];
            if !action
                .operation
                .paths()
                .into_iter()
                .flatten()
                .any(|other| prerequisite_path(original_process, other) == path)
            {
                continue;
            }
            // Coupled replacements, renames and partial descriptor ranges need
            // a different reduction. Never create a new payload error by
            // rewriting just one possible source or leaving its reader stale.
            let expected = match &mut action.operation {
                ActionOp::ReadBlob { expected, .. } if !stream => expected,
                ActionOp::DescriptorRead {
                    expected,
                    flags,
                    offset,
                    ..
                } if !stream && *flags == libc::O_RDONLY && *offset == 0 => expected,
                ActionOp::StreamRead { expected, .. }
                | ActionOp::StreamReadWithRetry { expected, .. }
                | ActionOp::StreamReadInto { expected, .. }
                    if stream =>
                {
                    expected
                }
                ActionOp::Lookup { .. }
                | ActionOp::AwaitEntry { .. }
                | ActionOp::WaitForQuiescent { .. }
                | ActionOp::Unlink { .. } => continue,
                _ => return None,
            };
            if expected.as_slice() != payload.as_ref() {
                return None;
            }
            *expected = reduced.to_vec();
        }
    }
    match &mut candidate.processes[process_index].actions[action_index].operation {
        ActionOp::PublishBlob { bytes, .. } => bytes.truncate(reduced.len()),
        ActionOp::DescriptorWrite { bytes, length, .. } => {
            bytes.truncate(reduced.len());
            *length = Some(reduced.len() as u64);
        }
        ActionOp::StreamWrite { chunks, .. } => *chunks = vec![reduced.to_vec()],
        _ => unreachable!("supported payload operation checked before cloning"),
    }
    Some(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Action;

    #[test]
    fn payload_candidates_keep_halving_order_and_matching_reader_contract() {
        let mut case = crate::corpus::stable_corpus(2, 1).remove(0);
        let path = "/cases/shrink/payload".to_owned();
        case.processes[0].actions = vec![Action::ok(ActionOp::PublishBlob {
            path: path.clone(),
            bytes: b"abcdef".to_vec(),
        })];
        case.processes[1].actions = vec![
            Action::ok(ActionOp::ReadBlob {
                path: path.clone(),
                expected: b"abcdef".to_vec(),
            }),
            Action::ok(ActionOp::ReadBlob {
                path: "/cases/shrink/other".to_owned(),
                expected: b"unchanged".to_vec(),
            }),
        ];
        for expected in [b"abc".as_slice(), b"a".as_slice(), b"".as_slice()] {
            case = shrink_payload_candidate(&case, 0, 0).unwrap();
            assert!(matches!(&case.processes[0].actions[0].operation,
                ActionOp::PublishBlob { bytes, .. } if bytes == expected));
            assert!(matches!(&case.processes[1].actions[0].operation,
                ActionOp::ReadBlob { expected: bytes, .. } if bytes == expected));
            assert!(matches!(&case.processes[1].actions[1].operation,
                ActionOp::ReadBlob { expected, .. } if expected == b"unchanged"));
        }
        assert!(shrink_payload_candidate(&case, 0, 0).is_none());
        assert!(shrink_payload_candidate(&case, 1, 0).is_none());
        case.processes[0].actions[0].operation = ActionOp::StreamWrite {
            path,
            chunks: vec![Vec::new(), Vec::new()],
            replace: false,
        };
        assert!(shrink_payload_candidate(&case, 0, 0).is_none());
    }

    #[test]
    fn descriptor_payload_reduction_preserves_allocation_and_scoped_readers() {
        use crate::ir::{DescriptorFinish, DescriptorReadMethod, DescriptorWriteMethod};

        let mut case = crate::corpus::stable_corpus(2, 1).remove(0);
        let path = "/runs/self/blob";
        case.processes[0].actions = vec![
            Action::ok(ActionOp::DescriptorWrite {
                path: path.to_owned(),
                flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                length: Some(6),
                bytes: b"abcdef".to_vec(),
                method: DescriptorWriteMethod::Write,
                finish: DescriptorFinish::Close,
            }),
            Action::ok(ActionOp::DescriptorRead {
                path: path.to_owned(),
                flags: libc::O_RDONLY,
                expected: b"abcdef".to_vec(),
                method: DescriptorReadMethod::Mapping,
                offset: 0,
                finish: DescriptorFinish::Close,
            }),
        ];
        case.processes[1].actions = vec![Action::ok(ActionOp::ReadBlob {
            path: path.to_owned(),
            expected: b"unrelated".to_vec(),
        })];
        let candidate = shrink_payload_candidate(&case, 0, 0).unwrap();
        assert!(matches!(&candidate.processes[0].actions[0].operation,
            ActionOp::DescriptorWrite { bytes, length: Some(3), .. } if bytes == b"abc"));
        assert!(matches!(&candidate.processes[0].actions[1].operation,
            ActionOp::DescriptorRead { expected, .. } if expected == b"abc"));
        assert_eq!(candidate.processes[1], case.processes[1]);

        case.processes[0].actions.push(Action::ok(ActionOp::Rename {
            source: path.to_owned(),
            destination: "/runs/self/moved".to_owned(),
            replace: false,
        }));
        assert!(shrink_payload_candidate(&case, 0, 0).is_none());
    }
}
