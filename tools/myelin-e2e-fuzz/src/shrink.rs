//! Failure-case reduction for persisted regressions.

use std::collections::{BTreeMap, BTreeSet};

use crate::ir::{ActionOp, BehaviorCase, FailureInjection};

pub fn shrink_failure(
    mut case: BehaviorCase,
    mut still_fails: impl FnMut(&BehaviorCase) -> bool,
) -> BehaviorCase {
    let mut index = case.processes.len();
    while index > 1 {
        index -= 1;
        let mut candidate = case.clone();
        let removed = candidate.processes.remove(index).id;
        for process in &mut candidate.processes {
            process
                .depends_on
                .retain(|dependency| dependency != &removed);
        }
        if candidate.validate().is_ok() && still_fails(&candidate) {
            case = candidate;
        }
    }
    for process_index in 0..case.processes.len() {
        let mut action_index = case.processes[process_index].actions.len();
        while action_index > 1 {
            action_index -= 1;
            let mut candidate = case.clone();
            candidate.processes[process_index]
                .actions
                .remove(action_index);
            if candidate.validate().is_ok() && still_fails(&candidate) {
                case = candidate;
            }
        }
    }
    for process_index in 1..case.processes.len() {
        for predecessor_index in (0..process_index).rev() {
            let predecessor = case.processes[predecessor_index].id.clone();
            if case.processes[process_index]
                .depends_on
                .contains(&predecessor)
            {
                continue;
            }
            let mut candidate = case.clone();
            candidate.processes[process_index]
                .depends_on
                .push(predecessor);
            if candidate.validate().is_ok() && still_fails(&candidate) {
                case = candidate;
                break;
            }
        }
    }
    for process_index in 0..case.processes.len() {
        for action_index in 0..case.processes[process_index].actions.len() {
            let stream = match &case.processes[process_index].actions[action_index].operation {
                ActionOp::StreamWrite { path, chunks, .. } if chunks.len() > 1 => {
                    Some((path.clone(), chunks.concat()))
                }
                _ => None,
            };
            let Some((path, payload)) = stream else {
                continue;
            };
            let mut candidate = case.clone();
            if let ActionOp::StreamWrite { chunks, .. } =
                &mut candidate.processes[process_index].actions[action_index].operation
            {
                *chunks = vec![payload.clone()];
            }
            for process in &mut candidate.processes {
                for action in &mut process.actions {
                    if let ActionOp::StreamRead {
                        path: reader_path,
                        expected,
                    }
                    | ActionOp::StreamReadInto {
                        path: reader_path,
                        expected,
                        ..
                    } = &mut action.operation
                        && reader_path == &path
                    {
                        *expected = payload.clone();
                    }
                }
            }
            if candidate.validate().is_ok() && still_fails(&candidate) {
                case = candidate;
            }
        }
    }
    for process_index in 0..case.processes.len() {
        for action_index in 0..case.processes[process_index].actions.len() {
            while let Some(candidate) = shrink_payload_candidate(&case, process_index, action_index)
            {
                if candidate.validate().is_ok() && still_fails(&candidate) {
                    case = candidate;
                } else {
                    break;
                }
            }
        }
    }
    let mut used_nodes = case
        .processes
        .iter()
        .map(|process| process.logical_node_id)
        .collect::<BTreeSet<_>>();
    if let FailureInjection::KillNode { logical_node_id } = &case.failure {
        used_nodes.insert(*logical_node_id);
    }
    let node_mapping = used_nodes
        .into_iter()
        .enumerate()
        .map(|(index, logical_node_id)| (logical_node_id, index as u64 + 1))
        .collect::<BTreeMap<_, _>>();
    let reduced_nodes = node_mapping.len().max(2) as u8;
    let topology_changed = reduced_nodes != case.node_count
        || node_mapping
            .iter()
            .any(|(logical_node_id, replacement)| logical_node_id != replacement);
    if topology_changed {
        let mut candidate = case.clone();
        candidate.node_count = reduced_nodes;
        for process in &mut candidate.processes {
            process.logical_node_id = node_mapping[&process.logical_node_id];
        }
        if let FailureInjection::KillNode { logical_node_id } = &mut candidate.failure {
            *logical_node_id = node_mapping[logical_node_id];
        }
        if candidate.validate().is_ok() && still_fails(&candidate) {
            case = candidate;
        }
    }
    case
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
    let mut candidate = case.clone();
    match operation {
        ActionOp::PublishBlob { path, bytes } if !bytes.is_empty() => {
            let reduced = bytes[..bytes.len() / 2].to_vec();
            if let ActionOp::PublishBlob { bytes, .. } =
                &mut candidate.processes[process_index].actions[action_index].operation
            {
                *bytes = reduced.clone();
            }
            for process in &mut candidate.processes {
                for action in &mut process.actions {
                    if let ActionOp::ReadBlob {
                        path: reader_path,
                        expected,
                    } = &mut action.operation
                        && reader_path == path
                    {
                        *expected = reduced.clone();
                    }
                }
            }
        }
        ActionOp::StreamWrite { path, chunks, .. }
            if chunks.iter().map(Vec::len).sum::<usize>() != 0 =>
        {
            let payload = chunks.concat();
            let reduced = payload[..payload.len() / 2].to_vec();
            if let ActionOp::StreamWrite { chunks, .. } =
                &mut candidate.processes[process_index].actions[action_index].operation
            {
                *chunks = vec![reduced.clone()];
            }
            for process in &mut candidate.processes {
                for action in &mut process.actions {
                    if let ActionOp::StreamRead {
                        path: reader_path,
                        expected,
                    }
                    | ActionOp::StreamReadInto {
                        path: reader_path,
                        expected,
                        ..
                    } = &mut action.operation
                        && reader_path == path
                    {
                        *expected = reduced.clone();
                    }
                }
            }
        }
        ActionOp::DescriptorWrite { bytes, .. } if !bytes.is_empty() => {
            if let ActionOp::DescriptorWrite {
                bytes: candidate_bytes,
                ..
            } = &mut candidate.processes[process_index].actions[action_index].operation
            {
                candidate_bytes.truncate(bytes.len() / 2);
            }
        }
        _ => return None,
    }
    Some(candidate)
}
