//! Ordered state history and deterministic accumulated-state reduction.
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use protocol_telemetry::{ActorType, Protocol, TransitionRecord};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use swactor::actor::ActorAddress;

use crate::evidence::{Journal, ObjectRef, Store};
use crate::ir::BehaviorCase;

const DIAGNOSIS_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureFingerprint {
    pub typed_signature: Value,
    pub cleanup_restored: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolOwnerKind {
    Actor,
    Process,
    Session,
    Binding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalDisposition {
    NotProduced,
    Produced,
    Forwarded,
    Acknowledged,
    Lost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnedWorkKind {
    Child,
    Task,
    Timer,
    Resource,
    Reply,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedProtocolWork {
    pub kind: OwnedWorkKind,
    pub identity: String,
    pub state: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedProtocolObligation {
    pub correlation_identity: String,
    pub operation: Option<[u8; 16]>,
    pub protocol: Protocol,
    pub owner_kind: ProtocolOwnerKind,
    pub owner_identity: String,
    pub actor: Option<ActorAddress>,
    pub actor_type: Option<ActorType>,
    pub awaited_condition: String,
    pub generation: Option<u64>,
    pub incarnation: Option<u64>,
    pub owned_work: Vec<OwnedProtocolWork>,
    pub terminal: TerminalDisposition,
    pub latest_transition: Option<TransitionRecord>,
}

impl UnresolvedProtocolObligation {
    fn transition_matches(&self, transition: &TransitionRecord) -> bool {
        transition.protocol == self.protocol
            && transition.operation == self.operation
            && transition.generation == self.generation
            && transition.incarnation == self.incarnation
            && self.actor.is_none_or(|actor| actor == transition.actor)
            && self
                .actor_type
                .is_none_or(|actor_type| actor_type == transition.actor_type)
    }

    fn validate(&self) -> Result<(), String> {
        if self.correlation_identity.is_empty()
            || self.correlation_identity.len() > 4096
            || self.owner_identity.is_empty()
            || self.owner_identity.len() > 4096
            || self.awaited_condition.is_empty()
            || self.awaited_condition.len() > 4096
            || self.owned_work.len() > 1024
            || self.owned_work.iter().any(|work| {
                work.identity.is_empty()
                    || work.identity.len() > 4096
                    || work.state.is_empty()
                    || work.state.len() > 4096
            })
        {
            return Err(
                "protocol obligation contains missing or unbounded ownership evidence".to_owned(),
            );
        }
        if self.owner_kind == ProtocolOwnerKind::Actor
            && (self.actor.is_none() || self.actor_type.is_none())
        {
            return Err("actor-owned obligation lacks concrete actor identity".to_owned());
        }
        if self
            .latest_transition
            .as_ref()
            .is_some_and(|transition| !self.transition_matches(transition))
        {
            return Err("latest transition does not belong to protocol obligation".to_owned());
        }
        let mut work = self.owned_work.clone();
        work.sort();
        if work.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("protocol obligation contains duplicate owned work".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolFailureEvidence {
    pub schema_version: u32,
    pub obligations: Vec<UnresolvedProtocolObligation>,
}

impl ProtocolFailureEvidence {
    /// Join authoritative ownership with best-effort transitions. Missing
    /// telemetry remains visible as `None`; it never changes the obligation.
    pub fn new(
        mut obligations: Vec<UnresolvedProtocolObligation>,
        transitions: &[TransitionRecord],
    ) -> Result<Self, String> {
        if obligations.is_empty() || obligations.len() > 1024 {
            return Err("failure evidence requires bounded unresolved obligations".to_owned());
        }
        for obligation in &mut obligations {
            obligation.owned_work.sort();
            obligation.latest_transition = transitions
                .iter()
                .rev()
                .find(|transition| obligation.transition_matches(transition))
                .copied();
            obligation.validate()?;
        }
        obligations.sort_by(|left, right| {
            (&left.correlation_identity, &left.owner_identity)
                .cmp(&(&right.correlation_identity, &right.owner_identity))
        });
        if obligations.windows(2).any(|pair| {
            pair[0].correlation_identity == pair[1].correlation_identity
                && pair[0].owner_identity == pair[1].owner_identity
        }) {
            return Err("duplicate unresolved protocol obligation".to_owned());
        }
        Ok(Self {
            schema_version: DIAGNOSIS_SCHEMA_VERSION,
            obligations,
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != DIAGNOSIS_SCHEMA_VERSION
            || self.obligations.is_empty()
            || self.obligations.len() > 1024
        {
            return Err("failure evidence has invalid schema or obligation count".to_owned());
        }
        for obligation in &self.obligations {
            obligation.validate()?;
        }
        if self.obligations.windows(2).any(|pair| {
            (&pair[0].correlation_identity, &pair[0].owner_identity)
                >= (&pair[1].correlation_identity, &pair[1].owner_identity)
        }) {
            return Err("failure evidence obligations are duplicate or unordered".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum HistoryBoundary {
    FixtureEstablished,
    Case,
    Restart,
    Recovery,
    TopologyTransition(String),
    PeriodicAudit,
    FinalCleanup,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEntry {
    pub sequence: u64,
    pub boundary: HistoryBoundary,
    pub fixture_identity: String,
    pub case: Option<ObjectRef>,
    pub observation: Option<ObjectRef>,
    pub execution: ObjectRef,
    pub failure: Option<FailureFingerprint>,
    pub attempt_id: Option<u64>,
    pub failure_evidence: Option<ObjectRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reproducer {
    pub schema_version: u32,
    pub fixture_identity: String,
    pub destructive_anchor: usize,
    pub prefix: Vec<ObjectRef>,
    pub failing_case: ObjectRef,
    pub fingerprint: FailureFingerprint,
    pub failure_evidence: Option<ObjectRef>,
}

pub struct HistoryAppend<'a, T, O> {
    pub boundary: HistoryBoundary,
    pub fixture_identity: String,
    pub case: Option<&'a T>,
    pub observation: Option<&'a O>,
    pub execution: &'a Value,
    pub failure: Option<FailureFingerprint>,
    pub failure_evidence: Option<&'a ProtocolFailureEvidence>,
    pub attempt_id: Option<u64>,
}

pub struct OrderedHistory {
    root: PathBuf,
    store: Store,
    journal: Journal,
}

impl OrderedHistory {
    pub fn open(root: &Path) -> Result<Self, String> {
        let root = root.join("ordered-history");
        let store = Store::new(&root)?;
        let journal = Journal::open(&root.join("progress.jsonl"), &store)?;
        Ok(Self {
            root,
            store,
            journal,
        })
    }

    pub fn append<T: Serialize, O: Serialize>(
        &mut self,
        record: HistoryAppend<'_, T, O>,
    ) -> Result<HistoryEntry, String> {
        let HistoryAppend {
            boundary,
            fixture_identity,
            case,
            observation,
            execution,
            failure,
            failure_evidence,
            attempt_id,
        } = record;
        if fixture_identity.is_empty()
            || failure.is_some() && case.is_none()
            || failure_evidence.is_some() && failure.is_none()
            || failure
                .as_ref()
                .is_some_and(|fingerprint| !fingerprint.typed_signature.is_object())
        {
            return Err("history failure, case, or fixture identity is inconsistent".to_owned());
        }
        if let Some(evidence) = failure_evidence {
            evidence.validate()?;
        }
        let case = case.map(|value| self.store.put(value)).transpose()?;
        let observation = observation.map(|value| self.store.put(value)).transpose()?;
        let entry = HistoryEntry {
            sequence: self.entries()?.len() as u64,
            boundary,
            fixture_identity,
            case,
            observation,
            execution: self.store.put(execution)?,
            failure,
            failure_evidence: failure_evidence
                .map(|evidence| self.store.put(evidence))
                .transpose()?,
            attempt_id,
        };
        self.journal.append(json!({
            "kind": "history_entry",
            "schema_version": DIAGNOSIS_SCHEMA_VERSION,
            "entry": entry,
        }))?;
        Ok(entry)
    }

    pub fn entries(&self) -> Result<Vec<HistoryEntry>, String> {
        self.journal
            .records
            .iter()
            .filter(|record| record.get("kind").and_then(Value::as_str) == Some("history_entry"))
            .map(|record| {
                if record.get("schema_version").and_then(Value::as_u64)
                    != Some(u64::from(DIAGNOSIS_SCHEMA_VERSION))
                {
                    return Err("ordered history schema mismatch".to_owned());
                }
                serde_json::from_value(record.get("entry").cloned().ok_or("history entry absent")?)
                    .map_err(|error| format!("decode ordered history: {error}"))
            })
            .collect()
    }

    pub fn reproducer(&mut self) -> Result<(ObjectRef, Reproducer), String> {
        let entries = self.entries()?;
        let failed = entries
            .iter()
            .rposition(|entry| entry.failure.is_some() && entry.case.is_some())
            .ok_or("ordered history has no typed failing case")?;
        let failure = &entries[failed];
        let destructive_anchor = entries[..failed]
            .iter()
            .rposition(|entry| {
                matches!(
                    entry.boundary,
                    HistoryBoundary::Restart
                        | HistoryBoundary::Recovery
                        | HistoryBoundary::TopologyTransition(_)
                )
            })
            .map_or(0, |index| index + 1);
        let prefix = entries[destructive_anchor..failed]
            .iter()
            .filter_map(|entry| entry.case.clone())
            .collect();
        let reproducer = Reproducer {
            schema_version: DIAGNOSIS_SCHEMA_VERSION,
            fixture_identity: failure.fixture_identity.clone(),
            destructive_anchor,
            prefix,
            failing_case: failure.case.clone().expect("checked above"),
            fingerprint: failure.failure.clone().expect("checked above"),
            failure_evidence: failure.failure_evidence.clone(),
        };
        let reference = self.store.put(&reproducer)?;
        self.journal.append(json!({
            "kind": "reproducer",
            "schema_version": DIAGNOSIS_SCHEMA_VERSION,
            "object": reference,
        }))?;
        Ok((reference, reproducer))
    }

    pub fn compact(&mut self) -> Result<ObjectRef, String> {
        let entries = self.entries()?;
        self.journal.compact(&self.store, &entries)
    }

    /// Export the recursively referenced immutable closure for a standalone
    /// reproducer. The destination verifies every digest while copying.
    pub fn export_reproducer(
        &self,
        reference: &ObjectRef,
        destination: &Path,
    ) -> Result<(), String> {
        let destination = Store::new(destination)?;
        self.store.export(reference, &destination)
    }

    pub fn object<T: serde::de::DeserializeOwned>(
        &self,
        reference: &ObjectRef,
    ) -> Result<T, String> {
        self.store.get(reference)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct ReducedAccumulatedFailure {
    pub prefix: Vec<ObjectRef>,
    pub failing_case: BehaviorCase,
    pub fingerprint: FailureFingerprint,
}

/// Reduce the final case and its state-producing prefix against one exact typed
/// failure. Candidate cases are ordered by checked resource cost and canonical
/// encoding, so scheduling or caller iteration order cannot alter the result.
pub fn reduce_accumulated_failure<F>(
    prefix: &[ObjectRef],
    failing_case: &BehaviorCase,
    case_candidates: impl IntoIterator<Item = BehaviorCase>,
    expected: &FailureFingerprint,
    mut replay: F,
) -> Result<ReducedAccumulatedFailure, String>
where
    F: FnMut(&[ObjectRef], &BehaviorCase) -> Result<Option<FailureFingerprint>, String>,
{
    failing_case.validate()?;
    let mut candidates = case_candidates
        .into_iter()
        .chain(std::iter::once(failing_case.clone()))
        .map(|case| {
            case.validate()?;
            let resources = case.resource_summary()?;
            let encoded = serde_json::to_vec(&case).map_err(|error| error.to_string())?;
            Ok((
                (
                    resources.process_count,
                    resources.action_count,
                    resources.payload_bytes,
                    resources.allocation_bytes,
                    encoded.len(),
                    encoded,
                ),
                case,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    let mut seen = BTreeSet::new();
    candidates.retain(|(key, _)| seen.insert(key.5.clone()));
    let mut reduced_case = failing_case.clone();
    for (_, candidate) in candidates {
        if replay(prefix, &candidate)?.as_ref() == Some(expected) {
            reduced_case = candidate;
            break;
        }
    }
    let reduced_prefix = reduce_prefix(prefix, expected, |candidate| {
        replay(candidate, &reduced_case)
    })?;
    Ok(ReducedAccumulatedFailure {
        prefix: reduced_prefix,
        failing_case: reduced_case,
        fingerprint: expected.clone(),
    })
}

/// Deterministic, order-preserving ddmin. A candidate is accepted only when it
/// reproduces the exact typed failure and the same cleanup-restoration result.
pub fn reduce_prefix<F>(
    prefix: &[ObjectRef],
    expected: &FailureFingerprint,
    mut replay: F,
) -> Result<Vec<ObjectRef>, String>
where
    F: FnMut(&[ObjectRef]) -> Result<Option<FailureFingerprint>, String>,
{
    let mut current = prefix.to_vec();
    let mut granularity = 2_usize;
    while !current.is_empty() {
        let chunk = current.len().div_ceil(granularity);
        let mut reduced = false;
        let mut start = 0;
        while start < current.len() {
            let end = (start + chunk).min(current.len());
            let mut candidate = Vec::with_capacity(current.len() - (end - start));
            candidate.extend_from_slice(&current[..start]);
            candidate.extend_from_slice(&current[end..]);
            if replay(&candidate)?.as_ref() == Some(expected) {
                current = candidate;
                granularity = granularity.saturating_sub(1).max(2);
                reduced = true;
                break;
            }
            start = end;
        }
        if !reduced {
            if granularity >= current.len() {
                break;
            }
            granularity = (granularity * 2).min(current.len());
        }
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol_telemetry::Phase;

    fn object(digest: char) -> ObjectRef {
        ObjectRef {
            digest: digest.to_string().repeat(64),
            length: 1,
        }
    }

    #[test]
    fn reduction_preserves_order_signature_and_cleanup_behavior() {
        let prefix = vec![object('a'), object('b'), object('c'), object('d')];
        let expected = FailureFingerprint {
            typed_signature: json!({"class":"stalled", "owner":"reader"}),
            cleanup_restored: false,
        };
        let reduced = reduce_prefix(&prefix, &expected, |candidate| {
            let digests = candidate
                .iter()
                .map(|object| object.digest.as_bytes()[0])
                .collect::<Vec<_>>();
            // Irrelevant setup steps must not invalidate the original failing
            // prefix. Removing b changes the failure; removing d changes cleanup.
            Ok(Some(FailureFingerprint {
                typed_signature: if digests.contains(&b'b') {
                    expected.typed_signature.clone()
                } else {
                    json!({"class":"unrelated", "owner":"writer"})
                },
                cleanup_restored: !digests.contains(&b'd'),
            }))
        })
        .unwrap();
        assert_eq!(reduced, vec![object('b'), object('d')]);
    }

    #[test]
    fn accumulated_reduction_minimizes_case_and_ordered_history_together() {
        let mut original = crate::corpus::stable_corpus(5, 17)
            .into_iter()
            .find(|case| {
                case.processes
                    .iter()
                    .any(|process| process.actions.len() > 1)
            })
            .unwrap();
        let mut smaller = original.clone();
        let process = smaller
            .processes
            .iter_mut()
            .find(|process| process.actions.len() > 1)
            .unwrap();
        process.actions.pop();
        smaller.validate().unwrap();
        // Make the original strictly more expensive even if its removed action
        // had no payload or allocation charge.
        original.resource_bounds.max_actions =
            original.resource_bounds.max_actions.saturating_add(1);
        let expected = FailureFingerprint {
            typed_signature: json!({"class":"stalled", "owner":"reader"}),
            cleanup_restored: false,
        };
        let prefix = vec![object('a'), object('b'), object('c'), object('d')];
        let reduced = reduce_accumulated_failure(
            &prefix,
            &original,
            [smaller.clone()],
            &expected,
            |candidate_prefix, candidate_case| {
                let history = candidate_prefix
                    .iter()
                    .map(|object| object.digest.as_bytes()[0])
                    .collect::<Vec<_>>();
                Ok((candidate_case == &smaller
                    && history.contains(&b'b')
                    && history.contains(&b'd'))
                .then_some(expected.clone()))
            },
        )
        .unwrap();
        assert_eq!(reduced.prefix, vec![object('b'), object('d')]);
        assert_eq!(reduced.failing_case, smaller);
        assert_eq!(reduced.fingerprint, expected);
    }

    #[test]
    fn failure_evidence_joins_exact_owner_to_latest_observational_transition() {
        let actor = ActorAddress([7; 32]);
        let operation = Some([9; 16]);
        let transition = |previous, next| TransitionRecord {
            actor_type: ActorType::StreamConsumerActor,
            actor,
            protocol: Protocol::StreamConsumer,
            operation,
            generation: Some(11),
            incarnation: Some(3),
            previous,
            next,
            reason: protocol_telemetry::Reason::Requested,
            outcome: None,
        };
        let obligation = UnresolvedProtocolObligation {
            correlation_identity: "read-17".to_owned(),
            operation,
            protocol: Protocol::StreamConsumer,
            owner_kind: ProtocolOwnerKind::Actor,
            owner_identity: actor.to_full_hex(),
            actor: Some(actor),
            actor_type: Some(ActorType::StreamConsumerActor),
            awaited_condition: "terminal read result".to_owned(),
            generation: Some(11),
            incarnation: Some(3),
            owned_work: vec![
                OwnedProtocolWork {
                    kind: OwnedWorkKind::Timer,
                    identity: "deadline".to_owned(),
                    state: "armed".to_owned(),
                },
                OwnedProtocolWork {
                    kind: OwnedWorkKind::Reply,
                    identity: "caller".to_owned(),
                    state: "pending".to_owned(),
                },
            ],
            terminal: TerminalDisposition::NotProduced,
            latest_transition: None,
        };
        let latest = transition(Phase::Open, Phase::Awaiting);
        let evidence = ProtocolFailureEvidence::new(
            vec![obligation],
            &[
                transition(Phase::Created, Phase::Open),
                TransitionRecord {
                    generation: Some(12),
                    ..latest
                },
                latest,
            ],
        )
        .unwrap();
        assert_eq!(evidence.obligations[0].latest_transition, Some(latest));
        assert_eq!(
            evidence.obligations[0]
                .owned_work
                .iter()
                .map(|work| work.kind)
                .collect::<Vec<_>>(),
            vec![OwnedWorkKind::Timer, OwnedWorkKind::Reply],
        );
    }
}
