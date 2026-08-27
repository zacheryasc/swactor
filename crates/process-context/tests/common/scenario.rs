use std::collections::{BTreeMap, BTreeSet};

use swactor_process::ProcessOutput;

use swactor_process_context::{Event, EventKind, ExecutionIdentity};

pub type ScenarioNodeId = u32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScenarioDag {
    nodes: BTreeMap<ScenarioNodeId, Event>,
    edges: BTreeSet<(ScenarioNodeId, ScenarioNodeId)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScenarioError {
    DuplicateNode(ScenarioNodeId),
    UnknownNode(ScenarioNodeId),
    SelfEdge(ScenarioNodeId),
    Cycle,
    ContradictoryOutcomes(ExecutionIdentity),
    MissingCausalEdge {
        before: ScenarioNodeId,
        after: ScenarioNodeId,
    },
}

impl ScenarioDag {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            edges: BTreeSet::new(),
        }
    }

    pub fn add_node(&mut self, id: ScenarioNodeId, event: Event) -> Result<(), ScenarioError> {
        if self.nodes.insert(id, event).is_some() {
            return Err(ScenarioError::DuplicateNode(id));
        }
        Ok(())
    }

    pub fn add_edge(
        &mut self,
        before: ScenarioNodeId,
        after: ScenarioNodeId,
    ) -> Result<(), ScenarioError> {
        if before == after {
            return Err(ScenarioError::SelfEdge(before));
        }
        if !self.nodes.contains_key(&before) {
            return Err(ScenarioError::UnknownNode(before));
        }
        if !self.nodes.contains_key(&after) {
            return Err(ScenarioError::UnknownNode(after));
        }
        self.edges.insert((before, after));
        if self.has_cycle() {
            self.edges.remove(&(before, after));
            return Err(ScenarioError::Cycle);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ScenarioError> {
        if self.has_cycle() {
            return Err(ScenarioError::Cycle);
        }
        for identity in self.identities() {
            self.validate_outcomes(identity)?;
            self.validate_causality(identity)?;
        }
        Ok(())
    }

    pub fn linearizations(&self, limit: usize) -> Result<Vec<Vec<Event>>, ScenarioError> {
        self.validate()?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut indegree = self
            .nodes
            .keys()
            .map(|id| (*id, 0_usize))
            .collect::<BTreeMap<_, _>>();
        for (_, after) in &self.edges {
            *indegree.get_mut(after).expect("validated edge target") += 1;
        }
        let mut orders = Vec::new();
        let mut current = Vec::with_capacity(self.nodes.len());
        if self.nodes.len() <= 10 {
            self.enumerate(&mut indegree, &mut current, &mut orders, limit, false);
        } else {
            let ascending_limit = limit.div_ceil(2);
            self.enumerate(
                &mut indegree,
                &mut current,
                &mut orders,
                ascending_limit,
                false,
            );
            let mut descending = Vec::new();
            self.enumerate(&mut indegree, &mut current, &mut descending, limit, true);
            for order in descending {
                if orders.len() == limit {
                    break;
                }
                if !orders.contains(&order) {
                    orders.push(order);
                }
            }
        }
        Ok(orders)
    }

    fn enumerate(
        &self,
        indegree: &mut BTreeMap<ScenarioNodeId, usize>,
        current: &mut Vec<ScenarioNodeId>,
        orders: &mut Vec<Vec<Event>>,
        limit: usize,
        reverse: bool,
    ) {
        if orders.len() >= limit {
            return;
        }
        if current.len() == self.nodes.len() {
            orders.push(
                current
                    .iter()
                    .map(|id| self.nodes.get(id).expect("linearized node").clone())
                    .collect(),
            );
            return;
        }
        let mut available = indegree
            .iter()
            .filter_map(|(id, degree)| (*degree == 0 && !current.contains(id)).then_some(*id))
            .collect::<Vec<_>>();
        if reverse {
            available.reverse();
        }
        for id in available {
            current.push(id);
            let successors = self
                .edges
                .iter()
                .filter_map(|(before, after)| (*before == id).then_some(*after))
                .collect::<Vec<_>>();
            for successor in &successors {
                *indegree.get_mut(successor).expect("successor indegree") -= 1;
            }
            self.enumerate(indegree, current, orders, limit, reverse);
            for successor in successors {
                *indegree.get_mut(&successor).expect("successor indegree") += 1;
            }
            current.pop();
            if orders.len() >= limit {
                return;
            }
        }
    }

    fn identities(&self) -> BTreeSet<ExecutionIdentity> {
        self.nodes.values().map(|event| event.identity).collect()
    }

    fn validate_outcomes(&self, identity: ExecutionIdentity) -> Result<(), ScenarioError> {
        let events = self
            .nodes
            .values()
            .filter(|event| event.identity == identity)
            .map(|event| &event.kind)
            .collect::<Vec<_>>();
        let provision_success = events
            .iter()
            .any(|event| matches!(event, EventKind::ProvisionSucceeded));
        let provision_failure = events
            .iter()
            .any(|event| matches!(event, EventKind::ProvisionFailed(_)));
        let process_started = events
            .iter()
            .any(|event| matches!(event, EventKind::Process(ProcessOutput::Started { .. })));
        let spawn_failed = events
            .iter()
            .any(|event| matches!(event, EventKind::Process(ProcessOutput::SpawnFailed { .. })));
        let attachment_success = events
            .iter()
            .any(|event| matches!(event, EventKind::AttachmentSucceeded));
        let attachment_failure = events
            .iter()
            .any(|event| matches!(event, EventKind::AttachmentFailed(_)));
        if (provision_success && provision_failure)
            || (process_started && spawn_failed)
            || (attachment_success && attachment_failure)
        {
            return Err(ScenarioError::ContradictoryOutcomes(identity));
        }
        Ok(())
    }

    fn validate_causality(&self, identity: ExecutionIdentity) -> Result<(), ScenarioError> {
        let node = |predicate: &dyn Fn(&EventKind) -> bool| {
            self.nodes.iter().find_map(|(id, event)| {
                (event.identity == identity && predicate(&event.kind)).then_some(*id)
            })
        };
        let spawn = node(&|event| matches!(event, EventKind::SpawnRequested));
        let provision = node(&|event| {
            matches!(
                event,
                EventKind::ProvisionSucceeded | EventKind::ProvisionFailed(_)
            )
        });
        if let (Some(before), Some(after)) = (spawn, provision) {
            self.require_path(before, after)?;
        }
        let started =
            node(&|event| matches!(event, EventKind::Process(ProcessOutput::Started { .. })));
        if let Some(before) = provision {
            for (after, event) in &self.nodes {
                if event.identity == identity
                    && matches!(
                        event.kind,
                        EventKind::Process(
                            ProcessOutput::Started { .. } | ProcessOutput::SpawnFailed { .. }
                        )
                    )
                {
                    self.require_path(before, *after)?;
                }
            }
        }
        let claim = node(&|event| matches!(event, EventKind::BootstrapClaimed { .. }));
        if let Some(before) = started {
            for (after, event) in &self.nodes {
                if event.identity == identity
                    && matches!(
                        event.kind,
                        EventKind::BootstrapClaimed { .. }
                            | EventKind::AttachmentDeadline
                            | EventKind::Process(ProcessOutput::Exited { .. })
                    )
                {
                    self.require_path(before, *after)?;
                }
            }
        }
        if let Some(before) = claim {
            for (after, event) in &self.nodes {
                if event.identity == identity
                    && matches!(
                        event.kind,
                        EventKind::AttachmentSucceeded | EventKind::AttachmentFailed(_)
                    )
                {
                    self.require_path(before, *after)?;
                }
            }
        }
        Ok(())
    }

    fn require_path(
        &self,
        before: ScenarioNodeId,
        after: ScenarioNodeId,
    ) -> Result<(), ScenarioError> {
        if self.reachable(before, after) {
            Ok(())
        } else {
            Err(ScenarioError::MissingCausalEdge { before, after })
        }
    }

    fn reachable(&self, before: ScenarioNodeId, after: ScenarioNodeId) -> bool {
        let mut frontier = vec![before];
        let mut seen = BTreeSet::new();
        while let Some(current) = frontier.pop() {
            if current == after {
                return true;
            }
            if !seen.insert(current) {
                continue;
            }
            frontier.extend(
                self.edges
                    .iter()
                    .filter_map(|(source, target)| (*source == current).then_some(*target)),
            );
        }
        false
    }

    fn has_cycle(&self) -> bool {
        self.nodes.keys().any(|id| {
            self.edges
                .iter()
                .filter(|(source, _)| source == id)
                .any(|(_, target)| self.reachable(*target, *id))
        })
    }
}

impl Default for ScenarioDag {
    fn default() -> Self {
        Self::new()
    }
}
