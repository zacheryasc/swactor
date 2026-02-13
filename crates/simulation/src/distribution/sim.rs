use std::collections::HashSet;
use std::net::SocketAddr;

use distribution::node::{DistributedNode, DistributedNodeConfig, ResolveResult};
use distribution::swim::node::NodeAction;
use distribution::swim::probe::SwimConfig;
use distribution::types::NodeId;
use swactor::actor::ActorAddress;

use crate::trace::{Event, SimulationTrace};

use super::trace::{DistributionEventKind, DistributionSnapshot};

/// A network partition between two sets of nodes.
/// Nodes in `side_a` cannot communicate with nodes in `side_b`.
#[derive(Debug, Clone)]
pub struct Partition {
    pub side_a: Vec<usize>,
    pub side_b: Vec<usize>,
    /// If true, A→B is blocked but B→A works (asymmetric).
    pub asymmetric: bool,
}

/// An action to execute at a specific round during the simulation.
#[derive(Debug, Clone)]
pub enum SimAction {
    /// Register a name on the given node, binding it to a fresh random actor.
    RegisterName { node_idx: usize, name: String },
    /// Register a name on the given node, binding it to a specific actor address.
    RegisterNameWithActor { node_idx: usize, name: String, actor: ActorAddress },
    /// Unregister a name on the given node (creates a tombstone).
    UnregisterName { node_idx: usize, name: String },
    /// Graceful leave — node announces its own death before being removed.
    GracefulLeave { node_idx: usize },
}

/// Schedule entry for network faults.
#[derive(Debug, Clone)]
pub enum NetworkFault {
    /// Introduce a partition at the given round.
    Partition { round: usize, partition: Partition },
    /// Heal a partition at the given round (restores full connectivity).
    Heal { round: usize },
    /// Set message drop rate (0.0 = no drops, 1.0 = drop all).
    SetDropRate { round: usize, rate: f64 },
}

/// Configuration for a distribution simulation run.
#[derive(Debug, Clone)]
pub struct DistributionSimConfig {
    pub name: String,
    pub num_nodes: usize,
    pub num_rounds: usize,
    pub ticks_per_round: usize,
    pub swim: SwimConfig,
    /// Number of actors to register per node.
    pub actors_per_node: usize,
    /// (round, node_idx) — kill the node at the specified round.
    pub kill_schedule: Vec<(usize, usize)>,
    /// (round, node_idx) — revive the node at the specified round.
    pub revive_schedule: Vec<(usize, usize)>,
    pub cache_capacity: usize,
    /// Network fault schedule.
    pub network_faults: Vec<NetworkFault>,
    /// Actions to execute at specific rounds (e.g. register/unregister names).
    pub action_schedule: Vec<(usize, SimAction)>,
    /// Custom registry config overrides.
    pub registry_tombstone_ttl: Option<u64>,
    pub registry_gc_interval: Option<u64>,
    pub registry_dissemination_lambda: Option<usize>,
}

impl Default for DistributionSimConfig {
    fn default() -> Self {
        Self {
            name: "distribution-sim".into(),
            num_nodes: 5,
            num_rounds: 50,
            ticks_per_round: 3,
            swim: SwimConfig {
                probe_interval: 1,
                probe_timeout: 3,
                indirect_probes: 1,
                suspicion_timeout: 5,
                dead_reprobe_interval: 10,
            },
            actors_per_node: 2,
            kill_schedule: Vec::new(),
            revive_schedule: Vec::new(),
            cache_capacity: 100,
            network_faults: Vec::new(),
            action_schedule: Vec::new(),
            registry_tombstone_ttl: None,
            registry_gc_interval: None,
            registry_dissemination_lambda: None,
        }
    }
}

/// Tracks active network state during simulation.
struct NetworkState {
    /// Set of (from_idx, to_idx) pairs where messages are blocked.
    blocked: HashSet<(usize, usize)>,
    /// Probability of dropping a message [0.0, 1.0].
    drop_rate: f64,
    /// Simple counter-based deterministic "random" for drop decisions.
    drop_counter: u64,
}

impl NetworkState {
    fn new() -> Self {
        Self {
            blocked: HashSet::new(),
            drop_rate: 0.0,
            drop_counter: 0x853c49e6748fea9b, // Non-zero seed for better distribution
        }
    }

    fn apply_fault(&mut self, fault: &NetworkFault, num_nodes: usize) {
        match fault {
            NetworkFault::Partition { partition, .. } => {
                for &a in &partition.side_a {
                    for &b in &partition.side_b {
                        if a < num_nodes && b < num_nodes {
                            self.blocked.insert((a, b));
                            if !partition.asymmetric {
                                self.blocked.insert((b, a));
                            }
                        }
                    }
                }
            }
            NetworkFault::Heal { .. } => {
                self.blocked.clear();
            }
            NetworkFault::SetDropRate { rate, .. } => {
                self.drop_rate = rate.clamp(0.0, 1.0);
            }
        }
    }

    /// Returns true if this message should be delivered.
    fn should_deliver(&mut self, from_idx: usize, to_idx: usize) -> bool {
        if self.blocked.contains(&(from_idx, to_idx)) {
            return false;
        }
        if self.drop_rate > 0.0 {
            self.drop_counter = self.drop_counter.wrapping_mul(6364136223846793005).wrapping_add(1);
            let r = (self.drop_counter >> 33) as f64 / (u32::MAX as f64);
            if r < self.drop_rate {
                return false;
            }
        }
        true
    }
}

pub type DistTrace = SimulationTrace<DistributionEventKind, DistributionSnapshot>;

/// Run a distribution simulation, returning both the trace and the final node states.
///
/// The returned `Vec<Option<DistributedNode>>` has the same length as `config.num_nodes`.
/// Dead nodes are `None`.
pub fn run_simulation_with_nodes(config: DistributionSimConfig) -> (DistTrace, Vec<Option<DistributedNode>>) {
    let (trace, nodes, _) = run_simulation_inner(config);
    (trace, nodes)
}

/// Run a distribution simulation.
///
/// Creates N `DistributedNode` instances, forms a cluster via join protocol,
/// registers actors, then runs rounds of tick + deliver + resolve.
pub fn run_simulation(config: DistributionSimConfig) -> DistTrace {
    let (trace, _, _) = run_simulation_inner(config);
    trace
}

fn run_simulation_inner(config: DistributionSimConfig) -> (DistTrace, Vec<Option<DistributedNode>>, Vec<NodeId>) {
    let mut events: Vec<Event<DistributionEventKind>> = Vec::new();
    let mut snapshots_per_round: Vec<Vec<(String, DistributionSnapshot)>> = Vec::new();

    let n = config.num_nodes;
    let node_names: Vec<String> = (0..n).map(|i| format!("node-{i}")).collect();

    // Create nodes with sequential addresses.
    let mut nodes: Vec<Option<DistributedNode>> = Vec::with_capacity(n);
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    let mut node_ids: Vec<NodeId> = Vec::with_capacity(n);

    for i in 0..n {
        let addr: SocketAddr = format!("127.0.0.1:{}", 10001 + i).parse().unwrap();
        let mut node_config = DistributedNodeConfig {
            listen_addr: addr,
            swim: config.swim.clone(),
            cache_capacity: config.cache_capacity,
            republish_interval: 50,
            ..Default::default()
        };
        apply_registry_overrides(&mut node_config, &config);
        let node = DistributedNode::new(node_config);
        node_ids.push(node.node_id());
        addrs.push(addr);
        nodes.push(Some(node));
    }

    // Form cluster: nodes[1..] join via seed (node 0).
    let seed_addr = addrs[0];
    for i in 1..n {
        let join_actions = nodes[i].as_ref().unwrap().join(&[seed_addr]);
        events.push(Event {
            tick: 0,
            node_name: node_names[i].clone(),
            kind: DistributionEventKind::Joined {
                seed_addr: seed_addr.to_string(),
            },
        });

        // Deliver join actions and responses (no network faults during setup).
        let mut clean_net = NetworkState::new();
        let tagged_responses = deliver_actions_tagged_with_net(
            &join_actions,
            i,
            node_ids[i],
            addrs[i],
            &mut nodes,
            &node_ids,
            &addrs,
            &mut clean_net,
        );
        for (responder_idx, response_actions) in tagged_responses {
            deliver_actions_tagged_with_net(
                &response_actions,
                responder_idx,
                node_ids[responder_idx],
                addrs[responder_idx],
                &mut nodes,
                &node_ids,
                &addrs,
                &mut clean_net,
            );
        }
    }

    // Tick-settle: several rounds to let SWIM converge initial membership.
    let mut clean_net = NetworkState::new();
    for _ in 0..10 {
        tick_all_and_deliver(&mut nodes, &node_ids, &addrs, &mut events, &node_names, 0, &mut clean_net);
    }

    // Register actors on each node, then propagate entries.
    let mut actor_registry: Vec<(ActorAddress, usize)> = Vec::new(); // (actor, owning_node_idx)
    let mut pending_entries = Vec::new(); // (entry, owning_node_idx)
    for node_idx in 0..n {
        if let Some(ref mut node) = nodes[node_idx] {
            for a in 0..config.actors_per_node {
                let actor = ActorAddress::new_random();
                let entry = node.register_actor(actor, a as u64 + 1);

                events.push(Event {
                    tick: 0,
                    node_name: node_names[node_idx].clone(),
                    kind: DistributionEventKind::ActorRegistered {
                        actor_id: format!("{:?}", &actor.0[..4]),
                    },
                });

                pending_entries.push((entry, actor, node_idx));
                actor_registry.push((actor, node_idx));
            }
        }
    }

    // Propagate directory entries to all other nodes.
    for (entry, actor, owner_idx) in &pending_entries {
        let actor_id = format!("{:?}", &actor.0[..4]);
        for other_idx in 0..n {
            if other_idx != *owner_idx {
                if let Some(ref mut other_node) = nodes[other_idx] {
                    other_node.store_directory_entry(entry.clone());
                    events.push(Event {
                        tick: 0,
                        node_name: node_names[other_idx].clone(),
                        kind: DistributionEventKind::ActorStored {
                            actor_id: actor_id.clone(),
                            on_node: node_names[*owner_idx].clone(),
                        },
                    });
                }
            }
        }
    }

    // Run simulation rounds.
    let mut rng_buf = [0u8; 8];
    let mut net = NetworkState::new();

    for round in 1..=config.num_rounds {
        // Apply network faults for this round.
        for fault in &config.network_faults {
            let fault_round = match fault {
                NetworkFault::Partition { round, .. } => *round,
                NetworkFault::Heal { round } => *round,
                NetworkFault::SetDropRate { round, .. } => *round,
            };
            if fault_round == round {
                net.apply_fault(fault, n);
            }
        }

        // Apply kill schedule.
        for &(kill_round, kill_idx) in &config.kill_schedule {
            if kill_round == round && kill_idx < n {
                nodes[kill_idx] = None;
                events.push(Event {
                    tick: round as u64,
                    node_name: node_names[kill_idx].clone(),
                    kind: DistributionEventKind::NodeKilled,
                });
            }
        }

        // Apply revive schedule.
        for &(revive_round, revive_idx) in &config.revive_schedule {
            if revive_round == round && revive_idx < n {
                let mut node_config = DistributedNodeConfig {
                    listen_addr: addrs[revive_idx],
                    swim: config.swim.clone(),
                    cache_capacity: config.cache_capacity,
                    republish_interval: 50,
                    ..Default::default()
                };
                apply_registry_overrides(&mut node_config, &config);
                let revived = DistributedNode::new(node_config);
                // Rejoin the cluster.
                let join_actions = revived.join(&[seed_addr]);
                nodes[revive_idx] = Some(revived);
                node_ids[revive_idx] = nodes[revive_idx].as_ref().unwrap().node_id();

                let tagged_responses = deliver_actions_tagged_with_net(
                    &join_actions,
                    revive_idx,
                    node_ids[revive_idx],
                    addrs[revive_idx],
                    &mut nodes,
                    &node_ids,
                    &addrs,
                    &mut net,
                );
                for (responder_idx, response_actions) in tagged_responses {
                    deliver_actions_tagged_with_net(
                        &response_actions,
                        responder_idx,
                        node_ids[responder_idx],
                        addrs[responder_idx],
                        &mut nodes,
                        &node_ids,
                        &addrs,
                        &mut net,
                    );
                }

                events.push(Event {
                    tick: round as u64,
                    node_name: node_names[revive_idx].clone(),
                    kind: DistributionEventKind::NodeRevived,
                });
            }
        }

        // Execute scheduled actions for this round.
        for (action_round, action) in &config.action_schedule {
            if *action_round == round {
                match action {
                    SimAction::RegisterName { node_idx, name } => {
                        if *node_idx < n {
                            if let Some(ref mut node) = nodes[*node_idx] {
                                let actor = ActorAddress::new_random();
                                node.register_name(name.clone(), actor);
                                events.push(Event {
                                    tick: round as u64,
                                    node_name: node_names[*node_idx].clone(),
                                    kind: DistributionEventKind::NameRegistered {
                                        name: name.clone(),
                                        node_idx: *node_idx,
                                    },
                                });
                            }
                        }
                    }
                    SimAction::RegisterNameWithActor { node_idx, name, actor } => {
                        if *node_idx < n {
                            if let Some(ref mut node) = nodes[*node_idx] {
                                node.register_name(name.clone(), *actor);
                                events.push(Event {
                                    tick: round as u64,
                                    node_name: node_names[*node_idx].clone(),
                                    kind: DistributionEventKind::NameRegistered {
                                        name: name.clone(),
                                        node_idx: *node_idx,
                                    },
                                });
                            }
                        }
                    }
                    SimAction::UnregisterName { node_idx, name } => {
                        if *node_idx < n {
                            if let Some(ref mut node) = nodes[*node_idx] {
                                node.unregister_name(name);
                                events.push(Event {
                                    tick: round as u64,
                                    node_name: node_names[*node_idx].clone(),
                                    kind: DistributionEventKind::NameUnregistered {
                                        name: name.clone(),
                                        node_idx: *node_idx,
                                    },
                                });
                            }
                        }
                    }
                    SimAction::GracefulLeave { node_idx } => {
                        if *node_idx < n {
                            if let Some(ref mut node) = nodes[*node_idx] {
                                let leave_actions = node.leave();
                                // Deliver the leave actions (disseminate death announcement)
                                let tagged_responses = deliver_actions_tagged_with_net(
                                    &leave_actions,
                                    *node_idx,
                                    node_ids[*node_idx],
                                    addrs[*node_idx],
                                    &mut nodes,
                                    &node_ids,
                                    &addrs,
                                    &mut net,
                                );
                                for (responder_idx, response_actions) in tagged_responses {
                                    deliver_actions_tagged_with_net(
                                        &response_actions,
                                        responder_idx,
                                        node_ids[responder_idx],
                                        addrs[responder_idx],
                                        &mut nodes,
                                        &node_ids,
                                        &addrs,
                                        &mut net,
                                    );
                                }
                            }
                            // Remove the node after leave
                            nodes[*node_idx] = None;
                            events.push(Event {
                                tick: round as u64,
                                node_name: node_names[*node_idx].clone(),
                                kind: DistributionEventKind::NodeKilled,
                            });
                        }
                    }
                }
            }
        }

        // Tick all live nodes and deliver actions.
        for _ in 0..config.ticks_per_round {
            tick_all_and_deliver(
                &mut nodes,
                &node_ids,
                &addrs,
                &mut events,
                &node_names,
                round as u64,
                &mut net,
            );
        }

        // Resolve actors from random nodes.
        getrandom::getrandom(&mut rng_buf).unwrap();
        let resolver_idx = usize::from_ne_bytes(rng_buf) % n;
        for &(actor, _owner_idx) in &actor_registry {
            if let Some(ref mut resolver) = nodes[resolver_idx] {
                let result = resolver.resolve_actor(&actor);
                let actor_id = format!("{:?}", &actor.0[..4]);
                match result {
                    ResolveResult::Cached(found_on) => {
                        events.push(Event {
                            tick: round as u64,
                            node_name: node_names[resolver_idx].clone(),
                            kind: DistributionEventKind::ActorResolved {
                                actor_id,
                                found_on: format!("{found_on:?}"),
                            },
                        });
                    }
                    ResolveResult::NeedsLookup { .. } => {
                        events.push(Event {
                            tick: round as u64,
                            node_name: node_names[resolver_idx].clone(),
                            kind: DistributionEventKind::ActorResolveFailed {
                                actor_id,
                                reason: "needs_lookup".into(),
                            },
                        });
                    }
                    ResolveResult::NotFound => {
                        events.push(Event {
                            tick: round as u64,
                            node_name: node_names[resolver_idx].clone(),
                            kind: DistributionEventKind::ActorResolveFailed {
                                actor_id,
                                reason: "not_found".into(),
                            },
                        });
                    }
                }
            }
        }

        // Snapshot all nodes.
        let mut round_snapshots = Vec::new();
        for (idx, maybe_node) in nodes.iter_mut().enumerate() {
            let snap = match maybe_node {
                Some(node) => DistributionSnapshot {
                    member_count: node.members().len(),
                    routing_table_size: node.routing_table().len(),
                    directory_entry_count: node.directory().entry_count(),
                    cache_size: node.cache().len(),
                    repair_queue_size: node.repair_queue().len(),
                    registry_size: node.registry().len(),
                    registry_tombstone_count: node.registry().tombstone_count(),
                    is_alive: true,
                },
                None => DistributionSnapshot {
                    member_count: 0,
                    routing_table_size: 0,
                    directory_entry_count: 0,
                    cache_size: 0,
                    repair_queue_size: 0,
                    registry_size: 0,
                    registry_tombstone_count: 0,
                    is_alive: false,
                },
            };
            round_snapshots.push((node_names[idx].clone(), snap));
        }
        snapshots_per_round.push(round_snapshots);
    }

    // Build topology edges (all-to-seed for the join topology).
    let topology_edges: Vec<(String, String)> = (1..n)
        .map(|i| (node_names[i].clone(), node_names[0].clone()))
        .collect();

    let trace = SimulationTrace {
        name: config.name,
        trace_type: "distribution".into(),
        node_names,
        topology_edges,
        events,
        snapshots_per_round,
        num_rounds: config.num_rounds,
    };
    (trace, nodes, node_ids)
}

fn apply_registry_overrides(node_config: &mut DistributedNodeConfig, config: &DistributionSimConfig) {
    if let Some(ttl) = config.registry_tombstone_ttl {
        node_config.registry.tombstone_ttl = ttl;
    }
    if let Some(interval) = config.registry_gc_interval {
        node_config.registry.gc_interval = interval;
    }
    if let Some(lambda) = config.registry_dissemination_lambda {
        node_config.registry.dissemination_lambda = lambda;
    }
}

/// Tick all live nodes and deliver their actions to other nodes.
fn tick_all_and_deliver(
    nodes: &mut [Option<DistributedNode>],
    node_ids: &[NodeId],
    addrs: &[SocketAddr],
    events: &mut Vec<Event<DistributionEventKind>>,
    node_names: &[String],
    tick: u64,
    net: &mut NetworkState,
) {
    let n = nodes.len();

    // Collect tick actions from all live nodes.
    let mut all_actions: Vec<(usize, Vec<NodeAction>)> = Vec::new();
    for idx in 0..n {
        if let Some(ref mut node) = nodes[idx] {
            let actions = node.tick();

            // Record membership changes as events.
            for action in &actions {
                if let NodeAction::MembershipChanged { node_id, state, .. } = action {
                    events.push(Event {
                        tick,
                        node_name: node_names[idx].clone(),
                        kind: DistributionEventKind::MembershipChanged {
                            target: format!("{node_id:?}"),
                            new_state: format!("{state:?}"),
                        },
                    });
                }
            }

            if !actions.is_empty() {
                all_actions.push((idx, actions));
            }
        }
    }

    // Deliver all actions and collect responses.
    for (sender_idx, actions) in all_actions {
        let tagged_responses = deliver_actions_tagged_with_net(
            &actions,
            sender_idx,
            node_ids[sender_idx],
            addrs[sender_idx],
            nodes,
            node_ids,
            addrs,
            net,
        );
        // Deliver responses back, using the actual responder's identity.
        for (responder_idx, response_actions) in tagged_responses {
            deliver_actions_tagged_with_net(
                &response_actions,
                responder_idx,
                node_ids[responder_idx],
                addrs[responder_idx],
                nodes,
                node_ids,
                addrs,
                net,
            );
        }
    }
}

/// Deliver actions to the appropriate target nodes, respecting network conditions.
/// Returns responses tagged with the index of the responding node.
/// `None` nodes (killed) silently drop actions — simulates network loss.
fn deliver_actions_tagged_with_net(
    actions: &[NodeAction],
    sender_idx: usize,
    sender_id: NodeId,
    sender_addr: SocketAddr,
    nodes: &mut [Option<DistributedNode>],
    node_ids: &[NodeId],
    node_addrs: &[SocketAddr],
    net: &mut NetworkState,
) -> Vec<(usize, Vec<NodeAction>)> {
    let mut tagged_responses: Vec<(usize, Vec<NodeAction>)> = Vec::new();

    for action in actions {
        match action {
            NodeAction::SendPing {
                to,
                sequence,
                piggyback,
                ..
            } => {
                if let Some(idx) = node_ids.iter().position(|id| id == to) {
                    if net.should_deliver(sender_idx, idx) {
                        if let Some(ref mut node) = nodes[idx] {
                            let resp =
                                node.handle_ping(sender_id, sender_addr, *sequence, piggyback);
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
                    }
                }
            }
            NodeAction::SendAck {
                to,
                sequence,
                piggyback,
                ..
            } => {
                if let Some(idx) = node_ids.iter().position(|id| id == to) {
                    if net.should_deliver(sender_idx, idx) {
                        if let Some(ref mut node) = nodes[idx] {
                            let resp = node.handle_ack(sender_id, *sequence, piggyback);
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
                    }
                }
            }
            NodeAction::SendJoinRequest { to_addr } => {
                if let Some(idx) = node_addrs.iter().position(|a| a == to_addr) {
                    if net.should_deliver(sender_idx, idx) {
                        if let Some(ref mut node) = nodes[idx] {
                            let resp = node.handle_join_request(sender_id, sender_addr);
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
                    }
                }
            }
            NodeAction::SendJoinResponse { to, members, .. } => {
                if let Some(idx) = node_ids.iter().position(|id| id == to) {
                    if net.should_deliver(sender_idx, idx) {
                        if let Some(ref mut node) = nodes[idx] {
                            let resp = node.handle_join_response(members.clone());
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
                    }
                }
            }
            NodeAction::SendPingReq {
                relay,
                target,
                target_addr,
                sequence,
                piggyback,
                ..
            } => {
                if let Some(idx) = node_ids.iter().position(|id| id == relay) {
                    if net.should_deliver(sender_idx, idx) {
                        if let Some(ref mut node) = nodes[idx] {
                            let resp = node.handle_ping_req(
                                sender_id,
                                *target,
                                *target_addr,
                                *sequence,
                                piggyback,
                            );
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
                    }
                }
            }
            NodeAction::MembershipChanged { .. } => {
                // Notifications — no delivery needed
            }
        }
    }

    tagged_responses
}
