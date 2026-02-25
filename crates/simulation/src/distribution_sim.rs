use distribution::node::{DistributedNode, DistributedNodeConfig, ResolveResult};
use distribution::swim::node::NodeAction;
use distribution::swim::probe::SwimConfig;
use distribution::types::NodeId;
use swactor::actor::ActorAddress;

use crate::runner::NetworkState;
pub use crate::runner::{NetworkFault, NetworkTopology, NodeLocation, Partition};
use crate::{Event, SimulationTrace};

use super::trace::{DistributionEventKind, DistributionSnapshot};

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
    /// Mid-simulation join: node_idx sends a join request to seed_idx.
    Join { node_idx: usize, seed_idx: usize },
    /// Bidirectional introduction (models POST /api/peers/add from deploy script).
    Introduce { node_a: usize, node_b: usize },
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
    /// Network topology for NAT/firewall simulation. None = full connectivity.
    pub topology: Option<NetworkTopology>,
    /// Node indices that skip the initial join phase (must be joined via SimAction).
    pub deferred_join: Vec<usize>,
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
                probe_mode: distribution::swim::probe::ProbeMode::Periodic,
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
            topology: None,
            deferred_join: Vec::new(),
        }
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

    // Create nodes.
    let mut nodes: Vec<Option<DistributedNode>> = Vec::with_capacity(n);
    let mut node_ids: Vec<NodeId> = Vec::with_capacity(n);

    for _i in 0..n {
        let mut node_config = DistributedNodeConfig {
            swim: config.swim.clone(),
            cache_capacity: config.cache_capacity,
            republish_interval: 50,
            ..Default::default()
        };
        apply_registry_overrides(&mut node_config, &config);
        let node = DistributedNode::new(node_config);
        node_ids.push(node.node_id());
        nodes.push(Some(node));
    }

    // Form cluster: nodes[1..] join via seed (node 0), skipping deferred nodes.
    let seed_id = node_ids[0];
    for i in 1..n {
        if config.deferred_join.contains(&i) {
            continue;
        }
        // Seed handles join request from node i
        let join_actions = nodes[0].as_mut().unwrap().handle_join_request(node_ids[i]);
        events.push(Event {
            tick: 0,
            node_name: node_names[i].clone(),
            kind: DistributionEventKind::Joined {
                seed_addr: format!("node-0 ({seed_id:?})"),
            },
        });

        // Deliver join response to node i (no network faults during setup).
        let mut clean_net = NetworkState::new();
        let tagged_responses = deliver_actions_tagged_with_net(
            &join_actions,
            0,
            seed_id,
            &mut nodes,
            &node_ids,
            &mut clean_net,
        );
        for (responder_idx, response_actions) in tagged_responses {
            deliver_actions_tagged_with_net(
                &response_actions,
                responder_idx,
                node_ids[responder_idx],
                &mut nodes,
                &node_ids,
                &mut clean_net,
            );
        }
    }

    // Tick-settle: several rounds to let SWIM converge initial membership.
    let mut clean_net = NetworkState::new();
    for _ in 0..10 {
        tick_all_and_deliver(&mut nodes, &node_ids, &mut events, &node_names, 0, &mut clean_net);
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
            if other_idx != *owner_idx
                && let Some(ref mut other_node) = nodes[other_idx] {
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

    // Run simulation rounds.
    let mut rng_buf = [0u8; 8];
    let mut net = NetworkState::new_with_topology(config.topology.clone(), n);

    for round in 1..=config.num_rounds {
        // Apply network faults for this round.
        for fault in &config.network_faults {
            if fault.round() == round {
                net.apply_fault(fault, n);
            }
        }

        // Apply kill schedule.
        for &(kill_round, kill_idx) in &config.kill_schedule {
            if kill_round == round && kill_idx < n {
                nodes[kill_idx] = None;
                net.set_alive(kill_idx, false);
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
                    swim: config.swim.clone(),
                    cache_capacity: config.cache_capacity,
                    republish_interval: 50,
                    ..Default::default()
                };
                apply_registry_overrides(&mut node_config, &config);
                let revived = DistributedNode::new(node_config);
                node_ids[revive_idx] = revived.node_id();
                nodes[revive_idx] = Some(revived);
                net.set_alive(revive_idx, true);

                // Rejoin the cluster via seed.
                let join_actions = nodes[0].as_mut().unwrap().handle_join_request(node_ids[revive_idx]);

                let tagged_responses = deliver_actions_tagged_with_net(
                    &join_actions,
                    0,
                    node_ids[0],
                    &mut nodes,
                    &node_ids,
                    &mut net,
                );
                for (responder_idx, response_actions) in tagged_responses {
                    deliver_actions_tagged_with_net(
                        &response_actions,
                        responder_idx,
                        node_ids[responder_idx],
                        &mut nodes,
                        &node_ids,
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
                        if *node_idx < n
                            && let Some(ref mut node) = nodes[*node_idx] {
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
                    SimAction::RegisterNameWithActor { node_idx, name, actor } => {
                        if *node_idx < n
                            && let Some(ref mut node) = nodes[*node_idx] {
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
                    SimAction::UnregisterName { node_idx, name } => {
                        if *node_idx < n
                            && let Some(ref mut node) = nodes[*node_idx] {
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
                    SimAction::GracefulLeave { node_idx } => {
                        if *node_idx < n {
                            if let Some(ref mut node) = nodes[*node_idx] {
                                let leave_actions = node.leave();
                                let tagged_responses = deliver_actions_tagged_with_net(
                                    &leave_actions,
                                    *node_idx,
                                    node_ids[*node_idx],
                                    &mut nodes,
                                    &node_ids,
                                    &mut net,
                                );
                                for (responder_idx, response_actions) in tagged_responses {
                                    deliver_actions_tagged_with_net(
                                        &response_actions,
                                        responder_idx,
                                        node_ids[responder_idx],
                                        &mut nodes,
                                        &node_ids,
                                        &mut net,
                                    );
                                }
                            }
                            nodes[*node_idx] = None;
                            events.push(Event {
                                tick: round as u64,
                                node_name: node_names[*node_idx].clone(),
                                kind: DistributionEventKind::NodeKilled,
                            });
                        }
                    }
                    SimAction::Join { node_idx, seed_idx } => {
                        if *node_idx < n && *seed_idx < n {
                            // Mirror IrohDriver::join(): clear dead state before re-peering
                            // so stale Dead gossip doesn't leak from the dissemination queue.
                            if let Some(ref mut joining_node) = nodes[*node_idx] {
                                joining_node.clear_dead_member(node_ids[*seed_idx]);
                            }
                            if let Some(ref mut seed_node) = nodes[*seed_idx] {
                                let join_actions = seed_node.handle_join_request(node_ids[*node_idx]);
                                let tagged_responses = deliver_actions_tagged_with_net(
                                    &join_actions,
                                    *seed_idx,
                                    node_ids[*seed_idx],
                                    &mut nodes,
                                    &node_ids,
                                    &mut net,
                                );
                                for (responder_idx, response_actions) in tagged_responses {
                                    deliver_actions_tagged_with_net(
                                        &response_actions,
                                        responder_idx,
                                        node_ids[responder_idx],
                                        &mut nodes,
                                        &node_ids,
                                        &mut net,
                                    );
                                }
                            }
                            events.push(Event {
                                tick: round as u64,
                                node_name: node_names[*node_idx].clone(),
                                kind: DistributionEventKind::MidSimJoin {
                                    node_idx: *node_idx,
                                    seed_idx: *seed_idx,
                                },
                            });
                        }
                    }
                    SimAction::Introduce { node_a, node_b } => {
                        if *node_a < n && *node_b < n {
                            // A introduces itself to B
                            if let Some(ref mut b_node) = nodes[*node_b] {
                                let join_actions = b_node.handle_join_request(node_ids[*node_a]);
                                let tagged_responses = deliver_actions_tagged_with_net(
                                    &join_actions,
                                    *node_b,
                                    node_ids[*node_b],
                                    &mut nodes,
                                    &node_ids,
                                    &mut net,
                                );
                                for (responder_idx, response_actions) in tagged_responses {
                                    deliver_actions_tagged_with_net(
                                        &response_actions,
                                        responder_idx,
                                        node_ids[responder_idx],
                                        &mut nodes,
                                        &node_ids,
                                        &mut net,
                                    );
                                }
                            }
                            // B introduces itself to A
                            if let Some(ref mut a_node) = nodes[*node_a] {
                                let join_actions = a_node.handle_join_request(node_ids[*node_b]);
                                let tagged_responses = deliver_actions_tagged_with_net(
                                    &join_actions,
                                    *node_a,
                                    node_ids[*node_a],
                                    &mut nodes,
                                    &node_ids,
                                    &mut net,
                                );
                                for (responder_idx, response_actions) in tagged_responses {
                                    deliver_actions_tagged_with_net(
                                        &response_actions,
                                        responder_idx,
                                        node_ids[responder_idx],
                                        &mut nodes,
                                        &node_ids,
                                        &mut net,
                                    );
                                }
                            }
                            events.push(Event {
                                tick: round as u64,
                                node_name: node_names[*node_a].clone(),
                                kind: DistributionEventKind::PeerIntroduced {
                                    node_a: *node_a,
                                    node_b: *node_b,
                                },
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
            nodes,
            node_ids,
            net,
        );
        // Deliver responses back, using the actual responder's identity.
        for (responder_idx, response_actions) in tagged_responses {
            deliver_actions_tagged_with_net(
                &response_actions,
                responder_idx,
                node_ids[responder_idx],
                nodes,
                node_ids,
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
    nodes: &mut [Option<DistributedNode>],
    node_ids: &[NodeId],
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
                if let Some(idx) = node_ids.iter().position(|id| id == to)
                    && net.should_deliver(sender_idx, idx)
                        && let Some(ref mut node) = nodes[idx] {
                            let resp =
                                node.handle_ping(sender_id, *sequence, piggyback);
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
            }
            NodeAction::SendAck {
                to,
                sequence,
                piggyback,
                ..
            } => {
                if let Some(idx) = node_ids.iter().position(|id| id == to)
                    && net.should_deliver(sender_idx, idx)
                        && let Some(ref mut node) = nodes[idx] {
                            let resp = node.handle_ack(sender_id, *sequence, piggyback);
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
            }
            NodeAction::SendJoinResponse { to, members, .. } => {
                if let Some(idx) = node_ids.iter().position(|id| id == to)
                    && net.should_deliver(sender_idx, idx)
                        && let Some(ref mut node) = nodes[idx] {
                            let resp = node.handle_join_response(members.clone());
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
            }
            NodeAction::SendPingReq {
                relay,
                target,
                sequence,
                piggyback,
                ..
            } => {
                if let Some(idx) = node_ids.iter().position(|id| id == relay)
                    && net.should_deliver(sender_idx, idx)
                        && let Some(ref mut node) = nodes[idx] {
                            let resp = node.handle_ping_req(
                                sender_id,
                                *target,
                                *sequence,
                                piggyback,
                            );
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
                            }
                        }
            }
            NodeAction::ForwardAck { to, target, sequence, piggyback } => {
                if let Some(idx) = node_ids.iter().position(|id| id == to)
                    && net.should_deliver(sender_idx, idx)
                        && let Some(ref mut node) = nodes[idx] {
                            let resp = node.handle_indirect_ack(*target, *sequence, piggyback);
                            if !resp.is_empty() {
                                tagged_responses.push((idx, resp));
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
