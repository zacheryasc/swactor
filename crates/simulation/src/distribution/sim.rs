use std::net::SocketAddr;

use distribution::node::{DistributedNode, DistributedNodeConfig, ResolveResult};
use distribution::swim::node::NodeAction;
use distribution::swim::probe::SwimConfig;
use distribution::types::NodeId;
use swactor::actor::ActorAddress;

use crate::trace::{Event, SimulationTrace};

use super::trace::{DistributionEventKind, DistributionSnapshot};

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
            },
            actors_per_node: 2,
            kill_schedule: Vec::new(),
            revive_schedule: Vec::new(),
            cache_capacity: 100,
        }
    }
}

type DistTrace = SimulationTrace<DistributionEventKind, DistributionSnapshot>;

/// Run a distribution simulation.
///
/// Creates N `DistributedNode` instances, forms a cluster via join protocol,
/// registers actors, then runs rounds of tick + deliver + resolve.
pub fn run_simulation(config: DistributionSimConfig) -> DistTrace {
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
        let node_config = DistributedNodeConfig {
            listen_addr: addr,
            swim: config.swim.clone(),
            cache_capacity: config.cache_capacity,
            republish_interval: 50,
        };
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

        // Deliver join actions and responses.
        let tagged_responses = deliver_actions_tagged(
            &join_actions,
            node_ids[i],
            addrs[i],
            &mut nodes,
            &node_ids,
            &addrs,
        );
        for (responder_idx, response_actions) in tagged_responses {
            deliver_actions_tagged(
                &response_actions,
                node_ids[responder_idx],
                addrs[responder_idx],
                &mut nodes,
                &node_ids,
                &addrs,
            );
        }
    }

    // Tick-settle: several rounds to let SWIM converge initial membership.
    for _ in 0..10 {
        tick_all_and_deliver(&mut nodes, &node_ids, &addrs, &mut events, &node_names, 0);
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
    for round in 1..=config.num_rounds {
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
                let node_config = DistributedNodeConfig {
                    listen_addr: addrs[revive_idx],
                    swim: config.swim.clone(),
                    cache_capacity: config.cache_capacity,
                    republish_interval: 50,
                };
                let revived = DistributedNode::new(node_config);
                // Rejoin the cluster.
                let join_actions = revived.join(&[seed_addr]);
                nodes[revive_idx] = Some(revived);
                node_ids[revive_idx] = nodes[revive_idx].as_ref().unwrap().node_id();

                let tagged_responses = deliver_actions_tagged(
                    &join_actions,
                    node_ids[revive_idx],
                    addrs[revive_idx],
                    &mut nodes,
                    &node_ids,
                    &addrs,
                );
                for (responder_idx, response_actions) in tagged_responses {
                    deliver_actions_tagged(
                        &response_actions,
                        node_ids[responder_idx],
                        addrs[responder_idx],
                        &mut nodes,
                        &node_ids,
                        &addrs,
                    );
                }

                events.push(Event {
                    tick: round as u64,
                    node_name: node_names[revive_idx].clone(),
                    kind: DistributionEventKind::NodeRevived,
                });
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
                    is_alive: true,
                },
                None => DistributionSnapshot {
                    member_count: 0,
                    routing_table_size: 0,
                    directory_entry_count: 0,
                    cache_size: 0,
                    repair_queue_size: 0,
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

    SimulationTrace {
        name: config.name,
        node_names,
        topology_edges,
        events,
        snapshots_per_round,
        num_rounds: config.num_rounds,
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
        let tagged_responses = deliver_actions_tagged(
            &actions,
            node_ids[sender_idx],
            addrs[sender_idx],
            nodes,
            node_ids,
            addrs,
        );
        // Deliver responses back, using the actual responder's identity.
        for (responder_idx, response_actions) in tagged_responses {
            deliver_actions_tagged(
                &response_actions,
                node_ids[responder_idx],
                addrs[responder_idx],
                nodes,
                node_ids,
                addrs,
            );
        }
    }
}

/// Deliver actions to the appropriate target nodes.
/// Returns responses tagged with the index of the responding node.
/// `None` nodes (killed) silently drop actions — simulates network loss.
fn deliver_actions_tagged(
    actions: &[NodeAction],
    sender_id: NodeId,
    sender_addr: SocketAddr,
    nodes: &mut [Option<DistributedNode>],
    node_ids: &[NodeId],
    node_addrs: &[SocketAddr],
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
                    if let Some(ref mut node) = nodes[idx] {
                        let resp =
                            node.handle_ping(sender_id, sender_addr, *sequence, piggyback);
                        if !resp.is_empty() {
                            tagged_responses.push((idx, resp));
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
                    if let Some(ref mut node) = nodes[idx] {
                        let resp = node.handle_ack(sender_id, *sequence, piggyback);
                        if !resp.is_empty() {
                            tagged_responses.push((idx, resp));
                        }
                    }
                }
            }
            NodeAction::SendJoinRequest { to_addr } => {
                if let Some(idx) = node_addrs.iter().position(|a| a == to_addr) {
                    if let Some(ref mut node) = nodes[idx] {
                        let resp = node.handle_join_request(sender_id, sender_addr);
                        if !resp.is_empty() {
                            tagged_responses.push((idx, resp));
                        }
                    }
                }
            }
            NodeAction::SendJoinResponse { to, members, .. } => {
                if let Some(idx) = node_ids.iter().position(|id| id == to) {
                    if let Some(ref mut node) = nodes[idx] {
                        let resp = node.handle_join_response(members.clone());
                        if !resp.is_empty() {
                            tagged_responses.push((idx, resp));
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
            NodeAction::MembershipChanged { .. } => {
                // Notifications — no delivery needed
            }
        }
    }

    tagged_responses
}
