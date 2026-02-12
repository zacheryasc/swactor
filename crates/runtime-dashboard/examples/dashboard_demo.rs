use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use distribution::node::{DistributedNode, DistributedNodeConfig, ResolveResult};
use distribution::snapshot::DistributionNodeSnapshot;
use distribution::swim::node::NodeAction;
use distribution::swim::probe::SwimConfig;
use distribution::types::NodeId;

use runtime_dashboard::collector::StatsCollector;
use runtime_dashboard::distribution_collector::DistributionStatsProvider;
use runtime_dashboard::{start_dashboard, DashboardConfig};

// ── Demo actors ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct Ping(ActorAddress);

struct PingActor {
    count: u32,
}

impl PingActor {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl ActorInterface for PingActor {
    type Incoming = Ping;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.count += 1;
        // Forward to the target — creates cross-worker traffic
        if self.count < 10_000 {
            let _ = ctx.send(msg.0, Ping(ctx.self_addr()));
        }
    }
}

#[derive(Clone)]
struct Tick;

struct CounterActor {
    ticks: u64,
}

impl CounterActor {
    fn new() -> Self {
        Self { ticks: 0 }
    }
}

impl ActorInterface for CounterActor {
    type Incoming = Tick;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Tick) {
        self.ticks += 1;
    }
}

// ── Snapshot provider ───────────────────────────────────────────────────

struct SnapshotProvider {
    snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>>,
}

impl DistributionStatsProvider for SnapshotProvider {
    fn snapshot(&self) -> Option<DistributionNodeSnapshot> {
        self.snapshot.lock().unwrap().clone()
    }
}

// ── In-process action delivery ──────────────────────────────────────────

/// Tick all live nodes and deliver their actions to other nodes.
fn tick_all_and_deliver(
    nodes: &mut [Option<DistributedNode>],
    node_ids: &[NodeId],
    addrs: &[SocketAddr],
) {
    let n = nodes.len();

    // Collect tick actions from all live nodes.
    let mut all_actions: Vec<(usize, Vec<NodeAction>)> = Vec::new();
    for idx in 0..n {
        if let Some(ref mut node) = nodes[idx] {
            let actions = node.tick();
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

// ── Main ────────────────────────────────────────────────────────────────

fn main() {
    let stop = Arc::new(AtomicBool::new(false));

    // Handle Ctrl+C gracefully
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        })
        .expect("failed to set Ctrl+C handler");
    }

    let dash = start_dashboard(DashboardConfig {
        port: 9090,
        ..Default::default()
    });
    dash.install_tracing();

    let num_threads = 4;
    let collector = StatsCollector::new(num_threads);

    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    });
    rt.set_stats_hook(collector.clone());

    // Spawn ping actors for cross-worker traffic
    let mut ping_addrs = Vec::new();
    for _ in 0..16 {
        let addr = rt.spawn(PingActor::new()).unwrap();
        ping_addrs.push(addr);
    }

    // Spawn counter actors for sustained traffic
    let mut counter_addrs = Vec::new();
    for _ in 0..20 {
        let addr = rt.spawn(CounterActor::new()).unwrap();
        counter_addrs.push(addr);
    }

    let handle = rt.run().expect("failed to start runtime");
    dash.set_runtime(handle.runtime.clone(), collector);

    // ── Distribution cluster ────────────────────────────────────────────

    let swim_config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 2,
        indirect_probes: 2,
        suspicion_timeout: 20,
    };

    let num_nodes = 9; // 1 main + 8 peers
    let mut nodes: Vec<Option<DistributedNode>> = Vec::with_capacity(num_nodes);
    let mut node_ids: Vec<NodeId> = Vec::with_capacity(num_nodes);
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(num_nodes);

    for i in 0..num_nodes {
        let addr: SocketAddr = format!("127.0.0.1:{}", 7000 + i).parse().unwrap();
        let config = DistributedNodeConfig {
            listen_addr: addr,
            swim: swim_config.clone(),
            cache_capacity: if i == 0 { 1000 } else { 100 },
            republish_interval: 500,
        };
        let node = DistributedNode::new(config);
        node_ids.push(node.node_id());
        addrs.push(addr);
        nodes.push(Some(node));
    }

    // Join handshakes: nodes[1..] join via seed (node 0).
    let seed_addr = addrs[0];
    for i in 1..num_nodes {
        let join_actions = nodes[i].as_ref().unwrap().join(&[seed_addr]);
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

    // Settle: let SWIM converge initial membership.
    for _ in 0..5 {
        tick_all_and_deliver(&mut nodes, &node_ids, &addrs);
    }

    // Register spawned actors in the main node's directory.
    for addr in ping_addrs.iter().chain(counter_addrs.iter()) {
        if let Some(ref mut node) = nodes[0] {
            node.register_actor(*addr, 1);
        }
    }

    // Snapshot provider for the dashboard.
    let cached_snapshot = Arc::new(Mutex::new(
        nodes[0].as_ref().map(|n| n.snapshot()),
    ));
    let provider = SnapshotProvider {
        snapshot: Arc::clone(&cached_snapshot),
    };
    dash.set_distribution(Arc::new(provider));

    // ── Run ─────────────────────────────────────────────────────────────

    eprintln!("Dashboard at http://localhost:9090 — press Ctrl+C to stop");
    eprintln!("Distribution at http://localhost:9090/distribution");

    // Kick off ping-pong chains
    for i in 0..ping_addrs.len() {
        let target = ping_addrs[(i + 1) % ping_addrs.len()];
        let _ = handle.runtime.send_to(ping_addrs[i], Ping(target));
    }

    let mut round: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        // Send ticks to all counter actors
        for addr in &counter_addrs {
            let _ = handle.runtime.send_to(*addr, Tick);
        }

        // Periodically spawn more actors and register them
        if round % 150 == 75 && counter_addrs.len() < 500 {
            for _ in 0..8 {
                match handle.runtime.spawn(CounterActor::new()) {
                    Ok(addr) => {
                        counter_addrs.push(addr);
                        if let Some(ref mut node) = nodes[0] {
                            node.register_actor(addr, 1);
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        // Periodically re-kick ping chains
        if round % 80 == 0 && round > 0 {
            for i in 0..ping_addrs.len() {
                let target = ping_addrs[(i + 1) % ping_addrs.len()];
                let _ = handle.runtime.send_to(ping_addrs[i], Ping(target));
            }
        }

        // Tick all distribution nodes and deliver SWIM actions
        tick_all_and_deliver(&mut nodes, &node_ids, &addrs);

        // Periodically resolve actors from main node
        if round % 50 == 25 {
            if let Some(ref mut main_node) = nodes[0] {
                let actor = ping_addrs[(round as usize / 50) % ping_addrs.len()];
                match main_node.resolve_actor(&actor) {
                    ResolveResult::Cached(found_on) => {
                        tracing::info!(actor = ?&actor.0[..4], ?found_on, "resolved actor (cached)");
                    }
                    ResolveResult::NeedsLookup { .. } => {
                        tracing::info!(actor = ?&actor.0[..4], "resolve: needs lookup");
                    }
                    ResolveResult::NotFound => {
                        tracing::info!(actor = ?&actor.0[..4], "resolve: not found");
                    }
                }
            }
        }

        // Periodically register actors on a peer and propagate entries to main node
        if round % 100 == 0 && round > 0 {
            let peer_idx = 1 + ((round as usize / 100) % (num_nodes - 1));
            // Register on the peer, collect entries
            let mut entries = Vec::new();
            if let Some(ref mut peer) = nodes[peer_idx] {
                for _ in 0..3 {
                    let actor = ActorAddress::new_random();
                    let entry = peer.register_actor(actor, round);
                    entries.push(entry);
                }
            }
            // Propagate to main node (separate borrow)
            if let Some(ref mut main_node) = nodes[0] {
                for entry in entries {
                    main_node.store_directory_entry(entry);
                }
            }
        }

        // ── Churn cycle (repeats every 400 rounds, starts at round 200) ──
        //
        // Offsets within each 400-round cycle:
        //   0   → kill peer 8 (simulated crash)
        //   150 → revive peer 8 (rejoin cluster)
        //   200 → graceful leave for peer 7
        //   350 → rejoin peer 7
        if round >= 200 {
            let churn_pos = (round - 200) % 400;

            // Kill peer 8 (simulated crash — set to None)
            if churn_pos == 0 {
                nodes[8] = None;
                tracing::info!("killed peer 8 (simulated crash)");
            }

            // Revive peer 8 (new node + rejoin)
            if churn_pos == 150 {
                let config = DistributedNodeConfig {
                    listen_addr: addrs[8],
                    swim: swim_config.clone(),
                    cache_capacity: 100,
                    republish_interval: 500,
                };
                let revived = DistributedNode::new(config);
                let join_actions = revived.join(&[seed_addr]);
                nodes[8] = Some(revived);
                node_ids[8] = nodes[8].as_ref().unwrap().node_id();

                let tagged_responses = deliver_actions_tagged(
                    &join_actions,
                    node_ids[8],
                    addrs[8],
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
                tracing::info!("revived peer 8 (rejoined cluster)");
            }

            // Graceful leave for peer 7
            if churn_pos == 200 {
                let leave_actions = nodes[7]
                    .as_mut()
                    .map(|n| n.leave())
                    .unwrap_or_default();
                if !leave_actions.is_empty() {
                    let tagged_responses = deliver_actions_tagged(
                        &leave_actions,
                        node_ids[7],
                        addrs[7],
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
                nodes[7] = None;
                tracing::info!("peer 7 gracefully left the cluster");
            }

            // Rejoin peer 7
            if churn_pos == 350 {
                let config = DistributedNodeConfig {
                    listen_addr: addrs[7],
                    swim: swim_config.clone(),
                    cache_capacity: 100,
                    republish_interval: 500,
                };
                let revived = DistributedNode::new(config);
                let join_actions = revived.join(&[seed_addr]);
                nodes[7] = Some(revived);
                node_ids[7] = nodes[7].as_ref().unwrap().node_id();

                let tagged_responses = deliver_actions_tagged(
                    &join_actions,
                    node_ids[7],
                    addrs[7],
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
                tracing::info!("peer 7 rejoined the cluster");
            }
        }

        // Update cached snapshot for the dashboard
        *cached_snapshot.lock().unwrap() = nodes[0].as_ref().map(|n| n.snapshot());

        round += 1;
        thread::sleep(Duration::from_millis(200));
    }

    eprintln!("\nShutting down...");
    handle.shutdown();
    dash.shutdown();
    handle.join();
}
