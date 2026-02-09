use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use crate::protocol::{GossipActor, GossipMessage};
use crate::trace::{
    EventLog, GossipEventKind, NameRegistry, NodeSnapshot, SimulationTrace, TickCounter,
};

// ── Configuration ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Topology {
    /// Each node gossips to the next; last gossips to first.
    Ring,
    /// Node 0 is the hub; all others gossip to/from it.
    Star,
    /// Every node gossips to every other node.
    FullMesh,
    /// Unidirectional chain: 0→1→2→…→(n-1).
    Chain,
    /// Two halves with no cross-links (healed later via `heal_after_round`).
    Partitioned,
}

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub name: String,
    pub topology: Topology,
    pub num_nodes: usize,
    /// `(key, value)` pairs to set on node 0 before gossip starts.
    pub initial_data: Vec<(String, Vec<u8>)>,
    pub num_rounds: usize,
    pub ticks_per_round: usize,
    /// If `Some(r)`, cross-partition links are added after round `r`.
    pub heal_after_round: Option<usize>,
    /// Number of worker threads: 1 = deterministic single-threaded, >1 = multi-threaded.
    pub num_threads: usize,
}

// ── Public entry point ───────────────────────────────────────────────────

pub fn run_simulation(config: SimConfig) -> SimulationTrace {
    let num_threads = config.num_threads.max(1);

    if num_threads < 2 {
        run_simulation_single_threaded(config)
    } else {
        run_simulation_multi_threaded(config)
    }
}

fn run_simulation_single_threaded(config: SimConfig) -> SimulationTrace {
    let event_log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let tick_counter: TickCounter = Arc::new(AtomicU64::new(0));
    let name_registry: NameRegistry = Arc::new(Mutex::new(HashMap::new()));

    let rt = Runtime::new(RuntimeConfig {
        num_threads: 1,
        max_actors: (config.num_nodes + 64).next_power_of_two(),
        actor_max_messages: (config.num_nodes * 4).max(1_000),
        ..Default::default()
    });

    // Spawn nodes.
    let mut addrs = Vec::with_capacity(config.num_nodes);
    let mut names = Vec::with_capacity(config.num_nodes);
    for i in 0..config.num_nodes {
        let name = format!("node-{i}");
        let actor = GossipActor::traced(
            Arc::clone(&event_log),
            Arc::clone(&tick_counter),
            Arc::clone(&name_registry),
        );
        let addr = rt.spawn(actor).unwrap();
        name_registry.lock().unwrap().insert(addr, name.clone());
        addrs.push(addr);
        names.push(name);
    }

    // Wire topology.
    let edges = wire_topology(&rt, &config.topology, &addrs, &names);
    // Deliver AddPeer messages.
    for _ in 0..3 {
        rt.tick();
    }

    // Set initial data on node 0.
    let total_keys = config.initial_data.len();
    for (key, value) in &config.initial_data {
        rt.send_to(
            addrs[0],
            GossipMessage::Set {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .unwrap();
    }
    rt.tick();

    // Run gossip rounds.
    let mut snapshots_per_round: Vec<Vec<(String, NodeSnapshot)>> = Vec::new();

    for round in 0..config.num_rounds {
        // Heal partition if needed.
        if config.heal_after_round == Some(round) {
            heal_partition(&rt, &config.topology, &addrs, &names);
            for _ in 0..3 {
                rt.tick();
            }
        }

        tick_counter.store((round + 1) as u64, Ordering::Relaxed);

        // Trigger gossip on all nodes.
        for &addr in &addrs {
            rt.send_to(addr, GossipMessage::DoGossipRound).unwrap();
        }
        for _ in 0..config.ticks_per_round {
            rt.tick();
        }

        // Take snapshots.
        for &addr in &addrs {
            rt.send_to(addr, GossipMessage::TakeSnapshot).unwrap();
        }
        for _ in 0..3 {
            rt.tick();
        }

        // Extract snapshots from event log.
        let current_round_tick = (round + 1) as u64;
        let log = event_log.lock().unwrap();
        let mut round_snapshots: Vec<(String, NodeSnapshot)> = Vec::new();
        for event in log.iter().rev() {
            if event.tick != current_round_tick {
                break;
            }
            if let GossipEventKind::StateSnapshot { ref snapshot } = event.kind {
                round_snapshots.push((event.node_name.clone(), snapshot.clone()));
            }
        }
        round_snapshots.reverse();
        snapshots_per_round.push(round_snapshots);
    }

    let events = event_log.lock().unwrap().clone();

    SimulationTrace {
        name: config.name,
        node_names: names,
        node_addrs: addrs,
        topology_edges: edges,
        events,
        snapshots_per_round,
        num_rounds: config.num_rounds,
        total_keys,
    }
}

fn run_simulation_multi_threaded(config: SimConfig) -> SimulationTrace {
    let event_log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let tick_counter: TickCounter = Arc::new(AtomicU64::new(0));
    let name_registry: NameRegistry = Arc::new(Mutex::new(HashMap::new()));

    let ticks_per_round = config.ticks_per_round;
    // Safety factor for non-deterministic scheduling: allow more settle time.
    let settle_ms = (ticks_per_round as u64 * 2).max(10);

    let rt = Runtime::new(RuntimeConfig {
        num_threads: config.num_threads,
        max_actors: (config.num_nodes + 64).next_power_of_two(),
        actor_max_messages: (config.num_nodes * 4).max(1_000),
        ..Default::default()
    });

    // Spawn nodes.
    let mut addrs = Vec::with_capacity(config.num_nodes);
    let mut names = Vec::with_capacity(config.num_nodes);
    for i in 0..config.num_nodes {
        let name = format!("node-{i}");
        let actor = GossipActor::traced(
            Arc::clone(&event_log),
            Arc::clone(&tick_counter),
            Arc::clone(&name_registry),
        );
        let addr = rt.spawn(actor).unwrap();
        name_registry.lock().unwrap().insert(addr, name.clone());
        addrs.push(addr);
        names.push(name);
    }

    // Wire topology — send AddPeer messages before starting worker threads.
    let edges = wire_topology(&rt, &config.topology, &addrs, &names);

    // Set initial data on node 0 before starting.
    let total_keys = config.initial_data.len();
    for (key, value) in &config.initial_data {
        rt.send_to(
            addrs[0],
            GossipMessage::Set {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .unwrap();
    }

    // Start worker threads — consumes `rt`, returns handle.
    let handle = rt.run().expect("failed to start multi-threaded runtime");

    // Let initial messages (spawn + AddPeer + Set) settle.
    thread::sleep(Duration::from_millis(settle_ms * 2));

    // Run gossip rounds using sleep-based ticking.
    let mut snapshots_per_round: Vec<Vec<(String, NodeSnapshot)>> = Vec::new();

    for round in 0..config.num_rounds {
        // Heal partition if needed.
        if config.heal_after_round == Some(round) {
            heal_partition_via_handle(&handle, &config.topology, &addrs, &names);
            thread::sleep(Duration::from_millis(settle_ms));
        }

        tick_counter.store((round + 1) as u64, Ordering::Relaxed);

        // Trigger gossip on all nodes.
        for &addr in &addrs {
            handle
                .runtime
                .send_to(addr, GossipMessage::DoGossipRound)
                .unwrap();
        }

        // Let gossip messages propagate.
        thread::sleep(Duration::from_millis(settle_ms));

        // Take snapshots.
        for &addr in &addrs {
            handle
                .runtime
                .send_to(addr, GossipMessage::TakeSnapshot)
                .unwrap();
        }
        thread::sleep(Duration::from_millis(settle_ms / 2));

        // Extract snapshots from event log.
        let current_round_tick = (round + 1) as u64;
        let log = event_log.lock().unwrap();
        let mut round_snapshots: Vec<(String, NodeSnapshot)> = Vec::new();
        for event in log.iter().rev() {
            if event.tick != current_round_tick {
                break;
            }
            if let GossipEventKind::StateSnapshot { ref snapshot } = event.kind {
                round_snapshots.push((event.node_name.clone(), snapshot.clone()));
            }
        }
        round_snapshots.reverse();
        snapshots_per_round.push(round_snapshots);
    }

    // Shutdown worker threads.
    handle.shutdown();
    handle.join();

    let events = event_log.lock().unwrap().clone();

    SimulationTrace {
        name: config.name,
        node_names: names,
        node_addrs: addrs,
        topology_edges: edges,
        events,
        snapshots_per_round,
        num_rounds: config.num_rounds,
        total_keys,
    }
}

pub fn heal_partition_via_handle(
    handle: &swactor::runtime::RuntimeHandle,
    topology: &Topology,
    addrs: &[ActorAddress],
    names: &[String],
) -> Vec<(String, String)> {
    if !matches!(topology, Topology::Partitioned) {
        return Vec::new();
    }
    let n = addrs.len();
    let half = n / 2;
    let mut new_edges = Vec::new();
    if half > 0 && half < n {
        handle
            .runtime
            .send_to(addrs[half - 1], GossipMessage::AddPeer(addrs[half]))
            .unwrap();
        handle
            .runtime
            .send_to(addrs[half], GossipMessage::AddPeer(addrs[half - 1]))
            .unwrap();
        new_edges.push((names[half - 1].clone(), names[half].clone()));
        new_edges.push((names[half].clone(), names[half - 1].clone()));
    }
    new_edges
}

// ── Topology wiring ──────────────────────────────────────────────────────

pub fn wire_topology(
    rt: &Runtime,
    topology: &Topology,
    addrs: &[ActorAddress],
    names: &[String],
) -> Vec<(String, String)> {
    let n = addrs.len();
    let mut edges = Vec::new();

    let mut add_edge = |from: usize, to: usize| {
        rt.send_to(addrs[from], GossipMessage::AddPeer(addrs[to]))
            .unwrap();
        edges.push((names[from].clone(), names[to].clone()));
    };

    match topology {
        Topology::Ring => {
            for i in 0..n {
                add_edge(i, (i + 1) % n);
            }
        }
        Topology::Star => {
            for i in 1..n {
                add_edge(0, i);
                add_edge(i, 0);
            }
        }
        Topology::FullMesh => {
            for i in 0..n {
                for j in 0..n {
                    if i != j {
                        add_edge(i, j);
                    }
                }
            }
        }
        Topology::Chain => {
            for i in 0..n.saturating_sub(1) {
                add_edge(i, i + 1);
            }
        }
        Topology::Partitioned => {
            let half = n / 2;
            // Wire each half as a full mesh.
            for i in 0..half {
                for j in 0..half {
                    if i != j {
                        add_edge(i, j);
                    }
                }
            }
            for i in half..n {
                for j in half..n {
                    if i != j {
                        add_edge(i, j);
                    }
                }
            }
        }
    }
    edges
}

pub fn heal_partition(
    rt: &Runtime,
    topology: &Topology,
    addrs: &[ActorAddress],
    names: &[String],
) -> Vec<(String, String)> {
    if !matches!(topology, Topology::Partitioned) {
        return Vec::new();
    }
    let n = addrs.len();
    let half = n / 2;
    let mut new_edges = Vec::new();
    // Add bidirectional links between the two halves (bridge nodes).
    if half > 0 && half < n {
        rt.send_to(addrs[half - 1], GossipMessage::AddPeer(addrs[half]))
            .unwrap();
        rt.send_to(addrs[half], GossipMessage::AddPeer(addrs[half - 1]))
            .unwrap();
        new_edges.push((names[half - 1].clone(), names[half].clone()));
        new_edges.push((names[half].clone(), names[half - 1].clone()));
    }
    new_edges
}
