//! Transport-seam behavioral tests for `SwimActor`.
//!
//! The in-process `swim_actor.rs` tests prove the actor against peers that are
//! *local* actors (`ctx.send` resolves in-runtime). These tests prove the part
//! production actually depends on: peers live on a **different runtime**, reached
//! only through the swactor transport seam —
//!   egress: `ctx.send(SwimIn::…)` → non-local addr → `CodecRegistry` encode →
//!           `TransportRouter` → `Transport::send` (a serialized `WireEnvelope`);
//!   ingress: decode the frame → `Runtime::deliver_raw` into the peer's mailbox.
//!
//! This is the exact path the iroh bridge will drive; here a `Link` stands in for
//! iroh, carrying a `WireEnvelope` from one runtime into another in-process. We
//! observe ONLY the `MembershipChanged` stream (§6.3), and pin eventuality by
//! converge-or-timeout — never a fixed tick count.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use swactor::Error;
use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};
use swactor::std::StdExtension;
use swactor_transport::{CodecRegistry, Transport, TransportRouter, WireEnvelope};

use distribution::messages::actor_codec_registry;
use distribution::swim::actor::{MembershipChanged, SharedPeerDirectory, SwimActor, SwimIn};
use distribution::swim::member_list::MemberList;
use distribution::swim::probe::{ProbeMode, SwimConfig};
use distribution::types::{MemberState, NodeId};

const TICK: Duration = Duration::from_millis(10);

fn id(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

/// The deterministic, reversible `NodeId → ActorAddress` mapping the production
/// `PeerBinder` will use: a remote peer's mailbox address is its node id's bytes.
/// Guaranteed non-local on every other runtime, so `ctx.send` takes the transport
/// path; never collides with a locally `new_random()`-addressed actor.
fn peer_addr(node: NodeId) -> ActorAddress {
    ActorAddress(node.0)
}

/// Brisk Periodic config (mirrors the in-process harness) so detection completes
/// in a manageable number of rounds.
fn brisk_config() -> SwimConfig {
    SwimConfig {
        probe_interval: TICK,
        probe_timeout: TICK * 2,
        indirect_probes: 2,
        suspicion_timeout: TICK * 3,
        dead_reprobe_interval: Duration::ZERO,
        probe_mode: ProbeMode::Periodic,
        lifeguard: None,
    }
}

/// Stands in for iroh: serializes nothing itself (the runtime already encoded to
/// a `WireEnvelope`), it just carries the frame into the destination runtime and
/// performs the production ingress — `codec.decode(...)` → `deliver_raw(...)`.
/// A `partition` set models nodes that have fallen off the network: a frame whose
/// source or destination is partitioned is dropped (genuine silence).
struct Link {
    src: usize,
    dst: usize,
    dst_rt: Arc<Runtime>,
    dst_swim: ActorAddress,
    codec: Arc<CodecRegistry>,
    partition: Arc<Mutex<HashSet<usize>>>,
}

impl Transport for Link {
    fn send(&self, wire: WireEnvelope) -> Result<(), Error> {
        {
            let p = self.partition.lock().unwrap();
            if p.contains(&self.src) || p.contains(&self.dst) {
                // Unreachable: the egress fails, which the SwimActor turns into
                // SendFailed{to} (§4.3) — exactly how a real dropped connection
                // feeds failure detection.
                return Err(Error::from("link partitioned"));
            }
        }
        let msg = self.codec.decode(&wire.type_tag, &wire.payload)?;
        self.dst_rt.deliver_raw(self.dst_swim, msg)
    }
}

/// A cluster of `n` `SwimActor`s, each on its **own** runtime, meshed through
/// `Link` transports — the multi-runtime analog of `swim_actor.rs::ActorCluster`.
struct TransportCluster {
    rts: Vec<Arc<Runtime>>,
    swims: Vec<ActorAddress>,
    inboxes: Vec<Inbox<MembershipChanged>>,
    streams: Vec<Vec<MembershipChanged>>,
    ids: Vec<NodeId>,
    partition: Arc<Mutex<HashSet<usize>>>,
    clock: Instant,
}

impl TransportCluster {
    fn new(n: usize) -> Self {
        let codec = Arc::new(actor_codec_registry());
        let now = Instant::now();
        let ids: Vec<NodeId> = (0..n).map(|i| id(i as u8)).collect();
        let partition = Arc::new(Mutex::new(HashSet::new()));

        let mut rts = Vec::new();
        let mut swims = Vec::new();
        let mut dirs = Vec::new();
        let mut routers = Vec::new();
        let mut inboxes = Vec::new();

        // Phase 1: one runtime per node, each with the actor codec registry + an
        // (initially empty) transport router. Spawn the SwimActor and subscribe.
        for &nid in &ids {
            let mut rt = Runtime::new(RuntimeConfig::default())
                .with_extension(Arc::new(StdExtension::new()));
            let router = Arc::new(TransportRouter::new());
            rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
                codec.clone(),
                router.clone(),
            )));
            let rt = Arc::new(rt);

            let dir = SharedPeerDirectory::new();
            let swim = rt
                .spawn(SwimActor::new(
                    nid,
                    brisk_config(),
                    now,
                    Arc::new(dir.clone()),
                ))
                .expect("spawn SwimActor");
            // Self resolves to the local mailbox; peers are wired in phase 2.
            dir.bind(nid, swim, 0);
            let inbox = rt.new_inbox::<MembershipChanged>().expect("inbox");
            rt.send_to(
                swim,
                SwimIn::Subscribe {
                    observer: *inbox.addr(),
                },
            )
            .unwrap();

            rts.push(rt);
            swims.push(swim);
            dirs.push(dir);
            routers.push(router);
            inboxes.push(inbox);
        }

        // Phase 2: mesh. Each node resolves every peer's NodeId to its synthetic
        // peer address and routes that address through a Link into the peer's
        // runtime. Now both Arcs exist, so the mutual references close cleanly.
        for i in 0..n {
            for j in 0..n {
                if i == j {
                    continue;
                }
                let syn = peer_addr(ids[j]);
                dirs[i].bind(ids[j], syn, 0);
                routers[i].add_route(
                    syn,
                    Arc::new(Link {
                        src: i,
                        dst: j,
                        dst_rt: rts[j].clone(),
                        dst_swim: swims[j],
                        codec: codec.clone(),
                        partition: partition.clone(),
                    }),
                );
            }
        }

        let mut c = TransportCluster {
            rts,
            swims,
            inboxes,
            streams: vec![Vec::new(); n],
            ids,
            partition,
            clock: now,
        };
        c.pump(4); // settle Subscribe

        // Nodes 1..n join via the seed (node 0).
        for i in 1..n {
            c.rts[i]
                .send_to(
                    c.swims[i],
                    SwimIn::Join {
                        seeds: vec![c.ids[0]],
                    },
                )
                .unwrap();
        }
        c.pump(12);
        c.drain();
        c
    }

    /// Tick every runtime `k` times. Each cross-runtime hop needs the destination
    /// to tick after the source, so one ping→ack round trip settles in a few
    /// iterations; `k` is sized so a probe + its notification settle per round.
    fn pump(&self, k: usize) {
        for _ in 0..k {
            for rt in &self.rts {
                rt.tick();
            }
        }
    }

    fn drain(&mut self) {
        for (i, inbox) in self.inboxes.iter().enumerate() {
            while let Some(m) = inbox.try_recv() {
                self.streams[i].push(m);
            }
        }
    }

    /// Advance the clock and deliver `Tick{now}` to every non-partitioned actor
    /// (a partitioned node is genuinely silent), then let the runtimes settle.
    fn round(&mut self) {
        self.clock += TICK;
        let now = self.clock;
        let partitioned = self.partition.lock().unwrap().clone();
        for i in 0..self.ids.len() {
            if !partitioned.contains(&i) {
                let _ = self.rts[i].send_to(self.swims[i], SwimIn::Tick { now });
            }
        }
        self.pump(12);
        self.drain();
    }

    fn run_until<F: Fn(&TransportCluster) -> bool>(&mut self, cap: usize, cond: F) -> bool {
        if cond(self) {
            return true;
        }
        for _ in 0..cap {
            self.round();
            if cond(self) {
                return true;
            }
        }
        false
    }

    /// Fold node `observer`'s `MembershipChanged` stream under the §7 merge rule.
    fn folded_view(&self, observer: usize) -> BTreeMap<[u8; 32], MemberState> {
        let mut ml = MemberList::new(id(250)); // sentinel self never appears
        for m in &self.streams[observer] {
            ml.apply(m.node_id, m.state, m.incarnation);
        }
        ml.all_members()
            .iter()
            .map(|e| (e.node_id.0, e.state))
            .collect()
    }

    fn sees(&self, observer: usize, subject: NodeId, state: MemberState) -> bool {
        self.folded_view(observer).get(&subject.0) == Some(&state)
    }

    fn all_converged_alive(&self) -> bool {
        let n = self.ids.len();
        (0..n).all(|o| (0..n).all(|s| o == s || self.sees(o, id(s as u8), MemberState::Alive)))
    }
}

#[test]
fn actors_on_separate_runtimes_converge_alive_over_the_transport() {
    // The gossip that makes them converge crosses the codec/TransportRouter/
    // deliver_raw seam on every hop — proving SwimActor works against genuinely
    // remote peers, not just co-located actors.
    let mut c = TransportCluster::new(3);
    assert!(
        c.run_until(600, |c| c.all_converged_alive()),
        "actors on separate runtimes did not converge Alive over the transport"
    );
}

#[test]
fn a_partitioned_actor_is_detected_dead_over_the_transport() {
    // Once a node is partitioned, survivors' probes to it fail at the transport
    // (Link returns Err → SendFailed), and it can no longer refute — so the
    // survivors must converge on Dead. Real detection over the wire seam.
    let mut c = TransportCluster::new(3);
    assert!(
        c.run_until(600, |c| c.all_converged_alive()),
        "precondition: must converge Alive first"
    );

    let dead = 2usize;
    c.partition.lock().unwrap().insert(dead);

    let detected = c.run_until(3000, |c| {
        (0..3).all(|o| o == dead || c.sees(o, id(dead as u8), MemberState::Dead))
    });
    assert!(
        detected,
        "survivors must converge on the partitioned node being Dead"
    );
}
