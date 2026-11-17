use std::sync::Arc;

use distribution::membership_actor::{ObservedMembershipTransition, TransportMembershipActor};
use distribution::messages::{JoinRequest, MembershipGossip, MembershipPeer};
use distribution::swim::actor::{SharedPeerDirectory, SwimIn};
use distribution::types::{MemberState, NodeId};
use parking_lot::Mutex;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime, RuntimeConfig, RuntimeParts};
use swactor::std::StdExtension;
use swactor_engine::{Engine, SteppingBackend};

const LOCAL: NodeId = NodeId([1; 32]);
const PEER: NodeId = NodeId([2; 32]);
const OTHER: NodeId = NodeId([3; 32]);

struct WireRecorder(Arc<Mutex<Vec<SwimIn>>>);

impl ActorInterface for WireRecorder {
    type Incoming = SwimIn;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx<'_>, message: SwimIn) {
        self.0.lock().push(message);
    }
}

struct TransitionRecorder(Arc<Mutex<Vec<ObservedMembershipTransition>>>);

impl ActorInterface for TransitionRecorder {
    type Incoming = ObservedMembershipTransition;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx<'_>, message: ObservedMembershipTransition) {
        self.0.lock().push(message);
    }
}

fn peer(node_id: NodeId) -> MembershipPeer {
    MembershipPeer {
        node_id,
        direct_addrs: Vec::new(),
        relay_url: None,
    }
}

fn roster() -> SwimIn {
    SwimIn::MembershipGossip(MembershipGossip {
        from: peer(OTHER),
        peers: vec![peer(PEER)],
        leaving: false,
    })
}

struct MembershipHarness {
    runtime: Runtime,
    _engine: Engine,
    backend: SteppingBackend,
    actor: ActorAddress,
    wire: Arc<Mutex<Vec<SwimIn>>>,
    transitions: Arc<Mutex<Vec<ObservedMembershipTransition>>>,
}

impl MembershipHarness {
    fn new() -> Self {
        let parts = RuntimeParts::new(RuntimeConfig::default())
            .with_extension(Arc::new(StdExtension::new()));
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).expect("create stepping engine");
        let wire = Arc::new(Mutex::new(Vec::new()));
        let wire_actor = runtime.spawn(WireRecorder(wire.clone())).unwrap();
        let transitions = Arc::new(Mutex::new(Vec::new()));
        let observer = runtime
            .spawn(TransitionRecorder(transitions.clone()))
            .unwrap();
        let directory = SharedPeerDirectory::new();
        directory.bind(PEER, wire_actor, 0);
        let actor = runtime
            .spawn(TransportMembershipActor::new(
                LOCAL,
                Arc::new(directory),
                observer,
            ))
            .unwrap();
        Self {
            runtime,
            _engine: engine,
            backend,
            actor,
            wire,
            transitions,
        }
    }

    fn send(&self, message: SwimIn) {
        self.runtime.send_to(self.actor, message).unwrap();
        // Dispatch membership input, then its synchronous recording sinks.
        self.backend.step();
        self.backend.step();
    }

    fn take_join_request(&self) {
        let mut wire = self.wire.lock();
        assert!(
            matches!(wire.as_slice(), [SwimIn::JoinRequest(request)] if request.from == LOCAL),
            "each concrete discovery must emit exactly one admission request"
        );
        wire.clear();
    }
}

#[test]
fn failed_admission_waits_for_a_later_roster_or_explicit_join() {
    let harness = MembershipHarness::new();
    harness.send(SwimIn::Join { seeds: vec![PEER] });
    harness.take_join_request();

    harness.send(roster());
    assert!(
        harness.wire.lock().is_empty(),
        "pending admission suppresses duplicate rosters"
    );
    harness.send(SwimIn::SendFailed { to: OTHER });
    harness.send(roster());
    assert!(
        harness.wire.lock().is_empty(),
        "another peer's failure must not release admission"
    );

    harness.send(SwimIn::SendFailed { to: PEER });
    assert!(
        harness.wire.lock().is_empty(),
        "failure must not immediately retry"
    );
    assert!(
        harness.transitions.lock().is_empty(),
        "unadmitted peers must not become dead"
    );

    harness.send(roster());
    harness.take_join_request();
    harness.send(roster());
    assert!(
        harness.wire.lock().is_empty(),
        "the new attempt is pending again"
    );

    harness.send(SwimIn::SendFailed { to: PEER });
    assert!(harness.wire.lock().is_empty());
    harness.send(SwimIn::Join { seeds: vec![PEER] });
    harness.take_join_request();
    harness.send(SwimIn::Join { seeds: vec![PEER] });
    assert!(
        harness.wire.lock().is_empty(),
        "duplicate explicit joins remain suppressed"
    );
    assert!(harness.transitions.lock().is_empty());
}

#[test]
fn send_failure_preserves_established_membership() {
    let harness = MembershipHarness::new();
    harness.send(SwimIn::TransportConnected {
        peer: peer(PEER),
        local: peer(LOCAL),
    });
    assert!(matches!(
        harness.wire.lock().as_slice(),
        [SwimIn::MembershipGossip(_)]
    ));
    harness.wire.lock().clear();

    harness.send(SwimIn::SendFailed { to: PEER });
    harness.send(roster());
    harness.send(SwimIn::Join { seeds: vec![PEER] });
    assert!(
        harness.wire.lock().is_empty(),
        "an established peer must not be readmitted on send failure"
    );
    let transitions = harness.transitions.lock();
    assert!(matches!(
        transitions.as_slice(),
        [ObservedMembershipTransition {
            peer: PEER,
            from: None,
            to: MemberState::Alive,
            ..
        }]
    ));
    drop(transitions);

    harness.send(SwimIn::JoinRequest(JoinRequest { from: PEER }));
    let wire = harness.wire.lock();
    let [SwimIn::JoinResponse(response)] = wire.as_slice() else {
        panic!("expected only the requested roster response");
    };
    assert_eq!(response.members.len(), 2);
    assert!(
        response
            .members
            .iter()
            .any(|member| { member.node_id == PEER && member.state == MemberState::Alive })
    );
}
