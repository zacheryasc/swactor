#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    pub edge_id: EdgeId,
    pub object_id: ObjectId,
}

impl ObjectKey {
    pub fn new(edge_id: EdgeId, object_id: ObjectId) -> Self {
        Self { edge_id, object_id }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectIdAllocator {
    edge_id: EdgeId,
    next: u64,
}

impl ObjectIdAllocator {
    pub fn new(edge_id: EdgeId) -> Self {
        Self { edge_id, next: 1 }
    }

    pub fn alloc(&mut self) -> ObjectKey {
        let object_key = ObjectKey::new(self.edge_id, ObjectId(self.next));
        self.next += 1;
        object_key
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpaqueHandle(pub u64);

impl OpaqueHandle {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxConfig {
    pub edge_id: EdgeId,
    pub role_port: PortId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RxConfig {
    pub edge_id: EdgeId,
    pub role_port: PortId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorFaultReason {
    MismatchedEdgeId,
    StreamFault,
    ObjectFault,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActorMessage {
    Lifecycle {
        edge_id: EdgeId,
        ready: bool,
    },
    ObjectIdentity {
        edge_id: EdgeId,
        object_id: ObjectId,
        sequence: u64,
    },
    OpaqueHandle {
        edge_id: EdgeId,
        object_id: ObjectId,
        sequence: u64,
        handle: OpaqueHandle,
    },
    CoarseFault {
        edge_id: EdgeId,
        reason: ActorFaultReason,
    },
}

impl ActorMessage {
    pub fn edge_id(&self) -> EdgeId {
        match self {
            ActorMessage::Lifecycle { edge_id, .. }
            | ActorMessage::ObjectIdentity { edge_id, .. }
            | ActorMessage::OpaqueHandle { edge_id, .. }
            | ActorMessage::CoarseFault { edge_id, .. } => *edge_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxEvent {
    EdgeReady {
        edge_id: EdgeId,
    },
    ObjectProduced {
        edge_id: EdgeId,
        object_id: ObjectId,
        sequence: u64,
    },
    StreamFault {
        edge_id: EdgeId,
    },
    ObjectFailed {
        edge_id: EdgeId,
    },
    StopEdge {
        edge_id: EdgeId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RxEvent {
    EdgeReady {
        edge_id: EdgeId,
    },
    ObjectLoaded {
        edge_id: EdgeId,
        object_id: ObjectId,
        sequence: u64,
        handle: OpaqueHandle,
    },
    StreamFault {
        edge_id: EdgeId,
    },
    ObjectFailed {
        edge_id: EdgeId,
    },
    StopEdge {
        edge_id: EdgeId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActorState {
    Provisioning,
    Ready,
    Faulted,
    Stopped,
}

pub struct TxEdgeActor {
    config: TxConfig,
    state: ActorState,
    messages: Vec<ActorMessage>,
}

impl TxEdgeActor {
    pub fn new(config: TxConfig) -> Self {
        Self {
            config,
            state: ActorState::Provisioning,
            messages: Vec::new(),
        }
    }

    pub fn observe(&mut self, event: TxEvent) {
        match event {
            TxEvent::EdgeReady { edge_id } => {
                if !self.check_edge(edge_id) {
                    return;
                }
                if self.state == ActorState::Provisioning {
                    self.state = ActorState::Ready;
                    self.messages.push(ActorMessage::Lifecycle {
                        edge_id,
                        ready: true,
                    });
                }
            }
            TxEvent::ObjectProduced {
                edge_id,
                object_id,
                sequence,
            } => {
                if self.state == ActorState::Ready && self.check_edge(edge_id) {
                    self.messages.push(ActorMessage::ObjectIdentity {
                        edge_id,
                        object_id,
                        sequence,
                    });
                }
            }
            TxEvent::StreamFault { edge_id } => {
                self.fault_if_edge(edge_id, ActorFaultReason::StreamFault)
            }
            TxEvent::ObjectFailed { edge_id } => {
                self.fault_if_edge(edge_id, ActorFaultReason::ObjectFault)
            }
            TxEvent::StopEdge { edge_id } => {
                if edge_id == self.config.edge_id {
                    self.state = ActorState::Stopped;
                } else {
                    self.mismatched();
                }
            }
        }
    }

    pub fn messages(&self) -> &[ActorMessage] {
        &self.messages
    }

    fn check_edge(&mut self, edge_id: EdgeId) -> bool {
        if edge_id == self.config.edge_id {
            true
        } else {
            self.mismatched();
            false
        }
    }

    fn fault_if_edge(&mut self, edge_id: EdgeId, reason: ActorFaultReason) {
        if self.check_edge(edge_id) && self.state != ActorState::Stopped {
            self.state = ActorState::Faulted;
            self.messages
                .push(ActorMessage::CoarseFault { edge_id, reason });
        }
    }

    fn mismatched(&mut self) {
        self.state = ActorState::Faulted;
        self.messages.push(ActorMessage::CoarseFault {
            edge_id: self.config.edge_id,
            reason: ActorFaultReason::MismatchedEdgeId,
        });
    }
}

pub struct RxEdgeActor {
    config: RxConfig,
    state: ActorState,
    messages: Vec<ActorMessage>,
}

impl RxEdgeActor {
    pub fn new(config: RxConfig) -> Self {
        Self {
            config,
            state: ActorState::Provisioning,
            messages: Vec::new(),
        }
    }

    pub fn observe(&mut self, event: RxEvent) {
        match event {
            RxEvent::EdgeReady { edge_id } => {
                if !self.check_edge(edge_id) {
                    return;
                }
                if self.state == ActorState::Provisioning {
                    self.state = ActorState::Ready;
                    self.messages.push(ActorMessage::Lifecycle {
                        edge_id,
                        ready: true,
                    });
                }
            }
            RxEvent::ObjectLoaded {
                edge_id,
                object_id,
                sequence,
                handle,
            } => {
                if self.state == ActorState::Ready && self.check_edge(edge_id) {
                    self.messages.push(ActorMessage::OpaqueHandle {
                        edge_id,
                        object_id,
                        sequence,
                        handle,
                    });
                }
            }
            RxEvent::StreamFault { edge_id } => {
                self.fault_if_edge(edge_id, ActorFaultReason::StreamFault)
            }
            RxEvent::ObjectFailed { edge_id } => {
                self.fault_if_edge(edge_id, ActorFaultReason::ObjectFault)
            }
            RxEvent::StopEdge { edge_id } => {
                if edge_id == self.config.edge_id {
                    self.state = ActorState::Stopped;
                } else {
                    self.mismatched();
                }
            }
        }
    }

    pub fn messages(&self) -> &[ActorMessage] {
        &self.messages
    }

    fn check_edge(&mut self, edge_id: EdgeId) -> bool {
        if edge_id == self.config.edge_id {
            true
        } else {
            self.mismatched();
            false
        }
    }

    fn fault_if_edge(&mut self, edge_id: EdgeId, reason: ActorFaultReason) {
        if self.check_edge(edge_id) && self.state != ActorState::Stopped {
            self.state = ActorState::Faulted;
            self.messages
                .push(ActorMessage::CoarseFault { edge_id, reason });
        }
    }

    fn mismatched(&mut self) {
        self.state = ActorState::Faulted;
        self.messages.push(ActorMessage::CoarseFault {
            edge_id: self.config.edge_id,
            reason: ActorFaultReason::MismatchedEdgeId,
        });
    }
}

pub type TxActorHarness = TxEdgeActor;
pub type RxActorHarness = RxEdgeActor;
