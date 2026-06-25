use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RingId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Alpn(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverConfig {
    pub local_node_id: NodeId,
    pub alpn: Alpn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EstablishSend {
    pub edge_id: EdgeId,
    pub peer_node_id: NodeId,
    pub layout: RingLayout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EstablishRecv {
    pub edge_id: EdgeId,
    pub layout: RingLayout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingDirection {
    Egress,
    Ingress,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingLayout {
    pub ring_id: RingId,
    pub byte_capacity: usize,
    pub direction: RingDirection,
}

impl RingLayout {
    pub fn test_egress() -> Self {
        Self {
            ring_id: RingId(1),
            byte_capacity: 4096,
            direction: RingDirection::Egress,
        }
    }

    pub fn test_ingress() -> Self {
        Self {
            ring_id: RingId(2),
            byte_capacity: 4096,
            direction: RingDirection::Ingress,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriverEvent {
    EstablishSend(EstablishSend),
    EstablishRecv(EstablishRecv),
    IncomingUniStream {
        edge_id: EdgeId,
        stream_id: StreamId,
    },
    RingReadable {
        edge_id: EdgeId,
    },
    RingWritable {
        edge_id: EdgeId,
    },
    EgressBytesCommitted {
        edge_id: EdgeId,
        bytes: Vec<u8>,
    },
    StreamBytesRead {
        edge_id: EdgeId,
        bytes: Vec<u8>,
    },
    WriteAllAccepted {
        edge_id: EdgeId,
        byte_count: usize,
    },
    NetworkStalled {
        edge_id: EdgeId,
    },
    IngressRingFull {
        edge_id: EdgeId,
    },
    ReadError {
        edge_id: EdgeId,
    },
    WriteError {
        edge_id: EdgeId,
    },
    StopEdge {
        edge_id: EdgeId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriverCommand {
    OpenOrReuseConnection {
        peer_node_id: NodeId,
        alpn: Alpn,
        local_node_id: NodeId,
    },
    OpenUniStream {
        edge_id: EdgeId,
        peer_node_id: NodeId,
    },
    SpawnSendPump {
        edge_id: EdgeId,
        ring_id: RingId,
    },
    SpawnRecvPump {
        edge_id: EdgeId,
        ring_id: RingId,
        stream_id: StreamId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActorMessage {
    PollStreamFuture { edge_id: EdgeId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriverEventOut {
    DriverEdgeReady {
        edge_id: EdgeId,
    },
    StreamClosed {
        edge_id: EdgeId,
    },
    StreamFault {
        edge_id: EdgeId,
        reason: StreamFaultReason,
    },
    PumpStopped {
        edge_id: EdgeId,
        ring_id: RingId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamFaultReason {
    ReadError,
    WriteError,
    ProtocolError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeHint {
    RingReadable { edge_id: EdgeId },
    RingWritable { edge_id: EdgeId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamWrite {
    pub edge_id: EdgeId,
    pub bytes: Vec<u8>,
}

pub fn encode_edge_preamble(edge_id: EdgeId) -> Vec<u8> {
    edge_id.0.to_le_bytes().to_vec()
}

pub fn count_preamble_occurrences(bytes: &[u8], edge_id: EdgeId) -> usize {
    let preamble = encode_edge_preamble(edge_id);
    bytes
        .windows(preamble.len())
        .filter(|window| *window == preamble.as_slice())
        .count()
}

pub fn fake_object_header_bytes() -> Vec<u8> {
    b"OBJ\0fake-header".to_vec()
}

pub struct Driver {
    state: DriverState,
}

impl Driver {
    pub fn new(config: DriverConfig) -> Self {
        Self {
            state: DriverState::new(config),
        }
    }

    pub fn observe(&mut self, event: DriverEvent) {
        self.state.observe(event);
    }

    pub fn commands(&self) -> &[DriverCommand] {
        &self.state.commands
    }

    pub fn events(&self) -> &[DriverEventOut] {
        &self.state.events
    }

    pub fn wake_hints(&self) -> &[WakeHint] {
        &self.state.wakes
    }
}

#[derive(Debug)]
struct DriverState {
    config: DriverConfig,
    connections: BTreeSet<(NodeId, Alpn)>,
    sends: BTreeMap<EdgeId, SendPumpState>,
    recv_specs: BTreeMap<EdgeId, EstablishRecv>,
    pending_streams: BTreeMap<EdgeId, StreamId>,
    recvs: BTreeMap<EdgeId, RecvPumpState>,
    commands: Vec<DriverCommand>,
    events: Vec<DriverEventOut>,
    wakes: Vec<WakeHint>,
    #[cfg(test)]
    actor_messages: Vec<ActorMessage>,
    stream_writes: Vec<StreamWrite>,
    read_started: BTreeSet<StreamId>,
}

impl DriverState {
    fn new(config: DriverConfig) -> Self {
        Self {
            config,
            connections: BTreeSet::new(),
            sends: BTreeMap::new(),
            recv_specs: BTreeMap::new(),
            pending_streams: BTreeMap::new(),
            recvs: BTreeMap::new(),
            commands: Vec::new(),
            events: Vec::new(),
            wakes: Vec::new(),
            #[cfg(test)]
            actor_messages: Vec::new(),
            stream_writes: Vec::new(),
            read_started: BTreeSet::new(),
        }
    }

    fn observe(&mut self, event: DriverEvent) {
        match event {
            DriverEvent::EstablishSend(spec) => self.establish_send(spec),
            DriverEvent::EstablishRecv(spec) => self.establish_recv(spec),
            DriverEvent::IncomingUniStream { edge_id, stream_id } => {
                self.incoming_uni_stream(edge_id, stream_id);
            }
            DriverEvent::RingReadable { edge_id } => self.flush_send_bytes(edge_id),
            DriverEvent::RingWritable { edge_id } => self.resume_recv(edge_id),
            DriverEvent::EgressBytesCommitted { edge_id, bytes } => {
                if let Some(send) = self.sends.get_mut(&edge_id) {
                    send.pending_bytes.extend(bytes);
                }
            }
            DriverEvent::StreamBytesRead { edge_id, bytes } => {
                self.copy_recv_bytes(edge_id, &bytes)
            }
            DriverEvent::WriteAllAccepted {
                edge_id,
                byte_count,
            } => {
                if let Some(send) = self.sends.get_mut(&edge_id) {
                    send.consume_cursor += byte_count;
                    send.network_stalled = false;
                    self.wakes.push(WakeHint::RingWritable { edge_id });
                }
            }
            DriverEvent::NetworkStalled { edge_id } => {
                if let Some(send) = self.sends.get_mut(&edge_id) {
                    send.network_stalled = true;
                }
            }
            DriverEvent::IngressRingFull { edge_id } => {
                if let Some(recv) = self.recvs.get_mut(&edge_id) {
                    recv.reading = false;
                }
            }
            DriverEvent::ReadError { edge_id } => {
                self.events.push(DriverEventOut::StreamFault {
                    edge_id,
                    reason: StreamFaultReason::ReadError,
                });
            }
            DriverEvent::WriteError { edge_id } => {
                self.events.push(DriverEventOut::StreamFault {
                    edge_id,
                    reason: StreamFaultReason::WriteError,
                });
            }
            DriverEvent::StopEdge { edge_id } => self.stop_edge(edge_id),
        }
    }

    fn establish_send(&mut self, spec: EstablishSend) {
        let connection_key = (spec.peer_node_id, self.config.alpn.clone());
        self.connections.insert(connection_key);
        self.commands.push(DriverCommand::OpenOrReuseConnection {
            peer_node_id: spec.peer_node_id,
            alpn: self.config.alpn.clone(),
            local_node_id: self.config.local_node_id,
        });

        self.commands.push(DriverCommand::SpawnSendPump {
            edge_id: spec.edge_id,
            ring_id: spec.layout.ring_id,
        });

        let send = SendPumpState::new(spec.layout.ring_id);
        self.commands.push(DriverCommand::OpenUniStream {
            edge_id: spec.edge_id,
            peer_node_id: spec.peer_node_id,
        });
        self.stream_writes.push(StreamWrite {
            edge_id: spec.edge_id,
            bytes: encode_edge_preamble(spec.edge_id),
        });
        self.sends.insert(spec.edge_id, send);
        self.events.push(DriverEventOut::DriverEdgeReady {
            edge_id: spec.edge_id,
        });
    }

    fn establish_recv(&mut self, spec: EstablishRecv) {
        let edge_id = spec.edge_id;
        self.recv_specs.insert(edge_id, spec);
        if let Some(stream_id) = self.pending_streams.remove(&edge_id) {
            self.spawn_recv(edge_id, stream_id);
        }
    }

    fn incoming_uni_stream(&mut self, edge_id: EdgeId, stream_id: StreamId) {
        if self.recv_specs.contains_key(&edge_id) {
            self.spawn_recv(edge_id, stream_id);
        } else {
            self.pending_streams.insert(edge_id, stream_id);
        }
    }

    fn spawn_recv(&mut self, edge_id: EdgeId, stream_id: StreamId) {
        let Some(spec) = self.recv_specs.get(&edge_id) else {
            self.pending_streams.insert(edge_id, stream_id);
            return;
        };
        let ring_id = spec.layout.ring_id;
        self.commands.push(DriverCommand::SpawnRecvPump {
            edge_id,
            ring_id,
            stream_id,
        });
        self.read_started.insert(stream_id);
        self.recvs.insert(
            edge_id,
            RecvPumpState {
                ring_id,
                commit_cursor: 0,
                reading: true,
            },
        );
        self.events
            .push(DriverEventOut::DriverEdgeReady { edge_id });
    }

    fn flush_send_bytes(&mut self, edge_id: EdgeId) {
        let Some(send) = self.sends.get_mut(&edge_id) else {
            return;
        };
        if send.network_stalled || send.pending_bytes.is_empty() {
            return;
        }

        let bytes = std::mem::take(&mut send.pending_bytes);
        self.stream_writes.push(StreamWrite { edge_id, bytes });
    }

    fn resume_recv(&mut self, edge_id: EdgeId) {
        if let Some(recv) = self.recvs.get_mut(&edge_id) {
            recv.reading = true;
        }
    }

    fn copy_recv_bytes(&mut self, edge_id: EdgeId, bytes: &[u8]) {
        let Some(recv) = self.recvs.get_mut(&edge_id) else {
            return;
        };
        if !recv.reading {
            return;
        }
        recv.commit_cursor += bytes.len();
        self.wakes.push(WakeHint::RingReadable { edge_id });
    }

    fn stop_edge(&mut self, edge_id: EdgeId) {
        let ring_id = self
            .sends
            .get(&edge_id)
            .map(|send| send.ring_id)
            .or_else(|| self.recvs.get(&edge_id).map(|recv| recv.ring_id))
            .or_else(|| {
                self.recv_specs
                    .get(&edge_id)
                    .map(|spec| spec.layout.ring_id)
            })
            .unwrap_or(RingId(0));

        self.sends.remove(&edge_id);
        self.recvs.remove(&edge_id);
        self.recv_specs.remove(&edge_id);
        self.pending_streams.remove(&edge_id);
        self.events
            .push(DriverEventOut::PumpStopped { edge_id, ring_id });
    }
}

#[derive(Debug)]
struct SendPumpState {
    ring_id: RingId,
    pending_bytes: Vec<u8>,
    consume_cursor: usize,
    network_stalled: bool,
}

impl SendPumpState {
    fn new(ring_id: RingId) -> Self {
        Self {
            ring_id,
            pending_bytes: Vec::new(),
            consume_cursor: 0,
            network_stalled: false,
        }
    }
}

#[derive(Debug)]
struct RecvPumpState {
    ring_id: RingId,
    commit_cursor: usize,
    reading: bool,
}

#[cfg(test)]
pub struct CommandLog {
    commands: Vec<DriverCommand>,
}

#[cfg(test)]
impl CommandLog {
    pub fn iter(&self) -> std::vec::IntoIter<DriverCommand> {
        self.commands.clone().into_iter()
    }
}

#[cfg(test)]
pub struct DriverHarness {
    driver: Driver,
}

#[cfg(test)]
impl DriverHarness {
    pub fn new(config: DriverConfig) -> Self {
        Self {
            driver: Driver::new(config),
        }
    }

    pub fn observe(&mut self, event: DriverEvent) {
        self.driver.observe(event);
    }

    pub fn commands(&self) -> CommandLog {
        CommandLog {
            commands: self.driver.state.commands.clone(),
        }
    }

    pub fn events(&self) -> &[DriverEventOut] {
        &self.driver.state.events
    }

    pub fn wake_hints(&self) -> &[WakeHint] {
        &self.driver.state.wakes
    }

    pub fn actor_messages(&self) -> &[ActorMessage] {
        &self.driver.state.actor_messages
    }

    pub fn stream_writes(&self, edge_id: EdgeId) -> Vec<StreamWrite> {
        self.driver
            .state
            .stream_writes
            .iter()
            .filter(|write| write.edge_id == edge_id)
            .cloned()
            .collect()
    }

    pub fn stream_reads_started(&self, stream_id: StreamId) -> bool {
        self.driver.state.read_started.contains(&stream_id)
    }

    pub fn is_reading_stream(&self, edge_id: EdgeId) -> bool {
        self.driver
            .state
            .recvs
            .get(&edge_id)
            .map(|recv| recv.reading)
            .unwrap_or(false)
    }

    pub fn ring_commit(&self, edge_id: EdgeId) -> usize {
        self.driver
            .state
            .recvs
            .get(&edge_id)
            .map(|recv| recv.commit_cursor)
            .unwrap_or(0)
    }

    pub fn ring_consume(&self, edge_id: EdgeId) -> usize {
        self.driver
            .state
            .sends
            .get(&edge_id)
            .map(|send| send.consume_cursor)
            .unwrap_or(0)
    }
}
