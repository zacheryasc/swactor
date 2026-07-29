#![allow(dead_code)]

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct EdgeId(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RingId(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StreamId(pub(crate) u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DriverEventOut {
    DriverEdgeReady { edge_id: EdgeId },
    StreamFault { edge_id: EdgeId },
    PumpStopped { edge_id: EdgeId, ring_id: RingId },
}

#[derive(Debug)]
pub(crate) struct Driver {
    sends: BTreeMap<EdgeId, RingId>,
    recv_specs: BTreeMap<EdgeId, RingId>,
    pending_streams: BTreeMap<EdgeId, StreamId>,
    recvs: BTreeMap<EdgeId, RingId>,
    events: Vec<DriverEventOut>,
}

impl Driver {
    pub(crate) fn new() -> Self {
        Self {
            sends: BTreeMap::new(),
            recv_specs: BTreeMap::new(),
            pending_streams: BTreeMap::new(),
            recvs: BTreeMap::new(),
            events: Vec::new(),
        }
    }

    pub(crate) fn establish_send(&mut self, edge_id: EdgeId, ring_id: RingId) {
        self.sends.insert(edge_id, ring_id);
        self.events
            .push(DriverEventOut::DriverEdgeReady { edge_id });
    }

    pub(crate) fn establish_recv(&mut self, edge_id: EdgeId, ring_id: RingId) {
        self.recv_specs.insert(edge_id, ring_id);
        if let Some(stream_id) = self.pending_streams.remove(&edge_id) {
            self.spawn_recv(edge_id, stream_id);
        }
    }

    pub(crate) fn incoming_uni_stream(&mut self, edge_id: EdgeId, stream_id: StreamId) {
        if self.recv_specs.contains_key(&edge_id) {
            self.spawn_recv(edge_id, stream_id);
        } else {
            self.pending_streams.insert(edge_id, stream_id);
        }
    }

    fn spawn_recv(&mut self, edge_id: EdgeId, stream_id: StreamId) {
        let Some(ring_id) = self.recv_specs.get(&edge_id).copied() else {
            self.pending_streams.insert(edge_id, stream_id);
            return;
        };
        self.recvs.insert(edge_id, ring_id);
        self.events
            .push(DriverEventOut::DriverEdgeReady { edge_id });
    }

    pub(crate) fn read_error(&mut self, edge_id: EdgeId) {
        self.events.push(DriverEventOut::StreamFault { edge_id });
    }

    pub(crate) fn stop_edge(&mut self, edge_id: EdgeId) {
        let ring_id = self
            .sends
            .get(&edge_id)
            .copied()
            .or_else(|| self.recvs.get(&edge_id).copied())
            .or_else(|| self.recv_specs.get(&edge_id).copied())
            .unwrap_or(RingId(0));

        self.sends.remove(&edge_id);
        self.recvs.remove(&edge_id);
        self.recv_specs.remove(&edge_id);
        self.pending_streams.remove(&edge_id);
        self.events
            .push(DriverEventOut::PumpStopped { edge_id, ring_id });
    }
    pub(crate) fn events(&self) -> &[DriverEventOut] {
        &self.events
    }
}
