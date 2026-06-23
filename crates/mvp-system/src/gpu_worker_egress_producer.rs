#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerGeneration(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RingId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortId(pub String);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StepId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceHandle {
    pub generation: WorkerGeneration,
    pub id: u64,
}

impl DeviceHandle {
    pub fn new(generation: WorkerGeneration, id: u64) -> Self {
        Self { generation, id }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingDirection {
    Ingress,
    Egress,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectLayout {
    Token,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectSpec {
    pub max_extent: u64,
    pub alignment: u64,
    pub layout: ObjectLayout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallRing {
    pub ring_id: RingId,
    pub edge_id: EdgeId,
    pub port_id: PortId,
    pub direction: RingDirection,
    pub object_spec: ObjectSpec,
    pub generation: WorkerGeneration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ObjectFlags {
    pub end_of_sequence: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputBinding {
    pub ring_id: RingId,
    pub object_id: ObjectId,
    pub sequence: u64,
    pub extent: u64,
    pub flags: ObjectFlags,
    pub device_source: DeviceHandle,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerEgressEvent {
    InstallRing(InstallRing),
    ExecuteStep {
        step_id: StepId,
        outputs: Vec<OutputBinding>,
    },
    HeaderReady {
        object_id: ObjectId,
    },
    EgressRingFull {
        ring_id: RingId,
    },
    RingWritable {
        ring_id: RingId,
    },
    DeviceToHostCopyCompleted {
        object_id: ObjectId,
        byte_count: u64,
    },
    DeviceCopyFailed {
        object_id: ObjectId,
    },
    RoleStateUpdated {
        step_id: StepId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepFailureReason {
    InvalidOutputRing,
    OutputExtentViolation,
    DeviceCopyFailed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerEgressOut {
    ObjectProduced {
        ring_id: RingId,
        object_id: ObjectId,
        sequence: u64,
    },
    StepCompleted {
        step_id: StepId,
    },
    StepFailed {
        step_id: StepId,
        reason: StepFailureReason,
    },
    RingFault {
        ring_id: RingId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeHint {
    RingReadable { ring_id: RingId },
    RingWritable { ring_id: RingId },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectHeader {
    pub object_id: ObjectId,
    pub sequence: u64,
    pub extent: u64,
}

impl ObjectHeader {
    pub fn decode(bytes: &[u8]) -> Result<Self, HeaderDecodeError> {
        if bytes.len() < HEADER_LEN || &bytes[0..4] != b"MO01" || bytes[4] != 1 {
            return Err(HeaderDecodeError);
        }
        Ok(Self {
            object_id: ObjectId(u64::from_le_bytes(bytes[8..16].try_into().unwrap())),
            sequence: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            extent: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderDecodeError;

const HEADER_LEN: usize = 48;

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingStep {
    step_id: StepId,
    outputs: Vec<OutputBinding>,
    role_state_updated: bool,
}

#[cfg(test)]
pub struct EgressProducerHarness {
    generation: WorkerGeneration,
    rings: std::collections::BTreeMap<RingId, InstallRing>,
    pending_outputs: Vec<OutputBinding>,
    steps: Vec<PendingStep>,
    committed: std::collections::BTreeMap<RingId, Vec<u8>>,
    payload_committed: std::collections::BTreeMap<RingId, u64>,
    full_rings: std::collections::BTreeSet<RingId>,
    produced: std::collections::BTreeSet<ObjectId>,
    wake_hints: Vec<WakeHint>,
    events: Vec<WorkerEgressOut>,
}

#[cfg(test)]
impl EgressProducerHarness {
    pub fn new(generation: WorkerGeneration) -> Self {
        Self {
            generation,
            rings: std::collections::BTreeMap::new(),
            pending_outputs: Vec::new(),
            steps: Vec::new(),
            committed: std::collections::BTreeMap::new(),
            payload_committed: std::collections::BTreeMap::new(),
            full_rings: std::collections::BTreeSet::new(),
            produced: std::collections::BTreeSet::new(),
            wake_hints: Vec::new(),
            events: Vec::new(),
        }
    }

    pub fn observe(&mut self, event: WorkerEgressEvent) {
        match event {
            WorkerEgressEvent::InstallRing(install) => {
                if install.direction == RingDirection::Egress
                    && install.generation == self.generation
                {
                    self.rings.insert(install.ring_id, install);
                }
            }
            WorkerEgressEvent::ExecuteStep { step_id, outputs } => {
                self.execute_step(step_id, outputs)
            }
            WorkerEgressEvent::HeaderReady { object_id } => self.header_ready(object_id),
            WorkerEgressEvent::EgressRingFull { ring_id } => {
                self.full_rings.insert(ring_id);
            }
            WorkerEgressEvent::RingWritable { ring_id } => {
                self.full_rings.remove(&ring_id);
                self.wake_hints.push(WakeHint::RingWritable { ring_id });
            }
            WorkerEgressEvent::DeviceToHostCopyCompleted {
                object_id,
                byte_count,
            } => self.copy_completed(object_id, byte_count),
            WorkerEgressEvent::DeviceCopyFailed { object_id } => self.copy_failed(object_id),
            WorkerEgressEvent::RoleStateUpdated { step_id } => {
                if let Some(step) = self.steps.iter_mut().find(|step| step.step_id == step_id) {
                    step.role_state_updated = true;
                }
                self.maybe_step_completed(step_id);
            }
        }
    }

    pub fn pending_outputs(&self) -> &[OutputBinding] {
        &self.pending_outputs
    }

    pub fn committed_bytes(&self, ring_id: RingId) -> &[u8] {
        self.committed
            .get(&ring_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn committed_payload_bytes(&self, ring_id: RingId) -> u64 {
        self.payload_committed.get(&ring_id).copied().unwrap_or(0)
    }

    pub fn wake_hints(&self) -> &[WakeHint] {
        &self.wake_hints
    }

    pub fn events(&self) -> &[WorkerEgressOut] {
        &self.events
    }

    pub fn complete_output(&mut self, object_id: ObjectId) {
        self.header_ready(object_id);
        let Some(output) = self
            .pending_outputs
            .iter()
            .find(|output| output.object_id == object_id)
            .copied()
        else {
            return;
        };
        self.copy_completed(object_id, output.extent);
    }

    fn execute_step(&mut self, step_id: StepId, outputs: Vec<OutputBinding>) {
        for output in &outputs {
            let Some(ring) = self.rings.get(&output.ring_id) else {
                self.events.push(WorkerEgressOut::StepFailed {
                    step_id,
                    reason: StepFailureReason::InvalidOutputRing,
                });
                return;
            };
            if output.extent > ring.object_spec.max_extent
                || (ring.object_spec.alignment != 0
                    && output.extent % ring.object_spec.alignment != 0)
            {
                self.events.push(WorkerEgressOut::StepFailed {
                    step_id,
                    reason: StepFailureReason::OutputExtentViolation,
                });
                return;
            }
        }
        self.pending_outputs.extend(outputs.iter().copied());
        self.steps.push(PendingStep {
            step_id,
            outputs,
            role_state_updated: false,
        });
    }

    fn header_ready(&mut self, object_id: ObjectId) {
        let Some(output) = self
            .pending_outputs
            .iter()
            .find(|output| output.object_id == object_id)
            .copied()
        else {
            return;
        };
        let bytes = encode_header(output);
        self.committed
            .entry(output.ring_id)
            .or_default()
            .extend(bytes);
        self.wake_hints.push(WakeHint::RingReadable {
            ring_id: output.ring_id,
        });
    }

    fn copy_completed(&mut self, object_id: ObjectId, byte_count: u64) {
        let Some(output) = self
            .pending_outputs
            .iter()
            .find(|output| output.object_id == object_id)
            .copied()
        else {
            return;
        };
        if self.full_rings.contains(&output.ring_id) || byte_count != output.extent {
            return;
        }
        self.committed
            .entry(output.ring_id)
            .or_default()
            .extend(std::iter::repeat(0).take(byte_count as usize));
        *self.payload_committed.entry(output.ring_id).or_insert(0) += byte_count;
        if self.produced.insert(object_id) {
            self.events.push(WorkerEgressOut::ObjectProduced {
                ring_id: output.ring_id,
                object_id,
                sequence: output.sequence,
            });
        }
        let step_ids = self
            .steps
            .iter()
            .filter(|step| {
                step.outputs
                    .iter()
                    .any(|output| output.object_id == object_id)
            })
            .map(|step| step.step_id)
            .collect::<Vec<_>>();
        for step_id in step_ids {
            self.maybe_step_completed(step_id);
        }
    }

    fn copy_failed(&mut self, object_id: ObjectId) {
        let step_id = self
            .steps
            .iter()
            .find(|step| {
                step.outputs
                    .iter()
                    .any(|output| output.object_id == object_id)
            })
            .map(|step| step.step_id)
            .unwrap_or(StepId(0));
        self.events.push(WorkerEgressOut::StepFailed {
            step_id,
            reason: StepFailureReason::DeviceCopyFailed,
        });
    }

    fn maybe_step_completed(&mut self, step_id: StepId) {
        let Some(step) = self.steps.iter().find(|step| step.step_id == step_id) else {
            return;
        };
        if !step.role_state_updated {
            return;
        }
        if step.outputs.iter().all(|output| self.produced.contains(&output.object_id))
            && !self.events.iter().any(|event| matches!(event, WorkerEgressOut::StepCompleted { step_id: seen } if *seen == step_id))
        {
            self.events.push(WorkerEgressOut::StepCompleted { step_id });
        }
    }
}

fn encode_header(output: OutputBinding) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN);
    out.extend_from_slice(b"MO01");
    out.push(1);
    out.push(HEADER_LEN as u8);
    out.extend_from_slice(&[0u8; 2]);
    out.extend_from_slice(&output.object_id.0.to_le_bytes());
    out.extend_from_slice(&output.sequence.to_le_bytes());
    out.extend_from_slice(&output.extent.to_le_bytes());
    out.extend_from_slice(&[0u8; 16]);
    out
}
