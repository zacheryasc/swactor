#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RingId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArenaBase(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessLocalPointer(pub u64);

impl ProcessLocalPointer {
    pub fn from_base_plus_offset(base: ArenaBase, offset: u64) -> Self {
        Self(base.0 + offset)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingConfig {
    pub node_id: NodeId,
    pub ring_id: RingId,
    pub capacity: u64,
    pub producer: EndpointId,
    pub consumer: EndpointId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingIdentity {
    pub ring_id: RingId,
    pub capacity: u64,
    pub producer_count: u32,
    pub consumer_count: u32,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorSnapshot {
    pub commit: u64,
    pub consume: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeHint {
    RingReadable { ring_id: RingId },
    RingWritable { ring_id: RingId },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReserveError {
    InsufficientSpace,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadError {
    BeyondCommittedBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reservation {
    start: u64,
    len: u64,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StaleWake {
    ring_id: RingId,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WakeDelivery {
    pub was_accepted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchedulerState {
    pub readable_rings: std::collections::BTreeSet<RingId>,
    pub writable_rings: std::collections::BTreeSet<RingId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingLayout {
    pub data_offset: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessLocalView {
    pub layout: RingLayout,
    pub data_pointer: ProcessLocalPointer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PythonOperation {
    ReserveViaHelper { ring_id: RingId },
    CommitViaHelper { ring_id: RingId },
    ReadableViaHelper { ring_id: RingId },
    ConsumeViaHelper { ring_id: RingId },
    MapPointerViaHelper { ring_id: RingId },
    DirectAtomicAccess { ring_id: RingId },
    DirectWrapArithmetic { ring_id: RingId },
}

#[cfg(test)]
pub struct RingHelperHarness {
    identity: RingIdentity,
    buffer: Vec<u8>,
    commit: u64,
    consume: u64,
    retired: bool,
    wake_hints: Vec<WakeHint>,
    wake_log: Vec<WakeDelivery>,
    scheduler_state: SchedulerState,
}

#[cfg(test)]
impl RingHelperHarness {
    pub fn create(config: RingConfig) -> Self {
        Self::with_generation(config.ring_id, config.capacity, 1)
    }

    pub fn create_replacement(ring_id: RingId) -> Self {
        Self::with_generation(ring_id, 8, 2)
    }

    fn with_generation(ring_id: RingId, capacity: u64, generation: u64) -> Self {
        Self {
            identity: RingIdentity {
                ring_id,
                capacity,
                producer_count: 1,
                consumer_count: 1,
                generation,
            },
            buffer: vec![0; capacity as usize],
            commit: 0,
            consume: 0,
            retired: false,
            wake_hints: Vec::new(),
            wake_log: Vec::new(),
            scheduler_state: SchedulerState {
                readable_rings: std::collections::BTreeSet::new(),
                writable_rings: std::collections::BTreeSet::new(),
            },
        }
    }

    pub fn identity(&self) -> RingIdentity {
        self.identity.clone()
    }

    pub fn retire_and_capture_stale_wake(&mut self) -> StaleWake {
        self.retired = true;
        StaleWake {
            ring_id: self.identity.ring_id,
            generation: self.identity.generation,
        }
    }

    pub fn deliver_wake(&mut self, wake: StaleWake) {
        self.wake_log.push(WakeDelivery {
            was_accepted: wake.ring_id == self.identity.ring_id
                && wake.generation == self.identity.generation
                && !self.retired,
        });
    }

    pub fn wake_log(&self) -> &[WakeDelivery] {
        &self.wake_log
    }

    pub fn producer_reserve(&self, len: u64) -> Result<Reservation, ReserveError> {
        let used = self.commit.saturating_sub(self.consume);
        if len <= self.identity.capacity.saturating_sub(used) {
            Ok(Reservation {
                start: self.commit,
                len,
                generation: self.identity.generation,
            })
        } else {
            Err(ReserveError::InsufficientSpace)
        }
    }

    pub fn producer_write(&mut self, reservation: &Reservation, bytes: &[u8]) {
        assert_eq!(reservation.generation, self.identity.generation);
        assert_eq!(reservation.len as usize, bytes.len());
        for (i, byte) in bytes.iter().copied().enumerate() {
            let idx = (reservation.start + i as u64) % self.identity.capacity;
            self.buffer[idx as usize] = byte;
        }
    }

    pub fn producer_commit(&mut self, reservation: Reservation) {
        assert_eq!(reservation.start, self.commit);
        self.commit = self.commit.saturating_add(reservation.len);
        self.wake_hints.push(WakeHint::RingReadable {
            ring_id: self.identity.ring_id,
        });
        self.scheduler_state
            .readable_rings
            .insert(self.identity.ring_id);
    }

    pub fn consumer_readable(&self) -> u64 {
        self.commit.saturating_sub(self.consume)
    }

    pub fn consumer_try_read(&self, len: u64) -> Result<Vec<u8>, ReadError> {
        if len > self.consumer_readable() {
            return Err(ReadError::BeyondCommittedBytes);
        }
        Ok(self.consumer_read(len))
    }

    pub fn consumer_read(&self, len: u64) -> Vec<u8> {
        assert!(len <= self.consumer_readable());
        (0..len)
            .map(|i| self.buffer[((self.consume + i) % self.identity.capacity) as usize])
            .collect()
    }

    pub fn consumer_consume(&mut self, len: u64) {
        assert!(len <= self.consumer_readable());
        self.consume = self.consume.saturating_add(len);
        self.wake_hints.push(WakeHint::RingWritable {
            ring_id: self.identity.ring_id,
        });
        self.scheduler_state
            .writable_rings
            .insert(self.identity.ring_id);
    }

    pub fn cursor_snapshot(&self) -> CursorSnapshot {
        CursorSnapshot {
            commit: self.commit,
            consume: self.consume,
        }
    }

    pub fn wake_hints(&self) -> &[WakeHint] {
        &self.wake_hints
    }

    pub fn coalesce_duplicate_wakes(&mut self) {
        for wake in &self.wake_hints {
            match wake {
                WakeHint::RingReadable { ring_id } => {
                    self.scheduler_state.readable_rings.insert(*ring_id);
                }
                WakeHint::RingWritable { ring_id } => {
                    self.scheduler_state.writable_rings.insert(*ring_id);
                }
            }
        }
    }

    pub fn scheduler_state(&self) -> &SchedulerState {
        &self.scheduler_state
    }

    pub fn map_process_local_view(&self, base: ArenaBase) -> ProcessLocalView {
        let layout = RingLayout { data_offset: 128 };
        ProcessLocalView {
            layout,
            data_pointer: ProcessLocalPointer::from_base_plus_offset(base, layout.data_offset),
        }
    }

    pub fn python_visible_operations(&self) -> Vec<PythonOperation> {
        vec![
            PythonOperation::ReserveViaHelper {
                ring_id: self.identity.ring_id,
            },
            PythonOperation::CommitViaHelper {
                ring_id: self.identity.ring_id,
            },
            PythonOperation::ReadableViaHelper {
                ring_id: self.identity.ring_id,
            },
            PythonOperation::ConsumeViaHelper {
                ring_id: self.identity.ring_id,
            },
            PythonOperation::MapPointerViaHelper {
                ring_id: self.identity.ring_id,
            },
        ]
    }
}
