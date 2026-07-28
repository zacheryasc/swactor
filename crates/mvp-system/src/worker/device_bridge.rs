use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerGeneration(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StepId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CopyEvent(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceHandle {
    pub generation: WorkerGeneration,
    pub id: u64,
}

impl DeviceHandle {
    pub const fn new(generation: WorkerGeneration, id: u64) -> Self {
        Self { generation, id }
    }
}

pub type DeviceAllocation = DeviceHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DType {
    U32,
    F16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    Vector,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectSpec {
    pub max_extent: u64,
    pub alignment: u64,
    pub dtype: DType,
    pub shape: Shape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TensorViewSpec {
    pub dtype: DType,
    pub shape: Shape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TensorView {
    pub handle: DeviceHandle,
    pub dtype: DType,
    pub shape: Shape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostRange {
    pub offset: u64,
    pub len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceRange {
    pub handle: DeviceHandle,
    pub offset: u64,
    pub len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyMode {
    Sync,
    Async,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceError {
    AllocationFailed,
    InvalidExtent,
    InvalidAlignment,
    InvalidRange,
    UnknownDeviceHandle,
    OldGenerationHandle,
    DeviceCopyFailed,
    InvalidViewDType,
    InvalidTensorView,
    AllocationStillInUse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceEvent {
    CopyCompleted {
        copy: CopyEvent,
    },
    ComputeStarted {
        handle: DeviceHandle,
        step_id: StepId,
    },
    ComputeCompleted {
        handle: DeviceHandle,
        step_id: StepId,
    },
    WorkerRestarted {
        generation: WorkerGeneration,
    },
}

pub trait DeviceBridgeBackend {
    fn alloc_device(
        &mut self,
        allocation: DeviceAllocation,
        spec: ObjectSpec,
        extent: u64,
    ) -> Result<(), DeviceError>;

    fn free_device(&mut self, allocation: DeviceAllocation) -> Result<(), DeviceError>;

    fn host_to_device(
        &mut self,
        host: HostRange,
        device: DeviceRange,
        copy: CopyEvent,
        mode: CopyMode,
    ) -> Result<(), DeviceError>;

    fn device_to_host(
        &mut self,
        device: DeviceRange,
        host: HostRange,
        copy: CopyEvent,
        mode: CopyMode,
    ) -> Result<(), DeviceError>;

    fn wrap_for_tinygrad(
        &mut self,
        allocation: DeviceAllocation,
        view: TensorViewSpec,
    ) -> Result<TensorView, DeviceError>;
}

pub struct DeviceBridge<B> {
    current_generation: WorkerGeneration,
    next_handle_id: u64,
    next_copy_id: u64,
    allocations: BTreeMap<DeviceHandle, AllocationRecord>,
    copies: BTreeMap<CopyEvent, CopyRecord>,
    backend: B,
}

impl<B: DeviceBridgeBackend> DeviceBridge<B> {
    pub fn new(current_generation: WorkerGeneration, backend: B) -> Self {
        Self {
            current_generation,
            next_handle_id: 1,
            next_copy_id: 1,
            allocations: BTreeMap::new(),
            copies: BTreeMap::new(),
            backend,
        }
    }

    pub fn alloc_device(
        &mut self,
        spec: ObjectSpec,
        extent: u64,
    ) -> Result<DeviceAllocation, DeviceError> {
        validate_object_spec(spec, extent)?;

        let allocation = DeviceHandle::new(self.current_generation, self.next_handle_id);
        self.next_handle_id += 1;
        self.backend.alloc_device(allocation, spec, extent)?;
        self.allocations.insert(
            allocation,
            AllocationRecord {
                spec,
                extent,
                active_copies: BTreeSet::new(),
                active_steps: BTreeSet::new(),
            },
        );
        Ok(allocation)
    }

    pub fn free_device(&mut self, allocation: DeviceAllocation) -> Result<(), DeviceError> {
        self.validate_generation(allocation)?;
        let record = self
            .allocations
            .get(&allocation)
            .ok_or(DeviceError::UnknownDeviceHandle)?;
        if !record.active_copies.is_empty() || !record.active_steps.is_empty() {
            return Err(DeviceError::AllocationStillInUse);
        }

        self.backend.free_device(allocation)?;
        self.allocations.remove(&allocation);
        Ok(())
    }

    pub fn host_to_device(
        &mut self,
        host: HostRange,
        device: DeviceRange,
        mode: CopyMode,
    ) -> Result<CopyEvent, DeviceError> {
        self.validate_device_range(device)?;
        validate_equal_copy_len(host.len, device.len)?;

        let copy = self.next_copy_event();
        self.backend.host_to_device(host, device, copy, mode)?;
        self.record_copy(copy, device.handle, CopyDirection::HostToDevice, mode);
        Ok(copy)
    }

    pub fn device_to_host(
        &mut self,
        device: DeviceRange,
        host: HostRange,
        mode: CopyMode,
    ) -> Result<CopyEvent, DeviceError> {
        self.validate_device_range(device)?;
        validate_equal_copy_len(device.len, host.len)?;

        let copy = self.next_copy_event();
        self.backend.device_to_host(device, host, copy, mode)?;
        self.record_copy(copy, device.handle, CopyDirection::DeviceToHost, mode);
        Ok(copy)
    }

    pub fn wrap_for_tinygrad(
        &mut self,
        allocation: DeviceAllocation,
        view: TensorViewSpec,
    ) -> Result<TensorView, DeviceError> {
        let record = self.allocation_record(allocation)?;
        if view.dtype != record.spec.dtype {
            return Err(DeviceError::InvalidViewDType);
        }
        if view.shape != record.spec.shape {
            return Err(DeviceError::InvalidTensorView);
        }

        self.backend.wrap_for_tinygrad(allocation, view)
    }

    pub fn observe(&mut self, event: DeviceEvent) {
        match event {
            DeviceEvent::CopyCompleted { copy } => self.complete_copy(copy),
            DeviceEvent::ComputeStarted { handle, step_id } => {
                if let Some(record) = self.current_allocation_record_mut(handle) {
                    record.active_steps.insert(step_id);
                }
            }
            DeviceEvent::ComputeCompleted { handle, step_id } => {
                if let Some(record) = self.current_allocation_record_mut(handle) {
                    record.active_steps.remove(&step_id);
                }
            }
            DeviceEvent::WorkerRestarted { generation } => {
                self.current_generation = generation;
                self.next_handle_id = 1;
                self.next_copy_id = 1;
                self.allocations.clear();
                self.copies.clear();
            }
        }
    }

    pub fn safe_to_release_host(&self, copy: CopyEvent) -> bool {
        self.copies
            .get(&copy)
            .map(|record| record.direction == CopyDirection::HostToDevice && record.completed)
            .unwrap_or(false)
    }

    pub fn host_bytes_valid(&self, copy: CopyEvent) -> bool {
        self.copies
            .get(&copy)
            .map(|record| record.direction == CopyDirection::DeviceToHost && record.completed)
            .unwrap_or(false)
    }

    pub fn copy_event_complete(&self, copy: CopyEvent) -> bool {
        self.copies
            .get(&copy)
            .map(|record| record.completed)
            .unwrap_or(false)
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    fn allocation_record(
        &self,
        allocation: DeviceAllocation,
    ) -> Result<&AllocationRecord, DeviceError> {
        self.validate_generation(allocation)?;
        self.allocations
            .get(&allocation)
            .ok_or(DeviceError::UnknownDeviceHandle)
    }

    fn current_allocation_record_mut(
        &mut self,
        allocation: DeviceAllocation,
    ) -> Option<&mut AllocationRecord> {
        if allocation.generation != self.current_generation {
            return None;
        }
        self.allocations.get_mut(&allocation)
    }

    fn validate_generation(&self, allocation: DeviceAllocation) -> Result<(), DeviceError> {
        if allocation.generation != self.current_generation {
            return Err(DeviceError::OldGenerationHandle);
        }
        Ok(())
    }

    fn validate_device_range(&self, device: DeviceRange) -> Result<(), DeviceError> {
        let record = self.allocation_record(device.handle)?;
        let end = device
            .offset
            .checked_add(device.len)
            .ok_or(DeviceError::InvalidRange)?;
        if end > record.extent {
            return Err(DeviceError::InvalidRange);
        }
        Ok(())
    }

    fn next_copy_event(&mut self) -> CopyEvent {
        let copy = CopyEvent(self.next_copy_id);
        self.next_copy_id += 1;
        copy
    }

    fn record_copy(
        &mut self,
        copy: CopyEvent,
        allocation: DeviceAllocation,
        direction: CopyDirection,
        mode: CopyMode,
    ) {
        let completed = mode == CopyMode::Sync;
        self.copies.insert(
            copy,
            CopyRecord {
                allocation,
                direction,
                completed,
            },
        );
        if !completed {
            if let Some(record) = self.allocations.get_mut(&allocation) {
                record.active_copies.insert(copy);
            }
        }
    }

    fn complete_copy(&mut self, copy: CopyEvent) {
        let Some(record) = self.copies.get_mut(&copy) else {
            return;
        };
        record.completed = true;
        let allocation = record.allocation;
        if let Some(allocation_record) = self.allocations.get_mut(&allocation) {
            allocation_record.active_copies.remove(&copy);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AllocationRecord {
    spec: ObjectSpec,
    extent: u64,
    active_copies: BTreeSet<CopyEvent>,
    active_steps: BTreeSet<StepId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CopyDirection {
    HostToDevice,
    DeviceToHost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CopyRecord {
    allocation: DeviceAllocation,
    direction: CopyDirection,
    completed: bool,
}

fn validate_object_spec(spec: ObjectSpec, extent: u64) -> Result<(), DeviceError> {
    if spec.alignment == 0 {
        return Err(DeviceError::InvalidAlignment);
    }
    if extent > spec.max_extent || extent % spec.alignment != 0 {
        return Err(DeviceError::InvalidExtent);
    }
    Ok(())
}

fn validate_equal_copy_len(left: u64, right: u64) -> Result<(), DeviceError> {
    if left != right {
        return Err(DeviceError::InvalidRange);
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendFailure {
    AllocationFailed,
    CopyFailed,
    InvalidView,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackendCall {
    Alloc {
        allocation: DeviceAllocation,
        spec: ObjectSpec,
        extent: u64,
    },
    Free {
        freed: DeviceAllocation,
    },
    HostToDevice {
        host: HostRange,
        device: DeviceRange,
        copy: CopyEvent,
        mode: CopyMode,
    },
    DeviceToHost {
        device: DeviceRange,
        host: HostRange,
        copy: CopyEvent,
        mode: CopyMode,
    },
    WrapForTinygrad {
        allocation: DeviceAllocation,
        view: TensorViewSpec,
    },
}

#[cfg(test)]
#[derive(Default)]
pub struct MockDeviceBackend {
    calls: Vec<BackendCall>,
    next_failure: Option<BackendFailure>,
}

#[cfg(test)]
impl MockDeviceBackend {
    pub fn inject_failure(&mut self, failure: BackendFailure) {
        self.next_failure = Some(failure);
    }

    pub fn calls(&self) -> &[BackendCall] {
        &self.calls
    }

    fn take_failure(&mut self, failure: BackendFailure) -> bool {
        if self.next_failure == Some(failure) {
            self.next_failure = None;
            return true;
        }
        false
    }
}

#[cfg(test)]
impl DeviceBridgeBackend for MockDeviceBackend {
    fn alloc_device(
        &mut self,
        allocation: DeviceAllocation,
        spec: ObjectSpec,
        extent: u64,
    ) -> Result<(), DeviceError> {
        self.calls.push(BackendCall::Alloc {
            allocation,
            spec,
            extent,
        });
        if self.take_failure(BackendFailure::AllocationFailed) {
            return Err(DeviceError::AllocationFailed);
        }
        Ok(())
    }

    fn free_device(&mut self, allocation: DeviceAllocation) -> Result<(), DeviceError> {
        self.calls.push(BackendCall::Free { freed: allocation });
        Ok(())
    }

    fn host_to_device(
        &mut self,
        host: HostRange,
        device: DeviceRange,
        copy: CopyEvent,
        mode: CopyMode,
    ) -> Result<(), DeviceError> {
        self.calls.push(BackendCall::HostToDevice {
            host,
            device,
            copy,
            mode,
        });
        if self.take_failure(BackendFailure::CopyFailed) {
            return Err(DeviceError::DeviceCopyFailed);
        }
        Ok(())
    }

    fn device_to_host(
        &mut self,
        device: DeviceRange,
        host: HostRange,
        copy: CopyEvent,
        mode: CopyMode,
    ) -> Result<(), DeviceError> {
        self.calls.push(BackendCall::DeviceToHost {
            device,
            host,
            copy,
            mode,
        });
        if self.take_failure(BackendFailure::CopyFailed) {
            return Err(DeviceError::DeviceCopyFailed);
        }
        Ok(())
    }

    fn wrap_for_tinygrad(
        &mut self,
        allocation: DeviceAllocation,
        view: TensorViewSpec,
    ) -> Result<TensorView, DeviceError> {
        self.calls
            .push(BackendCall::WrapForTinygrad { allocation, view });
        if self.take_failure(BackendFailure::InvalidView) {
            return Err(DeviceError::InvalidTensorView);
        }
        Ok(TensorView {
            handle: allocation,
            dtype: view.dtype,
            shape: view.shape,
        })
    }
}

#[cfg(test)]
pub struct DeviceBridgeHarness {
    bridge: DeviceBridge<MockDeviceBackend>,
}

#[cfg(test)]
impl DeviceBridgeHarness {
    pub fn new(generation: WorkerGeneration) -> Self {
        Self {
            bridge: DeviceBridge::new(generation, MockDeviceBackend::default()),
        }
    }

    pub fn alloc_device(
        &mut self,
        spec: ObjectSpec,
        extent: u64,
    ) -> Result<DeviceHandle, DeviceError> {
        self.bridge.alloc_device(spec, extent)
    }

    pub fn free_device(&mut self, allocation: DeviceAllocation) -> Result<(), DeviceError> {
        self.bridge.free_device(allocation)
    }

    pub fn host_to_device(
        &mut self,
        host: HostRange,
        device: DeviceRange,
        mode: CopyMode,
    ) -> Result<CopyEvent, DeviceError> {
        self.bridge.host_to_device(host, device, mode)
    }

    pub fn device_to_host(
        &mut self,
        device: DeviceRange,
        host: HostRange,
        mode: CopyMode,
    ) -> Result<CopyEvent, DeviceError> {
        self.bridge.device_to_host(device, host, mode)
    }

    pub fn wrap_for_tinygrad(
        &mut self,
        allocation: DeviceAllocation,
        view: TensorViewSpec,
    ) -> Result<TensorView, DeviceError> {
        self.bridge.wrap_for_tinygrad(allocation, view)
    }

    pub fn observe(&mut self, event: DeviceEvent) {
        self.bridge.observe(event);
    }

    pub fn safe_to_release_host(&self, copy: CopyEvent) -> bool {
        self.bridge.safe_to_release_host(copy)
    }

    pub fn host_bytes_valid(&self, copy: CopyEvent) -> bool {
        self.bridge.host_bytes_valid(copy)
    }

    pub fn copy_event_complete(&self, copy: CopyEvent) -> bool {
        self.bridge.copy_event_complete(copy)
    }

    pub fn inject_backend_failure(&mut self, failure: BackendFailure) {
        self.bridge.backend_mut().inject_failure(failure);
    }

    pub fn backend_calls(&self) -> &[BackendCall] {
        self.bridge.backend().calls()
    }
}
