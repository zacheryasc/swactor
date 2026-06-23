//! Black-box contract tests for MVP device bridge behavior.
//!
//! These tests intentionally know only the public device-bridge surface:
//!
//! - allocation, host/device copy, view, compute-use, restart, and free requests
//!   in
//! - fake backend calls, completion events, handles, and failures out
//!
//! They assert the guarantees in
//! `specs/mvp_system/device_bridge_contract.md`.

use mvp_system::device_bridge as device;

// The object spec is small and aligned so exact range checks are readable. The
// bridge remains free to choose backend-specific allocation details.
fn object_spec() -> device::ObjectSpec {
    device::ObjectSpec {
        max_extent: 16,
        alignment: 4,
        dtype: device::DType::U32,
        shape: device::Shape::Vector,
    }
}

// The harness records fake backend calls and public bridge outcomes. Tests do
// not inspect device memory or tinygrad internals.
fn new_bridge() -> device::DeviceBridgeHarness {
    device::DeviceBridgeHarness::new(device::WorkerGeneration(1))
}

// This helper allocates one current-generation device object for copy and view
// tests. It keeps tests on the public allocation path.
fn allocated_bridge() -> (device::DeviceBridgeHarness, device::DeviceHandle) {
    let mut harness = new_bridge();
    let handle = harness
        .alloc_device(object_spec(), 8)
        .expect("valid allocation must succeed");
    (harness, handle)
}

// This proves alloc_device creates a current-generation allocation suitable for
// the object spec and extent, and allocation failure is reported as device
// allocation failure.
#[test]
fn allocation_creates_current_generation_handle_or_typed_failure() {
    // Allocate a valid object.
    let mut harness = new_bridge();
    let handle = harness
        .alloc_device(object_spec(), 8)
        .expect("valid allocation must succeed");

    // The handle is tied to the current worker generation.
    assert_eq!(handle.generation, device::WorkerGeneration(1));
    assert!(harness.backend_calls().iter().any(|call| {
        matches!(
            call,
            device::BackendCall::Alloc {
                spec,
                extent: 8,
                ..
            } if *spec == object_spec()
        )
    }));

    // Backend allocation failure becomes typed allocation failure.
    harness.inject_backend_failure(device::BackendFailure::AllocationFailed);
    let failure = harness
        .alloc_device(object_spec(), 8)
        .expect_err("backend allocation failure must surface");
    assert_eq!(failure, device::DeviceError::AllocationFailed);
}

// This proves host_to_device copies exactly the requested host range to the
// requested device range, and bytes become safe to release only after the copy
// returns or the asynchronous completion event fires.
#[test]
fn host_to_device_copies_exact_range_and_defers_release_until_safe() {
    // Allocate a device object and request an asynchronous copy.
    let (mut harness, handle) = allocated_bridge();
    let copy = harness
        .host_to_device(
            device::HostRange { offset: 4, len: 8 },
            device::DeviceRange {
                handle,
                offset: 0,
                len: 8,
            },
            device::CopyMode::Async,
        )
        .expect("copy request must be accepted");

    // The backend sees the exact ranges.
    assert!(harness.backend_calls().iter().any(|call| {
        matches!(
            call,
            device::BackendCall::HostToDevice {
                host: device::HostRange { offset: 4, len: 8 },
                device: device::DeviceRange {
                    offset: 0,
                    len: 8,
                    ..
                },
                ..
            }
        )
    }));

    // Async copy request alone is not a release proof.
    assert!(!harness.safe_to_release_host(copy));

    // Completion makes bytes safe to release.
    harness.observe(device::DeviceEvent::CopyCompleted { copy });
    assert!(harness.safe_to_release_host(copy));
}

// This proves device_to_host copies exactly the requested device range to the
// requested host range, and host bytes become valid only after return or copy
// completion.
#[test]
fn device_to_host_copies_exact_range_and_defers_host_validity_until_safe() {
    // Allocate a device object and request an asynchronous device-to-host copy.
    let (mut harness, handle) = allocated_bridge();
    let copy = harness
        .device_to_host(
            device::DeviceRange {
                handle,
                offset: 0,
                len: 8,
            },
            device::HostRange { offset: 12, len: 8 },
            device::CopyMode::Async,
        )
        .expect("copy request must be accepted");

    // The backend sees the exact ranges.
    assert!(harness.backend_calls().iter().any(|call| {
        matches!(
            call,
            device::BackendCall::DeviceToHost {
                device: device::DeviceRange {
                    offset: 0,
                    len: 8,
                    ..
                },
                host: device::HostRange { offset: 12, len: 8 },
                ..
            }
        )
    }));

    // Host bytes are invalid until completion.
    assert!(!harness.host_bytes_valid(copy));
    harness.observe(device::DeviceEvent::CopyCompleted { copy });
    assert!(harness.host_bytes_valid(copy));
}

// This proves wrap_for_tinygrad creates a compatible view matching the role
// tensor spec, and invalid view shape or dtype fails the step.
#[test]
fn tinygrad_view_matches_role_tensor_spec_or_fails_step() {
    // Allocate a valid object and wrap it for a matching role tensor view.
    let (mut harness, handle) = allocated_bridge();
    let view = harness
        .wrap_for_tinygrad(
            handle,
            device::TensorViewSpec {
                dtype: device::DType::U32,
                shape: device::Shape::Vector,
            },
        )
        .expect("matching tensor view must succeed");
    assert_eq!(view.dtype, device::DType::U32);
    assert_eq!(view.shape, device::Shape::Vector);

    // Invalid dtype fails the step at the bridge boundary.
    let failure = harness
        .wrap_for_tinygrad(
            handle,
            device::TensorViewSpec {
                dtype: device::DType::F16,
                shape: device::Shape::Vector,
            },
        )
        .expect_err("invalid dtype must fail");
    assert_eq!(failure, device::DeviceError::InvalidViewDType);
}

// This proves allocations are not freed while compute or copy events depend on
// them, free_device releases after dependencies clear, restart invalidates old
// handles, and old-generation handles are rejected.
#[test]
fn lifetime_blocks_free_until_dependencies_clear_and_rejects_old_generation() {
    // Allocate a handle and mark it used by compute and copy.
    let (mut harness, handle) = allocated_bridge();
    harness.observe(device::DeviceEvent::ComputeStarted {
        handle,
        step_id: device::StepId(77),
    });
    let copy = harness
        .host_to_device(
            device::HostRange { offset: 0, len: 8 },
            device::DeviceRange {
                handle,
                offset: 0,
                len: 8,
            },
            device::CopyMode::Async,
        )
        .expect("copy request must be accepted");

    // Free is blocked while compute/copy depends on the allocation.
    assert_eq!(
        harness.free_device(handle),
        Err(device::DeviceError::AllocationStillInUse)
    );

    // Complete dependencies, then free succeeds.
    harness.observe(device::DeviceEvent::ComputeCompleted {
        handle,
        step_id: device::StepId(77),
    });
    harness.observe(device::DeviceEvent::CopyCompleted { copy });
    assert_eq!(harness.free_device(handle), Ok(()));
    assert!(
        harness.backend_calls().iter().any(|call| {
            matches!(call, device::BackendCall::Free { freed } if *freed == handle)
        })
    );

    // Restart invalidates prior handles.
    harness.observe(device::DeviceEvent::WorkerRestarted {
        generation: device::WorkerGeneration(2),
    });
    assert_eq!(
        harness.free_device(handle),
        Err(device::DeviceError::OldGenerationHandle)
    );
}

// This proves copy and view backend failures surface as typed copy or step
// failures rather than ambiguous panics or logs.
#[test]
fn backend_copy_and_view_failures_are_typed() {
    // Copy failure is reported as device copy failure.
    let (mut harness, handle) = allocated_bridge();
    harness.inject_backend_failure(device::BackendFailure::CopyFailed);
    let copy_failure = harness
        .host_to_device(
            device::HostRange { offset: 0, len: 8 },
            device::DeviceRange {
                handle,
                offset: 0,
                len: 8,
            },
            device::CopyMode::Sync,
        )
        .expect_err("copy failure must surface");
    assert_eq!(copy_failure, device::DeviceError::DeviceCopyFailed);

    // View failure is reported as a step-visible invalid view.
    harness.inject_backend_failure(device::BackendFailure::InvalidView);
    let view_failure = harness
        .wrap_for_tinygrad(
            handle,
            device::TensorViewSpec {
                dtype: device::DType::U32,
                shape: device::Shape::Vector,
            },
        )
        .expect_err("invalid backend view must surface");
    assert_eq!(view_failure, device::DeviceError::InvalidTensorView);
}
