use std::collections::HashMap;
use std::ffi::{CString, c_int, c_void};
#[cfg(all(feature = "test-host", target_os = "linux"))]
use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd, RawFd};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
#[cfg(all(feature = "test-host", target_os = "linux"))]
use std::time::Instant;

use data_plane::blob::{Blob, BlobView, ContentDigest, WritableArenaView};
#[cfg(all(feature = "test-host", target_os = "linux"))]
use data_plane::bootstrap::channel::{
    BootstrapCancellation, BootstrapHost, CHILD_BOOTSTRAP_FD, SessionBootstrap,
};
use data_plane::bootstrap::channel::{GuestAttachment, GuestBootstrap};
use data_plane::data_plane::{
    BlobWriter, DataPlane, DataPlaneBootstrap, Descriptor, DescriptorMapping, MapRequest,
    MapTarget, Protection, Sharing, StreamReader, StreamWriter,
};
use data_plane::namespace::EntryKind;
use data_plane::path::DataPath;
#[cfg(all(feature = "test-host", target_os = "linux"))]
use data_plane::protocol::SessionCapability;
use data_plane::protocol::{
    AccessMode, BlobAllocation, BlobFailure, DataPlaneError, DescriptorKind, Errno, OpenOptions,
    register_data_plane_codecs,
};
use distribution::node::DistributedNodeConfig;
use distribution::transport_bridge::{
    Outbox, OutboxRouteBinder, RelayMirror, RouteBinder, RouteView, RouteViewTransport,
};
use futures_lite::future;
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::{IrohDriver, IrohDriverConfig};
use parking_lot::Mutex as ParkingMutex;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyBufferError, PyOSError, PyPermissionError, PyRuntimeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyModule};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Runtime, RuntimeParts};
#[cfg(all(feature = "test-host", target_os = "linux"))]
use swactor_engine::BlockingWorkSender;
use swactor_engine::{ActorCompletion, Engine, EngineHandle, TokioBackend, TokioConfig};
use swactor_transport::{CodecRegistry, CodecRemoteSink, TransportRouter};

const ROUTE_POLL: Duration = Duration::from_millis(5);

/// Bound on how long the child waits for its data-plane route to the host to
/// become ready. A host that never connects must fail the guest bootstrap
/// instead of hanging the Python entrypoint.
const ROUTE_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on how long the child waits for the host to acknowledge an
/// attachment notification (success or failure).
const ATTACHMENT_ACK_TIMEOUT: Duration = Duration::from_secs(30);

const O_RDONLY: i32 = 0;
const O_WRONLY: i32 = 1;
const O_RDWR: i32 = 2;
const O_ACCMODE: i32 = 3;
const O_CREAT: i32 = 0o100;
const O_EXCL: i32 = 0o200;
const O_TRUNC: i32 = 0o1000;
const O_NONBLOCK: i32 = 0o4000;
const KNOWN_OPEN_FLAGS: i32 = O_ACCMODE | O_CREAT | O_EXCL | O_TRUNC | O_NONBLOCK;

fn python_open_options(flags: i32, length: Option<u64>) -> PyResult<OpenOptions> {
    if flags & !KNOWN_OPEN_FLAGS != 0 {
        return Err(PyValueError::new_err(format!(
            "unsupported descriptor open flags: {:#x}",
            flags & !KNOWN_OPEN_FLAGS
        )));
    }
    let access = match flags & O_ACCMODE {
        O_RDONLY => AccessMode::ReadOnly,
        O_WRONLY => AccessMode::WriteOnly,
        O_RDWR => AccessMode::ReadWrite,
        _ => return Err(PyValueError::new_err("invalid descriptor access mode")),
    };
    let options = OpenOptions {
        access,
        create: flags & O_CREAT != 0,
        exclusive: flags & O_EXCL != 0,
        truncate: flags & O_TRUNC != 0,
        nonblocking: flags & O_NONBLOCK != 0,
        allocation: length.map(|length| BlobAllocation {
            length,
            digest: None,
        }),
    };
    options.validate().map_err(raw_data_plane_error)?;
    Ok(options)
}

pyo3::create_exception!(swactor, SwactorError, pyo3::exceptions::PyException);
pyo3::create_exception!(swactor, BootstrapError, SwactorError);
pyo3::create_exception!(swactor, DataPathError, SwactorError);
pyo3::create_exception!(swactor, BlobError, SwactorError);
pyo3::create_exception!(swactor, SessionError, SwactorError);
pyo3::create_exception!(swactor, StreamError, SwactorError);

fn bootstrap_error(message: impl Into<String>) -> PyErr {
    PyErr::new::<BootstrapError, _>(message.into())
}

fn data_plane_error(error: DataPlaneError) -> PyErr {
    match error {
        DataPlaneError::InvalidPath(reason) => PyErr::new::<DataPathError, _>(reason),
        DataPlaneError::Unauthorized { path, access } => PyPermissionError::new_err((
            libc::EACCES,
            format!("{access:?} is not authorized for {path}"),
        )),
        DataPlaneError::PathNotFound(path) => {
            PyOSError::new_err((libc::ENOENT, format!("data path not found: {path}")))
        }
        DataPlaneError::Blob(reason) => PyErr::new::<BlobError, _>(format!("{reason:?}")),
        DataPlaneError::WrongEntryType { .. }
        | DataPlaneError::PathReplaced(_)
        | DataPlaneError::PeerLost
        | DataPlaneError::StreamFault(_)
        | DataPlaneError::StreamClosed => PyErr::new::<StreamError, _>(error.to_string()),
        DataPlaneError::Attachment(reason) => {
            PyErr::new::<SessionError, _>(format!("attachment failed: {reason:?}"))
        }
        other => PyErr::new::<SessionError, _>(other.to_string()),
    }
}

fn raw_data_plane_error(error: DataPlaneError) -> PyErr {
    let code = match error.errno() {
        Errno::Eacces => libc::EACCES,
        Errno::Eagain => libc::EAGAIN,
        Errno::Ebadf => libc::EBADF,
        Errno::Ebusy => libc::EBUSY,
        Errno::Ecanceled => libc::ECANCELED,
        Errno::Econnreset => libc::ECONNRESET,
        Errno::Eexist => libc::EEXIST,
        Errno::Einval => libc::EINVAL,
        Errno::Eio => libc::EIO,
        Errno::Enodev => libc::ENODEV,
        Errno::Enoent => libc::ENOENT,
        Errno::Enomem => libc::ENOMEM,
        Errno::Enospc => libc::ENOSPC,
        Errno::Enotsup => libc::ENOTSUP,
        Errno::Enxio => libc::ENXIO,
        Errno::Epipe => libc::EPIPE,
        Errno::Estale => libc::ESTALE,
    };
    PyOSError::new_err((code, error.to_string()))
}

struct ContextRouting {
    _driver: Arc<IrohDriver>,
    _engine: Engine,
}

impl Drop for ContextRouting {
    fn drop(&mut self) {
        self._driver.shutdown();
    }
}

#[derive(Clone)]
struct RoutePoll;

struct RouteReadinessActor {
    driver: Arc<IrohDriver>,
    host_node: swactor_transport::NodeId,
    engine: EngineHandle,
    sender: swactor::runtime::ExternalSender,
    completion: ActorCompletion<Result<(), String>>,
    deadline: std::time::Instant,
}

impl RouteReadinessActor {
    fn schedule(&self, ctx: &Ctx<'_>) {
        self.engine
            .send_after(ROUTE_POLL, self.sender.clone(), ctx.self_addr(), RoutePoll);
    }

    fn fail(&self, reason: String) {
        let _ = self.completion.complete(Err(reason));
    }
}

impl ActorInterface for RouteReadinessActor {
    type Incoming = RoutePoll;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.schedule(ctx);
    }

    fn handle(&mut self, ctx: &Ctx<'_>, _message: RoutePoll) {
        if self.driver.has_active_connection(&self.host_node) {
            let _ = self.completion.complete(Ok(()));
            ctx.stop_self();
            return;
        }
        if std::time::Instant::now() >= self.deadline {
            self.fail(format!(
                "data-plane route to host {} did not become ready within {ROUTE_READY_TIMEOUT:?}",
                self.host_node
            ));
            ctx.stop_self();
            return;
        }
        self.schedule(ctx);
    }
}

fn build_child_routing(
    host_session: ActorAddress,
    host_endpoint: EndpointAddr,
) -> PyResult<(Runtime, ContextRouting, [u8; 32])> {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let mut codecs = CodecRegistry::new();
    register_data_plane_codecs(&mut codecs);
    let codecs = Arc::new(codecs);
    let router = Arc::new(TransportRouter::new());
    runtime.set_remote_sink(Arc::new(CodecRemoteSink::new(
        codecs.clone(),
        router.clone(),
    )));

    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default())
            .map_err(|error| bootstrap_error(format!("start child runtime: {error}")))?,
    )
    .map_err(|error| bootstrap_error(format!("start child engine: {error}")))?;
    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Default,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: Vec::new(),
        },
    )
    .map_err(|error| bootstrap_error(format!("start child actor transport: {error}")))?;

    let host_node = swactor_transport::NodeId(*host_endpoint.id.as_bytes());
    let route_view: RouteView = Arc::new(RwLock::new(HashMap::from([(host_session, host_node)])));
    let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
    if let Some(relay) = host_endpoint.relay_urls().next() {
        relay_mirror
            .write()
            .expect("relay mirror")
            .insert(host_node, relay.to_string());
    }
    let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
    let route_transport = Arc::new(RouteViewTransport::new(route_view.clone(), outbox.clone()));
    let binder = OutboxRouteBinder::new(router, route_transport);
    binder.ensure_routable(host_session);

    driver.enable_actor_bridge(iroh_driver::ActorBridgeConfig {
        runtime: runtime.clone(),
        codec: codecs,
        routes: HashMap::new(),
        swim: ActorAddress::default(),
        relay_mirror,
        route_view,
        outbox,
    });
    driver.install_actor_bridge_pump(ROUTE_POLL);
    driver.connect_peer(host_endpoint.clone());

    let driver = Arc::new(driver);
    let completion: ActorCompletion<Result<(), String>> = ActorCompletion::new();
    runtime
        .spawn(RouteReadinessActor {
            driver: driver.clone(),
            host_node,
            engine: engine.handle(),
            sender: runtime.create_sender(),
            completion: completion.clone(),
            deadline: std::time::Instant::now() + ROUTE_READY_TIMEOUT,
        })
        .map_err(|error| bootstrap_error(format!("start route readiness actor: {error}")))?;
    match completion.wait_deadline(ROUTE_READY_TIMEOUT + ROUTE_POLL) {
        Some(Ok(())) => {}
        Some(Err(reason)) => return Err(bootstrap_error(reason)),
        None => {
            return Err(bootstrap_error(format!(
                "data-plane route to host did not become ready within {ROUTE_READY_TIMEOUT:?}"
            )));
        }
    }

    let child_node = driver.node_id().0;
    Ok((
        runtime,
        ContextRouting {
            _driver: driver,
            _engine: engine,
        },
        child_node,
    ))
}

/// Static namespace facts for one path.
#[pyclass(name = "NamespaceEntry")]
pub struct PyNamespaceEntry {
    kind: EntryKind,
    revision: u64,
    active: bool,
}

#[pymethods]
impl PyNamespaceEntry {
    #[getter]
    fn kind(&self) -> &'static str {
        match self.kind {
            EntryKind::Blob => "blob",
            EntryKind::Stream => "stream",
        }
    }

    #[getter]
    fn revision(&self) -> u64 {
        self.revision
    }

    #[getter]
    fn active(&self) -> bool {
        self.active
    }
}

/// Namespace, blob, and stream operations for the attached context.
#[pyclass(name = "DataPlane")]
pub struct PyDataPlane {
    inner: Arc<DataPlane>,
    _context_routing: Arc<ContextRouting>,
}

#[pymethods]
impl PyDataPlane {
    #[pyo3(signature = (path, flags, *, length = None))]
    fn open<'py>(
        &self,
        py: Python<'py>,
        path: String,
        flags: i32,
        length: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let path = DataPath::parse(path).map_err(|error| {
            raw_data_plane_error(DataPlaneError::InvalidPath(error.to_string()))
        })?;
        let options = python_open_options(flags, length)?;
        let data_plane = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let descriptor = data_plane
                .open(&path, options)
                .await
                .map_err(raw_data_plane_error)?;
            let kind = descriptor.kind();
            Python::with_gil(|py| {
                Py::new(
                    py,
                    PyDescriptor {
                        descriptor: Arc::new(tokio::sync::Mutex::new(descriptor)),
                        kind,
                    },
                )
            })
        })
    }

    /// Read a whole blob into bytes. Raises FileNotFoundError for a
    /// missing path.
    fn read_blob<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let data_plane = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let blob = data_plane
                .read_blob_path(&path)
                .await
                .map_err(data_plane_error)?;
            Python::with_gil(|py| Py::new(py, PyBlob { inner: blob }))
        })
    }

    #[pyo3(signature = (path, *, length))]
    /// Open an async write context that seals a new blob of `length`
    /// bytes on clean exit and aborts it on exception.
    fn write_blob(&self, path: String, length: u64) -> PyResult<PyWriteBlobContext> {
        DataPath::parse(path.clone())
            .map_err(|error| PyErr::new::<DataPathError, _>(error.to_string()))?;
        Ok(PyWriteBlobContext {
            data_plane: self.inner.clone(),
            path,
            length,
            state: Arc::new(ParkingMutex::new(PyWriteState::default())),
        })
    }

    /// Describe one namespace entry (kind, revision, active).
    fn lookup<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let path = DataPath::parse(path).map_err(|error| {
            raw_data_plane_error(DataPlaneError::InvalidPath(error.to_string()))
        })?;
        let data_plane = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let node = data_plane
                .lookup(&path)
                .await
                .map_err(raw_data_plane_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    PyNamespaceEntry {
                        kind: node.kind,
                        revision: node.revision,
                        active: node.active,
                    },
                )
            })
        })
    }

    /// Remove one namespace entry.
    fn unlink<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let path = DataPath::parse(path).map_err(|error| {
            raw_data_plane_error(DataPlaneError::InvalidPath(error.to_string()))
        })?;
        let data_plane = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            data_plane.unlink(&path).await.map_err(raw_data_plane_error)
        })
    }

    /// Move a namespace entry; `replace` allows overwriting an existing
    /// destination.
    #[pyo3(signature = (source, destination, *, replace = false))]
    fn rename<'py>(
        &self,
        py: Python<'py>,
        source: String,
        destination: String,
        replace: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source = DataPath::parse(source).map_err(|error| {
            raw_data_plane_error(DataPlaneError::InvalidPath(error.to_string()))
        })?;
        let destination = DataPath::parse(destination).map_err(|error| {
            raw_data_plane_error(DataPlaneError::InvalidPath(error.to_string()))
        })?;
        let data_plane = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            data_plane
                .rename(&source, &destination, replace)
                .await
                .map_err(raw_data_plane_error)
        })
    }

    /// Open a stream for reading; yields records as bytes.
    fn read_stream<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let path = DataPath::parse(path)
            .map_err(|error| PyErr::new::<DataPathError, _>(error.to_string()))?;
        let data_plane = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let reader = data_plane
                .read_stream(&path)
                .await
                .map_err(data_plane_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    PyStreamReader {
                        reader: Arc::new(tokio::sync::Mutex::new(reader)),
                    },
                )
            })
        })
    }

    #[pyo3(signature = (path, *, replace = false))]
    /// Open a stream for writing; `replace` displaces an existing
    /// entry at the path.
    fn write_stream(&self, path: String, replace: bool) -> PyResult<PyStreamContext> {
        let path = DataPath::parse(path)
            .map_err(|error| PyErr::new::<DataPathError, _>(error.to_string()))?;
        Ok(PyStreamContext {
            data_plane: self.inner.clone(),
            path,
            replace,
            stream: Arc::new(tokio::sync::Mutex::new(None)),
            entered: Arc::new(AtomicBool::new(false)),
        })
    }
}

fn byte_buffer(object: &Bound<'_, PyAny>, writable: bool) -> PyResult<PyBuffer<u8>> {
    let buffer = PyBuffer::<u8>::get(object)?;
    if writable && buffer.readonly() {
        return Err(PyBufferError::new_err("buffer is read-only"));
    }
    if !buffer.is_c_contiguous() {
        return Err(PyBufferError::new_err("buffer is not C-contiguous"));
    }
    Ok(buffer)
}

unsafe fn mutable_buffer_bytes(buffer: &mut PyBuffer<u8>) -> &mut [u8] {
    // SAFETY: `byte_buffer` checked writability and contiguity, and the
    // retained `PyBuffer` keeps the exporter and pointer valid.
    unsafe { std::slice::from_raw_parts_mut(buffer.buf_ptr().cast(), buffer.len_bytes()) }
}

unsafe fn buffer_bytes(buffer: &PyBuffer<u8>) -> &[u8] {
    // SAFETY: `byte_buffer` checked contiguity and retains the exporter.
    unsafe { std::slice::from_raw_parts(buffer.buf_ptr().cast_const().cast(), buffer.len_bytes()) }
}

/// An open descriptor over a blob or stream entry.
#[pyclass(name = "Descriptor")]
pub struct PyDescriptor {
    descriptor: Arc<tokio::sync::Mutex<Descriptor>>,
    kind: DescriptorKind,
}

#[pymethods]
impl PyDescriptor {
    #[getter]
    fn kind(&self) -> &'static str {
        match self.kind {
            DescriptorKind::Blob => "blob",
            DescriptorKind::Stream => "stream",
        }
    }

    fn read<'py>(&self, py: Python<'py>, size: usize) -> PyResult<Bound<'py, PyAny>> {
        let descriptor = self.descriptor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut bytes = vec![0_u8; size];
            let count = descriptor
                .lock()
                .await
                .read(&mut bytes)
                .await
                .map_err(raw_data_plane_error)?;
            bytes.truncate(count);
            Python::with_gil(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }

    fn readinto<'py>(
        &self,
        py: Python<'py>,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut buffer = byte_buffer(buffer, true)?;
        let descriptor = self.descriptor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // SAFETY: the owned `PyBuffer` remains alive through completion or
            // cancellation and no pointer is retained by the Rust primitive.
            let bytes = unsafe { mutable_buffer_bytes(&mut buffer) };
            descriptor
                .lock()
                .await
                .read(bytes)
                .await
                .map_err(raw_data_plane_error)
        })
    }

    fn write<'py>(&self, py: Python<'py>, bytes: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let descriptor = self.descriptor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            descriptor
                .lock()
                .await
                .write(&bytes)
                .await
                .map_err(raw_data_plane_error)
        })
    }

    fn writefrom<'py>(
        &self,
        py: Python<'py>,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let buffer = byte_buffer(buffer, false)?;
        let descriptor = self.descriptor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // SAFETY: the owned `PyBuffer` retains a contiguous exporter until
            // the descriptor primitive completes or is cancelled.
            let bytes = unsafe { buffer_bytes(&buffer) };
            descriptor
                .lock()
                .await
                .write(bytes)
                .await
                .map_err(raw_data_plane_error)
        })
    }

    #[pyo3(signature = (*, offset = 0, length, writable = false))]
    fn map<'py>(
        &self,
        py: Python<'py>,
        offset: u64,
        length: u64,
        writable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let descriptor = self.descriptor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mapping = descriptor
                .lock()
                .await
                .map(MapRequest {
                    protection: if writable {
                        Protection::ReadWrite
                    } else {
                        Protection::Read
                    },
                    sharing: Sharing::Shared,
                    target: MapTarget::Host,
                    offset,
                    length,
                })
                .map_err(raw_data_plane_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    PyDescriptorMapping {
                        inner: Some(mapping),
                        exports: 0,
                    },
                )
            })
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let descriptor = self.descriptor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            descriptor
                .lock()
                .await
                .close()
                .await
                .map_err(raw_data_plane_error)
        })
    }

    fn abort<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let descriptor = self.descriptor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            descriptor
                .lock()
                .await
                .abort()
                .await
                .map_err(raw_data_plane_error)
        })
    }
}

#[pyclass(name = "DescriptorMapping")]
pub struct PyDescriptorMapping {
    inner: Option<DescriptorMapping>,
    exports: usize,
}

#[pymethods]
impl PyDescriptorMapping {
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.inner.is_none() {
            return Err(PyBufferError::new_err("descriptor mapping is closed"));
        }
        Ok(slf)
    }

    fn __exit__(
        &mut self,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }

    fn close(&mut self) -> PyResult<()> {
        if self.exports != 0 {
            return Err(PyBufferError::new_err(
                "cannot close a descriptor mapping with active buffer exports",
            ));
        }
        self.inner.take();
        Ok(())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let (pointer, length, readonly) = {
            let mut borrowed = slf.borrow_mut();
            let inner = borrowed
                .inner
                .as_mut()
                .ok_or_else(|| PyBufferError::new_err("descriptor mapping is closed"))?;
            match inner {
                DescriptorMapping::ReadOnly(mapping) => {
                    (mapping.as_ptr().cast_mut(), mapping.len(), true)
                }
                DescriptorMapping::WritableReadOnly(mapping) => {
                    (mapping.as_ptr(), mapping.len(), true)
                }
                DescriptorMapping::Writable(mapping) => (mapping.as_ptr(), mapping.len(), false),
            }
        };
        // SAFETY: the mapping owns the stable arena lease and the Python
        // buffer retains `slf` until release.
        unsafe {
            fill_buffer(
                view,
                flags,
                pointer,
                length,
                readonly,
                slf.clone().into_any(),
            )
        }?;
        slf.borrow_mut().exports += 1;
        Ok(())
    }

    unsafe fn __releasebuffer__(&mut self, view: *mut ffi::Py_buffer) {
        self.exports = self.exports.saturating_sub(1);
        // SAFETY: `fill_buffer` allocated the format string for this export.
        unsafe { release_buffer_format(view) };
    }
}

#[pyclass(name = "Blob")]
pub struct PyBlob {
    inner: Blob,
}

#[pymethods]
impl PyBlob {
    #[getter]
    fn length(&self) -> u64 {
        self.inner.length()
    }

    #[getter]
    fn digest(&self) -> Option<String> {
        self.inner.digest().map(digest_hex)
    }

    fn map(&self, py: Python<'_>) -> PyResult<Py<PyBlobView>> {
        let inner = self
            .inner
            .map()
            .map_err(|error| data_plane_error(DataPlaneError::Blob(BlobFailure::from(error))))?;
        Py::new(
            py,
            PyBlobView {
                inner: Some(inner),
                exports: 0,
            },
        )
    }
}

fn digest_hex(digest: &ContentDigest) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest.bytes() {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[pyclass(name = "BlobView")]
pub struct PyBlobView {
    inner: Option<BlobView>,
    exports: usize,
}

#[pymethods]
impl PyBlobView {
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.inner.is_none() {
            return Err(PyBufferError::new_err("blob view is closed"));
        }
        Ok(slf)
    }

    fn __exit__(
        &mut self,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }

    fn close(&mut self) -> PyResult<()> {
        if self.exports != 0 {
            return Err(PyBufferError::new_err(
                "cannot close a blob view with active buffer exports",
            ));
        }
        self.inner.take();
        Ok(())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let (pointer, length) = {
            let borrowed = slf.borrow();
            let inner = borrowed
                .inner
                .as_ref()
                .ok_or_else(|| PyBufferError::new_err("blob view is closed"))?;
            (inner.as_ptr().cast_mut(), inner.len())
        };
        // SAFETY: the retained `BlobView` owns the stable mapping and the
        // Python buffer owns the cloned `slf` reference until release.
        unsafe { fill_buffer(view, flags, pointer, length, true, slf.clone().into_any()) }?;
        slf.borrow_mut().exports += 1;
        Ok(())
    }

    unsafe fn __releasebuffer__(&mut self, view: *mut ffi::Py_buffer) {
        self.exports = self.exports.saturating_sub(1);
        // SAFETY: format was allocated by `fill_buffer` for this view.
        unsafe { release_buffer_format(view) };
    }
}

#[derive(Default)]
struct PyWriteState {
    entered: bool,
    writer: Option<BlobWriter>,
}

/// Async context manager that owns one blob write.
#[pyclass(name = "_BlobWriteContext")]
pub struct PyWriteBlobContext {
    data_plane: Arc<DataPlane>,
    path: String,
    length: u64,
    state: Arc<ParkingMutex<PyWriteState>>,
}

#[pymethods]
impl PyWriteBlobContext {
    fn __aenter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        {
            let mut state = self.state.lock();
            if state.entered {
                return Err(PyRuntimeError::new_err(
                    "blob write context cannot be entered twice",
                ));
            }
            state.entered = true;
        }
        let data_plane = self.data_plane.clone();
        let path = self.path.clone();
        let length = self.length;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match data_plane.write_blob_path(&path, length).await {
                Ok(writer) => {
                    state.lock().writer = Some(writer);
                    Python::with_gil(|py| {
                        Py::new(
                            py,
                            PyBlobWriter {
                                state: state.clone(),
                            },
                        )
                    })
                }
                Err(error) => {
                    state.lock().entered = false;
                    Err(data_plane_error(error))
                }
            }
        })
    }

    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let clean = exception_type.is_none();
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut writer = state
                .lock()
                .writer
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("blob write context is not active"))?;
            let result = if clean {
                writer.seal().await
            } else {
                writer.abort().await
            };
            match result {
                Ok(()) => Ok(false),
                Err(error) => {
                    state.lock().writer = Some(writer);
                    Err(data_plane_error(error))
                }
            }
        })
    }
}

/// Buffer-protocol writer handed to a blob write context body.
#[pyclass(name = "_BlobWriter")]
pub struct PyBlobWriter {
    state: Arc<ParkingMutex<PyWriteState>>,
}

#[pymethods]
impl PyBlobWriter {
    #[getter]
    fn length(&self) -> PyResult<u64> {
        self.state
            .lock()
            .writer
            .as_ref()
            .map(BlobWriter::length)
            .ok_or_else(|| PyRuntimeError::new_err("blob writer is closed"))
    }

    fn map(&self, py: Python<'_>) -> PyResult<Py<PyWritableArenaView>> {
        let inner = self
            .state
            .lock()
            .writer
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("blob writer is closed"))?
            .map()
            .map_err(data_plane_error)?;
        Py::new(
            py,
            PyWritableArenaView {
                inner: Some(inner),
                exports: 0,
            },
        )
    }
}

#[pyclass(name = "_WritableArenaView")]
pub struct PyWritableArenaView {
    inner: Option<WritableArenaView>,
    exports: usize,
}

#[pymethods]
impl PyWritableArenaView {
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.inner.is_none() {
            return Err(PyBufferError::new_err("writable arena view is closed"));
        }
        Ok(slf)
    }

    fn __exit__(
        &mut self,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }

    fn close(&mut self) -> PyResult<()> {
        if self.exports != 0 {
            return Err(PyBufferError::new_err(
                "cannot close a writable arena view with active buffer exports",
            ));
        }
        self.inner.take();
        Ok(())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let (pointer, length) = {
            let borrowed = slf.borrow();
            let inner = borrowed
                .inner
                .as_ref()
                .ok_or_else(|| PyBufferError::new_err("writable arena view is closed"))?;
            (inner.as_ptr(), inner.len())
        };
        // SAFETY: the retained writable view is the unique lease capability.
        unsafe { fill_buffer(view, flags, pointer, length, false, slf.clone().into_any()) }?;
        slf.borrow_mut().exports += 1;
        Ok(())
    }

    unsafe fn __releasebuffer__(&mut self, view: *mut ffi::Py_buffer) {
        self.exports = self.exports.saturating_sub(1);
        // SAFETY: format was allocated by `fill_buffer` for this view.
        unsafe { release_buffer_format(view) };
    }
}

unsafe fn fill_buffer(
    view: *mut ffi::Py_buffer,
    flags: c_int,
    pointer: *mut u8,
    length: usize,
    readonly: bool,
    owner: Bound<'_, PyAny>,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyBufferError::new_err("buffer view is null"));
    }
    if readonly && (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
        return Err(PyBufferError::new_err("arena view is read-only"));
    }
    let length = isize::try_from(length)
        .map_err(|_| PyBufferError::new_err("arena view is too large for Python"))?;
    // SAFETY: caller supplied a valid Python buffer pointer and stable owner.
    unsafe {
        (*view).obj = owner.into_ptr();
        (*view).buf = pointer.cast::<c_void>();
        (*view).len = length;
        (*view).readonly = i32::from(readonly);
        (*view).itemsize = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            CString::new("B").expect("static format").into_raw()
        } else {
            ptr::null_mut()
        };
        (*view).ndim = 1;
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            &mut (*view).len
        } else {
            ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            &mut (*view).itemsize
        } else {
            ptr::null_mut()
        };
        (*view).suboffsets = ptr::null_mut();
        (*view).internal = ptr::null_mut();
    }
    Ok(())
}

unsafe fn release_buffer_format(view: *mut ffi::Py_buffer) {
    if view.is_null() {
        return;
    }
    // SAFETY: `view` is supplied by Python for the matching export.
    let format = unsafe { (*view).format };
    if !format.is_null() {
        // SAFETY: `fill_buffer` allocated this exact CString.
        unsafe { drop(CString::from_raw(format)) };
    }
}

#[pyclass(name = "StreamReader")]
pub struct PyStreamReader {
    reader: Arc<tokio::sync::Mutex<StreamReader>>,
}

#[pymethods]
impl PyStreamReader {
    fn read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let reader = self.reader.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes = reader.lock().await.read().await.map_err(data_plane_error)?;
            Python::with_gil(|py| Ok(bytes.map(|bytes| PyBytes::new(py, &bytes).unbind())))
        })
    }

    fn readinto<'py>(
        &self,
        py: Python<'py>,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut buffer = byte_buffer(buffer, true)?;
        let reader = self.reader.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // SAFETY: the owned `PyBuffer` retains the writable exporter for
            // the complete async operation and cancellation path.
            let bytes = unsafe { mutable_buffer_bytes(&mut buffer) };
            reader
                .lock()
                .await
                .read_into(bytes)
                .await
                .map_err(data_plane_error)
        })
    }
}

#[pyclass(name = "StreamWriter")]
pub struct PyStreamWriter {
    stream: Arc<tokio::sync::Mutex<Option<StreamWriter>>>,
}

#[pymethods]
impl PyStreamWriter {
    fn write<'py>(&self, py: Python<'py>, bytes: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = stream.lock().await;
            let writer = stream
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("stream writer is closed"))?;
            writer.write(&bytes).await.map_err(data_plane_error)
        })
    }
}

/// Async context manager that owns one stream write.
#[pyclass(name = "_StreamWriteContext")]
pub struct PyStreamContext {
    data_plane: Arc<DataPlane>,
    path: DataPath,
    replace: bool,
    stream: Arc<tokio::sync::Mutex<Option<StreamWriter>>>,
    entered: Arc<AtomicBool>,
}

#[pymethods]
impl PyStreamContext {
    fn __aenter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.entered.swap(true, Ordering::AcqRel) {
            return Err(PyRuntimeError::new_err(
                "stream write context cannot be entered twice",
            ));
        }
        let data_plane = self.data_plane.clone();
        let path = self.path.clone();
        let state = self.stream.clone();
        let entered = self.entered.clone();
        let replace = self.replace;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let opened = if replace {
                data_plane.write_stream_replacing(&path).await
            } else {
                data_plane.write_stream(&path).await
            };
            match opened {
                Ok(writer) => {
                    *state.lock().await = Some(writer);
                    Python::with_gil(|py| Py::new(py, PyStreamWriter { stream: state }))
                }
                Err(error) => {
                    entered.store(false, Ordering::Release);
                    Err(data_plane_error(error))
                }
            }
        })
    }

    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let clean = exception_type.is_none();
        let state = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut writer = state
                .lock()
                .await
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("stream write context is not active"))?;
            if clean {
                writer.close().await.map_err(data_plane_error)?;
            } else {
                writer.abort().map_err(data_plane_error)?;
            }
            Ok(false)
        })
    }
}

/// Root object handed to `main`; exposes `.data`.
#[pyclass(name = "Context")]
pub struct PyContext {
    data: Py<PyDataPlane>,
}

#[pymethods]
impl PyContext {
    #[getter]
    fn data(&self, py: Python<'_>) -> Py<PyDataPlane> {
        self.data.clone_ref(py)
    }
}

fn report_attachment_failure(attachment: GuestAttachment, reason: &str) {
    if let Ok(notification) = attachment.begin_attachment_failed(reason) {
        // The original error is what the guest sees; a hung host must not
        // stall the failure path, so the acknowledgement is bounded and its
        // own failure ignored.
        let _ = notification.wait_deadline(ATTACHMENT_ACK_TIMEOUT);
    }
}

#[pyfunction]
/// Attach the calling process to its inherited data-plane bootstrap
/// context and drive the async `main(ctx)` coroutine to completion. The
/// attached session is closed before any error propagates.
fn run(py: Python<'_>, main: Bound<'_, PyAny>) -> PyResult<()> {
    let guest = GuestBootstrap::claim().map_err(|error| bootstrap_error(error.to_string()))?;
    let (material, arena_fd, attachment) = guest
        .into_parts()
        .map_err(|error| bootstrap_error(error.to_string()))?;
    let host_endpoint: EndpointAddr = match serde_json::from_slice(&material.routing) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            let message = format!("parse host data-plane routing material: {error}");
            report_attachment_failure(attachment, &message);
            return Err(bootstrap_error(message));
        }
    };
    let (arena, resolved) = match DataPlaneBootstrap::map_arena(arena_fd) {
        Ok(mapped) => mapped,
        Err(error) => {
            report_attachment_failure(attachment, &error.to_string());
            return Err(bootstrap_error(error.to_string()));
        }
    };
    let (runtime, routing, child_node) =
        match build_child_routing(material.host_session, host_endpoint) {
            Ok(routing) => routing,
            Err(error) => {
                report_attachment_failure(attachment, &error.to_string());
                return Err(error);
            }
        };
    let bootstrap = match future::block_on(DataPlaneBootstrap::attach_mapped(
        arena,
        resolved,
        runtime,
        material.host_session,
        material.session_capability,
        Some(child_node),
    )) {
        Ok(bootstrap) => bootstrap,
        Err(error) => {
            let translated = data_plane_error(error.clone());
            report_attachment_failure(attachment, &error.to_string());
            return Err(translated);
        }
    };
    attachment
        .begin_attachment_succeeded()
        .and_then(|notification| notification.wait_deadline(ATTACHMENT_ACK_TIMEOUT))
        .map_err(|error| bootstrap_error(error.to_string()))?;

    let data_plane = Arc::new(bootstrap.data_plane);
    let data = Py::new(
        py,
        PyDataPlane {
            inner: Arc::clone(&data_plane),
            _context_routing: Arc::new(routing),
        },
    )?;
    let context = Py::new(py, PyContext { data })?;
    // Run the guest coroutine, then close the attached session before
    // propagating either failure: the session must not outlive a `main` that
    // failed to start or raised.
    let main_result = (|| -> PyResult<_> {
        let coroutine = main.call1((context,))?;
        let asyncio = PyModule::import(py, "asyncio")?;
        Ok(asyncio.call_method1("run", (coroutine,)))
    })();
    let close = data_plane.close().map_err(data_plane_error);
    main_result??;
    close
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugHostRouteRegistrar {
    route_view: RouteView,
    route_binder: Arc<OutboxRouteBinder>,
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl data_plane::host::HostRouteRegistrar for DebugHostRouteRegistrar {
    fn register_child(
        &self,
        child_session: ActorAddress,
        child_node: [u8; 32],
    ) -> Result<(), String> {
        self.route_view
            .write()
            .map_err(|_| "debug host route view is poisoned".to_owned())?
            .insert(child_session, swactor_transport::NodeId(child_node));
        self.route_binder.ensure_routable(child_session);
        Ok(())
    }

    fn revoke_child(&self, child_session: ActorAddress) -> Result<(), String> {
        self.route_view
            .write()
            .map_err(|_| "debug host route view is poisoned".to_owned())?
            .remove(&child_session);
        self.route_binder.remove_route(&child_session);
        Ok(())
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugBlobSender {
    runtime: Runtime,
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl data_plane::blob_transfer::BlobTransferSender for DebugBlobSender {
    fn start_file(
        &self,
        request: data_plane::blob_transfer::FileTransferRequest,
    ) -> Result<(), String> {
        use std::os::unix::fs::FileExt;
        let mut bytes = vec![0_u8; request.length as usize];
        request
            .file
            .read_exact_at(&mut bytes, request.offset)
            .map_err(|error| error.to_string())?;
        self.runtime
            .send_to(
                request.offer.destination,
                data_plane::blob_transfer::BlobTransferEvent::Chunk {
                    transfer_id: request.offer.transfer_id,
                    bytes,
                },
            )
            .map_err(|error| error.to_string())?;
        self.runtime
            .send_to(
                request.offer.destination,
                data_plane::blob_transfer::BlobTransferEvent::Finished {
                    transfer_id: request.offer.transfer_id,
                },
            )
            .map_err(|error| error.to_string())?;
        request.completion.complete(Ok(()));
        Ok(())
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugBlobReceiver;

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl data_plane::blob_transfer::BlobTransferReceiver for DebugBlobReceiver {
    fn open(
        &self,
        destination: ActorAddress,
        transfer_id: data_plane::blob_transfer::BlobTransferId,
    ) -> Result<data_plane::blob_transfer::BlobTransferOffer, String> {
        Ok(data_plane::blob_transfer::BlobTransferOffer {
            transfer_id,
            destination,
            failure_proxy: None,
            transport: Vec::new(),
        })
    }

    fn cancel(&self, _offer: &data_plane::blob_transfer::BlobTransferOffer) {}
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugNamespaceDiscovery(ActorAddress);

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl data_plane::namespace::NamespaceDiscovery for DebugNamespaceDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        Some(self.0)
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugSourceRegistrar;

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl data_plane::source::BlobSourcePublisher for DebugSourceRegistrar {
    fn publish_source(&self, _source: ActorAddress) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugStreamCaptureConsumer {
    bytes: Arc<ParkingMutex<Vec<u8>>>,
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl data_plane::data_plane::StreamConsumer for DebugStreamCaptureConsumer {
    fn consume(&self, bytes: &[u8]) -> Result<(), String> {
        self.bytes.lock().extend_from_slice(bytes);
        Ok(())
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugBootstrap {
    host: BootstrapHost,
    cancellation: BootstrapCancellation,
    child: OwnedFd,
    arena_fd: OwnedFd,
    material: SessionBootstrap,
}
#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugContextProvisioner {
    context: ParkingMutex<Option<swactor_process_context::ProvisionedContext>>,
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl swactor_process_context::ContextProvisioner for DebugContextProvisioner {
    fn provision(
        &self,
        _identity: swactor_process_context::ExecutionIdentity,
        _access: data_plane::path::SessionAccess,
    ) -> Result<swactor_process_context::ProvisionedContext, String> {
        self.context
            .lock()
            .take()
            .ok_or_else(|| "debug contextual process was already provisioned".to_owned())
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
struct DebugContextLaunchActor {
    spawner: Arc<swactor_process_context::ContextualProcessSpawner>,
    sender: swactor::runtime::ExternalSender,
    spec: Option<swactor_process_context::ContextualProcessSpec>,
    output: Option<swactor_process_context::ContextualProcessOutputConfig>,
    spawned: Arc<ParkingMutex<Option<swactor_process_context::SpawnedContextualProcess>>>,
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl ActorInterface for DebugContextLaunchActor {
    type Incoming = ();
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if let (Some(spec), Some(output)) = (self.spec.take(), self.output.take())
            && let Ok(spawned) = self.spawner.spawn(ctx, &self.sender, spec, output)
        {
            *self.spawned.lock() = Some(spawned);
        }
        ctx.stop_self();
    }
    fn handle(&mut self, _ctx: &Ctx<'_>, _message: ()) {}
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
#[pyclass(name = "_TestDataPlaneHost")]
struct PyTestDataPlaneHost {
    _driver: Arc<IrohDriver>,
    _engine: Engine,
    runtime: Runtime,
    bootstrap: Option<DebugBootstrap>,
    blocking_work: BlockingWorkSender,
    namespace_root: std::path::PathBuf,
    stream_data_plane: Arc<DataPlane>,
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
impl Drop for PyTestDataPlaneHost {
    fn drop(&mut self) {
        self._driver.shutdown();
        let _ = std::fs::remove_dir_all(&self.namespace_root);
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
#[pymethods]
impl PyTestDataPlaneHost {
    #[pyo3(signature = (invalid_capability = false, corrupt_arena = false))]
    fn install_bootstrap(
        &mut self,
        invalid_capability: bool,
        corrupt_arena: bool,
    ) -> PyResult<RawFd> {
        let mut bootstrap = self
            .bootstrap
            .take()
            .ok_or_else(|| BootstrapError::new_err("bootstrap handle was already installed"))?;
        if invalid_capability {
            bootstrap.material.session_capability = SessionCapability::new([0; 32]);
        }
        if corrupt_arena {
            let invalid_magic = [0_u8; 4];
            // SAFETY: arena_fd is writable and invalid_magic is a readable buffer.
            if unsafe {
                libc::pwrite(
                    bootstrap.arena_fd.as_raw_fd(),
                    invalid_magic.as_ptr().cast(),
                    invalid_magic.len(),
                    0,
                )
            } != invalid_magic.len() as isize
            {
                return Err(PyRuntimeError::new_err(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
        }

        let source = bootstrap.child.into_raw_fd();
        if source == CHILD_BOOTSTRAP_FD {
            // SAFETY: source is live and now solely owned by the fixed ABI slot.
            let flags = unsafe { libc::fcntl(source, libc::F_GETFD) };
            if flags < 0
                || unsafe { libc::fcntl(source, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
            {
                // SAFETY: source ownership was transferred above.
                let _ = unsafe { libc::close(source) };
                return Err(PyRuntimeError::new_err(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
        } else {
            // SAFETY: source is live; dup3 atomically replaces the fixed target
            // and leaves it close-on-exec in this host process.
            let duplicated = unsafe { libc::dup3(source, CHILD_BOOTSTRAP_FD, libc::O_CLOEXEC) };
            // SAFETY: source ownership was transferred above and is no longer needed.
            let _ = unsafe { libc::close(source) };
            if duplicated < 0 {
                return Err(PyRuntimeError::new_err(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
        }

        let work = Box::new(move || {
            if let Ok(mut attachment) = bootstrap
                .host
                .accept_claim(&bootstrap.material, bootstrap.arena_fd.as_raw_fd())
                && attachment.receive_result().is_ok()
            {
                let _ = attachment.acknowledge();
            }
        });
        if self.blocking_work.submit(work).is_err() {
            // SAFETY: this method installed the fixed descriptor above.
            let _ = unsafe { libc::close(CHILD_BOOTSTRAP_FD) };
            return Err(PyRuntimeError::new_err(
                "debug bootstrap blocking service is unavailable",
            ));
        }
        Ok(CHILD_BOOTSTRAP_FD)
    }
    fn run_contextual_process(&mut self, python: String, script: String) -> PyResult<Vec<String>> {
        let bootstrap = self
            .bootstrap
            .take()
            .ok_or_else(|| BootstrapError::new_err("bootstrap handle was already consumed"))?;
        let host_session = bootstrap.material.host_session;
        let provisioned = swactor_process_context::ProvisionedContext {
            host_session,
            arena_fd: bootstrap.arena_fd,
            child_bootstrap: bootstrap.child,
            bootstrap_host: bootstrap.host,
            bootstrap_cancellation: bootstrap.cancellation,
            material: bootstrap.material,
        };
        let provisioner: Arc<dyn swactor_process_context::ContextProvisioner> =
            Arc::new(DebugContextProvisioner {
                context: ParkingMutex::new(Some(provisioned)),
            });
        let spawner = Arc::new(swactor_process_context::ContextualProcessSpawner::new(
            self._engine.handle(),
            provisioner,
        ));
        let output = self
            .runtime
            .new_inbox::<swactor_process_context::ContextualProcessOutput>()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let sender = self.runtime.create_sender();
        let spawned = Arc::new(ParkingMutex::new(None));
        self.runtime
            .spawn(DebugContextLaunchActor {
                spawner,
                sender: sender.clone(),
                spawned: spawned.clone(),
                spec: Some(swactor_process_context::ContextualProcessSpec {
                    process: swactor_process::ProcessSpec {
                        command: python,
                        args: vec![script],
                        env: HashMap::new(),
                        working_dir: None,
                        label: Some(format!(
                            "python-context-probe-{}",
                            ActorAddress::new_random().to_full_hex()
                        )),
                    },
                    access: data_plane::path::SessionAccess {
                        execution_id: "test-run".to_owned(),
                        read_prefixes: vec![
                            DataPath::parse("/models")
                                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
                            DataPath::parse("/runs/test-run/results")
                                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
                        ],
                        write_prefixes: vec![
                            DataPath::parse("/runs/test-run/results")
                                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
                        ],
                    },
                    attach_deadline: Duration::from_secs(30),
                }),
                output: Some(
                    swactor_process_context::ContextualProcessOutputConfig::disabled(
                        *output.addr(),
                    ),
                ),
            })
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let mut deadline = Instant::now() + Duration::from_secs(45);
        let mut events = Vec::new();
        let mut stop_sent = false;
        while Instant::now() < deadline {
            while let Some(output) = output.try_recv() {
                let terminal = matches!(
                    output,
                    swactor_process_context::ContextualProcessOutput::Process(
                        swactor_process::ProcessOutput::SpawnFailed { .. }
                            | swactor_process::ProcessOutput::Exited { .. }
                            | swactor_process::ProcessOutput::Error { .. }
                    )
                );
                let rendered = match output {
                    swactor_process_context::ContextualProcessOutput::Process(
                        swactor_process::ProcessOutput::Started { pid },
                    ) => format!("started:{pid}"),
                    swactor_process_context::ContextualProcessOutput::Process(
                        swactor_process::ProcessOutput::Stdout(bytes),
                    ) => format!("stdout:{}", String::from_utf8_lossy(&bytes)),
                    swactor_process_context::ContextualProcessOutput::Process(
                        swactor_process::ProcessOutput::Stderr(bytes),
                    ) => format!("stderr:{}", String::from_utf8_lossy(&bytes)),
                    swactor_process_context::ContextualProcessOutput::Process(
                        swactor_process::ProcessOutput::SpawnFailed { error },
                    ) => format!("spawn_failed:{error}"),
                    swactor_process_context::ContextualProcessOutput::Process(
                        swactor_process::ProcessOutput::Exited { status },
                    ) => format!("exited:{status:?}"),
                    swactor_process_context::ContextualProcessOutput::Process(
                        swactor_process::ProcessOutput::Error { error },
                    ) => format!("process_error:{error}"),
                    swactor_process_context::ContextualProcessOutput::ContextReady => {
                        "context_ready".to_owned()
                    }
                    swactor_process_context::ContextualProcessOutput::BootstrapFailed {
                        reason,
                    } => format!("bootstrap_failed:{reason:?}"),
                };
                events.push(rendered);
                if terminal {
                    return Ok(events);
                }
            }
            std::thread::sleep(Duration::from_millis(5));
            if !stop_sent && Instant::now() >= deadline {
                // The child overran its budget: order a stop so a hung
                // process cannot outlive the debug host, then grant a short
                // grace window to observe the terminal output.
                if let Some(spawned) = spawned.lock().take() {
                    let _ = swactor_process_context::send_contextual_process_command(
                        &sender,
                        spawned.actor,
                        swactor_process_context::ContextualProcessCommand::Stop {
                            kill_after: Some(Duration::from_secs(5)),
                        },
                    );
                    stop_sent = true;
                    deadline = Instant::now() + Duration::from_secs(10);
                } else {
                    break;
                }
            }
        }
        Err(PyRuntimeError::new_err(format!(
            "contextual Python process did not terminate: {events:?}"
        )))
    }

    fn read_stream<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let path = DataPath::parse(path)
            .map_err(|error| PyErr::new::<DataPathError, _>(error.to_string()))?;
        let data_plane = self.stream_data_plane.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let reader = data_plane
                .read_stream(&path)
                .await
                .map_err(data_plane_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    PyStreamReader {
                        reader: Arc::new(tokio::sync::Mutex::new(reader)),
                    },
                )
            })
        })
    }

    fn capture_stream(&self, path: String) -> PyResult<PyTestStreamCapture> {
        let path = DataPath::parse(path)
            .map_err(|error| PyErr::new::<DataPathError, _>(error.to_string()))?;
        let bytes = Arc::new(ParkingMutex::new(Vec::new()));
        let consumer: Arc<dyn data_plane::data_plane::StreamConsumer> =
            Arc::new(DebugStreamCaptureConsumer {
                bytes: Arc::clone(&bytes),
            });
        let completion = self
            .stream_data_plane
            .collect_stream(path, consumer)
            .map_err(data_plane_error)?;
        Ok(PyTestStreamCapture { completion, bytes })
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
#[pyclass(name = "_TestStreamCapture")]
struct PyTestStreamCapture {
    completion: ActorCompletion<Result<(), DataPlaneError>>,
    bytes: Arc<ParkingMutex<Vec<u8>>>,
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
#[pymethods]
impl PyTestStreamCapture {
    fn result<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.completion.wait().map_err(data_plane_error)?;
        Ok(PyBytes::new(py, &self.bytes.lock()))
    }
}

#[cfg(all(feature = "test-host", target_os = "linux"))]
#[pyfunction]
fn _test_data_plane_host() -> PyResult<PyTestDataPlaneHost> {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let mut codecs = CodecRegistry::new();
    register_data_plane_codecs(&mut codecs);
    let codecs = Arc::new(codecs);
    let router = Arc::new(TransportRouter::new());
    runtime.set_remote_sink(Arc::new(CodecRemoteSink::new(
        codecs.clone(),
        router.clone(),
    )));
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: Vec::new(),
        },
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

    let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
    let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
    let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
    let route_transport = Arc::new(RouteViewTransport::new(route_view.clone(), outbox.clone()));
    let route_binder = Arc::new(OutboxRouteBinder::new(router, route_transport));
    let registrar = Arc::new(DebugHostRouteRegistrar {
        route_view: route_view.clone(),
        route_binder,
    });

    let mut arena = data_plane::arena::ArenaManager::boot(data_plane::arena::ArenaConfig {
        node_id: data_plane::arena::NodeId(1),
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .map_err(|error| PyRuntimeError::new_err(format!("debug host arena: {error:?}")))?;
    let prepared = data_plane::bootstrap::prepare_arena(
        &mut arena,
        data_plane::bootstrap::BootstrapSpec {
            arena_generation: 1,
            alignment: 64,
        },
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let capability = SessionCapability::new([0x5a; 32]);
    let namespace_root = std::env::temp_dir().join(format!(
        "swactor-python-namespace-{}",
        ActorAddress::new_random().to_full_hex()
    ));
    std::fs::create_dir_all(&namespace_root)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let source_sender: Arc<dyn data_plane::blob_transfer::BlobTransferSender> =
        Arc::new(DebugBlobSender {
            runtime: runtime.clone(),
        });
    let source_publisher: Arc<dyn data_plane::source::BlobSourcePublisher> =
        Arc::new(DebugSourceRegistrar);
    let namespace_service = data_plane::control::DataNamespaceService::recover(
        runtime.clone(),
        engine.handle(),
        namespace_root.join("namespace.json"),
        Arc::clone(&source_sender),
        Arc::clone(&source_publisher),
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let directory = namespace_service.directory();
    let weights_path = DataPath::parse("/models/tiny-linear/weights")
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let weights_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../apps/myelin/testdata/tiny_linear.weights");
    future::block_on(
        namespace_service
            .control()
            .register(weights_path, data_plane::blob::file(weights_file)),
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let namespace_proxy = runtime
        .spawn(data_plane::namespace::NamespaceClientActor::new(
            engine.handle(),
            runtime.create_sender(),
            Arc::new(DebugNamespaceDiscovery(directory)),
            ROUTE_POLL,
        ))
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let namespace = data_plane::namespace::NamespaceClient::new(runtime.clone(), namespace_proxy);
    let stream_transport: Arc<dyn data_plane::stream_transport::StreamTransport> =
        Arc::new(data_plane::stream_transport::LocalStreamTransport::new());
    let host_session = runtime
        .spawn(
            data_plane::host::HostDataPlaneSessionActor::new(
                data_plane::host::HostDataPlaneConfig {
                    runtime: runtime.clone(),
                    engine: engine.handle(),
                    arena,
                    arena_generation: 1,
                    session_generation: 1,
                    capability,
                    session_access: data_plane::path::SessionAccess {
                        execution_id: "test-run".to_owned(),
                        read_prefixes: vec![
                            DataPath::parse("/models")
                                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
                            DataPath::parse("/runs/test-run/results")
                                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
                        ],
                        write_prefixes: vec![
                            DataPath::parse("/runs/test-run/results")
                                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
                        ],
                    },
                    namespace: Some(namespace.clone()),
                    transfer_receiver: Some(Arc::new(DebugBlobReceiver)),
                    source_sender: Some(Arc::clone(&source_sender)),
                    source_publisher: Some(Arc::clone(&source_publisher)),
                    route_registrar: Some(registrar),
                    stream_transport: Some(Arc::clone(&stream_transport)),
                },
            )
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
        )
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

    let mut sink_arena = data_plane::arena::ArenaManager::boot(data_plane::arena::ArenaConfig {
        node_id: data_plane::arena::NodeId(2),
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .map_err(|error| PyRuntimeError::new_err(format!("debug sink arena: {error:?}")))?;
    let sink_prepared = data_plane::bootstrap::prepare_arena(
        &mut sink_arena,
        data_plane::bootstrap::BootstrapSpec {
            arena_generation: 2,
            alignment: 64,
        },
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let sink_session = runtime
        .spawn(
            data_plane::host::HostDataPlaneSessionActor::new(
                data_plane::host::HostDataPlaneConfig {
                    runtime: runtime.clone(),
                    engine: engine.handle(),
                    arena: sink_arena,
                    arena_generation: 2,
                    session_generation: 2,
                    capability,
                    session_access: data_plane::path::SessionAccess {
                        execution_id: "test-run".to_owned(),
                        read_prefixes: vec![
                            DataPath::parse("/runs/test-run/results")
                                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
                        ],
                        write_prefixes: Vec::new(),
                    },
                    namespace: Some(namespace),
                    transfer_receiver: None,
                    source_sender: None,
                    source_publisher: None,
                    route_registrar: None,
                    stream_transport: Some(stream_transport),
                },
            )
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
        )
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let stream_data_plane = Arc::new(
        future::block_on(DataPlaneBootstrap::attach(
            sink_prepared.arena_fd,
            runtime.clone(),
            sink_session,
            capability,
        ))
        .map_err(data_plane_error)?
        .data_plane,
    );

    driver.enable_actor_bridge(iroh_driver::ActorBridgeConfig {
        runtime: runtime.clone(),
        codec: codecs,
        routes: HashMap::new(),
        swim: ActorAddress::default(),
        relay_mirror,
        route_view,
        outbox,
    });
    driver.install_actor_bridge_pump(ROUTE_POLL);
    let (bootstrap_host, bootstrap_child) = data_plane::bootstrap::channel::bootstrap_channel()
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let bootstrap_cancellation = bootstrap_host
        .cancellation_handle()
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let routing = serde_json::to_vec(&driver.endpoint_addr())
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let blocking_work = engine.handle().blocking_work_sender();

    Ok(PyTestDataPlaneHost {
        _driver: Arc::new(driver),
        runtime: runtime.clone(),
        _engine: engine,
        bootstrap: Some(DebugBootstrap {
            host: bootstrap_host,
            cancellation: bootstrap_cancellation,
            child: bootstrap_child,
            arena_fd: prepared.arena_fd,
            material: SessionBootstrap {
                host_session,
                session_capability: capability,
                routing,
            },
        }),
        blocking_work,
        namespace_root,
        stream_data_plane,
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("SwactorError", module.py().get_type::<SwactorError>())?;
    module.add("BootstrapError", module.py().get_type::<BootstrapError>())?;
    module.add("DataPathError", module.py().get_type::<DataPathError>())?;
    module.add("BlobError", module.py().get_type::<BlobError>())?;
    module.add("SessionError", module.py().get_type::<SessionError>())?;
    module.add("StreamError", module.py().get_type::<StreamError>())?;
    module.add("O_RDONLY", O_RDONLY)?;
    module.add("O_WRONLY", O_WRONLY)?;
    module.add("O_RDWR", O_RDWR)?;
    module.add("O_CREAT", O_CREAT)?;
    module.add("O_EXCL", O_EXCL)?;
    module.add("O_TRUNC", O_TRUNC)?;
    module.add("O_NONBLOCK", O_NONBLOCK)?;
    module.add_class::<PyDataPlane>()?;
    module.add_class::<PyNamespaceEntry>()?;
    module.add_class::<PyDescriptor>()?;
    module.add_class::<PyDescriptorMapping>()?;
    module.add_class::<PyBlob>()?;
    module.add_class::<PyBlobView>()?;
    module.add_class::<PyStreamWriter>()?;
    module.add_class::<PyStreamReader>()?;
    module.add_class::<PyContext>()?;
    module.add_function(wrap_pyfunction!(run, module)?)?;
    #[cfg(all(feature = "test-host", target_os = "linux"))]
    {
        module.add_class::<PyTestStreamCapture>()?;
        module.add_class::<PyTestDataPlaneHost>()?;
        module.add_function(wrap_pyfunction!(_test_data_plane_host, module)?)?;
    }
    Ok(())
}
