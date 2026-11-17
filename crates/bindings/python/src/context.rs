use std::collections::HashMap;
use std::ffi::{CString, c_int, c_void};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use data_plane::blob::{Blob, BlobView, ContentDigest, WritableArenaView};
use data_plane::bootstrap::channel::{GuestAttachment, GuestBootstrap};
use data_plane::data_plane::{
    BlobWriter, DataPlane, DataPlaneBootstrap, Descriptor, DescriptorMapping, MapRequest,
    MapTarget, Protection, Sharing, StreamReader, StreamWriter,
};
use data_plane::namespace::{EntryKind, StreamIncarnation};
use data_plane::path::DataPath;
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
use swactor_core::actor::ActorAddress;
use swactor_core::runtime::Runtime;
use swactor_engine::{ActorCompletion, Engine, TaskSpawner};
use swactor_transport::{CodecRegistry, CodecRemoteSink, TransportRouter};

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
        DataPlaneError::PathReplaced(path) => {
            PyOSError::new_err((libc::ESTALE, format!("stream path was replaced: {path}")))
        }
        DataPlaneError::Unauthorized { path, access } => PyPermissionError::new_err((
            libc::EACCES,
            format!("{access:?} is not authorized for {path}"),
        )),
        DataPlaneError::PathNotFound(path) => {
            PyOSError::new_err((libc::ENOENT, format!("data path not found: {path}")))
        }
        DataPlaneError::Blob(reason) => PyErr::new::<BlobError, _>(format!("{reason:?}")),
        DataPlaneError::WrongEntryType { .. }
        | DataPlaneError::BrokenPipe
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

fn build_child_routing(
    runtime: Runtime,
    engine: Engine,
    tasks: TaskSpawner,
    host_session: ActorAddress,
    host_endpoint: EndpointAddr,
) -> PyResult<(Runtime, ContextRouting, [u8; 32])> {
    let mut codecs = CodecRegistry::new();
    register_data_plane_codecs(&mut codecs);
    let codecs = Arc::new(codecs);
    let router = Arc::new(TransportRouter::new());
    runtime.set_remote_sink(Arc::new(CodecRemoteSink::new(
        codecs.clone(),
        router.clone(),
    )));

    let mut driver = IrohDriver::with_engine(
        tasks,
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            // A contextual child communicates with its advertised host, not
            // unrelated public relays. Preserve the host's relay topology.
            relay_mode: if host_endpoint.relay_urls().next().is_some() {
                RelayMode::Custom(host_endpoint.relay_urls().cloned().collect())
            } else {
                RelayMode::Disabled
            },
            bind_port: None,
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
    let outbox: Outbox = Arc::new(Default::default());
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
    driver.install_actor_bridge();
    driver.connect_peer(host_endpoint.clone());

    let driver = Arc::new(driver);
    let routing = ContextRouting {
        _driver: Arc::clone(&driver),
        _engine: engine,
    };
    let completion: ActorCompletion<()> = ActorCompletion::new();
    let readiness_driver = Arc::downgrade(&driver);
    let readiness_completion = completion.clone();
    let watch = driver
        .connection_observer()
        .watch_connections(Arc::new(move || {
            if readiness_driver
                .upgrade()
                .is_some_and(|driver| driver.has_active_connection(&host_node))
            {
                let _ = readiness_completion.complete(());
            }
        }));
    if completion.wait_deadline(ROUTE_READY_TIMEOUT).is_none() {
        return Err(bootstrap_error(format!(
            "data-plane route to host did not become ready within {ROUTE_READY_TIMEOUT:?}"
        )));
    }
    drop(watch);

    let child_node = driver.node_id().0;
    Ok((runtime, routing, child_node))
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
///
/// Driver and engine teardown is owned solely by the native contextual
/// workload executable. Python-visible handles therefore retain
/// only a weak reference: cycles or delayed interpreter garbage collection
/// cannot move native thread teardown into interpreter finalization.
#[pyclass(name = "DataPlane")]
pub struct PyDataPlane {
    inner: Weak<DataPlane>,
    tasks: TaskSpawner,
}

impl PyDataPlane {
    fn data_plane(&self) -> PyResult<Arc<DataPlane>> {
        self.inner
            .upgrade()
            .ok_or_else(|| PyRuntimeError::new_err("data-plane context is closed"))
    }
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
        let data_plane = self.data_plane()?;
        let tasks = self.tasks.clone();
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
                        tasks,
                    },
                )
            })
        })
    }

    /// Read a whole blob into bytes. Raises FileNotFoundError for a
    /// missing path.
    fn read_blob<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let data_plane = self.data_plane()?;
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
            tasks: self.tasks.clone(),
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
        let data_plane = self.data_plane()?;
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        let data_plane = self.data_plane()?;
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        let data_plane = self.data_plane()?;
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        let data_plane = self.data_plane()?;
        let tasks = self.tasks.clone();
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
            let reader = data_plane
                .read_stream(&path)
                .await
                .map_err(data_plane_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    PyStreamReader {
                        incarnation: reader.incarnation(),
                        reader: Arc::new(tokio::sync::Mutex::new(reader)),
                        tasks,
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
            tasks: self.tasks.clone(),
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
    tasks: TaskSpawner,
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
    data_plane: Weak<DataPlane>,
    path: String,
    length: u64,
    state: Arc<ParkingMutex<PyWriteState>>,
    tasks: TaskSpawner,
}

#[pymethods]
impl PyWriteBlobContext {
    fn __aenter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let data_plane = self
            .data_plane
            .upgrade()
            .ok_or_else(|| PyRuntimeError::new_err("data-plane context is closed"))?;
        {
            let mut state = self.state.lock();
            if state.entered {
                return Err(PyRuntimeError::new_err(
                    "blob write context cannot be entered twice",
                ));
            }
            state.entered = true;
        }
        let data_plane = data_plane;
        let path = self.path.clone();
        let length = self.length;
        let state = self.state.clone();
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
    incarnation: StreamIncarnation,
    reader: Arc<tokio::sync::Mutex<StreamReader>>,
    tasks: TaskSpawner,
}

#[pymethods]
impl PyStreamReader {
    /// Namespace revision of the attached stream, unchanged after EOF or replacement.
    #[getter]
    fn incarnation(&self) -> u64 {
        self.incarnation.revision
    }

    /// Namespace authority epoch of the attached stream.
    #[getter]
    fn authority_epoch(&self) -> u64 {
        self.incarnation.authority_epoch
    }

    fn read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let reader = self.reader.clone();
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
    incarnation: StreamIncarnation,
    stream: Arc<tokio::sync::Mutex<Option<StreamWriter>>>,
    tasks: TaskSpawner,
}

#[pymethods]
impl PyStreamWriter {
    /// Namespace revision of the attached stream, unchanged after close or replacement.
    #[getter]
    fn incarnation(&self) -> u64 {
        self.incarnation.revision
    }

    /// Namespace authority epoch of the attached stream.
    #[getter]
    fn authority_epoch(&self) -> u64 {
        self.incarnation.authority_epoch
    }

    fn write<'py>(&self, py: Python<'py>, bytes: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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
    data_plane: Weak<DataPlane>,
    path: DataPath,
    replace: bool,
    stream: Arc<tokio::sync::Mutex<Option<StreamWriter>>>,
    entered: Arc<AtomicBool>,
    tasks: TaskSpawner,
}

#[pymethods]
impl PyStreamContext {
    fn __aenter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let data_plane = self
            .data_plane
            .upgrade()
            .ok_or_else(|| PyRuntimeError::new_err("data-plane context is closed"))?;
        if self.entered.swap(true, Ordering::AcqRel) {
            return Err(PyRuntimeError::new_err(
                "stream write context cannot be entered twice",
            ));
        }
        let data_plane = data_plane;
        let path = self.path.clone();
        let state = self.stream.clone();
        let entered = self.entered.clone();
        let replace = self.replace;
        let tasks = self.tasks.clone();
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
            let opened = if replace {
                data_plane.write_stream_replacing(&path).await
            } else {
                data_plane.write_stream(&path).await
            };
            match opened {
                Ok(writer) => {
                    let incarnation = writer.incarnation();
                    *state.lock().await = Some(writer);
                    Python::with_gil(|py| {
                        Py::new(
                            py,
                            PyStreamWriter {
                                incarnation,
                                stream: state,
                                tasks,
                            },
                        )
                    })
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
        crate::async_bridge::future_into_py(&self.tasks, py, async move {
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

/// Compile one generated source unit and execute its `async main(ctx)` entrypoint.
pub fn run_source(
    source: &str,
    runtime: Runtime,
    engine: Engine,
    tasks: TaskSpawner,
) -> PyResult<()> {
    let source = CString::new(source)
        .map_err(|_| PyValueError::new_err("generated Python source contains a NUL byte"))?;
    let filename =
        CString::new("<myelin-e2e-generated>").expect("static Python filename contains no NUL");
    let module_name =
        CString::new("_myelin_e2e_generated").expect("static Python module name contains no NUL");
    Python::with_gil(|py| {
        let module = PyModule::from_code(py, &source, &filename, &module_name)?;
        let main = module.getattr("main")?;
        run(py, main, runtime, engine, tasks)
    })
}

/// Attach the calling process to its inherited data-plane bootstrap
/// context and drive the async `main(ctx)` coroutine to completion. The
/// attached session is closed before any error propagates.
fn run(
    py: Python<'_>,
    main: Bound<'_, PyAny>,
    runtime: Runtime,
    engine: Engine,
    tasks: TaskSpawner,
) -> PyResult<()> {
    let host_route = data_plane::bootstrap::host_route::BootstrapHostRoute::inherited(
        engine.handle().blocking_work_sender(),
    )
    .map_err(|error| bootstrap_error(error.to_string()))?;
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
    // `routing` owns the driver and engine; it remains alive through
    // acknowledged session shutdown and is then torn down explicitly.
    let (runtime, routing, child_node) = match build_child_routing(
        runtime,
        engine,
        tasks.clone(),
        material.host_session,
        host_endpoint,
    ) {
        Ok(routing) => routing,
        Err(error) => {
            report_attachment_failure(attachment, &error.to_string());
            return Err(error);
        }
    };
    let bootstrap = match future::block_on(DataPlaneBootstrap::attach_routed(
        arena_fd,
        runtime,
        material.host_session,
        material.session_capability,
        Some(child_node),
        None,
        Arc::new(host_route),
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
            inner: Arc::downgrade(&data_plane),
            tasks,
        },
    )?;
    let context = Py::new(py, PyContext { data })?;
    // Run the guest coroutine while retaining our own context reference, then
    // release its data-plane ownership before acknowledged session shutdown
    // and routing teardown.
    let main_result = (|| -> PyResult<()> {
        let coroutine = main.call1((context.clone_ref(py),))?;
        let asyncio = PyModule::import(py, "asyncio")?;
        asyncio.call_method1("run", (coroutine,))?;
        Ok(())
    })();
    drop(context);
    let close_result = future::block_on(data_plane.close_acknowledged()).map_err(data_plane_error);
    drop(data_plane);
    // Driver and engine destruction can block; release the GIL while keeping
    // teardown synchronous with this call.
    py.allow_threads(move || drop(routing));
    main_result.and(close_result)
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
    // Runtime startup belongs to the native contextual-workload executable.
    Ok(())
}
