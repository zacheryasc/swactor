use std::collections::HashMap;
use std::ffi::{CString, c_int, c_void};
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use data_plane::blob::{Blob, BlobView, ContentDigest, WritableArenaView};
use data_plane::bootstrap as dp_bootstrap;
use data_plane::data_plane::{
    BlobWriter, DataPlane, DataPlaneBootstrap, Descriptor, DescriptorMapping, MapRequest,
    MapTarget, Protection, Sharing, StreamReader, StreamWriter, parse_actor_address,
};
use data_plane::path::DataPath;
use data_plane::protocol::{
    AccessMode, BlobAllocation, BlobFailure, DataPlaneError, DescriptorKind, Errno, JobCapability,
    OpenOptions, register_data_plane_codecs,
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
use swactor_engine::{ActorCompletion, Engine, EngineHandle, TokioBackend, TokioConfig};
use swactor_transport::{CodecRegistry, CodecRemoteSink, TransportRouter};

const ROUTE_POLL: Duration = Duration::from_millis(5);
const ROUTE_DEADLINE: Duration = Duration::from_secs(5);

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
        DataPlaneError::Unauthorized { path, access } => {
            PyPermissionError::new_err(format!("{access:?} is not authorized for {path}"))
        }
        DataPlaneError::PathNotFound(path) => {
            PyErr::new::<DataPathError, _>(format!("data path not found: {path}"))
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

fn bootstrap_env(name: &str) -> PyResult<String> {
    std::env::var(name)
        .map_err(|_| bootstrap_error(format!("bootstrap environment variable {name} is not set")))
}

fn bootstrap_env_fd(name: &str) -> PyResult<RawFd> {
    let value = bootstrap_env(name)?;
    value
        .parse::<RawFd>()
        .map_err(|_| bootstrap_error(format!("{name}={value:?} is not a descriptor number")))
}

fn validate_descriptor(name: &str, fd: RawFd) -> PyResult<()> {
    if fd < 0 {
        return Err(bootstrap_error(format!("{name} must be non-negative")));
    }
    // SAFETY: F_GETFD only inspects the descriptor table entry.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(bootstrap_error(format!(
            "{name}={fd} is unusable: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

struct JobRouting {
    _driver: Arc<IrohDriver>,
    _engine: Engine,
}

#[derive(Clone)]
struct RoutePoll;

struct RouteReadinessActor {
    driver: Arc<IrohDriver>,
    host_node: swactor_transport::NodeId,
    engine: EngineHandle,
    sender: swactor::runtime::ExternalSender,
    attempts_remaining: usize,
    completion: ActorCompletion<Result<(), String>>,
}

impl RouteReadinessActor {
    fn schedule(&self, ctx: &Ctx<'_>) {
        self.engine
            .send_after(ROUTE_POLL, self.sender.clone(), ctx.self_addr(), RoutePoll);
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
        if self.attempts_remaining == 0 {
            let _ = self.completion.complete(Err(
                "timed out connecting the child actor runtime to the host session".to_owned(),
            ));
            ctx.stop_self();
            return;
        }
        self.attempts_remaining -= 1;
        self.schedule(ctx);
    }
}

fn build_child_routing(
    host_session: ActorAddress,
    host_endpoint: EndpointAddr,
) -> PyResult<(Runtime, JobRouting, [u8; 32])> {
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
    driver.join(std::slice::from_ref(&host_endpoint));

    let driver = Arc::new(driver);
    let completion = ActorCompletion::new();
    let attempts = (ROUTE_DEADLINE.as_millis() / ROUTE_POLL.as_millis()) as usize;
    runtime
        .spawn(RouteReadinessActor {
            driver: driver.clone(),
            host_node,
            engine: engine.handle(),
            sender: runtime.create_sender(),
            attempts_remaining: attempts,
            completion: completion.clone(),
        })
        .map_err(|error| bootstrap_error(format!("start route readiness actor: {error}")))?;
    completion.wait().map_err(bootstrap_error)?;

    let child_node = driver.node_id().0;
    Ok((
        runtime,
        JobRouting {
            _driver: driver,
            _engine: engine,
        },
        child_node,
    ))
}

#[pyclass(name = "DataPlane")]
pub struct PyDataPlane {
    inner: Arc<DataPlane>,
    _routing: Arc<JobRouting>,
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
            let capabilities = descriptor.capabilities().bits();
            Python::with_gil(|py| {
                Py::new(
                    py,
                    PyDescriptor {
                        descriptor: Arc::new(tokio::sync::Mutex::new(descriptor)),
                        kind,
                        capabilities,
                    },
                )
            })
        })
    }

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

    fn write_stream(&self, path: String) -> PyResult<PyStreamContext> {
        let path = DataPath::parse(path)
            .map_err(|error| PyErr::new::<DataPathError, _>(error.to_string()))?;
        Ok(PyStreamContext {
            data_plane: self.inner.clone(),
            path,
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

unsafe fn mutable_buffer_bytes<'a>(buffer: &'a PyBuffer<u8>) -> &'a mut [u8] {
    // SAFETY: `byte_buffer` checked writability and contiguity, and the
    // retained `PyBuffer` keeps the exporter and pointer valid.
    unsafe { std::slice::from_raw_parts_mut(buffer.buf_ptr().cast(), buffer.len_bytes()) }
}

unsafe fn buffer_bytes<'a>(buffer: &'a PyBuffer<u8>) -> &'a [u8] {
    // SAFETY: `byte_buffer` checked contiguity and retains the exporter.
    unsafe { std::slice::from_raw_parts(buffer.buf_ptr().cast_const().cast(), buffer.len_bytes()) }
}

#[pyclass(name = "Descriptor")]
pub struct PyDescriptor {
    descriptor: Arc<tokio::sync::Mutex<Descriptor>>,
    kind: DescriptorKind,
    capabilities: u16,
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

    #[getter]
    fn capabilities(&self) -> u16 {
        self.capabilities
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
        let buffer = byte_buffer(buffer, true)?;
        let descriptor = self.descriptor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // SAFETY: the owned `PyBuffer` remains alive through completion or
            // cancellation and no pointer is retained by the Rust primitive.
            let bytes = unsafe { mutable_buffer_bytes(&buffer) };
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
    finished: bool,
    writer: Option<BlobWriter>,
}

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
                Ok(()) => {
                    state.lock().finished = true;
                    Ok(false)
                }
                Err(error) => {
                    state.lock().writer = Some(writer);
                    Err(data_plane_error(error))
                }
            }
        })
    }
}

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
        let buffer = byte_buffer(buffer, true)?;
        let reader = self.reader.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // SAFETY: the owned `PyBuffer` retains the writable exporter for
            // the complete async operation and cancellation path.
            let bytes = unsafe { mutable_buffer_bytes(&buffer) };
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

#[pyclass(name = "_StreamWriteContext")]
pub struct PyStreamContext {
    data_plane: Arc<DataPlane>,
    path: DataPath,
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match data_plane.write_stream(&path).await {
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

#[pyfunction]
fn run(py: Python<'_>, main: Bound<'_, PyAny>) -> PyResult<()> {
    let arena_fd = bootstrap_env_fd(dp_bootstrap::ENV_ARENA_FD)?;
    validate_descriptor(dp_bootstrap::ENV_ARENA_FD, arena_fd)?;
    let host_session = parse_actor_address(&bootstrap_env(dp_bootstrap::ENV_DATA_PLANE_ACTOR)?)
        .map_err(data_plane_error)?;
    let capability = JobCapability::from_hex(&bootstrap_env(dp_bootstrap::ENV_JOB_CAPABILITY)?)
        .map_err(data_plane_error)?;
    let host_endpoint: EndpointAddr =
        serde_json::from_str(&bootstrap_env(dp_bootstrap::ENV_DATA_PLANE_ENDPOINT)?)
            .map_err(|error| bootstrap_error(format!("parse host data-plane endpoint: {error}")))?;

    // SAFETY: the bootstrap contract transfers sole ownership of this inherited
    // descriptor to `run`; `MappedArena::map` closes it after mapping.
    let arena_fd = unsafe { OwnedFd::from_raw_fd(arena_fd) };
    let (arena, resolved) = DataPlaneBootstrap::map_arena(arena_fd)
        .map_err(|error| bootstrap_error(error.to_string()))?;
    let (runtime, routing, child_node) = build_child_routing(host_session, host_endpoint)?;
    let attachment_engine = routing._engine.handle();
    let bootstrap = future::block_on(DataPlaneBootstrap::attach_mapped_with_deadline(
        arena,
        resolved,
        runtime,
        host_session,
        capability,
        Some(child_node),
        data_plane::data_plane::AttachDeadline {
            engine: attachment_engine,
            timeout: ROUTE_DEADLINE,
        },
    ))
    .map_err(data_plane_error)?;

    let data = Py::new(
        py,
        PyDataPlane {
            inner: Arc::new(bootstrap.data_plane),
            _routing: Arc::new(routing),
        },
    )?;
    let context = Py::new(py, PyContext { data })?;
    let coroutine = main.call1((context,))?;
    let asyncio = PyModule::import(py, "asyncio")?;
    asyncio.call_method1("run", (coroutine,))?;
    Ok(())
}

#[cfg(all(debug_assertions, target_os = "linux"))]
struct DebugHostRouteRegistrar {
    route_view: RouteView,
    route_binder: Arc<OutboxRouteBinder>,
}

#[cfg(all(debug_assertions, target_os = "linux"))]
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
}

#[cfg(all(debug_assertions, target_os = "linux"))]
struct DebugBlobSender {
    runtime: Runtime,
}

#[cfg(all(debug_assertions, target_os = "linux"))]
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

#[cfg(all(debug_assertions, target_os = "linux"))]
struct DebugBlobReceiver;

#[cfg(all(debug_assertions, target_os = "linux"))]
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

#[cfg(all(debug_assertions, target_os = "linux"))]
struct DebugNamespaceDiscovery(ActorAddress);

#[cfg(all(debug_assertions, target_os = "linux"))]
impl data_plane::namespace::NamespaceDiscovery for DebugNamespaceDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        Some(self.0)
    }
}

#[cfg(all(debug_assertions, target_os = "linux"))]
struct DebugSourceRegistrar;

#[cfg(all(debug_assertions, target_os = "linux"))]
impl data_plane::source::BlobSourcePublisher for DebugSourceRegistrar {
    fn publish_source(&self, _source: ActorAddress) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(all(debug_assertions, target_os = "linux"))]
struct DebugStreamCaptureConsumer {
    bytes: Arc<ParkingMutex<Vec<u8>>>,
}

#[cfg(all(debug_assertions, target_os = "linux"))]
impl data_plane::data_plane::StreamConsumer for DebugStreamCaptureConsumer {
    fn consume(&self, bytes: &[u8]) -> Result<(), String> {
        self.bytes.lock().extend_from_slice(bytes);
        Ok(())
    }
}

#[cfg(all(debug_assertions, target_os = "linux"))]
#[pyclass(name = "_TestDataPlaneHost")]
struct PyTestDataPlaneHost {
    _driver: Arc<IrohDriver>,
    _engine: Engine,
    handoff: data_plane::bootstrap::JobHandoff,
    namespace_root: std::path::PathBuf,
    stream_data_plane: Arc<DataPlane>,
}

#[cfg(all(debug_assertions, target_os = "linux"))]
impl Drop for PyTestDataPlaneHost {
    fn drop(&mut self) {
        self._driver.shutdown();
        let _ = std::fs::remove_dir_all(&self.namespace_root);
    }
}

#[cfg(all(debug_assertions, target_os = "linux"))]
#[pymethods]
impl PyTestDataPlaneHost {
    fn env<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let env = pyo3::types::PyDict::new(py);
        for (name, value) in &self.handoff.env {
            env.set_item(name, value)?;
        }
        Ok(env)
    }

    fn arena_fd(&self) -> RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.handoff.arena_fd)
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

#[cfg(all(debug_assertions, target_os = "linux"))]
#[pyclass(name = "_TestStreamCapture")]
struct PyTestStreamCapture {
    completion: ActorCompletion<Result<(), DataPlaneError>>,
    bytes: Arc<ParkingMutex<Vec<u8>>>,
}

#[cfg(all(debug_assertions, target_os = "linux"))]
#[pymethods]
impl PyTestStreamCapture {
    fn result<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.completion.wait().map_err(data_plane_error)?;
        Ok(PyBytes::new(py, &self.bytes.lock()))
    }
}

#[cfg(all(debug_assertions, target_os = "linux"))]
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
    let mut handoff = data_plane::bootstrap::write_bootstrap(
        &mut arena,
        data_plane::bootstrap::BootstrapSpec {
            arena_generation: 1,
            alignment: 64,
        },
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let capability = JobCapability::new([0x5a; 32]);
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
        namespace_root.join("namespace.json"),
        Arc::clone(&source_sender),
        Arc::clone(&source_publisher),
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let directory = namespace_service.directory();
    let weights_path = DataPath::parse("/models/tiny-linear/weights")
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let weights_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../apps/myelin/jobs/tiny_linear.weights");
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
                    arena,
                    arena_generation: 1,
                    session_generation: 1,
                    capability,
                    job_context: data_plane::path::JobContext {
                        run_id: "test-run".to_owned(),
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
    data_plane::host::install_session_env(&mut handoff, host_session, capability);

    let mut sink_arena = data_plane::arena::ArenaManager::boot(data_plane::arena::ArenaConfig {
        node_id: data_plane::arena::NodeId(2),
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .map_err(|error| PyRuntimeError::new_err(format!("debug sink arena: {error:?}")))?;
    let sink_handoff = data_plane::bootstrap::write_bootstrap(
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
                    arena: sink_arena,
                    arena_generation: 2,
                    session_generation: 2,
                    capability,
                    job_context: data_plane::path::JobContext {
                        run_id: "test-run".to_owned(),
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
            sink_handoff.arena_fd,
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
    handoff.env.insert(
        dp_bootstrap::ENV_DATA_PLANE_ENDPOINT.to_owned(),
        serde_json::to_string(&driver.endpoint_addr())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
    );

    Ok(PyTestDataPlaneHost {
        _driver: Arc::new(driver),
        _engine: engine,
        handoff,
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
    module.add_class::<PyDescriptor>()?;
    module.add_class::<PyDescriptorMapping>()?;
    module.add_class::<PyBlob>()?;
    module.add_class::<PyBlobView>()?;
    module.add_class::<PyStreamWriter>()?;
    module.add_class::<PyStreamReader>()?;
    module.add_class::<PyContext>()?;
    module.add_function(wrap_pyfunction!(run, module)?)?;
    #[cfg(all(debug_assertions, target_os = "linux"))]
    {
        module.add_class::<PyTestStreamCapture>()?;
        module.add_class::<PyTestDataPlaneHost>()?;
        module.add_function(wrap_pyfunction!(_test_data_plane_host, module)?)?;
    }
    Ok(())
}
