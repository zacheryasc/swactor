use std::collections::HashMap;
use std::ffi::{CString, c_int, c_void};
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use data_plane::blob::{Blob, BlobView, ContentDigest, WritableArenaView};
use data_plane::bootstrap as dp_bootstrap;
use data_plane::data_plane::{BlobWriter, DataPlane, DataPlaneBootstrap, parse_actor_address};
use data_plane::path::DataPath;
use data_plane::protocol::{
    BlobFailure, DataPlaneError, JobCapability, register_data_plane_codecs,
};
use distribution::node::DistributedNodeConfig;
use distribution::transport_bridge::{
    Outbox, OutboxRouteBinder, RelayMirror, RouteBinder, RouteView, RouteViewTransport,
};
use futures_lite::future;
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::{IrohDriver, IrohDriverConfig};
use parking_lot::Mutex as ParkingMutex;
use pyo3::exceptions::{PyBufferError, PyPermissionError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Runtime, RuntimeParts};
use swactor_engine::{ActorCompletion, Engine, EngineHandle, TokioBackend, TokioConfig};
use swactor_transport::{CodecRegistry, CodecRemoteSink, TransportRouter};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::UnixStream;

const ROUTE_POLL: Duration = Duration::from_millis(5);
const ROUTE_DEADLINE: Duration = Duration::from_secs(5);
const LEGACY_OUTPUT_ENV: &str = "SWACTOR_DATA_PLANE_OUTPUT";

pyo3::create_exception!(swactor, SwactorError, pyo3::exceptions::PyException);
pyo3::create_exception!(swactor, BootstrapError, SwactorError);
pyo3::create_exception!(swactor, DataPathError, SwactorError);
pyo3::create_exception!(swactor, BlobError, SwactorError);
pyo3::create_exception!(swactor, SessionError, SwactorError);

fn bootstrap_error(message: impl Into<String>) -> PyErr {
    PyErr::new::<BootstrapError, _>(message.into())
}

fn data_plane_error(error: DataPlaneError) -> PyErr {
    match error {
        DataPlaneError::InvalidPath(reason) => PyErr::new::<DataPathError, _>(reason),
        DataPlaneError::Unauthorized { path, operation } => {
            PyPermissionError::new_err(format!("{operation:?} is not authorized for {path}"))
        }
        DataPlaneError::PathNotFound(path) => {
            PyErr::new::<DataPathError, _>(format!("data path not found: {path}"))
        }
        DataPlaneError::Blob(reason) => PyErr::new::<BlobError, _>(format!("{reason:?}")),
        DataPlaneError::Attachment(reason) => {
            PyErr::new::<SessionError, _>(format!("attachment failed: {reason:?}"))
        }
        other => PyErr::new::<SessionError, _>(other.to_string()),
    }
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
    legacy_output: Option<String>,
}

#[pymethods]
impl PyDataPlane {
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

    fn read_stream(&self, _path: String) -> PyResult<()> {
        Err(PyRuntimeError::new_err(
            "actor-driven stream reads are not installed",
        ))
    }

    fn write_stream(&self, path: String) -> PyResult<PyStreamContext> {
        DataPath::parse(path).map_err(|error| PyErr::new::<DataPathError, _>(error.to_string()))?;
        let socket = self.legacy_output.clone().ok_or_else(|| {
            PyRuntimeError::new_err("temporary deployment stream bridge is not configured")
        })?;
        Ok(PyStreamContext {
            socket,
            stream: Arc::new(tokio::sync::Mutex::new(None)),
        })
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

#[pyclass(name = "StreamWriter")]
pub struct PyStreamWriter {
    stream: Arc<tokio::sync::Mutex<Option<BufWriter<UnixStream>>>>,
}

#[pymethods]
impl PyStreamWriter {
    fn write<'py>(&self, py: Python<'py>, bytes: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = stream.lock().await;
            let stream = stream
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("stream writer is closed"))?;
            stream
                .write_all(&bytes)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Ok(())
        })
    }
}

#[pyclass(name = "_StreamWriteContext")]
pub struct PyStreamContext {
    socket: String,
    stream: Arc<tokio::sync::Mutex<Option<BufWriter<UnixStream>>>>,
}

#[pymethods]
impl PyStreamContext {
    fn __aenter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let socket = self.socket.clone();
        let state = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = UnixStream::connect(&socket).await.map_err(|error| {
                PyRuntimeError::new_err(format!("connect output stream: {error}"))
            })?;
            *state.lock().await = Some(BufWriter::new(stream));
            Python::with_gil(|py| Py::new(py, PyStreamWriter { stream: state }))
        })
    }

    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(mut stream) = state.lock().await.take() {
                stream
                    .flush()
                    .await
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                stream
                    .shutdown()
                    .await
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
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
            legacy_output: std::env::var(LEGACY_OUTPUT_ENV).ok(),
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
#[pyclass(name = "_TestDataPlaneHost")]
struct PyTestDataPlaneHost {
    _driver: Arc<IrohDriver>,
    _engine: Engine,
    handoff: data_plane::bootstrap::JobHandoff,
    namespace_root: std::path::PathBuf,
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
                    namespace: Some(namespace),
                    transfer_receiver: Some(Arc::new(DebugBlobReceiver)),
                    source_sender: Some(source_sender),
                    source_publisher: Some(source_publisher),
                    route_registrar: Some(registrar),
                },
            )
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?,
        )
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    data_plane::host::install_session_env(&mut handoff, host_session, capability);

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
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("SwactorError", module.py().get_type::<SwactorError>())?;
    module.add("BootstrapError", module.py().get_type::<BootstrapError>())?;
    module.add("DataPathError", module.py().get_type::<DataPathError>())?;
    module.add("BlobError", module.py().get_type::<BlobError>())?;
    module.add("SessionError", module.py().get_type::<SessionError>())?;
    module.add_class::<PyDataPlane>()?;
    module.add_class::<PyBlob>()?;
    module.add_class::<PyBlobView>()?;
    module.add_class::<PyStreamWriter>()?;
    module.add_class::<PyContext>()?;
    module.add_function(wrap_pyfunction!(run, module)?)?;
    #[cfg(all(debug_assertions, target_os = "linux"))]
    {
        module.add_class::<PyTestDataPlaneHost>()?;
        module.add_function(wrap_pyfunction!(_test_data_plane_host, module)?)?;
    }
    Ok(())
}
