use std::any::Any;
use std::cell::RefCell;
use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyModule;

use ::swactor::actor::{
    Actor, ActorAddress, ActorInterface, AnyActor, Ctx, Environment, SpawnRequest,
};
use ::swactor::config::RuntimeConfig;
use ::swactor::runtime::{Inbox, Runtime, RuntimeParts};
use swactor_engine::{Engine, SteppingBackend};

use data_plane::bootstrap as dp_bootstrap;

// ─── PyMsg newtype ───────────────────────────────────────────────────────────

/// Newtype around `PyObject` that implements `Clone + Send + Sync`.
///
/// `Py<PyAny>` in pyo3 0.23 doesn't implement `Clone` by default.
/// We implement it by acquiring the GIL to bump the refcount.
/// `Send + Sync` are safe because `Py<T>` is a reference-counted
/// pointer to a Python object protected by the GIL.
#[derive(Debug)]
struct PyMsg(PyObject);

impl Clone for PyMsg {
    fn clone(&self) -> Self {
        Python::with_gil(|py| PyMsg(self.0.clone_ref(py)))
    }
}

// Safety: Py<PyAny> is Send + Sync — access is serialized by the GIL.
unsafe impl Send for PyMsg {}
unsafe impl Sync for PyMsg {}

impl PyMsg {
    fn into_inner(self) -> PyObject {
        self.0
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn to_py_err(e: ::swactor::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
}

// ─── PyActorAddress ──────────────────────────────────────────────────────────

#[pyclass(name = "ActorAddress")]
#[derive(Clone)]
pub struct PyActorAddress {
    inner: ActorAddress,
}

#[pymethods]
impl PyActorAddress {
    fn hex(&self) -> String {
        self.inner.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn to_bytes(&self) -> Vec<u8> {
        self.inner.0.to_vec()
    }

    fn __repr__(&self) -> String {
        let hex = self.hex();
        format!("ActorAddress({hex})")
    }

    fn __eq__(&self, other: &PyActorAddress) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.inner.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<ActorAddress> for PyActorAddress {
    fn from(inner: ActorAddress) -> Self {
        Self { inner }
    }
}

// ─── Effects / PyCtx ─────────────────────────────────────────────────────────

enum Effect {
    Send {
        addr: ActorAddress,
        msg: PyObject,
    },
    Spawn {
        addr: ActorAddress,
        handler: PyObject,
    },
}

#[pyclass(name = "Ctx", unsendable)]
pub struct PyCtx {
    self_addr: ActorAddress,
    effects: RefCell<Vec<Effect>>,
}

impl PyCtx {
    fn new(self_addr: ActorAddress) -> Self {
        Self {
            self_addr,
            effects: RefCell::new(Vec::new()),
        }
    }

    fn take_effects(&self) -> Vec<Effect> {
        self.effects.borrow_mut().drain(..).collect()
    }
}

#[pymethods]
impl PyCtx {
    #[getter]
    fn self_addr(&self) -> PyActorAddress {
        PyActorAddress::from(self.self_addr)
    }

    fn send(&self, addr: &PyActorAddress, msg: PyObject) {
        self.effects.borrow_mut().push(Effect::Send {
            addr: addr.inner,
            msg,
        });
    }

    fn spawn(&self, handler: PyObject) -> PyActorAddress {
        let addr = ActorAddress::new_random();
        self.effects
            .borrow_mut()
            .push(Effect::Spawn { addr, handler });
        PyActorAddress::from(addr)
    }
}

// ─── PyActor ─────────────────────────────────────────────────────────────────

struct PyActor {
    handler: PyObject,
}

impl PyActor {
    fn new(handler: PyObject) -> Self {
        Self { handler }
    }
}

impl ActorInterface for PyActor {
    type Incoming = PyMsg;
    type Response = PyMsg;

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        let py_ctx = PyCtx::new(ctx.self_addr());

        let call_result = Python::with_gil(|py| {
            let ctx_bound = Bound::new(py, py_ctx)?;
            self.handler.call1(py, (&ctx_bound, msg.into_inner()))?;
            let ctx_ref = ctx_bound.borrow();
            Ok::<Vec<Effect>, PyErr>(ctx_ref.take_effects())
        });

        match call_result {
            Ok(effects) => {
                for effect in effects {
                    match effect {
                        Effect::Send { addr, msg } => {
                            let _ = ctx
                                .raw_inner()
                                .send_any(addr, Box::new(PyMsg(msg)) as Box<dyn Any + Send>);
                        }
                        Effect::Spawn { addr, handler } => {
                            let actor = PyActor::new(handler);
                            let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
                            ctx.raw_inner().spawn_any(SpawnRequest {
                                addr,
                                actor: boxed,
                                parent: Some(ctx.self_addr()),
                                env: Environment::new(),
                            });
                        }
                    }
                }
            }
            Err(e) => {
                Python::with_gil(|py| {
                    e.print(py);
                });
            }
        }
    }
}

// ─── PyInbox ─────────────────────────────────────────────────────────────────

#[pyclass(name = "Inbox")]
pub struct PyInbox {
    inner: Inbox<PyMsg>,
}

#[pymethods]
impl PyInbox {
    #[getter]
    fn addr(&self) -> PyActorAddress {
        PyActorAddress::from(*self.inner.addr())
    }

    fn try_recv(&self) -> Option<PyObject> {
        self.inner.try_recv().map(|m| m.into_inner())
    }
}

// ─── PyRuntimeConfig ─────────────────────────────────────────────────────────

#[pyclass(name = "RuntimeConfig")]
#[derive(Clone)]
pub struct PyRuntimeConfig {
    #[pyo3(get, set)]
    max_actors: usize,
    #[pyo3(get, set)]
    channel_buffer_size: usize,
    #[pyo3(get, set)]
    actor_message_budget: usize,
    #[pyo3(get, set)]
    worker_count: usize,
    #[pyo3(get, set)]
    worker_ingress_budget: usize,
}

#[pymethods]
impl PyRuntimeConfig {
    #[new]
    #[pyo3(signature = (
        *,
        max_actors = 1_000,
        channel_buffer_size = 1_000,
        actor_message_budget = 64,
        worker_count = 1,
        worker_ingress_budget = 1_024,
    ))]
    fn new(
        max_actors: usize,
        channel_buffer_size: usize,
        actor_message_budget: usize,
        worker_count: usize,
        worker_ingress_budget: usize,
    ) -> Self {
        Self {
            max_actors,
            channel_buffer_size,
            actor_message_budget,
            worker_count,
            worker_ingress_budget,
        }
    }
}

impl From<PyRuntimeConfig> for RuntimeConfig {
    fn from(py: PyRuntimeConfig) -> Self {
        RuntimeConfig {
            max_actors: py.max_actors,
            channel_buffer_size: py.channel_buffer_size,
            actor_message_budget: py.actor_message_budget,
            worker_count: py.worker_count,
            worker_ingress_budget: py.worker_ingress_budget,
        }
    }
}

// ─── PyRuntime ───────────────────────────────────────────────────────────────

#[pyclass(name = "Runtime", unsendable)]
pub struct PyRuntime {
    runtime: Runtime,
    _engine: Engine,
    backend: SteppingBackend,
}

#[pymethods]
impl PyRuntime {
    #[new]
    #[pyo3(signature = (config=None))]
    fn new(config: Option<PyRuntimeConfig>) -> Self {
        let config: RuntimeConfig = match config {
            Some(c) => c.into(),
            None => RuntimeConfig::default(),
        };
        let parts = RuntimeParts::new(config);
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).expect("create Python actor engine");
        Self {
            runtime,
            _engine: engine,
            backend,
        }
    }

    fn spawn(&self, handler: PyObject) -> PyResult<PyActorAddress> {
        let rt = &self.runtime;
        let actor = PyActor::new(handler);
        let addr = rt.spawn(actor).map_err(to_py_err)?;
        Ok(PyActorAddress::from(addr))
    }

    fn send(&self, addr: &PyActorAddress, msg: PyObject) -> PyResult<()> {
        let rt = &self.runtime;
        rt.send_to(addr.inner, PyMsg(msg)).map_err(to_py_err)
    }

    fn inbox(&self) -> PyResult<PyInbox> {
        let rt = &self.runtime;
        let inbox: Inbox<PyMsg> = rt.new_inbox().map_err(to_py_err)?;
        Ok(PyInbox { inner: inbox })
    }

    fn tick(&mut self) -> PyResult<()> {
        self.backend.step();
        Ok(())
    }

    fn stats(&self) -> PyResult<PyRuntimeStats> {
        let rt = &self.runtime;
        Ok(build_stats(rt))
    }
}

// ─── ActorInfo / RuntimeStats ────────────────────────────────────────────────

#[pyclass(name = "ActorInfo")]
#[derive(Clone)]
pub struct PyActorInfo {
    #[pyo3(get)]
    address: PyActorAddress,
    #[pyo3(get)]
    worker_id: usize,
}

#[pymethods]
impl PyActorInfo {
    fn __repr__(&self) -> String {
        let hex = self.address.hex();
        format!("ActorInfo(address={hex}, worker={})", self.worker_id)
    }
}

#[pyclass(name = "WorkerInfo")]
#[derive(Clone)]
pub struct PyWorkerInfo {
    #[pyo3(get)]
    id: usize,
    #[pyo3(get)]
    num_actors: usize,
    #[pyo3(get)]
    mailbox_depth: usize,
    #[pyo3(get)]
    messages_processed: u64,
}

#[pymethods]
impl PyWorkerInfo {
    fn __repr__(&self) -> String {
        format!(
            "WorkerInfo(id={}, actors={}, queued={}, processed={})",
            self.id, self.num_actors, self.mailbox_depth, self.messages_processed
        )
    }
}

#[pyclass(name = "RuntimeStats")]
#[derive(Clone)]
pub struct PyRuntimeStats {
    #[pyo3(get)]
    num_actors: usize,
    #[pyo3(get)]
    num_workers: usize,
    #[pyo3(get)]
    uptime_ms: u64,
    #[pyo3(get)]
    actors: Vec<PyActorInfo>,
    #[pyo3(get)]
    workers: Vec<PyWorkerInfo>,
}

#[pymethods]
impl PyRuntimeStats {
    fn __repr__(&self) -> String {
        let mut out = format!(
            "RuntimeStats(actors={}, workers={})",
            self.num_actors, self.num_workers
        );

        for w in &self.workers {
            out.push_str(&format!(
                "\n  Worker {}: {} actors, {} queued, {} processed",
                w.id, w.num_actors, w.mailbox_depth, w.messages_processed
            ));
            for info in &self.actors {
                if info.worker_id == w.id {
                    out.push_str(&format!("\n    - {}", info.address.hex()));
                }
            }
        }

        out
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

fn build_stats(runtime: &Runtime) -> PyRuntimeStats {
    let stats = runtime.stats();
    let actors: Vec<PyActorInfo> = stats
        .actors
        .into_iter()
        .map(|(addr, wid)| PyActorInfo {
            address: PyActorAddress::from(addr),
            worker_id: wid,
        })
        .collect();
    let workers: Vec<PyWorkerInfo> = stats
        .workers
        .into_iter()
        .map(|w| PyWorkerInfo {
            id: w.id,
            num_actors: w.num_actors,
            mailbox_depth: w.mailbox_depth,
            messages_processed: w.messages_processed,
        })
        .collect();
    PyRuntimeStats {
        num_actors: actors.len(),
        num_workers: stats.num_workers,
        uptime_ms: stats.uptime_ms,
        actors,
        workers,
    }
}

// ─── Job entrypoint: swactor.run ─────────────────────────────────────────────

pyo3::create_exception!(swactor, SwactorError, pyo3::exceptions::PyException);
pyo3::create_exception!(swactor, BootstrapError, SwactorError);

/// Read-only shared mapping of the inherited arena.
///
/// The mapping is immutable for the process lifetime once created, which is
/// what makes the `Send + Sync` impls sound; `Drop` unmaps exactly once.
struct ArenaMap {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for ArenaMap {}
unsafe impl Sync for ArenaMap {}

impl ArenaMap {
    fn map(fd: std::os::fd::RawFd, len: usize) -> std::io::Result<Self> {
        // SAFETY: mmap with a validated length and live descriptor; the
        // mapping is checked against MAP_FAILED immediately.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(Self {
                ptr: ptr.cast(),
                len,
            })
        }
    }

    fn header(&self) -> &[u8] {
        let len = self.len.min(dp_bootstrap::HEADER_LEN);
        // SAFETY: `ptr..ptr+len` is inside the mapping by construction.
        unsafe { std::slice::from_raw_parts(self.ptr, len) }
    }
}

impl Drop for ArenaMap {
    fn drop(&mut self) {
        // SAFETY: unmaps exactly the mapping created in `ArenaMap::map`.
        unsafe {
            libc::munmap(self.ptr.cast(), self.len);
        }
    }
}

/// The job's data plane. Carries the resolved bootstrap state; the four path
/// primitives (`read_blob`, `write_blob`, `read_stream`, `write_stream`)
/// arrive with path resolution.
#[pyclass(name = "DataPlane")]
pub struct PyDataPlane {
    // Held (never read) to keep the arena mapping alive for the process
    // lifetime; dropping it unmaps.
    #[allow(dead_code)]
    arena: Arc<ArenaMap>,
    // Consumed by the path-resolution primitives (read_blob & co., next
    // slice); carried now so the bootstrap result lives with its owner.
    #[allow(dead_code)]
    resolved: dp_bootstrap::ResolvedBootstrap,
}

/// The job context handed to `main` by [`run`].
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

fn bootstrap_error(message: impl Into<String>) -> PyErr {
    PyErr::new::<BootstrapError, _>(message.into())
}

fn bootstrap_env_fd(name: &str) -> PyResult<std::os::fd::RawFd> {
    let value = std::env::var_os(name).ok_or_else(|| {
        bootstrap_error(format!("bootstrap environment variable {name} is not set"))
    })?;
    let text = value.to_string_lossy();
    text.parse::<std::os::fd::RawFd>()
        .map_err(|_| bootstrap_error(format!("{name}={text:?} is not a descriptor number")))
}

/// Arm `FD_CLOEXEC` on the wake descriptor. Failing also proves the
/// descriptor exists at all.
fn arm_cloexec(fd: std::os::fd::RawFd) -> PyResult<()> {
    // SAFETY: fcntl on a borrowed descriptor number.
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    if rc < 0 {
        Err(bootstrap_error(format!(
            "wake descriptor {fd} is unusable: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(())
    }
}

/// Job-process entrypoint: consume the bootstrap handoff, construct the
/// application [`Context`](crate::PyContext), and drive `main` to completion
/// on the ambient asyncio loop.
///
/// Bootstrap is fail-fast: if the inherited descriptors or the arena header
/// are absent or malformed, `main` is never invoked and `BootstrapError`
/// propagates (the process should exit non-zero).
#[pyfunction]
fn run(py: Python<'_>, main: Bound<'_, PyAny>) -> PyResult<()> {
    let arena_fd = bootstrap_env_fd(dp_bootstrap::ENV_ARENA_FD)?;
    let wake_fd = bootstrap_env_fd(dp_bootstrap::ENV_WAKE_FD)?;
    if arena_fd < 0 || wake_fd < 0 {
        return Err(bootstrap_error(
            "bootstrap descriptor numbers must be non-negative",
        ));
    }

    // Take ownership of both inherited descriptors for the process lifetime.
    // SAFETY: the environment contract hands us sole ownership of these.
    let arena_file = unsafe { File::from_raw_fd(arena_fd) };
    let wake_owned = unsafe { OwnedFd::from_raw_fd(wake_fd) };

    // Ground truth for the header's arena-size claim is the descriptor's own
    // length, never the header.
    let backing_len = arena_file
        .metadata()
        .map_err(|error| bootstrap_error(format!("stat arena descriptor: {error}")))?
        .len();
    if backing_len < dp_bootstrap::HEADER_LEN as u64 {
        return Err(bootstrap_error(format!(
            "arena backing is {backing_len} bytes, shorter than the {}-byte bootstrap header",
            dp_bootstrap::HEADER_LEN
        )));
    }
    let map = ArenaMap::map(arena_fd, usize::try_from(backing_len).expect("usize arena"))
        .map_err(|error| bootstrap_error(format!("map arena: {error}")))?;

    // The mapping keeps the arena alive; dropping the descriptor both stops
    // grandchild leakage and makes a second `run` fail loudly.
    drop(arena_file);

    // B5: the wake descriptor is the only bootstrap fd left open; arm
    // CLOEXEC so nothing this process spawns inherits it.
    arm_cloexec(wake_owned.as_raw_fd())?;

    let resolved = dp_bootstrap::parse_bootstrap(map.header(), backing_len)
        .map_err(|error| bootstrap_error(error.to_string()))?;

    let data = Py::new(
        py,
        PyDataPlane {
            arena: Arc::new(map),
            resolved,
        },
    )?;
    let context = Py::new(py, PyContext { data })?;
    let coroutine = main.call1((context,))?;
    let asyncio = PyModule::import(py, "asyncio")?;
    asyncio.call_method1("run", (coroutine,))?;
    Ok(())
}

// ─── Module registration ─────────────────────────────────────────────────────


fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SwactorError", m.py().get_type::<SwactorError>())?;
    m.add("BootstrapError", m.py().get_type::<BootstrapError>())?;
    m.add_class::<PyActorAddress>()?;
    m.add_class::<PyCtx>()?;
    m.add_class::<PyInbox>()?;
    m.add_class::<PyRuntimeConfig>()?;
    m.add_class::<PyRuntime>()?;
    m.add_class::<PyActorInfo>()?;
    m.add_class::<PyWorkerInfo>()?;
    m.add_class::<PyDataPlane>()?;
    m.add_class::<PyContext>()?;
    m.add_function(wrap_pyfunction!(run, m)?)?;
    Ok(())
}

#[pymodule]
fn swactor(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register(m)
}
