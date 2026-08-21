use std::any::Any;
use std::cell::RefCell;

use pyo3::prelude::*;
use pyo3::types::PyModule;

use ::swactor::actor::{
    Actor, ActorAddress, ActorInterface, AnyActor, Ctx, Environment, SpawnRequest,
};
use ::swactor::config::RuntimeConfig;
use ::swactor::runtime::{Inbox, Runtime, RuntimeParts};
use swactor_engine::{Engine, SteppingBackend};

mod job;

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

// ─── Module registration ─────────────────────────────────────────────────────

fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    job::register(m)?;
    m.add_class::<PyActorAddress>()?;
    m.add_class::<PyCtx>()?;
    m.add_class::<PyInbox>()?;
    m.add_class::<PyRuntimeConfig>()?;
    m.add_class::<PyRuntime>()?;
    m.add_class::<PyActorInfo>()?;
    m.add_class::<PyWorkerInfo>()?;
    Ok(())
}

#[pymodule]
fn swactor(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register(m)
}
