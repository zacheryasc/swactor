use std::any::Any;
use std::cell::RefCell;

use pyo3::prelude::*;
use pyo3::types::PyModule;

use crate::actor::{Actor, ActorAddress, ActorInterface, AnyActor};
use crate::config::{BackoffPolicy, RuntimeConfig};
use crate::runtime::{Ctx, Inbox, Runtime, RuntimeHandle};
use crate::worker::Mailbox;
use crate::Error;

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

fn to_py_err(e: Error) -> PyErr {
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
        self.inner
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
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
        self.effects.borrow_mut().push(Effect::Spawn {
            addr,
            handler,
        });
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
            self.handler
                .call1(py, (&ctx_bound, msg.into_inner()))?;
            let ctx_ref = ctx_bound.borrow();
            Ok::<Vec<Effect>, PyErr>(ctx_ref.take_effects())
        });

        match call_result {
            Ok(effects) => {
                for effect in effects {
                    match effect {
                        Effect::Send { addr, msg } => {
                            let _ = ctx.raw_inner().send_any(
                                addr,
                                Box::new(PyMsg(msg)) as Box<dyn Any + Send>,
                            );
                        }
                        Effect::Spawn { addr, handler } => {
                            let waterlevel = ctx.raw_inner().mailbox_waterlevel();
                            let actor = PyActor::new(handler);
                            let actor = Actor::new(addr, Mailbox::new(waterlevel), actor);
                            let boxed: Box<dyn AnyActor> = Box::new(actor);
                            let _ = ctx.raw_inner().spawn_any(addr, boxed);
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
    num_threads: usize,
    #[pyo3(get, set)]
    max_actors: usize,
    #[pyo3(get, set)]
    actor_max_messages: usize,
    #[pyo3(get, set)]
    mailbox_waterlevel: usize,
    #[pyo3(get, set)]
    spin_threshold: u32,
    #[pyo3(get, set)]
    yield_threshold: u32,
    #[pyo3(get, set)]
    sleep_increment_us: u64,
    #[pyo3(get, set)]
    sleep_max_us: u64,
}

#[pymethods]
impl PyRuntimeConfig {
    #[new]
    #[pyo3(signature = (
        *,
        num_threads = 1,
        max_actors = 1_000,
        actor_max_messages = 1_000,
        mailbox_waterlevel = 10,
        spin_threshold = 64,
        yield_threshold = 256,
        sleep_increment_us = 50,
        sleep_max_us = 1_000,
    ))]
    fn new(
        num_threads: usize,
        max_actors: usize,
        actor_max_messages: usize,
        mailbox_waterlevel: usize,
        spin_threshold: u32,
        yield_threshold: u32,
        sleep_increment_us: u64,
        sleep_max_us: u64,
    ) -> Self {
        Self {
            num_threads,
            max_actors,
            actor_max_messages,
            mailbox_waterlevel,
            spin_threshold,
            yield_threshold,
            sleep_increment_us,
            sleep_max_us,
        }
    }
}

impl From<PyRuntimeConfig> for RuntimeConfig {
    fn from(py: PyRuntimeConfig) -> Self {
        RuntimeConfig {
            num_threads: py.num_threads,
            max_actors: py.max_actors,
            actor_max_messages: py.actor_max_messages,
            mailbox_waterlevel: py.mailbox_waterlevel,
            backoff_policy: BackoffPolicy {
                spin_threshold: py.spin_threshold,
                yield_threshold: py.yield_threshold,
                sleep_increment_us: py.sleep_increment_us,
                sleep_max_us: py.sleep_max_us,
            },
        }
    }
}

// ─── PyRuntime ───────────────────────────────────────────────────────────────

#[pyclass(name = "Runtime")]
pub struct PyRuntime {
    inner: Option<Runtime>,
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
        Self {
            inner: Some(Runtime::new(config)),
        }
    }

    fn spawn(&self, handler: PyObject) -> PyResult<PyActorAddress> {
        let rt = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Runtime consumed by run()"))?;
        let actor = PyActor::new(handler);
        let addr = rt.spawn(actor).map_err(to_py_err)?;
        Ok(PyActorAddress::from(addr))
    }

    fn send(&self, addr: &PyActorAddress, msg: PyObject) -> PyResult<()> {
        let rt = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Runtime consumed by run()"))?;
        rt.send_to(addr.inner, PyMsg(msg)).map_err(to_py_err)
    }

    fn inbox(&self) -> PyResult<PyInbox> {
        let rt = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Runtime consumed by run()"))?;
        let inbox: Inbox<PyMsg> = rt.new_inbox().map_err(to_py_err)?;
        Ok(PyInbox { inner: inbox })
    }

    fn tick(&self) -> PyResult<()> {
        let rt = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Runtime consumed by run()"))?;
        rt.tick();
        Ok(())
    }

    fn run(&mut self, py: Python<'_>) -> PyResult<PyRuntimeHandle> {
        let rt = self
            .inner
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Runtime consumed by run()"))?;
        let handle = py.allow_threads(|| rt.run().map_err(to_py_err))?;
        Ok(PyRuntimeHandle {
            inner: Some(handle),
        })
    }

    fn stats(&self) -> PyResult<PyRuntimeStats> {
        let rt = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Runtime consumed by run()"))?;
        Ok(build_stats(rt))
    }

    fn shutdown(&self) -> PyResult<()> {
        let rt = self
            .inner
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Runtime consumed by run()"))?;
        rt.shutdown();
        Ok(())
    }
}

// ─── PyRuntimeHandle ─────────────────────────────────────────────────────────

#[pyclass(name = "RuntimeHandle")]
pub struct PyRuntimeHandle {
    inner: Option<RuntimeHandle>,
}

#[pymethods]
impl PyRuntimeHandle {
    fn spawn(&self, handler: PyObject) -> PyResult<PyActorAddress> {
        let handle = self
            .inner
            .as_ref()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("RuntimeHandle consumed by join()")
            })?;
        let actor = PyActor::new(handler);
        let addr = handle.runtime.spawn(actor).map_err(to_py_err)?;
        Ok(PyActorAddress::from(addr))
    }

    fn send(&self, addr: &PyActorAddress, msg: PyObject) -> PyResult<()> {
        let handle = self
            .inner
            .as_ref()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("RuntimeHandle consumed by join()")
            })?;
        handle
            .runtime
            .send_to(addr.inner, PyMsg(msg))
            .map_err(to_py_err)
    }

    fn inbox(&self) -> PyResult<PyInbox> {
        let handle = self
            .inner
            .as_ref()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("RuntimeHandle consumed by join()")
            })?;
        let inbox: Inbox<PyMsg> = handle.runtime.new_inbox().map_err(to_py_err)?;
        Ok(PyInbox { inner: inbox })
    }

    fn stats(&self) -> PyResult<PyRuntimeStats> {
        let handle = self
            .inner
            .as_ref()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("RuntimeHandle consumed by join()")
            })?;
        Ok(build_stats(&handle.runtime))
    }

    fn shutdown(&self) -> PyResult<()> {
        let handle = self
            .inner
            .as_ref()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("RuntimeHandle consumed by join()")
            })?;
        handle.shutdown();
        Ok(())
    }

    fn join(&mut self, py: Python<'_>) -> PyResult<()> {
        let handle = self
            .inner
            .take()
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("RuntimeHandle consumed by join()")
            })?;
        py.allow_threads(|| handle.join());
        Ok(())
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

#[pyclass(name = "RuntimeStats")]
#[derive(Clone)]
pub struct PyRuntimeStats {
    #[pyo3(get)]
    num_actors: usize,
    #[pyo3(get)]
    num_workers: usize,
    #[pyo3(get)]
    actors: Vec<PyActorInfo>,
}

#[pymethods]
impl PyRuntimeStats {
    fn __repr__(&self) -> String {
        let mut out = format!(
            "RuntimeStats(actors={}, workers={})",
            self.num_actors, self.num_workers
        );

        // Group actors by worker
        let mut by_worker: std::collections::BTreeMap<usize, Vec<&PyActorInfo>> =
            std::collections::BTreeMap::new();
        for info in &self.actors {
            by_worker.entry(info.worker_id).or_default().push(info);
        }

        for wid in 0..self.num_workers {
            let actors = by_worker.get(&wid);
            let count = actors.map_or(0, |v| v.len());
            out.push_str(&format!("\n  Worker {wid}: {count} actors"));
            if let Some(actors) = actors {
                for info in actors {
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
    let (num_workers, snapshot) = runtime.stats();
    let actors: Vec<PyActorInfo> = snapshot
        .into_iter()
        .map(|(addr, wid)| PyActorInfo {
            address: PyActorAddress::from(addr),
            worker_id: wid.as_usize(),
        })
        .collect();
    PyRuntimeStats {
        num_actors: actors.len(),
        num_workers,
        actors,
    }
}

// ─── Module registration ─────────────────────────────────────────────────────

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyActorAddress>()?;
    m.add_class::<PyCtx>()?;
    m.add_class::<PyInbox>()?;
    m.add_class::<PyRuntimeConfig>()?;
    m.add_class::<PyRuntime>()?;
    m.add_class::<PyRuntimeHandle>()?;
    m.add_class::<PyActorInfo>()?;
    m.add_class::<PyRuntimeStats>()?;
    Ok(())
}
