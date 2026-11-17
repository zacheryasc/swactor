//! Python awaitables backed only by explicitly supplied engine task authority.

use std::sync::Arc;

use pyo3::BoundObject;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyTuple};
use swactor_engine::TaskSpawner;
use tokio::sync::Notify;

/// Retain completion responsibility even before the engine first polls a task.
/// Dropping rejected, aborted, or panicking work must wake the Python awaiter.
struct CompletionGuard {
    event_loop: Py<PyAny>,
    future: Option<Py<PyAny>>,
}

impl CompletionGuard {
    fn finish(&mut self, py: Python<'_>, result: PyResult<Py<PyAny>>) {
        let Some(future) = self.future.take() else {
            return;
        };
        let scheduled = (|| -> PyResult<()> {
            let callback = PyCFunction::new_closure(
                py,
                None,
                None,
                move |args: &Bound<'_, PyTuple>,
                      _kwargs: Option<&Bound<'_, PyDict>>|
                      -> PyResult<()> {
                    let py = args.py();
                    let future = future.bind(py);
                    // Both this check and the state transition run on the loop
                    // thread: cancellation cannot race between them.
                    if !future.call_method0("done")?.is_truthy()? {
                        match &result {
                            Ok(value) => future.call_method1("set_result", (value.bind(py),))?,
                            Err(error) => {
                                future.call_method1("set_exception", (error.value(py),))?
                            }
                        };
                    }
                    Ok(())
                },
            )?;
            self.event_loop
                .bind(py)
                .call_method1("call_soon_threadsafe", (callback,))?;
            Ok(())
        })();
        if let Err(error) = scheduled {
            // A closed loop cannot have a live awaiter. Report scheduling
            // failures rather than trying to mutate its future off-thread.
            error.write_unraisable(py, Some(self.event_loop.bind(py)));
        }
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if self.future.is_some() {
            Python::with_gil(|py| {
                self.finish(
                    py,
                    Err(PyRuntimeError::new_err(
                        "engine-owned Python operation stopped before completion",
                    )),
                );
            });
        }
    }
}

pub(crate) fn future_into_py<'py, F, T>(
    tasks: &TaskSpawner,
    py: Python<'py>,
    future: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a>,
{
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    let py_future = event_loop.call_method0("create_future")?;
    let cancelled = Arc::new(Notify::new());
    let signal = cancelled.clone();
    let callback = PyCFunction::new_closure(
        py,
        None,
        None,
        move |args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| -> PyResult<()> {
            if args.get_item(0)?.call_method0("cancelled")?.is_truthy()? {
                signal.notify_one();
            }
            Ok(())
        },
    )?;
    py_future.call_method1("add_done_callback", (callback,))?;
    let mut completion = CompletionGuard {
        event_loop: event_loop.unbind(),
        future: Some(py_future.clone().unbind()),
    };
    tasks.spawn(async move {
        let result = tokio::select! {
            biased;
            _ = cancelled.notified() => None,
            result = future => Some(result),
        };
        match result {
            Some(result) => Python::with_gil(|py| {
                completion.finish(
                    py,
                    result.and_then(|value| {
                        value
                            .into_pyobject(py)
                            .map(|object| object.into_any().unbind())
                            .map_err(Into::into)
                    }),
                );
            }),
            None => {
                // `select!` has dropped the Rust future (including any owned
                // Python buffer exports). The Python future is already done.
                completion.future.take();
            }
        }
    });
    Ok(py_future)
}

#[cfg(all(test, feature = "launcher"))]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use pyo3::buffer::PyBuffer;
    use swactor_core::config::RuntimeConfig;
    use swactor_core::runtime::RuntimeParts;
    use swactor_engine::{Engine, SteppingBackend};

    fn run_scenario<'py>(py: Python<'py>, globals: &Bound<'py, PyDict>, source: &str) {
        let source = std::ffi::CString::new(source).unwrap();
        if let Err(error) = py.run(&source, Some(globals), Some(globals)) {
            error.print(py);
            panic!("Python bridge scenario failed: {error}");
        }
    }

    #[test]
    fn cancellation_drops_pending_work_and_releases_its_buffer() {
        let backend = SteppingBackend::new();
        let engine =
            Engine::new(RuntimeParts::new(RuntimeConfig::default()), backend.clone()).unwrap();
        let tasks = engine.task_spawner();
        Python::with_gil(|py| {
            let start = PyCFunction::new_closure(
                py,
                None,
                None,
                move |args: &Bound<'_, PyTuple>,
                      _kwargs: Option<&Bound<'_, PyDict>>|
                      -> PyResult<_> {
                    let buffer = PyBuffer::<u8>::get(&args.get_item(0)?)?;
                    Ok(future_into_py(&tasks, args.py(), async move {
                        let result = std::future::pending::<PyResult<()>>().await;
                        drop(buffer);
                        result
                    })?
                    .unbind())
                },
            )
            .unwrap();
            let step = PyCFunction::new_closure(
                py,
                None,
                None,
                move |_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| {
                    backend.step();
                },
            )
            .unwrap();
            let globals = PyDict::new(py);
            globals.set_item("start", start).unwrap();
            globals.set_item("step", step).unwrap();
            run_scenario(
                py,
                &globals,
                r#"
import asyncio

async def main():
    buffer = bytearray(b"held")
    pending = start(buffer)
    step()
    try:
        buffer.extend(b"!")
    except BufferError:
        pass
    else:
        raise AssertionError("pending I/O released its buffer")
    pending.cancel()
    await asyncio.sleep(0)
    step()
    try:
        await pending
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("cancellation was lost")
    buffer.extend(b"!")
    assert buffer == b"held!"

asyncio.run(main())
"#,
            );
        });
    }

    #[test]
    fn queued_completion_cannot_overwrite_python_cancellation() {
        let backend = SteppingBackend::new();
        let engine =
            Engine::new(RuntimeParts::new(RuntimeConfig::default()), backend.clone()).unwrap();
        let tasks = engine.task_spawner();
        Python::with_gil(|py| {
            let start = PyCFunction::new_closure(
                py,
                None,
                None,
                move |args: &Bound<'_, PyTuple>,
                      _kwargs: Option<&Bound<'_, PyDict>>|
                      -> PyResult<_> {
                    Ok(future_into_py(&tasks, args.py(), async { Ok(42) })?.unbind())
                },
            )
            .unwrap();
            let step = PyCFunction::new_closure(
                py,
                None,
                None,
                move |_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| {
                    backend.step();
                },
            )
            .unwrap();
            let globals = PyDict::new(py);
            globals.set_item("start", start).unwrap();
            globals.set_item("step", step).unwrap();
            run_scenario(
                py,
                &globals,
                r#"
import asyncio

async def main():
    errors = []
    asyncio.get_running_loop().set_exception_handler(lambda loop, context: errors.append(context))
    completed = start()
    step()
    assert await asyncio.wait_for(completed, 1) == 42
    cancelled = start()
    step()
    assert cancelled.cancel()
    await asyncio.sleep(0)
    try:
        await cancelled
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("late completion replaced cancellation")
    assert errors == []

asyncio.run(main())
"#,
            );
        });
    }

    #[test]
    fn engine_drop_and_rejected_submission_resolve_python_awaiters() {
        let backend = SteppingBackend::new();
        let engine =
            Engine::new(RuntimeParts::new(RuntimeConfig::default()), backend.clone()).unwrap();
        let tasks = engine.task_spawner();
        let owner = Arc::new(Mutex::new(Some((engine, backend))));
        Python::with_gil(|py| {
            let start = PyCFunction::new_closure(
                py,
                None,
                None,
                move |args: &Bound<'_, PyTuple>,
                      _kwargs: Option<&Bound<'_, PyDict>>|
                      -> PyResult<_> {
                    Ok(
                        future_into_py(&tasks, args.py(), std::future::pending::<PyResult<()>>())?
                            .unbind(),
                    )
                },
            )
            .unwrap();
            let controller = owner.clone();
            let step = PyCFunction::new_closure(
                py,
                None,
                None,
                move |_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| {
                    controller.lock().as_ref().unwrap().1.step();
                },
            )
            .unwrap();
            let shutdown = PyCFunction::new_closure(
                py,
                None,
                None,
                move |_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| {
                    owner.lock().take();
                },
            )
            .unwrap();
            let globals = PyDict::new(py);
            globals.set_item("start", start).unwrap();
            globals.set_item("step", step).unwrap();
            globals.set_item("shutdown", shutdown).unwrap();
            run_scenario(
                py,
                &globals,
                r#"
import asyncio

async def main():
    pending = start()
    step()
    never_polled = start()
    shutdown()
    rejected = start()
    results = await asyncio.wait_for(
        asyncio.gather(pending, never_polled, rejected, return_exceptions=True), 1
    )
    assert all(isinstance(result, RuntimeError) for result in results), results

asyncio.run(main())
"#,
            );
        });
    }
}
