#[cfg(feature = "direct")]
pub fn direct(py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> {
    runtime_api::tokio::future_into_py(py, async { Ok(()) })
}

#[cfg(feature = "aliased-helper")]
mod bridge {
    pub use runtime_api::tokio::future_into_py as convert;
}

#[cfg(feature = "aliased-helper")]
pub fn adapter(py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> {
    helper(py)
}

#[cfg(feature = "aliased-helper")]
fn helper(py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> {
    bridge::convert(py, async { Ok(()) })
}

#[cfg(feature = "with-locals")]
pub fn with_locals(
    py: pyo3::Python<'_>,
    locals: runtime_api::TaskLocals,
) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> {
    runtime_api::tokio::future_into_py_with_locals(py, locals, async { Ok(()) })
}

#[cfg(feature = "runtime-access")]
pub fn runtime_access() {
    let _ = runtime_api::tokio::get_runtime();
}

#[cfg(feature = "runtime-spawn")]
pub fn runtime_spawn<R: runtime_api::generic::Runtime>() {
    drop(R::spawn(async {}));
}
