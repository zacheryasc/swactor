pub mod actor;
pub mod worker;

pub(crate) mod channel;
pub(crate) mod error;
pub use error::Error;


pub(crate) mod address_map;
pub mod config;

pub mod runtime;

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
#[pyo3::pymodule]
fn swactor(m: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    python::register(m)
}

#[cfg(feature = "getrandom")]
pub(crate) fn get_random(buf: &mut [u8]) {
    getrandom::getrandom(buf).unwrap()
}

#[cfg(feature = "no_random")]
pub(crate) fn get_random(buf: &mut [u8]) {
    use core::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let value = COUNTER.fetch_add(1, Ordering::Relaxed);
    let bytes = value.to_ne_bytes();

    for (i, byte) in buf.iter_mut().enumerate() {
        *byte = bytes[i % core::mem::size_of::<usize>()];
    }
}
