pub mod actor;

mod channel;
pub(crate) mod error;
pub use error::Error;

mod router;
pub mod runtime;

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

/// FIXME: remove hard coded defaults
/// The strategy for message processing is such:
///
/// ```ignore
/// if total_messages < WATERLEVEL:
///     process all
/// else
///     process total_messages >> 1
/// ```
const WATERLEVEL: usize = 10;
