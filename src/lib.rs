pub mod actor;

pub(crate) mod error;
pub use error::Error;

mod ring_buffer;
mod router;
pub mod runtime;

#[cfg(feature = "getrandom")]
pub(crate) fn get_random(buf: &mut [u8]) {
    getrandom::getrandom(buf).unwrap()
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
