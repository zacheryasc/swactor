pub mod actor;
pub mod extension;
pub mod worker;

pub(crate) mod channel;
pub(crate) mod error;
pub use error::Error;

// Re-export identity hashing types for ActorAddress-keyed collections.
pub use delivery::{AddrBuildHasher, AddrMap, AddrSet};

pub mod config;
pub(crate) mod delivery;
pub mod stats;

pub mod runtime;

#[cfg(feature = "transport")]
pub mod transport;

#[cfg(feature = "getrandom")]
pub(crate) fn get_random(buf: &mut [u8]) {
    getrandom::getrandom(buf).unwrap()
}

#[cfg(all(feature = "no_random", not(feature = "getrandom")))]
pub(crate) fn get_random(buf: &mut [u8]) {
    use core::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let value = COUNTER.fetch_add(1, Ordering::Relaxed);
    let bytes = value.to_ne_bytes();

    for (i, byte) in buf.iter_mut().enumerate() {
        *byte = bytes[i % core::mem::size_of::<usize>()];
    }
}
