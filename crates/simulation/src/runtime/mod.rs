//! Runtime facade — the narrow trait family peer code calls instead of
//! `std::*` / OS APIs.
//!
//! Two backends — `prod` and `sim` — implement the trait surface
//! declared in [`traits`]. The active `facade()` constructor is
//! selected at build time by the mutually-exclusive `facade-prod` and
//! `facade-sim` features on the `simulation` crate; both modules are
//! always compiled (this is required for the surface fingerprint test
//! and for the sim runtime, which the engine drives directly).

// ── Feature-exclusivity guard ───────────────────────────────────────

#[cfg(all(feature = "facade-prod", feature = "facade-sim"))]
const _: () = panic!(
    "simulation: features `facade-prod` and `facade-sim` are mutually \
     exclusive; enable exactly one."
);

#[cfg(all(
    not(feature = "facade-prod"),
    not(feature = "facade-sim"),
    not(feature = "_facade-test"),
))]
const _: () = panic!(
    "simulation: enable exactly one of `facade-prod` or `facade-sim`."
);

// ── Surface fingerprint ─────────────────────────────────────────────

pub const SURFACE_DESCRIPTOR: &str =
    include_str!(concat!(env!("OUT_DIR"), "/surface_descriptor.txt"));

pub fn surface_fingerprint() -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(SURFACE_DESCRIPTOR.as_bytes());
    format!("{:x}", h.finalize())
}

// ── Trait surface ───────────────────────────────────────────────────

pub mod traits {
    //! The narrow trait family the peer code calls.
    //!
    //! Method bodies live in the backend modules (`prod`, `sim`);
    //! signatures are the contract. Adding/removing a signature here
    //! changes the surface fingerprint and requires a `surface.lock`
    //! bump in the same commit.

    use std::io;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime};

    /// Virtual or wall-clock time provider.
    pub trait Clock: Send + Sync {
        fn now(&self) -> SystemTime;
        fn monotonic(&self) -> Duration;
        fn sleep_until(&self, deadline: SystemTime) -> io::Result<()>;
        fn sleep_for(&self, duration: Duration) -> io::Result<()>;
    }

    /// UDP socket abstraction.
    pub trait Udp: Send + Sync {
        fn bind(&self, addr: SocketAddr) -> io::Result<Box<dyn UdpSocket>>;
    }

    pub trait UdpSocket: Send + Sync {
        fn local_addr(&self) -> io::Result<SocketAddr>;
        fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize>;
        fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)>;
    }

    /// QUIC endpoint (iroh's `Endpoint` shape).
    pub trait Quic: Send + Sync {
        fn bind(&self, addr: SocketAddr) -> io::Result<Box<dyn QuicEndpoint>>;
    }

    pub trait QuicEndpoint: Send + Sync {
        fn local_addr(&self) -> io::Result<SocketAddr>;
        fn close(&self) -> io::Result<()>;
    }

    /// DNS resolver.
    pub trait Dns: Send + Sync {
        fn resolve_a(&self, host: &str) -> io::Result<Vec<std::net::Ipv4Addr>>;
        fn resolve_aaaa(&self, host: &str) -> io::Result<Vec<std::net::Ipv6Addr>>;
    }

    /// Fiber/task spawner.
    pub trait Spawn: Send + Sync {
        fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>);
        fn spawn_local(&self, task: Box<dyn FnOnce() + 'static>);
    }

    /// Deterministic per-stream RNG handle.
    pub trait Rng: Send + Sync {
        fn get_stream(&self, label: &str) -> Box<dyn RngStream>;
    }

    pub trait RngStream: Send {
        fn fill_bytes(&mut self, dest: &mut [u8]);
        fn next_u64(&mut self) -> u64;
    }

    /// Sandboxed file-system access (rooted per node).
    pub trait Fs: Send + Sync {
        fn open_read(&self, path: &Path) -> io::Result<Box<dyn io::Read + Send>>;
        fn open_write(&self, path: &Path) -> io::Result<Box<dyn io::Write + Send>>;
        fn root(&self) -> PathBuf;
    }

    /// Environment-variable reader (per-node table in sim).
    pub trait Env: Send + Sync {
        fn get(&self, name: &str) -> Option<String>;
        fn iter(&self) -> Box<dyn Iterator<Item = (String, String)> + '_>;
    }

    /// Process metadata (hostname, pid).
    pub trait ProcessMeta: Send + Sync {
        fn hostname(&self) -> io::Result<String>;
        fn pid(&self) -> u32;
    }
}

// ── Backend modules ─────────────────────────────────────────────────

pub mod prod;
pub mod sim;

// Active backend selector. With no facade-* feature picked, neither
// re-export is in scope and `simulation::runtime::facade()` is a
// link error — call `prod::facade()` / `sim::facade()` directly.
#[cfg(all(feature = "facade-prod", not(feature = "facade-sim")))]
pub use prod::facade;

#[cfg(all(feature = "facade-sim", not(feature = "facade-prod")))]
pub use sim::facade;
