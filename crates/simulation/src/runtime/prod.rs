//! Production backend — stage 1 scaffolding.
//!
//! The traits in `crate::traits` are intentionally minimal at this
//! stage; each method body is wired to the obvious `std`/OS call but
//! the surface will widen as later stages need it.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use super::traits;

pub struct ProdFacade {
    boot: Instant,
}

impl ProdFacade {
    pub fn new() -> Self {
        Self { boot: Instant::now() }
    }
}

impl Default for ProdFacade {
    fn default() -> Self {
        Self::new()
    }
}

pub fn facade() -> Arc<ProdFacade> {
    Arc::new(ProdFacade::new())
}

// ── Trait impls ─────────────────────────────────────────────────────

pub struct ProdClock {
    boot: Instant,
}

impl traits::Clock for ProdClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
    fn monotonic(&self) -> Duration {
        self.boot.elapsed()
    }
    fn sleep_until(&self, deadline: SystemTime) -> io::Result<()> {
        match deadline.duration_since(SystemTime::now()) {
            Ok(d) => std::thread::sleep(d),
            Err(_) => {}
        }
        Ok(())
    }
    fn sleep_for(&self, duration: Duration) -> io::Result<()> {
        std::thread::sleep(duration);
        Ok(())
    }
}

pub struct ProdUdp;

impl traits::Udp for ProdUdp {
    fn bind(&self, addr: SocketAddr) -> io::Result<Box<dyn traits::UdpSocket>> {
        let sock = std::net::UdpSocket::bind(addr)?;
        Ok(Box::new(ProdUdpSocket { inner: sock }))
    }
}

pub struct ProdUdpSocket {
    inner: std::net::UdpSocket,
}

impl traits::UdpSocket for ProdUdpSocket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.inner.send_to(buf, target)
    }
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf)
    }
}

pub struct ProdQuic;

impl traits::Quic for ProdQuic {
    fn bind(&self, _addr: SocketAddr) -> io::Result<Box<dyn traits::QuicEndpoint>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "QUIC endpoint not wired in stage 1",
        ))
    }
}

pub struct ProdDns;

impl traits::Dns for ProdDns {
    fn resolve_a(&self, host: &str) -> io::Result<Vec<Ipv4Addr>> {
        use std::net::ToSocketAddrs;
        let mut out = Vec::new();
        for sa in (host, 0u16).to_socket_addrs()? {
            if let std::net::IpAddr::V4(v) = sa.ip() {
                out.push(v);
            }
        }
        Ok(out)
    }
    fn resolve_aaaa(&self, host: &str) -> io::Result<Vec<Ipv6Addr>> {
        use std::net::ToSocketAddrs;
        let mut out = Vec::new();
        for sa in (host, 0u16).to_socket_addrs()? {
            if let std::net::IpAddr::V6(v) = sa.ip() {
                out.push(v);
            }
        }
        Ok(out)
    }
}

pub struct ProdSpawn;

impl traits::Spawn for ProdSpawn {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(task);
    }
    fn spawn_local(&self, task: Box<dyn FnOnce() + 'static>) {
        // Production: run inline. Stage 4+ introduces a real executor.
        task();
    }
}

pub struct ProdRng;

impl traits::Rng for ProdRng {
    fn get_stream(&self, label: &str) -> Box<dyn traits::RngStream> {
        let mut seed = [0u8; 32];
        for (i, b) in label.as_bytes().iter().enumerate() {
            seed[i % 32] ^= *b;
        }
        Box::new(ProdRngStream { state: u64::from_le_bytes(seed[0..8].try_into().unwrap()) })
    }
}

pub struct ProdRngStream {
    state: u64,
}

impl traits::RngStream for ProdRngStream {
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for byte in dest {
            *byte = (self.next_u64() & 0xff) as u8;
        }
    }
    fn next_u64(&mut self) -> u64 {
        // splitmix64 — placeholder for stage 1. Replaced by per-node
        // deterministic stream in stage 4.
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

pub struct ProdFs {
    root: PathBuf,
}

impl traits::Fs for ProdFs {
    fn open_read(&self, path: &Path) -> io::Result<Box<dyn io::Read + Send>> {
        let f = std::fs::File::open(self.root.join(path))?;
        Ok(Box::new(f))
    }
    fn open_write(&self, path: &Path) -> io::Result<Box<dyn io::Write + Send>> {
        let full = self.root.join(path);
        if let Some(p) = full.parent() {
            std::fs::create_dir_all(p)?;
        }
        let f = std::fs::File::create(full)?;
        Ok(Box::new(f))
    }
    fn root(&self) -> PathBuf {
        self.root.clone()
    }
}

pub struct ProdEnv;

impl traits::Env for ProdEnv {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
    fn iter(&self) -> Box<dyn Iterator<Item = (String, String)> + '_> {
        Box::new(std::env::vars())
    }
}

pub struct ProdProcessMeta;

impl traits::ProcessMeta for ProdProcessMeta {
    fn hostname(&self) -> io::Result<String> {
        // Stage 1: stub via env var; later stages call libc.
        Ok(std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string()))
    }
    fn pid(&self) -> u32 {
        std::process::id()
    }
}

impl ProdFacade {
    pub fn clock(&self) -> ProdClock {
        ProdClock { boot: self.boot }
    }
    pub fn udp(&self) -> ProdUdp { ProdUdp }
    pub fn quic(&self) -> ProdQuic { ProdQuic }
    pub fn dns(&self) -> ProdDns { ProdDns }
    pub fn spawn(&self) -> ProdSpawn { ProdSpawn }
    pub fn rng(&self) -> ProdRng { ProdRng }
    pub fn fs(&self, root: PathBuf) -> ProdFs { ProdFs { root } }
    pub fn env(&self) -> ProdEnv { ProdEnv }
    pub fn process_meta(&self) -> ProdProcessMeta { ProdProcessMeta }
}
