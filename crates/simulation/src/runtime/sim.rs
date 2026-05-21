//! Simulator backend — stage 1 stubs.
//!
//! Every method here returns `unimplemented!()` (or its functional
//! equivalent: a structured `io::Error`) until later stages wire the
//! engine in. The trait identities must match `prod.rs`'s impls.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use super::traits;

pub struct SimFacade;

impl SimFacade {
    pub fn new() -> Self { Self }
}

impl Default for SimFacade {
    fn default() -> Self { Self }
}

pub fn facade() -> Arc<SimFacade> {
    Arc::new(SimFacade::new())
}

fn stub<T>(what: &'static str) -> io::Result<T> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("sim facade: {what} not implemented yet"),
    ))
}

pub struct SimClock;

impl traits::Clock for SimClock {
    fn now(&self) -> SystemTime { SystemTime::UNIX_EPOCH }
    fn monotonic(&self) -> Duration { Duration::ZERO }
    fn sleep_until(&self, _deadline: SystemTime) -> io::Result<()> { Ok(()) }
    fn sleep_for(&self, _duration: Duration) -> io::Result<()> { Ok(()) }
}

pub struct SimUdp;

impl traits::Udp for SimUdp {
    fn bind(&self, _addr: SocketAddr) -> io::Result<Box<dyn traits::UdpSocket>> {
        stub("Udp::bind")
    }
}

pub struct SimQuic;

impl traits::Quic for SimQuic {
    fn bind(&self, _addr: SocketAddr) -> io::Result<Box<dyn traits::QuicEndpoint>> {
        stub("Quic::bind")
    }
}

pub struct SimDns;

impl traits::Dns for SimDns {
    fn resolve_a(&self, _host: &str) -> io::Result<Vec<Ipv4Addr>> { Ok(Vec::new()) }
    fn resolve_aaaa(&self, _host: &str) -> io::Result<Vec<Ipv6Addr>> { Ok(Vec::new()) }
}

pub struct SimSpawn;

impl traits::Spawn for SimSpawn {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) { task(); }
    fn spawn_local(&self, task: Box<dyn FnOnce() + 'static>) { task(); }
}

pub struct SimRng;

impl traits::Rng for SimRng {
    fn get_stream(&self, _label: &str) -> Box<dyn traits::RngStream> {
        Box::new(SimRngStream { state: 0 })
    }
}

pub struct SimRngStream { state: u64 }

impl traits::RngStream for SimRngStream {
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for b in dest { *b = 0; self.state = self.state.wrapping_add(1); }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(1);
        0
    }
}

pub struct SimFs { root: PathBuf }

impl traits::Fs for SimFs {
    fn open_read(&self, _path: &Path) -> io::Result<Box<dyn io::Read + Send>> {
        stub("Fs::open_read")
    }
    fn open_write(&self, _path: &Path) -> io::Result<Box<dyn io::Write + Send>> {
        stub("Fs::open_write")
    }
    fn root(&self) -> PathBuf { self.root.clone() }
}

pub struct SimEnv;

impl traits::Env for SimEnv {
    fn get(&self, _name: &str) -> Option<String> { None }
    fn iter(&self) -> Box<dyn Iterator<Item = (String, String)> + '_> {
        Box::new(std::iter::empty())
    }
}

pub struct SimProcessMeta;

impl traits::ProcessMeta for SimProcessMeta {
    fn hostname(&self) -> io::Result<String> { Ok("sim-node".to_string()) }
    fn pid(&self) -> u32 { 0 }
}

impl SimFacade {
    pub fn clock(&self) -> SimClock { SimClock }
    pub fn udp(&self) -> SimUdp { SimUdp }
    pub fn quic(&self) -> SimQuic { SimQuic }
    pub fn dns(&self) -> SimDns { SimDns }
    pub fn spawn(&self) -> SimSpawn { SimSpawn }
    pub fn rng(&self) -> SimRng { SimRng }
    pub fn fs(&self, root: PathBuf) -> SimFs { SimFs { root } }
    pub fn env(&self) -> SimEnv { SimEnv }
    pub fn process_meta(&self) -> SimProcessMeta { SimProcessMeta }
}
