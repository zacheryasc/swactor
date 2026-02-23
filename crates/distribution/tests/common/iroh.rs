//! Test helpers for iroh-based integration tests.
//!
//! Provides `IrohTestCluster` — an N-node harness that owns real iroh
//! endpoints with `RelayMode::Disabled`, connected via direct addresses.
#![allow(dead_code)]

use std::ops::{Index, IndexMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::peer_auth::PeerAllowList;
use iroh::{PublicKey, RelayMode};

use super::test_config;

// ─── Single-driver helpers ──────────────────────────────────────────────

pub fn make_driver() -> IrohDriver {
    IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: test_config(),
        peer_auth: None,
        additional_alpns: vec![],
        #[cfg(feature = "relay")]
        embedded_relay_bind: None,
        #[cfg(feature = "relay")]
        relay_public_ip: None,
    })
    .expect("failed to create iroh driver")
}

pub fn make_driver_with_auth(auth: Arc<Mutex<PeerAllowList>>) -> IrohDriver {
    IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: test_config(),
        peer_auth: Some(auth),
        additional_alpns: vec![],
        #[cfg(feature = "relay")]
        embedded_relay_bind: None,
        #[cfg(feature = "relay")]
        relay_public_ip: None,
    })
    .expect("failed to create iroh driver")
}

pub fn make_driver_with_relay(relay_url: iroh::RelayUrl) -> IrohDriver {
    IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Custom(relay_url.into()),
        node: test_config(),
        peer_auth: None,
        additional_alpns: vec![],
        #[cfg(feature = "relay")]
        embedded_relay_bind: None,
        #[cfg(feature = "relay")]
        relay_public_ip: None,
    })
    .expect("failed to create iroh driver with relay")
}

/// Pump one driver: recv + tick.
pub fn pump_one(driver: &mut IrohDriver) {
    driver.recv();
    driver.tick();
}

/// Pump a slice of drivers: recv + tick on each.
pub fn pump_all(drivers: &mut [IrohDriver]) {
    for d in drivers.iter_mut() {
        d.recv();
        d.tick();
    }
}

/// Pump two drivers until a condition is met or timeout expires.
pub fn pump_until_pair(
    a: &mut IrohDriver,
    b: &mut IrohDriver,
    timeout: Duration,
    check_fn: fn(&IrohDriver, &IrohDriver) -> bool,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        pump_one(a);
        pump_one(b);
        if check_fn(a, b) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Pump N drivers until a condition is met or timeout expires.
pub fn pump_until<F>(
    drivers: &mut [IrohDriver],
    timeout: Duration,
    check_fn: F,
) -> bool
where
    F: Fn(&[IrohDriver]) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        pump_all(drivers);
        if check_fn(drivers) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Check whether `driver` sees `peer_key` as alive.
pub fn sees_alive(driver: &IrohDriver, peer_key: &PublicKey) -> bool {
    let snap = driver.snapshot();
    let peer_hex: String = peer_key
        .as_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    snap.members
        .iter()
        .any(|m| m.node_id == peer_hex && m.state == "alive")
}

// ─── Local relay ────────────────────────────────────────────────────────

/// Guard that keeps the relay server alive while it exists.
pub struct RelayGuard {
    _server: iroh_relay::server::Server,
    _rt: tokio::runtime::Runtime,
}

/// Spawn a local HTTP relay server for tests. Returns the relay URL and a
/// guard that shuts the server down on drop.
pub fn spawn_test_relay() -> (iroh::RelayUrl, RelayGuard) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let server = rt.block_on(async {
        iroh_relay::server::Server::spawn(iroh_relay::server::ServerConfig::<(), ()> {
            relay: Some(iroh_relay::server::RelayConfig {
                http_bind_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                tls: None,
                limits: Default::default(),
                key_cache_capacity: Some(256),
                access: iroh_relay::server::AccessConfig::Everyone,
            }),
            quic: None,
            metrics_addr: None,
        })
        .await
    })
    .unwrap();
    let url = server.http_url().expect("relay has no HTTP URL");
    (
        url,
        RelayGuard {
            _server: server,
            _rt: rt,
        },
    )
}

// ─── N-node cluster ─────────────────────────────────────────────────────

/// An N-node iroh test cluster with real QUIC endpoints.
pub struct IrohTestCluster {
    drivers: Vec<IrohDriver>,
}

impl IrohTestCluster {
    /// Create N disconnected drivers (no joins).
    pub fn disconnected(n: usize) -> Self {
        let drivers = (0..n).map(|_| make_driver()).collect();
        Self { drivers }
    }

    /// Create N drivers connected in a star topology through node 0.
    /// Nodes 1..N join node 0 using its full `EndpointAddr`.
    pub fn star(n: usize) -> Self {
        assert!(n >= 2, "star cluster requires at least 2 nodes");
        let mut drivers: Vec<IrohDriver> = (0..n).map(|_| make_driver()).collect();

        let addr_0 = drivers[0].endpoint_addr();
        for i in 1..n {
            drivers[i].join(&[addr_0.clone()]);
        }

        Self { drivers }
    }

    /// Pump all drivers until a condition is met or timeout expires.
    pub fn pump_until<F>(&mut self, timeout: Duration, check_fn: F) -> bool
    where
        F: Fn(&[IrohDriver]) -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            pump_all(&mut self.drivers);
            if check_fn(&self.drivers) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    /// Shut down all drivers.
    pub fn shutdown(&mut self) {
        for d in &mut self.drivers {
            d.shutdown();
        }
    }
}

impl Index<usize> for IrohTestCluster {
    type Output = IrohDriver;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.drivers[idx]
    }
}

impl IndexMut<usize> for IrohTestCluster {
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.drivers[idx]
    }
}
