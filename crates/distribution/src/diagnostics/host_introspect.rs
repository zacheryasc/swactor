//! Host context scrape for tier-3 snapshots (`DIAGNOSTICS_PLAN.md`
//! T3.1 + T3.2).
//!
//! Two independent caches:
//!
//! 1. **Host network.** A snapshot of `/proc/net/{dev,route,ipv6_route,
//!    udp,udp6}`, `/proc/sys/net/ipv6/conf/all/disable_ipv6`,
//!    `/sys/class/net/<name>/{operstate,mtu}`, `/etc/resolv.conf`, and
//!    `/proc/sys/net/netfilter/nf_conntrack_count`. Plus per-interface
//!    addresses via `getifaddrs(3)`. Refreshed at ~30s cadence (the
//!    walk is cheap but not free, and these values rarely change
//!    inside a single run).
//!
//! 2. **DNS.** Resolves each registered hostname every ~30s and stores
//!    the answer (A + AAAA records, or an error string). The post-
//!    processor diffs answers across nodes to catch the
//!    "different-nameservers-different-answers" failure class.
//!
//! Refresh runs in a tokio task spawned by [`HostIntrospect::start`].
//! Without that task the cache stays empty unless a caller invokes
//! [`HostIntrospect::refresh_now`] (which is what the tests do).
//!
//! Linux-only — every scrape path is `#[cfg(target_os = "linux")]`.
//! On other platforms the introspector still compiles and runs but the
//! `network` slot stays `None`. DNS works cross-platform; nothing in
//! the resolver path is Linux-specific.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use crate::diagnostics::sink::{DynEmitter, noop_emitter};
use crate::diagnostics::snapshot::{
    HostIntrospector, Tier3DnsResolution, Tier3HostNetwork, Tier3HostState,
};
use crate::diagnostics::wall_ms_now;

/// How often the background task refreshes the host-network and DNS
/// caches (`DIAGNOSTICS_PLAN.md` T3.1 + T3.2: "every ~30s").
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Cached host context. Holds two independent slots — network and DNS —
/// behind `Mutex`es so the background refresher and the snapshot path
/// never block each other for longer than a single field copy.
pub struct HostIntrospect {
    network: Mutex<Option<Tier3HostNetwork>>,
    dns: Mutex<HashMap<String, Tier3DnsResolution>>,
    emitter: Mutex<DynEmitter>,
    /// Once-flag — only fire the "conntrack capability missing" event
    /// the first time we observe the capability gap.
    conntrack_gap_reported: AtomicBool,
}

impl std::fmt::Debug for HostIntrospect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostIntrospect")
            .field("network", &self.network.lock().ok().map(|g| g.is_some()))
            .field(
                "dns_targets",
                &self.dns.lock().ok().map(|g| g.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl Default for HostIntrospect {
    fn default() -> Self {
        Self::new()
    }
}

impl HostIntrospect {
    pub fn new() -> Self {
        Self {
            network: Mutex::new(None),
            dns: Mutex::new(HashMap::new()),
            emitter: Mutex::new(noop_emitter()),
            conntrack_gap_reported: AtomicBool::new(false),
        }
    }

    /// Wire an event emitter so capability-gap notices (e.g. no
    /// permission to read `nf_conntrack_count`) reach the bundle's
    /// event stream. The default is [`crate::diagnostics::NoopEmitter`].
    pub fn set_emitter(&self, emitter: DynEmitter) {
        *self
            .emitter
            .lock()
            .expect("host introspect emitter mutex poisoned") = emitter;
    }

    /// Register a hostname for periodic DNS resolution. Duplicate calls
    /// are a no-op. The first entry for a hostname is left with an
    /// empty answer set until the next refresh.
    pub fn add_dns_target(&self, hostname: impl Into<String>) {
        let host = hostname.into();
        let mut dns = self.dns.lock().expect("host introspect dns mutex poisoned");
        dns.entry(host.clone())
            .or_insert_with(|| Tier3DnsResolution {
                hostname: host,
                a_records: Vec::new(),
                aaaa_records: Vec::new(),
                ttl_seconds: None,
                resolver_used: None,
                resolved_at_ms: 0,
                error: None,
            });
    }

    /// Synchronously refresh both caches. Safe to call from any
    /// context — does blocking IO (file reads, DNS lookups) but never
    /// awaits anything. The bulk path is this method; the background
    /// task spawned by [`Self::start`] calls it under
    /// `tokio::task::spawn_blocking`.
    pub fn refresh_now(&self) {
        self.refresh_network();
        self.refresh_dns_all();
    }

    fn refresh_network(&self) {
        #[cfg(target_os = "linux")]
        {
            let snap = linux::scrape_network(&self.emitter, &self.conntrack_gap_reported);
            *self
                .network
                .lock()
                .expect("host introspect network mutex poisoned") = Some(snap);
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Non-Linux: leave the network slot None. DNS still works.
        }
    }

    fn refresh_dns_all(&self) {
        // Snapshot the hostname list under the lock, then resolve
        // outside it so the snapshot path is never blocked by a slow
        // DNS lookup.
        let hostnames: Vec<String> = {
            let dns = self.dns.lock().expect("host introspect dns mutex poisoned");
            dns.keys().cloned().collect()
        };
        let resolver = read_first_nameserver();
        for host in hostnames {
            let entry = resolve_one(&host, resolver.clone());
            let mut dns = self.dns.lock().expect("host introspect dns mutex poisoned");
            dns.insert(host, entry);
        }
    }

    /// Spawn the background refresh task on the current tokio runtime.
    /// Returns the spawned `JoinHandle` — drop it (or `abort()` it) to
    /// stop refreshing. Only available when the `collector` feature is
    /// on, since the periodic snapshot task already requires tokio and
    /// adding a hard tokio dep just for tier-3 would defeat the
    /// existing feature gates.
    #[cfg(feature = "collector")]
    pub fn start(
        self: std::sync::Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Run an initial refresh immediately so the first snapshot
            // after `start()` has populated data. The background tick
            // then settles into a steady cadence.
            let s = self.clone();
            let _ = tokio::task::spawn_blocking(move || s.refresh_now()).await;
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await; // consume the immediate-fire tick
            loop {
                tick.tick().await;
                let s = self.clone();
                let _ = tokio::task::spawn_blocking(move || s.refresh_now()).await;
            }
        })
    }
}

impl HostIntrospector for HostIntrospect {
    fn capture(&self) -> Tier3HostState {
        let network = self
            .network
            .lock()
            .expect("host introspect network mutex poisoned")
            .clone();
        let mut dns: Vec<Tier3DnsResolution> = self
            .dns
            .lock()
            .expect("host introspect dns mutex poisoned")
            .values()
            .cloned()
            .collect();
        dns.sort_by(|a, b| a.hostname.cmp(&b.hostname));
        Tier3HostState {
            network,
            dns,
            scraped_at_ms: wall_ms_now(),
        }
    }
}

/// Reads `/etc/resolv.conf` and returns the first `nameserver` entry,
/// if any. Non-Linux callers just see whatever the file (or its
/// absence) yields.
fn read_first_nameserver() -> Option<String> {
    let body = std::fs::read_to_string("/etc/resolv.conf").ok()?;
    for line in body.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("nameserver") {
            let server = rest.trim();
            if !server.is_empty() {
                return Some(server.to_string());
            }
        }
    }
    None
}

/// Resolves a single hostname via `std::net::ToSocketAddrs`. The
/// `:0` port is a placeholder — we only care about the returned IPs.
fn resolve_one(hostname: &str, resolver: Option<String>) -> Tier3DnsResolution {
    use std::net::ToSocketAddrs;
    let target = format!("{}:0", strip_scheme(hostname));
    let now = wall_ms_now();
    match target.to_socket_addrs() {
        Ok(addrs) => {
            let mut a = Vec::new();
            let mut aaaa = Vec::new();
            for addr in addrs {
                match addr.ip() {
                    std::net::IpAddr::V4(v4) => {
                        let s = v4.to_string();
                        if !a.contains(&s) {
                            a.push(s);
                        }
                    }
                    std::net::IpAddr::V6(v6) => {
                        let s = v6.to_string();
                        if !aaaa.contains(&s) {
                            aaaa.push(s);
                        }
                    }
                }
            }
            Tier3DnsResolution {
                hostname: hostname.to_string(),
                a_records: a,
                aaaa_records: aaaa,
                ttl_seconds: None,
                resolver_used: resolver,
                resolved_at_ms: now,
                error: None,
            }
        }
        Err(e) => Tier3DnsResolution {
            hostname: hostname.to_string(),
            a_records: Vec::new(),
            aaaa_records: Vec::new(),
            ttl_seconds: None,
            resolver_used: resolver,
            resolved_at_ms: now,
            error: Some(e.to_string()),
        },
    }
}

/// Accept full URLs (e.g. `"https://relay.iroh.network./"`) as well as
/// bare hostnames. The resolver only cares about the authority part.
fn strip_scheme(input: &str) -> String {
    let after_scheme = match input.find("://") {
        Some(idx) => &input[idx + 3..],
        None => input,
    };
    let host_only = after_scheme.split('/').next().unwrap_or("");
    let host_only = host_only.split(':').next().unwrap_or("");
    host_only.trim_end_matches('.').to_string()
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use crate::diagnostics::event::Event;
    use crate::diagnostics::sink::DynEmitter;
    use crate::diagnostics::snapshot::{
        Tier3HostNetwork, Tier3Interface, Tier3Route, Tier3UdpSocket,
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    pub(super) fn scrape_network(
        emitter: &Mutex<DynEmitter>,
        conntrack_gap_reported: &AtomicBool,
    ) -> Tier3HostNetwork {
        let interfaces = read_interfaces();
        let default_routes = read_routes();
        let mut udp_sockets = read_udp("/proc/net/udp");
        udp_sockets.extend(read_udp6("/proc/net/udp6"));
        let conntrack_count =
            read_conntrack(emitter, conntrack_gap_reported);
        let ipv6_enabled = read_ipv6_enabled();
        let resolv_conf_nameservers = read_all_nameservers();
        Tier3HostNetwork {
            interfaces,
            default_routes,
            udp_sockets,
            conntrack_count,
            ipv6_enabled,
            resolv_conf_nameservers,
            refreshed_at_ms: wall_ms_now(),
        }
    }

    fn read_interfaces() -> Vec<Tier3Interface> {
        let mut by_name: BTreeMap<String, Tier3Interface> = BTreeMap::new();
        // Step 1: enumerate via /proc/net/dev so we always pick up at
        // least the loopback even when getifaddrs returns nothing.
        if let Ok(body) = std::fs::read_to_string("/proc/net/dev") {
            for line in body.lines().skip(2) {
                if let Some(idx) = line.find(':') {
                    let name = line[..idx].trim().to_string();
                    if name.is_empty() {
                        continue;
                    }
                    by_name.entry(name.clone()).or_insert_with(|| Tier3Interface {
                        name,
                        addresses: Vec::new(),
                        mtu: None,
                        up: false,
                    });
                }
            }
        }
        // Step 2: enrich with /sys/class/net per-interface metadata.
        // operstate is "up" for managed-state interfaces; for virtual
        // ones like loopback the kernel leaves operstate "unknown"
        // and only sets the IFF_UP flag bit (0x1) in `flags`. Check
        // both so neither path misses a live interface.
        for (name, iface) in by_name.iter_mut() {
            let operstate_up = read_sys_string(name, "operstate")
                .map(|s| s.eq_ignore_ascii_case("up"))
                .unwrap_or(false);
            let flag_up = read_sys_string(name, "flags")
                .and_then(|s| {
                    let trimmed = s.trim().trim_start_matches("0x");
                    u32::from_str_radix(trimmed, 16).ok()
                })
                .map(|flags| (flags & 0x1) != 0)
                .unwrap_or(false);
            iface.up = operstate_up || flag_up;
            iface.mtu = read_sys_string(name, "mtu")
                .and_then(|s| s.parse::<u32>().ok());
        }
        // Step 3: layer in addresses via libc::getifaddrs.
        let address_map = read_getifaddrs();
        for (name, addrs) in address_map {
            let entry = by_name.entry(name.clone()).or_insert_with(|| Tier3Interface {
                name,
                addresses: Vec::new(),
                mtu: None,
                up: false,
            });
            for a in addrs {
                if !entry.addresses.contains(&a) {
                    entry.addresses.push(a);
                }
            }
        }
        by_name.into_values().collect()
    }

    fn read_sys_string(iface: &str, field: &str) -> Option<String> {
        let path = format!("/sys/class/net/{iface}/{field}");
        std::fs::read_to_string(path)
            .ok()
            .map(|s| s.trim().to_string())
    }

    /// Build a name → addresses map by walking libc::getifaddrs. On any
    /// failure (alloc, syscall) returns an empty map so the rest of
    /// `scrape_network` still produces something useful.
    fn read_getifaddrs() -> BTreeMap<String, Vec<String>> {
        use std::collections::BTreeMap;
        let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
        // SAFETY: getifaddrs is a standard POSIX call; we walk the
        // returned linked list defensively, bound the iteration, and
        // always free with freeifaddrs in the drop guard.
        unsafe {
            let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
            if libc::getifaddrs(&mut head) != 0 || head.is_null() {
                return out;
            }
            struct Guard(*mut libc::ifaddrs);
            impl Drop for Guard {
                fn drop(&mut self) {
                    unsafe { libc::freeifaddrs(self.0) }
                }
            }
            let _guard = Guard(head);
            let mut node = head;
            // Defensive bound — the kernel will not return billions of
            // entries, but we keep the loop terminating no matter what.
            let mut hops = 0usize;
            while !node.is_null() && hops < 4096 {
                hops += 1;
                let entry = &*node;
                if entry.ifa_name.is_null() {
                    node = entry.ifa_next;
                    continue;
                }
                let name = match std::ffi::CStr::from_ptr(entry.ifa_name).to_str() {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        node = entry.ifa_next;
                        continue;
                    }
                };
                if !entry.ifa_addr.is_null() {
                    let family = (*entry.ifa_addr).sa_family as i32;
                    let addr_str = match family {
                        libc::AF_INET => {
                            let sin = entry.ifa_addr as *const libc::sockaddr_in;
                            let raw = (*sin).sin_addr.s_addr.to_ne_bytes();
                            let octets = std::net::Ipv4Addr::from(raw);
                            Some(octets.to_string())
                        }
                        libc::AF_INET6 => {
                            let sin6 = entry.ifa_addr as *const libc::sockaddr_in6;
                            let bytes = (*sin6).sin6_addr.s6_addr;
                            let v6 = std::net::Ipv6Addr::from(bytes);
                            Some(v6.to_string())
                        }
                        _ => None,
                    };
                    if let Some(addr) = addr_str {
                        out.entry(name).or_default().push(addr);
                    }
                }
                node = entry.ifa_next;
            }
        }
        out
    }

    fn read_routes() -> Vec<Tier3Route> {
        let mut out = Vec::new();
        if let Ok(body) = std::fs::read_to_string("/proc/net/route") {
            for (i, line) in body.lines().enumerate() {
                if i == 0 {
                    continue; // header
                }
                let cols: Vec<&str> = line.split_whitespace().collect();
                if cols.len() < 4 {
                    continue;
                }
                let iface = cols[0];
                let dest_hex = cols[1];
                let gateway_hex = cols[2];
                let mask_hex = cols.get(7).copied().unwrap_or("00000000");
                let dest = parse_ipv4_le_hex(dest_hex).unwrap_or_else(|| "?".into());
                let mask_bits = mask_to_prefix_v4(mask_hex);
                let destination = format!("{dest}/{mask_bits}");
                let gateway = parse_ipv4_le_hex(gateway_hex)
                    .filter(|gw| gw != "0.0.0.0");
                out.push(Tier3Route {
                    family: "v4".into(),
                    destination,
                    gateway,
                    interface: iface.to_string(),
                });
            }
        }
        if let Ok(body) = std::fs::read_to_string("/proc/net/ipv6_route") {
            for line in body.lines() {
                let cols: Vec<&str> = line.split_whitespace().collect();
                if cols.len() < 10 {
                    continue;
                }
                let dest = parse_ipv6_hex(cols[0]);
                let prefix_len = u8::from_str_radix(cols[1], 16).unwrap_or(0);
                let gateway = parse_ipv6_hex(cols[4]);
                let iface = cols[9];
                let destination = match dest {
                    Some(addr) => format!("{addr}/{prefix_len}"),
                    None => format!("?/{prefix_len}"),
                };
                let gateway = gateway.filter(|gw| gw != "::");
                out.push(Tier3Route {
                    family: "v6".into(),
                    destination,
                    gateway,
                    interface: iface.to_string(),
                });
            }
        }
        out
    }

    fn parse_ipv4_le_hex(hex: &str) -> Option<String> {
        let raw = u32::from_str_radix(hex, 16).ok()?;
        // /proc encodes the address little-endian as printed.
        let octets = raw.to_le_bytes();
        Some(std::net::Ipv4Addr::from(octets).to_string())
    }

    fn parse_ipv6_hex(hex: &str) -> Option<String> {
        if hex.len() != 32 {
            return None;
        }
        let mut bytes = [0u8; 16];
        for i in 0..16 {
            bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(std::net::Ipv6Addr::from(bytes).to_string())
    }

    fn mask_to_prefix_v4(hex: &str) -> u8 {
        let mask = u32::from_str_radix(hex, 16).unwrap_or(0).to_le();
        mask.count_ones() as u8
    }

    fn read_udp(path: &str) -> Vec<Tier3UdpSocket> {
        read_udp_inner(path, false)
    }

    fn read_udp6(path: &str) -> Vec<Tier3UdpSocket> {
        read_udp_inner(path, true)
    }

    fn read_udp_inner(path: &str, ipv6: bool) -> Vec<Tier3UdpSocket> {
        let body = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        for (i, line) in body.lines().enumerate() {
            if i == 0 {
                continue;
            }
            let cols: Vec<&str> = line.split_whitespace().collect();
            if cols.len() < 10 {
                continue;
            }
            let local = decode_proc_addr(cols[1], ipv6).unwrap_or_else(|| cols[1].to_string());
            let remote = decode_proc_addr(cols[2], ipv6).unwrap_or_else(|| cols[2].to_string());
            let state = cols[3].to_string();
            let inode = cols[9].parse::<u64>().unwrap_or(0);
            out.push(Tier3UdpSocket {
                local_addr: local,
                remote_addr: remote,
                state,
                inode,
            });
        }
        out
    }

    fn decode_proc_addr(field: &str, ipv6: bool) -> Option<String> {
        let (addr_hex, port_hex) = field.split_once(':')?;
        let port = u16::from_str_radix(port_hex, 16).ok()?;
        if ipv6 {
            if addr_hex.len() != 32 {
                return None;
            }
            // /proc encodes each u32 in host-endian (little-endian on
            // x86_64). Reassemble as 4 little-endian u32s.
            let mut bytes = [0u8; 16];
            for chunk in 0..4 {
                let word = u32::from_str_radix(&addr_hex[chunk * 8..chunk * 8 + 8], 16).ok()?;
                let be = word.to_le_bytes();
                bytes[chunk * 4..chunk * 4 + 4].copy_from_slice(&be);
            }
            let ip = std::net::Ipv6Addr::from(bytes);
            Some(format!("[{ip}]:{port}"))
        } else {
            if addr_hex.len() != 8 {
                return None;
            }
            let raw = u32::from_str_radix(addr_hex, 16).ok()?;
            let ip = std::net::Ipv4Addr::from(raw.to_le_bytes());
            Some(format!("{ip}:{port}"))
        }
    }

    fn read_conntrack(
        emitter: &Mutex<DynEmitter>,
        conntrack_gap_reported: &AtomicBool,
    ) -> Option<u64> {
        match std::fs::read_to_string("/proc/sys/net/netfilter/nf_conntrack_count") {
            Ok(s) => s.trim().parse::<u64>().ok(),
            Err(e) => {
                // Conntrack is best-effort: many containers can't see
                // it. Fire a one-time Error event so the bundle reader
                // knows it's missing, then stay quiet.
                if !conntrack_gap_reported.swap(true, Ordering::Relaxed) {
                    let dyn_emitter = emitter
                        .lock()
                        .expect("host introspect emitter mutex poisoned")
                        .clone();
                    use crate::diagnostics::sink::EventEmitter;
                    dyn_emitter.emit_event(Event::Error {
                        component: "host_introspect".into(),
                        message: format!("nf_conntrack_count unreadable: {e}"),
                        peer: None,
                    });
                }
                None
            }
        }
    }

    fn read_ipv6_enabled() -> Option<bool> {
        let body = std::fs::read_to_string("/proc/sys/net/ipv6/conf/all/disable_ipv6").ok()?;
        let n: u32 = body.trim().parse().ok()?;
        // The file is "1 means disabled", so enabled = (n == 0).
        Some(n == 0)
    }

    fn read_all_nameservers() -> Vec<String> {
        let body = match std::fs::read_to_string("/etc/resolv.conf") {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        for line in body.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("nameserver") {
                let server = rest.trim().to_string();
                if !server.is_empty() && seen.insert(server.clone()) {
                    out.push(server);
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_scheme_handles_urls_ports_and_trailing_dot() {
        assert_eq!(strip_scheme("relay.iroh.network."), "relay.iroh.network");
        assert_eq!(
            strip_scheme("https://relay.iroh.network./"),
            "relay.iroh.network",
        );
        assert_eq!(
            strip_scheme("https://relay.example.com:443/v1/"),
            "relay.example.com",
        );
        assert_eq!(strip_scheme("localhost"), "localhost");
    }

    #[test]
    fn capture_returns_empty_state_before_first_refresh() {
        let intro = HostIntrospect::new();
        let cap = intro.capture();
        assert!(cap.network.is_none());
        assert!(cap.dns.is_empty());
        assert!(cap.scraped_at_ms > 0);
    }

    #[test]
    fn add_dns_target_appears_in_capture_with_empty_answer_before_refresh() {
        let intro = HostIntrospect::new();
        intro.add_dns_target("relay.example.com");
        let cap = intro.capture();
        assert_eq!(cap.dns.len(), 1);
        assert_eq!(cap.dns[0].hostname, "relay.example.com");
        assert!(cap.dns[0].a_records.is_empty());
        assert!(cap.dns[0].error.is_none());
        assert_eq!(cap.dns[0].resolved_at_ms, 0);
    }

    #[test]
    fn add_dns_target_is_idempotent() {
        let intro = HostIntrospect::new();
        intro.add_dns_target("relay.example.com");
        intro.add_dns_target("relay.example.com");
        let cap = intro.capture();
        assert_eq!(cap.dns.len(), 1);
    }

    #[test]
    fn dns_for_unresolvable_host_records_an_error() {
        let intro = HostIntrospect::new();
        // Use a TLD known to never resolve in the public namespace.
        // RFC 6761 reserves `.invalid` for this purpose.
        intro.add_dns_target("nonexistent-host.invalid");
        intro.refresh_now();
        let cap = intro.capture();
        let entry = cap.dns.iter().find(|d| d.hostname.contains("invalid")).unwrap();
        assert!(entry.error.is_some());
        assert!(entry.a_records.is_empty());
        assert!(entry.aaaa_records.is_empty());
        assert!(entry.resolved_at_ms > 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn refresh_now_populates_network_block_on_linux() {
        let intro = HostIntrospect::new();
        intro.refresh_now();
        let cap = intro.capture();
        let net = cap.network.expect("network slot populated after refresh");
        // The loopback interface is universal on Linux. If we cannot
        // see it the scrape is broken in a way unit tests should
        // surface.
        let lo = net
            .interfaces
            .iter()
            .find(|i| i.name == "lo")
            .expect("loopback present");
        assert!(lo.up, "loopback should be up");
        assert!(net.refreshed_at_ms > 0);
    }
}

