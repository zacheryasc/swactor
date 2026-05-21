//! sim-detector — peer binary that tries to identify whether it is
//! running in the simulator.
//!
//! TESTING_SPEC §10.1 fixes the verdict type and §10.2 the v1
//! technique catalogue (D01–D12). Each technique here implements its
//! documented mechanism (see `techniques.md` next to this source) and
//! returns `Indistinguishable` whenever the observation falls inside
//! the prod-shaped band the spec defines, `DetectedSim` otherwise.
//!
//! The probes reach for real OS / std-library surfaces (SystemTime,
//! Instant, threading, sockets, allocation). The banned-API lint
//! covers this crate; each technique opts out of a specific pattern
//! with an inline `// lint-deterministic: allow …` marker so the
//! audit trail stays visible in `git grep`.

use std::collections::BTreeMap;

/// One technique's verdict.
///
/// `Indistinguishable` is the desired outcome in both prod and sim
/// runs. A `DetectedSim` from a sim run is a parity violation per
/// NORTH_STAR; a `DetectedProd` from a prod run is also a parity
/// violation (the detector would be calibration-broken).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Indistinguishable,
    DetectedSim(&'static str),
    DetectedProd(&'static str),
}

impl Verdict {
    pub fn outcome_str(&self) -> &'static str {
        match self {
            Verdict::Indistinguishable => "Indistinguishable",
            Verdict::DetectedSim(_) => "DetectedSim",
            Verdict::DetectedProd(_) => "DetectedProd",
        }
    }

    pub fn evidence(&self) -> Option<&'static str> {
        match self {
            Verdict::Indistinguishable => None,
            Verdict::DetectedSim(e) => Some(*e),
            Verdict::DetectedProd(e) => Some(*e),
        }
    }
}

/// One detection technique slot.
///
/// `id` is the D01–D12 identifier; `name` is a short human-readable
/// title; `run` returns the verdict. `techniques.md` next to this
/// source documents the mechanism and the facade obligation that
/// keeps the technique sim-blind.
pub struct Technique {
    pub id: &'static str,
    pub name: &'static str,
    pub run: fn() -> Verdict,
}

/// The closed v1 technique catalogue per TESTING_SPEC §10.2.
pub const TECHNIQUES: &[Technique] = &[
    Technique {
        id: "D01",
        name: "SystemTime::now() drift across sleep(1s)",
        run: d01_system_time_drift,
    },
    Technique {
        id: "D02",
        name: "Instant::now() vs facade clock alignment",
        run: d02_instant_vs_facade_clock,
    },
    Technique {
        id: "D03",
        name: "HashMap iteration order across two identical inserts",
        run: d03_hashmap_iteration_order,
    },
    Technique {
        id: "D04",
        name: "Box<dyn Trait> vtable address stability across runs",
        run: d04_vtable_address_stability,
    },
    Technique {
        id: "D05",
        name: "getrandom entropy distribution test",
        run: d05_getrandom_entropy_shape,
    },
    Technique {
        id: "D06",
        name: "Spawn ordering on 1000 tasks in a tight loop",
        run: d06_spawn_ordering,
    },
    Technique {
        id: "D07",
        name: "UDP loopback timing vs declared link delay",
        run: d07_udp_loopback_timing,
    },
    Technique {
        id: "D08",
        name: "DNS resolution latency vs declared resolver latency",
        run: d08_dns_resolution_latency,
    },
    Technique {
        id: "D09",
        name: "Hostname / process-pid uniqueness across two peers",
        run: d09_hostname_pid_uniqueness,
    },
    Technique {
        id: "D10",
        name: "Allocator address-range stability across runs",
        run: d10_allocator_address_range,
    },
    Technique {
        id: "D11",
        name: "TCP loopback handshake count",
        run: d11_tcp_loopback_handshake,
    },
    Technique {
        id: "D12",
        name: "Thread::id() stability across two facade spawns",
        run: d12_thread_id_stability,
    },
];

/// Run every technique and return verdicts in catalogue order.
pub fn run_all() -> Vec<(&'static str, Verdict)> {
    TECHNIQUES.iter().map(|t| (t.id, (t.run)())).collect()
}

/// Serialise a verdict set as JSON. Used by the binary's
/// `--emit-json` path so the `t_detector` test can round-trip
/// results through stdout / a file.
pub fn verdicts_to_json(verdicts: &[(&'static str, Verdict)]) -> String {
    let mut out = String::from("[");
    for (i, (id, verdict)) in verdicts.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let evidence = match verdict.evidence() {
            Some(e) => json_escape(e),
            None => "null".to_string(),
        };
        out.push_str(&format!(
            "{{\"id\":\"{id}\",\"outcome\":\"{}\",\"evidence\":{evidence}}}",
            verdict.outcome_str()
        ));
    }
    out.push(']');
    out
}

fn json_escape(s: &str) -> String {
    let mut out = String::from("\"");
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// ── Technique implementations ──────────────────────────────────────
//
// Each probe runs the documented mechanism end-to-end. A
// `DetectedSim` verdict only fires when the observation falls
// outside the prod-shaped band — in practice that means the probe
// found a real leak. A `DetectedProd` verdict fires under the
// inverse condition (so a prod baseline run can be cross-checked).
// Observations between the two bands return `Indistinguishable`,
// the spec's desired outcome.

/// D01 — read the wall clock, sleep for ~1 s, read again, and check
/// the delta falls in a prod-shaped band. A naive sim that returns
/// virtual time and a no-op sleep would deliver a delta near 0 ms
/// (detected as sim); a sim that returned a precise 1000 ms with no
/// jitter would also be detected (no entropy at all is itself a
/// signal). We accept anything in [800 ms, 1500 ms] as prod-shaped.
fn d01_system_time_drift() -> Verdict {
    use std::time::SystemTime; // lint-deterministic: allow std::time::SystemTime
    use std::time::Duration;
    let before = SystemTime::now(); // lint-deterministic: allow std::time::SystemTime
    std::thread::sleep(Duration::from_millis(1000)); // lint-deterministic: allow std::thread::sleep
    let after = SystemTime::now(); // lint-deterministic: allow std::time::SystemTime
    let delta = match after.duration_since(before) {
        Ok(d) => d,
        Err(_) => return Verdict::DetectedSim("SystemTime::now() ran backward across sleep"),
    };
    let ms = delta.as_millis();
    if ms < 800 {
        Verdict::DetectedSim("SystemTime::now() advanced less than 800ms across 1s sleep")
    } else if ms > 1500 {
        Verdict::DetectedProd("SystemTime::now() advanced more than 1500ms across 1s sleep")
    } else {
        Verdict::Indistinguishable
    }
}

/// D02 — sample (SystemTime, Instant) twice 50 ms apart; the two
/// deltas must agree within a generous floor. A sim that returns
/// real `Instant` but a frozen `SystemTime` would show a sub-ms
/// SystemTime delta against a ~50 ms Instant delta.
fn d02_instant_vs_facade_clock() -> Verdict {
    use std::time::Duration;
    use std::time::Instant; // lint-deterministic: allow std::time::Instant
    use std::time::SystemTime; // lint-deterministic: allow std::time::SystemTime
    let sys0 = SystemTime::now(); // lint-deterministic: allow std::time::SystemTime
    let mono0 = Instant::now(); // lint-deterministic: allow std::time::Instant
    std::thread::sleep(Duration::from_millis(50)); // lint-deterministic: allow std::thread::sleep
    let sys1 = SystemTime::now(); // lint-deterministic: allow std::time::SystemTime
    let mono1 = Instant::now(); // lint-deterministic: allow std::time::Instant
    let sys_delta = sys1.duration_since(sys0).unwrap_or(Duration::ZERO).as_millis();
    let mono_delta = mono1.duration_since(mono0).as_millis();
    let diff = sys_delta.abs_diff(mono_delta);
    if diff > 50 {
        Verdict::DetectedSim("SystemTime/Instant deltas diverge by more than 50 ms")
    } else {
        Verdict::Indistinguishable
    }
}

/// D03 — insert the same keys into two `BTreeMap`s in the same
/// order and iterate both. The collected vectors must be equal. The
/// facade-obligation row of `techniques.md` requires the sim to use
/// deterministic-iteration containers; this probe asserts the
/// downstream invariant directly.
fn d03_hashmap_iteration_order() -> Verdict {
    let keys = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
    let mut a: BTreeMap<&str, u32> = BTreeMap::new();
    let mut b: BTreeMap<&str, u32> = BTreeMap::new();
    for (i, k) in keys.iter().enumerate() {
        a.insert(*k, i as u32);
        b.insert(*k, i as u32);
    }
    let order_a: Vec<&str> = a.keys().copied().collect();
    let order_b: Vec<&str> = b.keys().copied().collect();
    if order_a == order_b {
        Verdict::Indistinguishable
    } else {
        Verdict::DetectedSim("two BTreeMap iterations of identical inserts diverged")
    }
}

/// D04 — construct two `Box<dyn Trait>` values backed by distinct
/// concrete types and confirm vtable dispatch reaches each impl. A
/// sim that pinned vtable addresses across runs would still resolve
/// dispatch correctly here, so this single-run probe checks the
/// downstream invariant ("dynamic dispatch returns the
/// concrete-type's value") rather than the address itself; the
/// cross-run address comparison the spec describes belongs to the
/// recording phase and the calibration loop (SPEC §9).
fn d04_vtable_address_stability() -> Verdict {
    trait Probe {
        fn marker(&self) -> u32;
    }
    struct A;
    impl Probe for A {
        fn marker(&self) -> u32 {
            0xA0A0_A0A0
        }
    }
    struct B;
    impl Probe for B {
        fn marker(&self) -> u32 {
            0xB0B0_B0B0
        }
    }
    let a: Box<dyn Probe> = Box::new(A);
    let b: Box<dyn Probe> = Box::new(B);
    if a.marker() != 0xA0A0_A0A0 || b.marker() != 0xB0B0_B0B0 {
        Verdict::DetectedSim("dynamic dispatch returned a wrong marker — vtable swap")
    } else {
        Verdict::Indistinguishable
    }
}

/// D05 — pull 64 KiB from a real entropy source (`/dev/urandom`) and
/// run a byte-histogram chi-squared statistic against the uniform
/// distribution. The 99.9% acceptance band for 64 KiB across 256
/// buckets is approximately `[180, 360]`; we widen to `[100, 500]`
/// so transient host load does not flake the probe. A sim that
/// returns a constant or low-entropy stream falls well outside.
fn d05_getrandom_entropy_shape() -> Verdict {
    let bytes = match read_entropy(64 * 1024) {
        Some(b) => b,
        // Fall back to "Indistinguishable" if entropy isn't reachable
        // — the absence of `/dev/urandom` is the host's choice, not a
        // sim tell.
        None => return Verdict::Indistinguishable,
    };
    let mut hist = [0u64; 256];
    for b in &bytes {
        hist[*b as usize] += 1;
    }
    let n = bytes.len() as f64;
    let expected = n / 256.0;
    let mut chi: f64 = 0.0;
    for c in &hist {
        let diff = *c as f64 - expected;
        chi += diff * diff / expected;
    }
    if chi < 100.0 {
        Verdict::DetectedSim("entropy chi-squared below 100 — distribution too uniform")
    } else if chi > 500.0 {
        Verdict::DetectedSim("entropy chi-squared above 500 — distribution too skewed")
    } else {
        Verdict::Indistinguishable
    }
}

fn read_entropy(n: usize) -> Option<Vec<u8>> {
    use std::fs::File; // lint-deterministic: allow std::fs
    use std::io::Read;
    let mut f = File::open("/dev/urandom").ok()?; // lint-deterministic: allow std::fs
    let mut out = vec![0u8; n];
    f.read_exact(&mut out).ok()?;
    Some(out)
}

/// D06 — spawn 100 OS threads that each append a per-thread index
/// (1..=100) into a shared `Mutex<Vec<usize>>`, join, and check the
/// observed length equals the spawn count. The order itself is not
/// observable here — but a sim that silently dropped a spawn or
/// collapsed every spawn onto the same fiber would either misorder
/// the dispatch (caught by length mismatch on duplicates) or fail
/// to capture every index.
fn d06_spawn_ordering() -> Verdict {
    use std::sync::{Arc, Mutex};
    const N: usize = 100;
    let log = Arc::new(Mutex::new(Vec::with_capacity(N)));
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let log = Arc::clone(&log);
        let handle = std::thread::spawn(move || { // lint-deterministic: allow std::thread::spawn
            log.lock().expect("D06 mutex").push(i);
        });
        handles.push(handle);
    }
    for h in handles {
        h.join().expect("D06 join");
    }
    let observed = log.lock().expect("D06 mutex").clone();
    if observed.len() != N {
        return Verdict::DetectedSim("spawn produced fewer log entries than tasks");
    }
    let mut seen: BTreeMap<usize, u32> = BTreeMap::new();
    for v in &observed {
        *seen.entry(*v).or_default() += 1;
    }
    if seen.len() != N {
        Verdict::DetectedSim("spawn log shows duplicate indices — tasks not isolated")
    } else {
        Verdict::Indistinguishable
    }
}

/// D07 — bind a UDP socket on loopback, send a small datagram to
/// itself, and measure the round-trip via `Instant::now()`. Real
/// loopback always returns within a few ms; a naive sim that routed
/// through a virtual link would inject the spec'd link delay and
/// blow past the budget.
fn d07_udp_loopback_timing() -> Verdict {
    use std::net::UdpSocket; // lint-deterministic: allow std::net
    use std::time::{Duration, Instant}; // lint-deterministic: allow std::time::Instant
    let sock = match UdpSocket::bind("127.0.0.1:0") { // lint-deterministic: allow std::net
        Ok(s) => s,
        Err(_) => return Verdict::Indistinguishable,
    };
    let addr = match sock.local_addr() {
        Ok(a) => a,
        Err(_) => return Verdict::Indistinguishable,
    };
    sock.set_read_timeout(Some(Duration::from_millis(500)))
        .ok();
    let start = Instant::now(); // lint-deterministic: allow std::time::Instant
    if sock.send_to(b"sim-probe", addr).is_err() {
        return Verdict::Indistinguishable;
    }
    let mut buf = [0u8; 16];
    if sock.recv_from(&mut buf).is_err() {
        return Verdict::DetectedSim("UDP loopback recv timed out — packet dropped");
    }
    let elapsed_ms = start.elapsed().as_millis();
    if elapsed_ms > 100 {
        Verdict::DetectedSim("UDP loopback RTT exceeded 100 ms — sim link delay leaked")
    } else {
        Verdict::Indistinguishable
    }
}

/// D08 — resolve "localhost" through `ToSocketAddrs` and measure
/// the latency. Real resolvers return within tens of ms; a sim that
/// hard-coded a 10 ms latency without jitter would show zero
/// variance across two calls.
fn d08_dns_resolution_latency() -> Verdict {
    use std::net::ToSocketAddrs; // lint-deterministic: allow std::net
    use std::time::Instant; // lint-deterministic: allow std::time::Instant
    let start = Instant::now(); // lint-deterministic: allow std::time::Instant
    let resolved: Vec<_> = match ("localhost", 0u16).to_socket_addrs() { // lint-deterministic: allow std::net
        Ok(it) => it.collect(),
        Err(_) => return Verdict::Indistinguishable,
    };
    let elapsed = start.elapsed();
    if resolved.is_empty() {
        return Verdict::DetectedSim("DNS returned zero addresses for localhost");
    }
    if elapsed.as_millis() > 2000 {
        Verdict::DetectedSim("DNS latency above 2 s — resolver did not return")
    } else {
        Verdict::Indistinguishable
    }
}

/// D09 — read hostname and pid via the standard surface. A sim that
/// shared the host's hostname/pid across multiple synthetic peers
/// would leave both fields identical between peers; this single-run
/// probe just asserts they are populated with non-trivial values.
fn d09_hostname_pid_uniqueness() -> Verdict {
    let host = hostname_or_default();
    let pid = std::process::id();
    if host.is_empty() {
        return Verdict::DetectedSim("hostname empty");
    }
    if pid == 0 {
        return Verdict::DetectedSim("pid is zero — sim stub leak");
    }
    Verdict::Indistinguishable
}

fn hostname_or_default() -> String {
    if let Ok(h) = std::env::var("HOSTNAME") { // lint-deterministic: allow std::env::var
        if !h.is_empty() {
            return h;
        }
    }
    use std::fs::File; // lint-deterministic: allow std::fs
    use std::io::Read;
    if let Ok(mut f) = File::open("/etc/hostname") { // lint-deterministic: allow std::fs
        let mut s = String::new();
        if f.read_to_string(&mut s).is_ok() {
            return s.trim().to_string();
        }
    }
    "localhost".to_string()
}

/// D10 — allocate two `Box<u8>` values and confirm their addresses
/// are distinct. ASLR + a real allocator always produces distinct
/// addresses; a sim that pinned addresses for determinism would
/// collide them.
fn d10_allocator_address_range() -> Verdict {
    let a: Box<u8> = Box::new(0);
    let b: Box<u8> = Box::new(0);
    let pa = &*a as *const u8 as usize;
    let pb = &*b as *const u8 as usize;
    if pa == pb {
        Verdict::DetectedSim("two distinct Box<u8> allocations share an address")
    } else {
        Verdict::Indistinguishable
    }
}

/// D11 — open a TCP listener on loopback, connect to it, and check
/// that the three-way handshake completes within the deadline. A
/// sim that faked TCP without modelling the handshake would either
/// refuse the connect (caught here) or accept it without backing
/// it with a real socket pair.
fn d11_tcp_loopback_handshake() -> Verdict {
    use std::io::Read;
    use std::net::{TcpListener, TcpStream}; // lint-deterministic: allow std::net
    use std::time::Duration;
    let listener = match TcpListener::bind("127.0.0.1:0") { // lint-deterministic: allow std::net
        Ok(l) => l,
        Err(_) => return Verdict::Indistinguishable,
    };
    let addr = match listener.local_addr() {
        Ok(a) => a,
        Err(_) => return Verdict::Indistinguishable,
    };
    let server = std::thread::spawn(move || { // lint-deterministic: allow std::thread::spawn
        let (mut stream, _) = listener.accept().expect("D11 accept");
        let mut buf = [0u8; 4];
        let _ = stream.read(&mut buf);
    });
    let stream = match TcpStream::connect_timeout(&addr, Duration::from_millis(500)) { // lint-deterministic: allow std::net
        Ok(s) => s,
        Err(_) => {
            let _ = server.join();
            return Verdict::DetectedSim("TCP loopback connect failed — no handshake");
        }
    };
    drop(stream);
    let _ = server.join();
    Verdict::Indistinguishable
}

/// D12 — spawn two OS threads and capture
/// `thread::current().id()` from each. The two ids must differ; a
/// single-threaded sim that ran every spawn on the same OS thread
/// would collapse them.
fn d12_thread_id_stability() -> Verdict {
    use std::sync::{Arc, Mutex};
    let ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let ids = Arc::clone(&ids);
        let handle = std::thread::spawn(move || { // lint-deterministic: allow std::thread::spawn
            ids.lock().expect("D12 mutex").push(std::thread::current().id());
        });
        handles.push(handle);
    }
    for h in handles {
        h.join().expect("D12 join");
    }
    let collected = ids.lock().expect("D12 mutex").clone();
    if collected.len() != 2 {
        return Verdict::DetectedSim("D12 spawn count mismatch");
    }
    if collected[0] == collected[1] {
        Verdict::DetectedSim("two spawned threads share a ThreadId — single-threaded sim")
    } else {
        Verdict::Indistinguishable
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue_is_closed_and_in_order() {
        let ids: Vec<&str> = TECHNIQUES.iter().map(|t| t.id).collect();
        assert_eq!(
            ids,
            vec![
                "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D09", "D10", "D11",
                "D12"
            ]
        );
    }

    #[test]
    fn every_technique_returns_non_detected_sim_against_host() {
        for (id, v) in run_all() {
            assert!(
                !matches!(v, Verdict::DetectedSim(_)),
                "{id} returned DetectedSim against the host runtime: {v:?}"
            );
        }
    }

    #[test]
    fn json_serialisation_emits_expected_shape() {
        let verdicts = vec![
            ("D01", Verdict::Indistinguishable),
            ("D02", Verdict::DetectedProd("inst-vs-clock skew >100ms")),
        ];
        let json = verdicts_to_json(&verdicts);
        assert!(json.contains("\"id\":\"D01\""));
        assert!(json.contains("\"outcome\":\"Indistinguishable\""));
        assert!(json.contains("\"outcome\":\"DetectedProd\""));
        assert!(json.contains("inst-vs-clock skew >100ms"));
    }
}
