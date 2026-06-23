//! Relay candidacy evaluation.
//!
//! Determines whether this node is eligible to run an embedded relay server
//! by checking a set of extensible rules (public IP, port availability, etc.).

use std::net::{IpAddr, SocketAddr, UdpSocket};

// ─── Candidacy ──────────────────────────────────────────────────────────

/// Result of evaluating a single candidacy rule.
pub enum CandidacyResult {
    Eligible,
    Ineligible(String),
}

/// Extensible rule for relay candidacy evaluation.
pub trait CandidacyRule: Send {
    fn evaluate(&self) -> CandidacyResult;
}

/// Checks whether the node's outbound IP is public (non-RFC1918, non-loopback).
pub struct PublicIpRule;

impl PublicIpRule {
    pub fn outbound_ip() -> Option<IpAddr> {
        let sock = UdpSocket::bind("0.0.0.0:0").ok()?;
        sock.connect("192.0.2.1:80").ok()?; // RFC 5737 TEST-NET-1 (non-routable)
        Some(sock.local_addr().ok()?.ip())
    }

    fn is_public(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(v4) => {
                !v4.is_loopback() && !v4.is_private() && !v4.is_link_local() && !v4.is_unspecified()
            }
            IpAddr::V6(v6) => !v6.is_loopback() && !v6.is_unspecified(),
        }
    }
}

impl CandidacyRule for PublicIpRule {
    fn evaluate(&self) -> CandidacyResult {
        match Self::outbound_ip() {
            Some(ip) if Self::is_public(&ip) => CandidacyResult::Eligible,
            Some(ip) => CandidacyResult::Ineligible(format!("outbound IP {ip} is private")),
            None => CandidacyResult::Ineligible("could not determine outbound IP".into()),
        }
    }
}

/// Checks whether the desired relay port is available for binding.
pub struct PortBindRule {
    addr: SocketAddr,
}

impl PortBindRule {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

impl CandidacyRule for PortBindRule {
    fn evaluate(&self) -> CandidacyResult {
        match std::net::TcpListener::bind(self.addr) {
            Ok(_) => CandidacyResult::Eligible,
            Err(e) => CandidacyResult::Ineligible(format!("cannot bind {}: {e}", self.addr)),
        }
    }
}

/// Evaluate all candidacy rules. Returns `Ok(())` if all pass, or
/// `Err(reason)` with the first failure reason.
pub fn evaluate_candidacy(rules: &[Box<dyn CandidacyRule>]) -> Result<(), String> {
    for rule in rules {
        if let CandidacyResult::Ineligible(reason) = rule.evaluate() {
            return Err(reason);
        }
    }
    Ok(())
}
