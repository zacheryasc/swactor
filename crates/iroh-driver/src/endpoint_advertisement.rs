//! Endpoint advertisement masking.
//!
//! Controls how much of an iroh [`EndpointAddr`] a node advertises to peers.
//! `relay-only` strips direct IP/socket addresses so peers can only reach the
//! node via its relay URL — useful for NAT-egress-only or hidden nodes.

use std::fmt;

use iroh::EndpointAddr;

/// Environment variable selecting the advertised endpoint address mask.
///
/// Recognized values: `full` (default) and `relay-only`.
pub const MVP_IROH_ENDPOINT_ADDR_MASK_ENV: &str = "MVP_IROH_ENDPOINT_ADDR_MASK";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EndpointAddrMask {
    /// Advertise the full endpoint address: relays and direct addresses.
    #[default]
    Full,
    /// Advertise relay URLs only, omitting direct socket addresses.
    RelayOnly,
}

impl EndpointAddrMask {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "full" | "none" => Ok(Self::Full),
            "relay-only" | "relay_only" | "relay" => Ok(Self::RelayOnly),
            other => Err(format!(
                "unsupported {MVP_IROH_ENDPOINT_ADDR_MASK_ENV}={other:?}; use full or relay-only"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::RelayOnly => "relay-only",
        }
    }

    pub fn requires_relay(self) -> bool {
        matches!(self, Self::RelayOnly)
    }
}

impl fmt::Display for EndpointAddrMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Apply `mask` to `endpoint`, returning the address to advertise to peers.
pub fn advertised_endpoint(
    endpoint: EndpointAddr,
    mask: EndpointAddrMask,
) -> Result<EndpointAddr, String> {
    match mask {
        EndpointAddrMask::Full => Ok(endpoint),
        EndpointAddrMask::RelayOnly => relay_only_endpoint(endpoint),
    }
}

fn relay_only_endpoint(endpoint: EndpointAddr) -> Result<EndpointAddr, String> {
    let relays = endpoint.relay_urls().cloned().collect::<Vec<_>>();
    if relays.is_empty() {
        return Err("relay-only endpoint address mask requires an endpoint relay URL".to_owned());
    }

    let mut out = EndpointAddr::new(endpoint.id);
    for relay in relays {
        out = out.with_relay_url(relay);
    }
    Ok(out)
}
