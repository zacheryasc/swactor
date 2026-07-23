use std::fmt;

use iroh::EndpointAddr;

pub const MVP_IROH_ENDPOINT_ADDR_MASK_ENV: &str = "MVP_IROH_ENDPOINT_ADDR_MASK";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EndpointAddrMask {
    #[default]
    Full,
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

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;

    #[test]
    fn relay_only_mask_preserves_relay_urls_and_removes_direct_addresses() {
        let relay = "http://relay.example.com"
            .parse::<iroh::RelayUrl>()
            .expect("relay URL parses");
        let endpoint = EndpointAddr::new(iroh::SecretKey::from_bytes(&[9; 32]).public())
            .with_relay_url(relay.clone())
            .with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 7777)));

        let masked = advertised_endpoint(endpoint, EndpointAddrMask::RelayOnly)
            .expect("relay-only endpoint builds");

        assert_eq!(masked.ip_addrs().count(), 0);
        assert_eq!(
            masked.relay_urls().next().map(ToString::to_string),
            Some(relay.to_string())
        );
    }

    #[test]
    fn relay_only_mask_rejects_endpoint_without_relay_url() {
        let endpoint = EndpointAddr::new(iroh::SecretKey::from_bytes(&[7; 32]).public())
            .with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 7777)));

        let error = advertised_endpoint(endpoint, EndpointAddrMask::RelayOnly)
            .expect_err("missing relay URL fails");

        assert!(
            error.contains("requires an endpoint relay URL"),
            "unexpected error: {error}"
        );
    }
}
