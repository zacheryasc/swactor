//! Behavior guarantees for the `transport` module.

use std::net::{Ipv4Addr, SocketAddr};

use mvp_system::transport::endpoint_advertisement::{EndpointAddrMask, advertised_endpoint};

#[test]
fn relay_only_mask_preserves_relay_urls_and_removes_direct_addresses() {
    let relay = "http://relay.example.com"
        .parse::<iroh::RelayUrl>()
        .expect("relay URL parses");
    let endpoint = iroh::EndpointAddr::new(iroh::SecretKey::from_bytes(&[9; 32]).public())
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
    let endpoint = iroh::EndpointAddr::new(iroh::SecretKey::from_bytes(&[7; 32]).public())
        .with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 7777)));

    let error = advertised_endpoint(endpoint, EndpointAddrMask::RelayOnly)
        .expect_err("missing relay URL fails");

    assert!(
        error.contains("requires an endpoint relay URL"),
        "unexpected error: {error}"
    );
}
