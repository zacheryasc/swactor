//! Spec §5 (host metadata forwarding, gap 5).
//!
//! After the upgrade a node's boot record carries everything the bundle
//! reader needs to identify which rental ran the stage — public IP,
//! datacenter, country, vast.ai contract id, container id, hostname,
//! relay URL, iroh version, git SHA. Missing means missing: a node not
//! on a cloud provider leaves the provider fields absent rather than
//! blank, and the post-processor's `## Hosts` section shows the
//! difference at a glance.

use distribution::diagnostics::identity::HostContext;
use distribution::diagnostics::{Identity, Role, IROH_VERSION};
use distribution::types::NodeId;

#[test]
fn host_context_overlays_only_set_fields_on_identity() {
    let id = Identity::new(NodeId([0xaa; 32]), Role::stage(), "run-h1");
    assert!(id.host_ip_public.is_none());
    assert!(id.vastai_contract_id.is_none());
    assert!(id.iroh_version.is_none());

    let ctx = HostContext {
        host_ip_public: Some("203.0.113.7".to_string()),
        datacenter_id: Some("dc-abc".to_string()),
        host_country: Some("US".to_string()),
        vastai_contract_id: Some("99999".to_string()),
        container_id: Some("docker-abc".to_string()),
        hostname: Some("c-99999".to_string()),
        home_relay_url_at_boot: Some("https://relay.example/".to_string()),
        git_sha: Some("deadbeef".to_string()),
        iroh_version: Some(IROH_VERSION.to_string()),
        binary_version: Some("0.1.0".to_string()),
    };
    let id = id.with_host_context(ctx);

    assert_eq!(id.host_ip_public.as_deref(), Some("203.0.113.7"));
    assert_eq!(id.datacenter_id.as_deref(), Some("dc-abc"));
    assert_eq!(id.host_country.as_deref(), Some("US"));
    assert_eq!(id.vastai_contract_id.as_deref(), Some("99999"));
    assert_eq!(id.container_id.as_deref(), Some("docker-abc"));
    assert_eq!(id.hostname.as_deref(), Some("c-99999"));
    assert_eq!(id.home_relay_url_at_boot.as_deref(), Some("https://relay.example/"));
    assert_eq!(id.git_sha.as_deref(), Some("deadbeef"));
    assert_eq!(id.iroh_version.as_deref(), Some(IROH_VERSION));
    assert_eq!(id.binary_version.as_deref(), Some("0.1.0"));
}

#[test]
fn empty_host_context_leaves_cloud_fields_absent() {
    // A node running outside the orchestrator's lease flow (local dev
    // node, sim node, etc.) gets an empty HostContext. Cloud-provider
    // fields stay None — never become Some("unknown") or Some("").
    let id = Identity::new(NodeId([0xbb; 32]), Role::stage(), "run-h2")
        .with_host_context(HostContext::default());
    assert!(id.host_ip_public.is_none(), "host_ip_public must stay absent");
    assert!(id.datacenter_id.is_none(), "datacenter_id must stay absent");
    assert!(id.host_country.is_none(), "host_country must stay absent");
    assert!(id.vastai_contract_id.is_none(), "vastai_contract_id must stay absent");
}

#[test]
fn host_context_with_iroh_version_records_the_linked_string() {
    // Spec §5 cross-references §6: the iroh version on Identity should
    // be the same string the tier-2 transport snapshots carry, sourced
    // from the build (not a literal).
    let ctx = HostContext::new().with_iroh_version(IROH_VERSION);
    assert_eq!(ctx.iroh_version.as_deref(), Some(IROH_VERSION));
    let id = Identity::new(NodeId([0xcc; 32]), Role::orchestrator(), "run-h3")
        .with_host_context(ctx);
    assert_eq!(id.iroh_version.as_deref(), Some(IROH_VERSION));
}
