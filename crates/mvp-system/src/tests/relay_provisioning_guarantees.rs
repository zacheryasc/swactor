use std::ffi::OsString;
use std::sync::Mutex;

use iroh::{RelayMode, RelayUrl};
use mvp_system::relay_provisioning::{
    LocalShimRelayProvider, MVP_IROH_RELAY_MODE_ENV, MVP_IROH_RELAY_URL_ENV, RelayProvider,
    RelayProviderKind, RelayProvisionRequest, RelayPurpose, SWACTOR_IROH_RELAY_URL_ENV,
    StaticRelayProvider, relay_runtime_config_from_env,
};

static ENV_LOCK: Mutex<()> = Mutex::new(());

const RELAY_ENV_KEYS: &[&str] = &[
    MVP_IROH_RELAY_MODE_ENV,
    MVP_IROH_RELAY_URL_ENV,
    SWACTOR_IROH_RELAY_URL_ENV,
];

struct RestoreEnv {
    saved: Vec<(&'static str, Option<OsString>)>,
}

impl Drop for RestoreEnv {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

fn with_relay_env<T>(settings: &[(&'static str, &'static str)], test: impl FnOnce() -> T) -> T {
    let _lock = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let saved = RELAY_ENV_KEYS
        .iter()
        .map(|&key| (key, std::env::var_os(key)))
        .collect::<Vec<_>>();
    for key in RELAY_ENV_KEYS {
        unsafe { std::env::remove_var(key) };
    }
    for (key, value) in settings {
        assert!(
            RELAY_ENV_KEYS.contains(key),
            "test env key {key} must be restored"
        );
        unsafe { std::env::set_var(key, value) };
    }
    let _restore = RestoreEnv { saved };
    test()
}

fn provision_request(run_id: u64) -> RelayProvisionRequest {
    RelayProvisionRequest {
        run_id,
        purpose: RelayPurpose::Combined,
    }
}

fn canonical_relay_url(raw: &str) -> String {
    raw.parse::<RelayUrl>()
        .expect("fixture relay URL parses")
        .to_string()
}

fn assert_custom_relay_mode(mode: RelayMode, expected_url: &str) {
    let expected_url = expected_url
        .parse::<RelayUrl>()
        .expect("fixture relay URL parses");
    match mode {
        RelayMode::Custom(relay_map) => {
            assert_eq!(relay_map.len(), 1, "custom relay map must contain one URL");
            assert!(
                relay_map.contains(&expected_url),
                "custom relay map must contain {expected_url}"
            );
        }
        other => panic!("expected custom relay mode for {expected_url}, got {other:?}"),
    }
}

#[test]
fn local_shim_provisions_disabled_lease_without_endpoints() {
    let mut provider = LocalShimRelayProvider;

    let lease = provider
        .provision_relay(provision_request(77))
        .expect("local shim provisioning succeeds");

    assert_eq!(lease.endpoints, Vec::new());
    assert!(matches!(
        provider
            .relay_mode(&lease)
            .expect("local shim relay mode resolves"),
        RelayMode::Disabled
    ));
}

#[test]
fn static_provider_provisions_one_endpoint_and_custom_relay_mode() {
    const RELAY_URL: &str = "https://relay-static.example.com";
    let mut provider =
        StaticRelayProvider::from_url_str(RELAY_URL).expect("static relay URL parses");
    let expected_url = canonical_relay_url(RELAY_URL);

    let lease = provider
        .provision_relay(provision_request(88))
        .expect("static relay provisioning succeeds");

    assert_eq!(lease.endpoints.len(), 1);
    assert_eq!(lease.endpoints[0].url, expected_url);
    assert_eq!(lease.endpoints[0].provider, RelayProviderKind::Static);
    assert_custom_relay_mode(
        provider
            .relay_mode(&lease)
            .expect("static relay mode resolves"),
        &expected_url,
    );
}

#[test]
fn default_relay_mode_uses_configured_mvp_or_swactor_relay_url() {
    const MVP_URL: &str = "https://relay-mvp.example.com";
    const SWACTOR_URL: &str = "https://relay-swactor.example.com";

    for (name, settings, expected_url) in [
        (
            "mvp relay URL",
            [
                (MVP_IROH_RELAY_MODE_ENV, "default"),
                (MVP_IROH_RELAY_URL_ENV, MVP_URL),
            ],
            MVP_URL,
        ),
        (
            "swactor relay URL fallback",
            [
                (MVP_IROH_RELAY_MODE_ENV, "default"),
                (SWACTOR_IROH_RELAY_URL_ENV, SWACTOR_URL),
            ],
            SWACTOR_URL,
        ),
    ] {
        with_relay_env(&settings, || {
            let config = relay_runtime_config_from_env(901).unwrap_or_else(|error| {
                panic!("{name} should resolve custom relay config: {error}")
            });
            let expected_url = canonical_relay_url(expected_url);
            assert_eq!(config.url.as_deref(), Some(expected_url.as_str()));
            assert_custom_relay_mode(config.mode, &expected_url);
        });
    }
}

#[test]
fn disabled_relay_mode_ignores_configured_url() {
    with_relay_env(
        &[
            (MVP_IROH_RELAY_MODE_ENV, "disabled"),
            (MVP_IROH_RELAY_URL_ENV, "https://ignored-relay.example.com"),
        ],
        || {
            let config = relay_runtime_config_from_env(902).expect("disabled relay mode resolves");
            assert!(matches!(config.mode, RelayMode::Disabled));
            assert_eq!(config.url, None);
        },
    );
}
