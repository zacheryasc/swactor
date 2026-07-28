//! Relay provisioning shims for MVP runtimes.
//!
//! The current implementations are deliberately small: local tests get the same
//! abstraction without an external relay, and deploy runs can point every node at
//! one operator-managed relay URL. A future provider can replace the static shim
//! with real relay leases without changing node provisioning.

use iroh::{RelayMode, RelayUrl};
use serde::{Deserialize, Serialize};

pub const MVP_IROH_RELAY_MODE_ENV: &str = "MVP_IROH_RELAY_MODE";
pub const MVP_IROH_RELAY_URL_ENV: &str = "MVP_IROH_RELAY_URL";
pub const SWACTOR_IROH_RELAY_URL_ENV: &str = "SWACTOR_IROH_RELAY_URL";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelayPurpose {
    Combined,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayProvisionRequest {
    pub run_id: u64,
    pub purpose: RelayPurpose,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayLeaseId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelayProviderKind {
    LocalShim,
    Static,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayEndpoint {
    pub url: String,
    pub provider: RelayProviderKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayLease {
    pub id: RelayLeaseId,
    pub endpoints: Vec<RelayEndpoint>,
}

#[derive(Clone, Debug)]
pub struct RelayRuntimeConfig {
    pub mode: RelayMode,
    pub url: Option<String>,
}

pub trait RelayProvider: Send {
    fn provision_relay(&mut self, request: RelayProvisionRequest) -> Result<RelayLease, String>;

    fn relay_mode(&self, lease: &RelayLease) -> Result<RelayMode, String>;

    fn release_relay(&mut self, _lease: RelayLease) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalShimRelayProvider;

impl RelayProvider for LocalShimRelayProvider {
    fn provision_relay(&mut self, request: RelayProvisionRequest) -> Result<RelayLease, String> {
        Ok(RelayLease {
            id: RelayLeaseId(format!("local-shim:{}", request.run_id)),
            endpoints: Vec::new(),
        })
    }

    fn relay_mode(&self, _lease: &RelayLease) -> Result<RelayMode, String> {
        Ok(RelayMode::Disabled)
    }
}

#[derive(Clone, Debug)]
pub struct StaticRelayProvider {
    url: RelayUrl,
}

impl StaticRelayProvider {
    pub fn new(url: RelayUrl) -> Self {
        Self { url }
    }

    pub fn from_url_str(raw: &str) -> Result<Self, String> {
        parse_relay_url(raw).map(Self::new)
    }

    pub fn from_env() -> Result<Option<Self>, String> {
        selected_relay_url_from_env()
            .map(|url| Self::from_url_str(&url).map(Some))
            .unwrap_or(Ok(None))
    }

    pub fn url(&self) -> String {
        self.url.to_string()
    }
}

impl RelayProvider for StaticRelayProvider {
    fn provision_relay(&mut self, request: RelayProvisionRequest) -> Result<RelayLease, String> {
        Ok(RelayLease {
            id: RelayLeaseId(format!("static-relay:{}:{}", request.run_id, self.url)),
            endpoints: vec![RelayEndpoint {
                url: self.url.to_string(),
                provider: RelayProviderKind::Static,
            }],
        })
    }

    fn relay_mode(&self, lease: &RelayLease) -> Result<RelayMode, String> {
        let urls = lease
            .endpoints
            .iter()
            .map(|endpoint| parse_relay_url(&endpoint.url))
            .collect::<Result<Vec<_>, _>>()?;
        if urls.is_empty() {
            return Err("static relay lease has no endpoints".to_owned());
        }
        Ok(RelayMode::custom(urls))
    }
}

pub fn relay_runtime_config_from_env(run_id: u64) -> Result<RelayRuntimeConfig, String> {
    let mode = relay_mode_setting_from_env();
    let url = selected_relay_url_from_env();
    relay_runtime_config_from_settings(run_id, mode.as_deref(), url.as_deref())
}

pub fn relay_runtime_config_from_settings(
    run_id: u64,
    mode: Option<&str>,
    url: Option<&str>,
) -> Result<RelayRuntimeConfig, String> {
    match mode {
        Some("disabled") => Ok(RelayRuntimeConfig {
            mode: RelayMode::Disabled,
            url: None,
        }),
        None | Some("default") => relay_runtime_config_from_optional_static_provider(run_id, url),
        Some(other) => Err(format!(
            "unsupported {MVP_IROH_RELAY_MODE_ENV}={other:?}; use disabled or default"
        )),
    }
}

pub fn relay_mode_env_value(mode: &RelayMode) -> &'static str {
    match mode {
        RelayMode::Disabled => "disabled",
        _ => "default",
    }
}

pub fn selected_relay_url_from_env() -> Option<String> {
    env_optional(MVP_IROH_RELAY_URL_ENV).or_else(|| env_optional(SWACTOR_IROH_RELAY_URL_ENV))
}

fn relay_runtime_config_from_optional_static_provider(
    run_id: u64,
    url: Option<&str>,
) -> Result<RelayRuntimeConfig, String> {
    let Some(raw_url) = url.map(str::trim).filter(|url| !url.is_empty()) else {
        return Ok(RelayRuntimeConfig {
            mode: RelayMode::Default,
            url: None,
        });
    };
    let mut provider = StaticRelayProvider::from_url_str(raw_url)?;
    let lease = provider.provision_relay(RelayProvisionRequest {
        run_id,
        purpose: RelayPurpose::Combined,
    })?;
    let mode = provider.relay_mode(&lease)?;
    Ok(RelayRuntimeConfig {
        mode,
        url: Some(provider.url()),
    })
}

fn relay_mode_setting_from_env() -> Option<String> {
    env_optional(MVP_IROH_RELAY_MODE_ENV).map(|value| value.to_ascii_lowercase())
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn parse_relay_url(raw: &str) -> Result<RelayUrl, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("relay URL cannot be empty".to_owned());
    }
    trimmed
        .parse::<RelayUrl>()
        .map_err(|e| format!("invalid relay URL {trimmed:?}: {e}"))
}
