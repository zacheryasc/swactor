use std::fs;
use std::path::Path;

use serde::Deserialize;

pub(crate) const DEFAULT_CONFIG_PATH: &str = ".config/config.toml";

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct VastAiConfig {
    pub api_key: Option<String>,
    pub image: Option<String>,
    pub relay_url: Option<String>,
    pub bootstrap_command: Option<String>,
    pub disk_gb: Option<u32>,
    pub ssh_user: Option<String>,
    pub confirm_lease: Option<bool>,
    pub gpu_name: Option<String>,
    pub min_gpu_ram_mb: Option<u64>,
    pub min_down_mbps: Option<f64>,
    pub min_up_mbps: Option<f64>,
    pub max_dph_total: Option<f64>,
    pub min_reliability: Option<f64>,
    pub require_verified: Option<bool>,
    pub blacklist_hosts: Vec<u64>,
    pub poll_interval_secs: Option<u64>,
    pub onstart: Option<String>,
    pub ssh_identity: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ResolvedVastAiConfig {
    pub api_key: String,
    pub relay_url: String,
    pub image: String,
    pub bootstrap_command: String,
    pub disk_gb: Option<u32>,
    pub gpu_name: Option<String>,
    pub min_gpu_ram_mb: Option<u64>,
    pub min_down_mbps: Option<f64>,
    pub min_up_mbps: Option<f64>,
    pub max_dph_total: Option<f64>,
    pub min_reliability: Option<f64>,
    pub require_verified: Option<bool>,
    pub blacklist_hosts: Vec<u64>,
    pub onstart: Option<String>,
    pub ssh_identity: Option<String>,
}

impl ResolvedVastAiConfig {
    pub(crate) fn validate(self) -> Result<Self, String> {
        require_non_empty("VAST_API_KEY", &self.api_key)?;
        require_non_empty("relay.url", &self.relay_url)?;
        require_non_empty("vastai.image", &self.image)?;
        require_non_empty("vastai.bootstrap_command", &self.bootstrap_command)?;
        if let Some(identity) = &self.ssh_identity {
            require_non_empty("vastai.ssh_identity", identity)?;
        }
        if !looks_remote_image(&self.image) {
            return Err(format!(
                "vastai.image {:?} must include a registry namespace",
                self.image
            ));
        }
        Ok(self)
    }
}

fn require_non_empty(label: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("missing required {label}"))
    } else {
        Ok(())
    }
}

pub(crate) fn looks_remote_image(image: &str) -> bool {
    let repository = image.split('@').next().unwrap_or(image);
    let last_slash = repository.rfind('/');
    let tag_separator = repository
        .rfind(':')
        .filter(|separator| last_slash.is_some_and(|slash| *separator > slash));
    let repository = tag_separator.map_or(repository, |separator| &repository[..separator]);
    let Some((host, _)) = repository.split_once('/') else {
        return false;
    };
    host == "localhost" || host.contains('.') || host.contains(':')
}

/// Shared overlay for legacy and multi-binary configuration parsing. This accepts
/// fields outside the fixed `myelin-chat` public config surface; `myelin-chat` uses a
/// bin-local strict config loader instead.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct TomlConfigOverlay {
    pub runtime: RuntimeConfigOverlay,
    pub provider: ProviderConfigOverlay,
    pub image: ImageConfig,
    pub relay: RelayConfig,
    pub prompt: PromptConfig,
    pub model: ModelConfig,
    pub docker: DockerConfigOverlay,
    pub observability: ObservabilityConfigOverlay,
    pub vastai: VastAiConfig,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct RuntimeConfigOverlay {
    pub profile: Option<String>,
    pub run_id: Option<u64>,
    pub node_id: Option<u64>,
    pub stage_index: Option<u32>,
    pub layer_end_exclusive: Option<u32>,
    pub pipeline_stages: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct ProviderConfigOverlay {
    pub kind: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct ImageConfig {
    pub node: Option<String>,
    pub tag: Option<String>,
    pub build: Option<bool>,
    pub push: Option<bool>,
    pub force_refresh: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct RelayConfig {
    pub mode: Option<String>,
    pub url: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct PromptConfig {
    pub rpc_addr: Option<String>,
    pub max_tokens: Option<u32>,
    pub dashboard: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct ModelConfig {
    pub id: Option<String>,
    pub gguf_local_path: Option<String>,
    pub gguf_repo: Option<String>,
    pub gguf_file: Option<String>,
    pub gguf_revision: Option<String>,
    pub tokenizer_local_path: Option<String>,
    pub max_context: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct DockerConfigOverlay {
    pub gpus: Option<String>,
    pub cached_model_host_path: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct ObservabilityConfigOverlay {
    pub dump_logs: Option<bool>,
    pub dump_log_path: Option<String>,
    pub telemetry_frame_log: Option<String>,
}

impl TomlConfigOverlay {
    pub(crate) fn load_optional(path: &Path) -> Result<Option<Self>, String> {
        if path.is_file() {
            Self::load_required(path).map(Some)
        } else {
            Ok(None)
        }
    }

    pub(crate) fn load_required(path: &Path) -> Result<Self, String> {
        let text =
            fs::read_to_string(path).map_err(|e| format!("read config {}: {e}", path.display()))?;
        Self::from_str(&text).map_err(|e| format!("parse config {}: {e}", path.display()))
    }

    pub(crate) fn from_str(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}
