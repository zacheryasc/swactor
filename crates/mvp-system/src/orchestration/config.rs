use std::fs;
use std::path::Path;

use serde::Deserialize;

pub const DEFAULT_CONFIG_PATH: &str = ".config/config.toml";

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct VastAiConfig {
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
pub struct ResolvedVastAiConfig {
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
    pub fn validate(self) -> Result<Self, String> {
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

pub fn looks_remote_image(image: &str) -> bool {
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
/// fields outside the fixed `mvp-chat` public config surface; `mvp-chat` uses a
/// bin-local strict config loader instead.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct TomlConfigOverlay {
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
pub struct RuntimeConfigOverlay {
    pub profile: Option<String>,
    pub run_id: Option<u64>,
    pub node_id: Option<u64>,
    pub stage_index: Option<u32>,
    pub layer_end_exclusive: Option<u32>,
    pub pipeline_stages: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ProviderConfigOverlay {
    pub kind: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ImageConfig {
    pub node: Option<String>,
    pub tag: Option<String>,
    pub build: Option<bool>,
    pub push: Option<bool>,
    pub force_refresh: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RelayConfig {
    pub mode: Option<String>,
    pub url: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PromptConfig {
    pub rpc_addr: Option<String>,
    pub max_tokens: Option<u32>,
    pub dashboard: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ModelConfig {
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
pub struct DockerConfigOverlay {
    pub gpus: Option<String>,
    pub cached_model_host_path: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ObservabilityConfigOverlay {
    pub dump_logs: Option<bool>,
    pub dump_log_path: Option<String>,
    pub datastream_frame_log: Option<String>,
}

impl TomlConfigOverlay {
    pub fn load_optional(path: &Path) -> Result<Option<Self>, String> {
        if path.is_file() {
            Self::load_required(path).map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn load_required(path: &Path) -> Result<Self, String> {
        let text =
            fs::read_to_string(path).map_err(|e| format!("read config {}: {e}", path.display()))?;
        Self::from_str(&text).map_err(|e| format!("parse config {}: {e}", path.display()))
    }

    pub fn from_str(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{ResolvedVastAiConfig, looks_remote_image};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn valid_resolved_vastai_config() -> ResolvedVastAiConfig {
        ResolvedVastAiConfig {
            api_key: "vast-key".to_owned(),
            relay_url: "https://relay.example.com".to_owned(),
            image: "ghcr.io/swactor/mvp-node:latest".to_owned(),
            bootstrap_command: "/usr/local/bin/mvp-node".to_owned(),
            disk_gb: Some(80),
            gpu_name: Some("RTX 4090".to_owned()),
            min_gpu_ram_mb: Some(16_000),
            min_down_mbps: Some(100.0),
            min_up_mbps: Some(25.0),
            max_dph_total: Some(0.10),
            min_reliability: Some(0.98),
            require_verified: Some(true),
            blacklist_hosts: vec![155385],
            onstart: None,
            ssh_identity: Some("~/.ssh/swactor_vastai_ed25519".to_owned()),
        }
    }

    #[test]
    fn config_toml_parses_explicit_chat_and_vastai_fields() {
        let config = TomlConfigOverlay::from_str(
            r#"

[runtime]
profile = "deploy"
run_id = 42
node_id = 7
stage_index = 2
layer_end_exclusive = 24
pipeline_stages = 3

[provider]
kind = "vastai"

[image]
node = "ghcr.io/swactor/mvp-node:latest"
tag = "trial"
build = false
push = true
force_refresh = true

[relay]
mode = "disabled"
url = "https://relay.example.com"

[prompt]
rpc_addr = "127.0.0.1:19000"
max_tokens = 128
dashboard = false


[model]
id = "smollm2-135m-instruct-q4"
gguf_local_path = "/models/local.gguf"
gguf_repo = "QuantFactory/SmolLM2-135M-Instruct-GGUF"
gguf_file = "SmolLM2-135M-Instruct.Q4_K_M.gguf"
gguf_revision = "main"
tokenizer_local_path = "/models/tokenizer.json"
max_context = 512

[docker]
gpus = "all"
cached_model_host_path = "/cache/model.gguf"

[observability]
datastream_frame_log = "/tmp/frames.jsonl"

[vastai]
api_key = "vast-key"
image = "registry.example.com/team/mvp-node:latest"
bootstrap_command = "/opt/mvp/node --join"
disk_gb = 80
ssh_user = "ubuntu"
confirm_lease = true
gpu_name = "RTX 4090"
min_gpu_ram_mb = 24000
min_down_mbps = 250.5
min_up_mbps = 50.25
max_dph_total = 0.10
min_reliability = 0.99
require_verified = true
blacklist_hosts = [155385, 59017]
onstart = "echo preparing"
ssh_identity = "~/.ssh/swactor_vastai_ed25519"
poll_interval_secs = 30
"#,
        )
        .expect("explicit config TOML parses");

        assert_eq!(config.runtime.profile.as_deref(), Some("deploy"));
        assert_eq!(config.runtime.run_id, Some(42));
        assert_eq!(config.runtime.node_id, Some(7));
        assert_eq!(config.runtime.stage_index, Some(2));
        assert_eq!(config.runtime.layer_end_exclusive, Some(24));
        assert_eq!(config.runtime.pipeline_stages, Some(3));
        assert_eq!(config.provider.kind.as_deref(), Some("vastai"));
        assert_eq!(
            config.image.node.as_deref(),
            Some("ghcr.io/swactor/mvp-node:latest")
        );
        assert_eq!(config.image.tag.as_deref(), Some("trial"));
        assert_eq!(config.image.build, Some(false));
        assert_eq!(config.image.push, Some(true));
        assert_eq!(config.image.force_refresh, Some(true));
        assert_eq!(config.relay.mode.as_deref(), Some("disabled"));
        assert_eq!(
            config.relay.url.as_deref(),
            Some("https://relay.example.com")
        );
        assert_eq!(config.prompt.rpc_addr.as_deref(), Some("127.0.0.1:19000"));
        assert_eq!(config.prompt.max_tokens, Some(128));
        assert_eq!(config.prompt.dashboard, Some(false));
        assert_eq!(config.model.id.as_deref(), Some("smollm2-135m-instruct-q4"));
        assert_eq!(
            config.model.gguf_local_path.as_deref(),
            Some("/models/local.gguf")
        );
        assert_eq!(
            config.model.gguf_repo.as_deref(),
            Some("QuantFactory/SmolLM2-135M-Instruct-GGUF")
        );
        assert_eq!(
            config.model.gguf_file.as_deref(),
            Some("SmolLM2-135M-Instruct.Q4_K_M.gguf")
        );
        assert_eq!(config.model.gguf_revision.as_deref(), Some("main"));
        assert_eq!(
            config.model.tokenizer_local_path.as_deref(),
            Some("/models/tokenizer.json")
        );
        assert_eq!(config.model.max_context, Some(512));
        assert_eq!(config.docker.gpus.as_deref(), Some("all"));
        assert_eq!(
            config.docker.cached_model_host_path.as_deref(),
            Some("/cache/model.gguf")
        );
        assert_eq!(
            config.observability.datastream_frame_log.as_deref(),
            Some("/tmp/frames.jsonl")
        );
        assert_eq!(config.vastai.api_key.as_deref(), Some("vast-key"));
        assert_eq!(
            config.vastai.image.as_deref(),
            Some("registry.example.com/team/mvp-node:latest")
        );
        assert_eq!(
            config.vastai.bootstrap_command.as_deref(),
            Some("/opt/mvp/node --join")
        );
        assert_eq!(config.vastai.disk_gb, Some(80));
        assert_eq!(config.vastai.ssh_user.as_deref(), Some("ubuntu"));
        assert_eq!(config.vastai.confirm_lease, Some(true));
        assert_eq!(config.vastai.gpu_name.as_deref(), Some("RTX 4090"));
        assert_eq!(config.vastai.min_gpu_ram_mb, Some(24_000));
        assert_eq!(config.vastai.min_down_mbps, Some(250.5));
        assert_eq!(config.vastai.min_up_mbps, Some(50.25));
        assert_eq!(config.vastai.max_dph_total, Some(0.10));
        assert_eq!(config.vastai.min_reliability, Some(0.99));
        assert_eq!(config.vastai.require_verified, Some(true));
        assert_eq!(config.vastai.blacklist_hosts, vec![155385, 59017]);
        assert_eq!(config.vastai.poll_interval_secs, Some(30));
        assert_eq!(config.vastai.onstart.as_deref(), Some("echo preparing"));
        assert_eq!(
            config.vastai.ssh_identity.as_deref(),
            Some("~/.ssh/swactor_vastai_ed25519")
        );
    }

    #[test]
    fn config_toml_rejects_non_numeric_pipeline_stages() {
        let error = TomlConfigOverlay::from_str(
            r#"
[runtime]
pipeline_stages = "many"
"#,
        )
        .expect_err("non-numeric pipeline_stages must not parse");

        assert!(
            error.to_string().contains("pipeline_stages"),
            "error should identify pipeline_stages: {error}"
        );
    }

    #[test]
    fn explicit_config_path_error_includes_the_requested_path() {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mvp-system-missing-config-{}-{counter}.toml",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);

        let error = TomlConfigOverlay::load_required(&path)
            .expect_err("missing explicit config path errors");

        assert!(
            error.contains(&path.display().to_string()),
            "error {error:?} must include requested path {}",
            path.display()
        );
    }

    #[test]
    fn looks_remote_image_accepts_registry_refs_and_rejects_local_refs() {
        for image in [
            "ghcr.io/swactor/mvp-node:latest",
            "registry.example.com:5000/team/mvp-node@sha256:abcdef",
            "localhost:5000/team/mvp-node:latest",
        ] {
            assert!(looks_remote_image(image), "{image:?} should be remote");
        }

        for image in [
            "swactor-mvp-node:latest",
            "team/mvp-node:latest",
            "mvp-node@sha256:abcdef",
        ] {
            assert!(!looks_remote_image(image), "{image:?} should be local");
        }
    }

    #[test]
    fn resolved_vastai_config_validate_rejects_missing_required_fields() {
        let cases = [
            (
                "VAST_API_KEY",
                ResolvedVastAiConfig {
                    api_key: "  ".to_owned(),
                    ..valid_resolved_vastai_config()
                },
            ),
            (
                "relay.url",
                ResolvedVastAiConfig {
                    relay_url: "\t".to_owned(),
                    ..valid_resolved_vastai_config()
                },
            ),
            (
                "vastai.image",
                ResolvedVastAiConfig {
                    image: String::new(),
                    ..valid_resolved_vastai_config()
                },
            ),
            (
                "vastai.bootstrap_command",
                ResolvedVastAiConfig {
                    bootstrap_command: "\n".to_owned(),
                    ..valid_resolved_vastai_config()
                },
            ),
            (
                "vastai.ssh_identity",
                ResolvedVastAiConfig {
                    ssh_identity: Some(" ".to_owned()),
                    ..valid_resolved_vastai_config()
                },
            ),
        ];

        for (label, config) in cases {
            let error = config
                .validate()
                .expect_err("missing field must be rejected");
            assert!(
                error.contains(label),
                "error {error:?} must identify missing {label}"
            );
        }
    }
}
