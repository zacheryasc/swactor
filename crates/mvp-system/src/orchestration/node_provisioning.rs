//! MVP-facing compatibility surface for reusable provider-neutral node provisioning.
//!
//! The reusable crate keeps `ProviderKind` opaque. MVP provider selection policy
//! lives here so the reusable contracts do not know about process, Docker, or
//! VastAI runtime choices.

pub use ::provisioning::ProviderKind;

pub mod provider_kind {
    use super::ProviderKind;

    pub fn process() -> ProviderKind {
        ProviderKind::new("process")
    }

    pub fn docker() -> ProviderKind {
        ProviderKind::new("docker")
    }

    pub fn vastai() -> ProviderKind {
        ProviderKind::new("vastai")
    }

    pub fn parse_deploy(value: &str) -> Result<ProviderKind, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "process" | "local_process" | "local-process" => Ok(process()),
            "docker" | "local_docker" | "local-docker" => Ok(docker()),
            "vastai" | "vast_ai" | "vast-ai" => Ok(vastai()),
            other => Err(format!(
                "unsupported provider {other:?}; use process, docker, or vastai"
            )),
        }
    }
}
