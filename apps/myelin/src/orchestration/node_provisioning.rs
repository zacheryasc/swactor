//! Myelin-facing compatibility surface for reusable provider-neutral node provisioning.
//!
//! The reusable crate keeps `ProviderKind` opaque. Myelin provider selection policy
//! lives here so the reusable contracts do not know about process, Docker, or
//! VastAI runtime choices.

pub use ::provisioning::ProviderKind;

pub(crate) mod provider_kind {
    use super::ProviderKind;

    pub(crate) fn process() -> ProviderKind {
        ProviderKind::new("process")
    }

    pub(crate) fn docker() -> ProviderKind {
        ProviderKind::new("docker")
    }

    pub(crate) fn vastai() -> ProviderKind {
        ProviderKind::new("vastai")
    }

    pub(crate) fn static_ssh() -> ProviderKind {
        ProviderKind::new("static-ssh")
    }

    pub(crate) fn parse_deploy(value: &str) -> Result<ProviderKind, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "process" | "local_process" | "local-process" => Ok(process()),
            "docker" | "local_docker" | "local-docker" => Ok(docker()),
            "vastai" | "vast_ai" | "vast-ai" => Ok(vastai()),
            "static-ssh" | "static_ssh" | "staticssh" => Ok(static_ssh()),
            other => Err(format!(
                "unsupported provider {other:?}; use process, docker, vastai, or static-ssh"
            )),
        }
    }
}
