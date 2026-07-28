use serde::Deserialize;

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
