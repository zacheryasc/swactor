use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use iroh::{PublicKey, RelayUrl};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub relay_url: RelayUrl,
    pub operator_public_key: PublicKey,
}

impl Bootstrap {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| {
            format!("read bootstrap config {}; bootstrap.json must be beside the executable", path.display())
        })?;
        let config: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse bootstrap config {}", path.display()))?;
        Ok(config)
    }

    pub fn default_path() -> Result<PathBuf> {
        let executable = std::env::current_exe().context("locate executable for bootstrap config")?;
        Ok(executable.parent().context("executable has no parent directory")?.join("bootstrap.json"))
    }
}
