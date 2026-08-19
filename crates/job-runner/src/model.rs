//! Job and cluster-config model — spec §3 (gpu-agnostic job) and §4 (fields).
//!
//! A [`Job`] describes *what to do* and never what hardware. Hardware lives in
//! a separate [`ClusterConfig`] bootstrapped before the job is submitted.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A single-node command job. Spec §4 fields, nothing else.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Job identity.
    pub name: String,
    /// The command that does the work.
    pub run: String,
    /// One-time command run before `run` (e.g. env install).
    #[serde(default)]
    pub setup: Option<String>,
    /// `{ workdir, exclude }` — pushed to the node first.
    #[serde(default)]
    pub workspace: Option<Workspace>,
    /// Paths to collect back after run, relative to the outputs root
    /// (the job's workdir on the node). Spec §4 outputs-root convention.
    #[serde(default)]
    pub outputs: Vec<String>,
    /// Environment variables injected into setup and run.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl Job {
    pub fn has_workspace(&self) -> bool {
        self.workspace.is_some()
    }
    pub fn has_setup(&self) -> bool {
        self.setup
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }
}

/// Workspace push target: the operator's workdir tree, minus `exclude` globs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub workdir: PathBuf,
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// Hardware (provider, GPU, image, disk, selection). Spec §3: lives in a
/// separate cluster config bootstrapped before any job is submitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub provider: ProviderConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// Provider kind. v1 realizes `"vastai"`.
    pub kind: String,
    /// Docker image launched on the node.
    pub image: String,
    #[serde(default = "default_disk_gb")]
    pub disk_gb: u32,
    #[serde(default)]
    pub gpu: GpuSpec,
    #[serde(default)]
    pub selection: SelectionSpec,
    /// Vast.ai API base URL.
    #[serde(default = "default_base_url")]
    pub base_url: String,
}

fn default_disk_gb() -> u32 {
    64
}
fn default_base_url() -> String {
    "https://cloud.vast.ai".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuSpec {
    #[serde(default = "default_gpu_name")]
    pub name: String,
    #[serde(default = "default_gpu_count")]
    pub count: u32,
    #[serde(default)]
    pub min_vram_gb: Option<u32>,
}

fn default_gpu_name() -> String {
    "RTX 3090".to_string()
}
fn default_gpu_count() -> u32 {
    1
}

impl Default for GpuSpec {
    fn default() -> Self {
        Self {
            name: default_gpu_name(),
            count: default_gpu_count(),
            min_vram_gb: None,
        }
    }
}

/// Offer selection knobs, mirrored from the swactor-vastai `SelectionPolicy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectionSpec {
    #[serde(default = "default_min_reliability")]
    pub min_reliability: f64,
    #[serde(default = "default_min_down_mbps")]
    pub min_down_mbps: f64,
    #[serde(default)]
    pub min_up_mbps: Option<f64>,
    #[serde(default = "default_require_verified")]
    pub require_verified: bool,
    #[serde(default)]
    pub max_price_per_hour: Option<f64>,
}

fn default_min_reliability() -> f64 {
    0.97
}
fn default_min_down_mbps() -> f64 {
    100.0
}
fn default_require_verified() -> bool {
    true
}

impl Default for SelectionSpec {
    fn default() -> Self {
        Self {
            min_reliability: default_min_reliability(),
            min_down_mbps: default_min_down_mbps(),
            min_up_mbps: None,
            require_verified: default_require_verified(),
            max_price_per_hour: None,
        }
    }
}
