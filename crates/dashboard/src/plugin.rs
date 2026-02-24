//! Dashboard plugin system.
//!
//! Plugins provide subsystem-specific metrics, API endpoints, and UI pages
//! to the dashboard without the dashboard knowing about the subsystem.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Response from a plugin's request handler.
pub enum PluginResponse {
    Json(String),
    Binary { content_type: String, data: Vec<u8> },
    Error { status: u16, message: String },
    NotFound,
}

impl PluginResponse {
    pub fn not_found() -> Self {
        Self::NotFound
    }

    pub fn json(s: String) -> Self {
        Self::Json(s)
    }

    pub fn error(status: u16, msg: impl Into<String>) -> Self {
        Self::Error {
            status,
            message: msg.into(),
        }
    }
}

/// A composable dashboard plugin.
///
/// Plugins provide subsystem-specific metrics, API endpoints, and UI pages
/// to the dashboard without the dashboard knowing about the subsystem.
pub trait DashboardPlugin: Send + Sync {
    /// Unique name — used as SSE event type and API route prefix `/api/plugin/{name}/...`
    fn name(&self) -> &str;

    /// JSON snapshot polled every ~200ms via SSE. Return None if no data available.
    fn snapshot_json(&self) -> Option<String>;

    /// Handle an API request to `/api/plugin/{name}/{path}`.
    fn handle_request(
        &self,
        _method: &str,
        _path: &str,
        _query: &HashMap<String, String>,
        _body: &[u8],
    ) -> PluginResponse {
        PluginResponse::not_found()
    }

    /// Optional HTML page content. Dashboard will serve at `/plugin/{name}`.
    fn html_page(&self) -> Option<&str> {
        None
    }
}

/// Thread-safe registry of plugins.
pub struct PluginRegistry {
    plugins: Mutex<Vec<Arc<dyn DashboardPlugin>>>,
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: Mutex::new(Vec::new()),
        }
    }

    pub fn register(&self, plugin: Arc<dyn DashboardPlugin>) {
        self.plugins.lock().unwrap().push(plugin);
    }

    pub fn snapshot(&self) -> Vec<Arc<dyn DashboardPlugin>> {
        self.plugins.lock().unwrap().clone()
    }
}
