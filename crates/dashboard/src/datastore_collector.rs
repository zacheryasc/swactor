//! Datastore stats provider for the runtime dashboard.
//!
//! The trait returns a pre-serialized JSON string so that `runtime-dashboard`
//! has no compile-time dependency on `swactor-datastore` (which would create a
//! circular dependency since `swactor-datastore[node]` depends on us).
//!
//! The `swactor-datastore` crate implements this trait in its `node` feature.

use std::sync::Arc;

/// Scope filter for listing objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListScope {
    Local,
    Swarm,
}

/// Trait for providing datastore stats and CRUD operations to the dashboard.
///
/// Implementations capture a point-in-time snapshot as serialized JSON.
/// The dashboard polls this every ~200ms via SSE.
///
/// All command methods have default implementations returning `Err` so that
/// existing `DatastoreMetrics` impls continue to compile without changes.
pub trait DatastoreStatsProvider: Send + Sync {
    /// Return a JSON-serialized datastore snapshot, or `None` if unavailable.
    fn snapshot_json(&self) -> Option<String>;

    /// List objects as a JSON string. `scope` selects local-only or swarm-wide.
    fn list_objects(&self, _name_filter: Option<&str>, _scope: ListScope) -> Result<String, String> {
        Err("not supported".into())
    }

    /// Get a single object's metadata + manifest as JSON.
    fn get_object(&self, _hash: &str) -> Result<String, String> {
        Err("not supported".into())
    }

    /// Get the raw binary data for an object.
    fn get_data(&self, _hash: &str) -> Result<Vec<u8>, String> {
        Err("not supported".into())
    }

    /// Store data, optionally with a name. Returns JSON with `content_hash`.
    fn put_data(&self, _data: Vec<u8>, _name: Option<String>) -> Result<String, String> {
        Err("not supported".into())
    }

    /// Delete an object by hash. Returns JSON confirmation.
    fn delete_object(&self, _hash: &str) -> Result<String, String> {
        Err("not supported".into())
    }

    /// Get node status as JSON.
    fn node_status(&self) -> Result<String, String> {
        Err("not supported".into())
    }

    /// Whether the datastore is currently running.
    fn is_running(&self) -> bool {
        false
    }

    /// Shut down the datastore actors.
    fn shutdown_datastore(&self) -> Result<(), String> {
        Err("not supported".into())
    }
}

/// Factory for creating a new datastore instance from the dashboard.
pub trait DatastoreFactory: Send + Sync {
    /// Start a datastore with optional persistent storage path.
    /// Returns a provider that can be installed into the dashboard.
    fn start_datastore(&self, storage_path: Option<String>) -> Result<Arc<dyn DatastoreStatsProvider>, String>;
}
