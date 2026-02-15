//! Datastore stats provider for the runtime dashboard.
//!
//! The trait returns a pre-serialized JSON string so that `runtime-dashboard`
//! has no compile-time dependency on `swactor-datastore` (which would create a
//! circular dependency since `swactor-datastore[node]` depends on us).
//!
//! The `swactor-datastore` crate implements this trait in its `node` feature.

/// Trait for providing datastore stats to the dashboard.
///
/// Implementations capture a point-in-time snapshot as serialized JSON.
/// The dashboard polls this every ~200ms via SSE.
pub trait DatastoreStatsProvider: Send + Sync {
    /// Return a JSON-serialized datastore snapshot, or `None` if unavailable.
    fn snapshot_json(&self) -> Option<String>;
}
