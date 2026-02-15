//! CLI command type definitions for `swactor-store`.
//!
//! Types only — no implementation. These define the CLI interface that will
//! be wired to the actor system in a future milestone.

use std::collections::BTreeMap;
use std::path::PathBuf;

use distribution::types::NodeId;

/// Top-level CLI commands for `swactor-store`.
#[derive(Debug, Clone)]
pub enum CliCommand {
    /// Store a local file as a distributed object.
    ///
    /// ```text
    /// swactor-store put <local-path> [--name <label>] [--tag key=value...]
    /// ```
    Put {
        /// Path to the local file to store.
        local_path: PathBuf,
        /// Optional human-readable name for the object.
        name: Option<String>,
        /// Key-value tags to attach to the object.
        tags: BTreeMap<String, String>,
    },

    /// Retrieve an object from the datastore by content hash.
    ///
    /// ```text
    /// swactor-store get <content-hash>[@<node>] [--output <local-path>]
    /// ```
    Get {
        /// Content hash (hex) of the object to retrieve.
        content_hash: String,
        /// Specific node to fetch from (optional).
        node: Option<NodeId>,
        /// Local path to write the object to.
        output: Option<PathBuf>,
    },

    /// Delete an object from the datastore by content hash.
    ///
    /// ```text
    /// swactor-store delete <content-hash>
    /// ```
    Delete {
        /// Content hash (hex) of the object to delete.
        content_hash: String,
    },

    /// List objects in the datastore.
    ///
    /// ```text
    /// swactor-store list [--name <substring>] [--node <node-name>] [--all]
    /// ```
    List {
        /// Filter by name substring.
        name: Option<String>,
        /// List objects from a specific node only.
        node: Option<NodeId>,
        /// If true, query all nodes (swarm-wide). Otherwise, local only.
        all: bool,
    },

    /// Show node status: identity, chunk count, storage usage.
    ///
    /// ```text
    /// swactor-store status
    /// ```
    Status,
}
