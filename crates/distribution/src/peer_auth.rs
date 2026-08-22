//! Runtime-mutable peer allow-list for network-level authentication.
//!
//! When enabled, only peers whose `NodeId` appears in the allow-list
//! can join the cluster or exchange messages with this node.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::types::NodeId;
use swactor_transport::{hex_decode, hex_encode};

/// A single trusted peer entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEntry {
    pub node_id: String,
    pub label: String,
}

/// File format for the peers.json allow-list.
#[derive(Debug, Serialize, Deserialize)]
struct PeersFile {
    version: u32,
    peers: Vec<PeerEntry>,
}

/// Runtime-mutable peer allow-list.
///
/// - `None` inner map = open mode (all peers accepted)
/// - `Some(map)` = only listed peers accepted
pub struct PeerAllowList {
    peers: Option<HashMap<NodeId, PeerEntry>>,
    path: Option<PathBuf>,
}

impl PeerAllowList {
    /// Create an open allow-list (no restrictions).
    pub fn open() -> Self {
        Self {
            peers: None,
            path: None,
        }
    }

    /// Load an allow-list from a JSON file.
    ///
    /// If the file doesn't exist, creates an empty allow-list (restrictive mode
    /// with zero peers). The file will be created on the first `save()`.
    pub fn from_file(path: &Path) -> io::Result<Self> {
        let peers = if path.exists() {
            let data = std::fs::read_to_string(path)?;
            let file: PeersFile = serde_json::from_str(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut map = HashMap::new();
            for entry in file.peers {
                if let Some(bytes) = hex_decode(&entry.node_id)
                    && let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice())
                {
                    map.insert(NodeId(arr), entry);
                }
            }
            map
        } else {
            HashMap::new()
        };

        Ok(Self {
            peers: Some(peers),
            path: Some(path.to_path_buf()),
        })
    }

    /// Check whether a peer is allowed.
    pub fn is_allowed(&self, node_id: &NodeId) -> bool {
        match &self.peers {
            None => true,
            Some(map) => map.contains_key(node_id),
        }
    }

    /// Whether the allow-list is in open mode.
    pub fn is_open(&self) -> bool {
        self.peers.is_none()
    }

    /// Add a peer to the allow-list.
    pub fn add_peer(&mut self, node_id: NodeId, label: String) {
        let map = self.peers.get_or_insert_with(HashMap::new);
        map.insert(
            node_id,
            PeerEntry {
                node_id: hex_encode(&node_id.0),
                label,
            },
        );
    }

    /// Remove a peer from the allow-list.
    pub fn remove_peer(&mut self, node_id: &NodeId) {
        if let Some(ref mut map) = self.peers {
            map.remove(node_id);
        }
    }

    /// List all trusted peers.
    pub fn list_peers(&self) -> Vec<&PeerEntry> {
        match &self.peers {
            None => Vec::new(),
            Some(map) => map.values().collect(),
        }
    }

    /// Persist the allow-list to disk.
    pub fn save(&self) -> io::Result<()> {
        let path = match &self.path {
            Some(p) => p,
            None => return Ok(()),
        };

        let entries: Vec<PeerEntry> = match &self.peers {
            Some(map) => map.values().cloned().collect(),
            None => Vec::new(),
        };

        let file = PeersFile {
            version: 1,
            peers: entries,
        };

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(&file).map_err(io::Error::other)?;
        std::fs::write(path, json)
    }
}
