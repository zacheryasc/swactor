use std::collections::HashMap;

use iroh::endpoint::Connection;
use iroh::{Endpoint, PublicKey};

use crate::streams::types::StreamError;
use crate::streams::wire::ALPN;

/// Cache of QUIC connections used for stream data transfer.
///
/// Separate from the SWIM connection pool in IrohDriver. All connections
/// are established using the stream ALPN (`swactor/stream/1`).
pub struct StreamConnectionCache {
    connections: HashMap<[u8; 32], Connection>,
}

impl Default for StreamConnectionCache {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamConnectionCache {
    pub fn new() -> Self {
        StreamConnectionCache {
            connections: HashMap::new(),
        }
    }

    /// Get an existing healthy connection or establish a new one.
    pub async fn get_or_connect(
        &mut self,
        endpoint: &Endpoint,
        node_id: [u8; 32],
    ) -> Result<Connection, StreamError> {
        // Check for cached connection that's still open
        if let Some(conn) = self.connections.get(&node_id) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
            // Connection closed, remove it
            self.connections.remove(&node_id);
        }

        let key = PublicKey::from_bytes(&node_id)
            .map_err(|e| StreamError::BrokenPipe(format!("invalid public key: {e}")))?;

        let conn = endpoint
            .connect(key, ALPN)
            .await
            .map_err(|e| StreamError::BrokenPipe(format!("connect failed: {e}")))?;

        self.connections.insert(node_id, conn.clone());
        Ok(conn)
    }

    /// Remove dead connections from the cache.
    pub fn prune_closed(&mut self) {
        self.connections.retain(|_, conn| conn.close_reason().is_none());
    }

    /// Remove a specific connection.
    pub fn remove(&mut self, node_id: &[u8; 32]) {
        self.connections.remove(node_id);
    }

    /// Insert a connection into the cache.
    pub fn insert(&mut self, node_id: [u8; 32], conn: Connection) {
        self.connections.insert(node_id, conn);
    }
}
