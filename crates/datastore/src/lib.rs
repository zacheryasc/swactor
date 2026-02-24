pub mod content_hash;
pub mod crypto;
pub mod types;
pub mod messages;
pub mod chunking;
pub mod storage;
pub mod actors;
pub mod auth;
pub mod blob_transfer;
pub mod bridge;
pub mod cli;
pub mod metrics;
pub mod streams;
#[cfg(feature = "node")]
pub mod api;
#[cfg(feature = "node")]
pub mod ui_html;
#[cfg(kani)]
mod kani_auth;

pub use types::{ChunkRef, ContentHash, DatastoreConfig, ObjectEntry, ObjectManifest};
pub use messages::{BlobStoreMsg, DatastoreNodeMsg, DatastoreResponse, MetadataMsg, TransferMsg};
pub use chunking::{chunk_blob, reassemble_blob, verify_integrity, ChunkingError};
pub use storage::{StorageBackend, FilesystemBackend, InMemoryBackend};
pub use actors::{BlobStoreActor, DatastoreNode, MetadataActor, TransferActor};
pub use bridge::{DatastoreAuthConfig, DatastoreGroup, DatastoreGroupConfig};
