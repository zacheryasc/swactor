//! Generic JSON codec for serde message types.
//!
//! [`JsonCodec<M>`] is the out-of-the-box [`Codec`] for messages that are
//! [`Serialize`] + [`DeserializeOwned`]. Use a custom codec when a wire format
//! needs schema stability or compactness beyond JSON.

use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;
use swactor::Error;

use crate::codec::Codec;

/// JSON codec for message type `M`.
///
/// Implements [`Codec<M>`] via `serde_json`. Construct with
/// [`JsonCodec::default`] / `JsonCodec::<M>::default()`.
pub struct JsonCodec<M>(PhantomData<M>);

impl<M> Default for JsonCodec<M> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<M> Codec<M> for JsonCodec<M>
where
    M: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn encode(&self, msg: &M) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(msg).map_err(|e| Error::from(format!("encode: {e}")))
    }

    fn decode(&self, bytes: &[u8]) -> Result<M, Error> {
        serde_json::from_slice(bytes).map_err(|e| Error::from(format!("decode: {e}")))
    }
}
