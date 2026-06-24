use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use swactor::Error;
use swactor_transport::Codec;

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
