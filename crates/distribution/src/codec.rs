//! Serde-JSON codec for all distribution protocol messages.

use swactor::transport::{Codec, CodecRegistry};
use swactor::Error;

use crate::messages::*;

/// JSON codec for distribution protocol messages.
///
/// Using JSON for simplicity and debuggability. Can be swapped for
/// bincode/msgpack in production via the Codec trait.
pub struct JsonCodec;

macro_rules! impl_json_codec {
    ($ty:ty) => {
        impl Codec<$ty> for JsonCodec {
            fn encode(&self, msg: &$ty) -> Result<Vec<u8>, Error> {
                serde_json::to_vec(msg).map_err(|e| Error::from(format!("encode: {e}")))
            }
            fn decode(&self, bytes: &[u8]) -> Result<$ty, Error> {
                serde_json::from_slice(bytes).map_err(|e| Error::from(format!("decode: {e}")))
            }
        }
    };
}

impl_json_codec!(Ping);
impl_json_codec!(Ack);
impl_json_codec!(PingReq);
impl_json_codec!(JoinRequest);
impl_json_codec!(JoinResponse);
impl_json_codec!(FindNodeRequest);
impl_json_codec!(FindNodeResponse);
impl_json_codec!(StoreRequest);
impl_json_codec!(FindValueRequest);
impl_json_codec!(FindValueResponse);

/// Build a `CodecRegistry` with all distribution protocol messages registered.
pub fn distribution_codec_registry() -> CodecRegistry {
    let mut cr = CodecRegistry::new();
    cr.register::<Ping, _>(JsonCodec);
    cr.register::<Ack, _>(JsonCodec);
    cr.register::<PingReq, _>(JsonCodec);
    cr.register::<JoinRequest, _>(JsonCodec);
    cr.register::<JoinResponse, _>(JsonCodec);
    cr.register::<FindNodeRequest, _>(JsonCodec);
    cr.register::<FindNodeResponse, _>(JsonCodec);
    cr.register::<StoreRequest, _>(JsonCodec);
    cr.register::<FindValueRequest, _>(JsonCodec);
    cr.register::<FindValueResponse, _>(JsonCodec);
    cr
}
