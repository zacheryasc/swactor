//! Actor wrapper that exposes a local [`DatastreamEndpoint`] to remote
//! collectors.
//!
//! The actor owns the process-local subscription step. Transport-specific code is
//! injected by the runtime crate so the datastream core does not depend on the
//! concrete QUIC writer implementation.

use std::marker::PhantomData;
use std::sync::Arc;

use iroh::EndpointAddr;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize as DeriveSerialize};
use swactor::Error;
use swactor::actor::ActorInterface;
use swactor::runtime::Ctx;
use swactor_transport::{Codec, CodecRegistry, NetworkMessage};

use crate::{DatastreamEndpoint, DatastreamSubscription, SubscriptionRequest};

/// Well-known actor registry name for node-side datastream subscription requests.
pub const DATASTREAM_PUBLISHER_NAME: &str = "datastream-publisher";

/// Request sent from a collector/orchestrator to a node-local datastream publisher.
#[derive(Clone, Debug, DeriveSerialize, Deserialize)]
pub struct DatastreamSubscribe {
    pub collector: EndpointAddr,
    pub request: SubscriptionRequest,
    pub flow_id: [u8; 16],
    pub token: Vec<u8>,
}

/// Messages accepted by [`DatastreamPublisherActor`].
#[derive(Clone, Debug, DeriveSerialize, Deserialize)]
pub enum DatastreamPublisherMsg {
    Subscribe(DatastreamSubscribe),
}

impl NetworkMessage for DatastreamPublisherMsg {
    fn type_tag() -> &'static str {
        "swactor::DatastreamPublisherMsg"
    }
}

/// Node-side actor that turns remote subscribe messages into local endpoint
/// subscriptions, then hands the subscription to the caller's transport writer.
pub struct DatastreamPublisherActor {
    endpoint: Arc<DatastreamEndpoint>,
    on_subscribe: Box<dyn FnMut(DatastreamSubscribe, DatastreamSubscription) + Send>,
}

impl DatastreamPublisherActor {
    pub fn new(
        endpoint: Arc<DatastreamEndpoint>,
        on_subscribe: impl FnMut(DatastreamSubscribe, DatastreamSubscription) + Send + 'static,
    ) -> Self {
        Self {
            endpoint,
            on_subscribe: Box::new(on_subscribe),
        }
    }
}

impl ActorInterface for DatastreamPublisherActor {
    type Incoming = DatastreamPublisherMsg;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            DatastreamPublisherMsg::Subscribe(subscribe) => {
                let subscription = self
                    .endpoint
                    .subscribe("remote-collector", subscribe.request.clone());
                (self.on_subscribe)(subscribe, subscription);
            }
        }
    }
}

/// Register JSON encoding for remote datastream publisher messages.
pub fn register_datastream_publisher_codec(registry: &mut CodecRegistry) {
    registry.register::<DatastreamPublisherMsg, JsonCodec<DatastreamPublisherMsg>>(JsonCodec(
        PhantomData,
    ));
}

struct JsonCodec<M>(PhantomData<M>);

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
