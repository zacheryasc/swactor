//! Actor wrapper that exposes a local [`TelemetryEndpoint`] to remote
//! collectors.
//!
//! The actor owns the process-local subscription step. Transport-specific code is
//! injected by the runtime crate so the telemetry core does not depend on the
//! concrete QUIC writer implementation.

use std::sync::Arc;

use iroh::EndpointAddr;
use serde::{Deserialize, Serialize as DeriveSerialize};
use swactor::actor::ActorInterface;
use swactor::runtime::Ctx;
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

use crate::{SubscriptionRequest, TelemetryEndpoint, TelemetrySubscription};

/// Well-known actor registry name for node-side telemetry subscription requests.
pub const TELEMETRY_PUBLISHER_NAME: &str = "telemetry-publisher";

/// Request sent from a collector/orchestrator to a node-local telemetry publisher.
#[derive(Clone, Debug, DeriveSerialize, Deserialize)]
pub struct TelemetrySubscribe {
    pub collector: EndpointAddr,
    pub request: SubscriptionRequest,
    pub flow_id: [u8; 16],
    pub token: Vec<u8>,
}

/// Messages accepted by [`TelemetryPublisherActor`].
#[derive(Clone, Debug, DeriveSerialize, Deserialize)]
pub enum TelemetryPublisherMsg {
    Subscribe(TelemetrySubscribe),
}

impl NetworkMessage for TelemetryPublisherMsg {
    fn type_tag() -> &'static str {
        "swactor::TelemetryPublisherMsg"
    }
}

/// Node-side actor that turns remote subscribe messages into local endpoint
/// subscriptions, then hands the subscription to the caller's transport writer.
pub struct TelemetryPublisherActor {
    endpoint: Arc<TelemetryEndpoint>,
    on_subscribe: Box<dyn FnMut(TelemetrySubscribe, TelemetrySubscription) + Send>,
}

impl TelemetryPublisherActor {
    pub fn new(
        endpoint: Arc<TelemetryEndpoint>,
        on_subscribe: impl FnMut(TelemetrySubscribe, TelemetrySubscription) + Send + 'static,
    ) -> Self {
        Self {
            endpoint,
            on_subscribe: Box::new(on_subscribe),
        }
    }
}

impl ActorInterface for TelemetryPublisherActor {
    type Incoming = TelemetryPublisherMsg;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            TelemetryPublisherMsg::Subscribe(subscribe) => {
                let subscription = self
                    .endpoint
                    .subscribe("remote-collector", subscribe.request.clone());
                (self.on_subscribe)(subscribe, subscription);
            }
        }
    }
}

/// Register JSON encoding for remote telemetry publisher messages.
pub fn register_telemetry_publisher_codec(registry: &mut CodecRegistry) {
    registry.register::<TelemetryPublisherMsg, _>(JsonCodec::<TelemetryPublisherMsg>::default());
}
