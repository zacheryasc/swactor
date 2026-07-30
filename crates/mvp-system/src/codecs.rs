//! Registers wire codecs for every MVP actor message type.
//!
//! The runtime's [`CodecRegistry`](swactor_transport::CodecRegistry) needs an
//! encoder/decoder entry for each inter-node message type. This is the single
//! aggregator that wires up the node, orchestrator, prompt, and datastream
//! publisher codecs.

pub(crate) fn register_mvp_actor_codecs(registry: &mut swactor_transport::CodecRegistry) {
    crate::node_actor::register_codecs(registry);
    crate::orchestration::actor::register_codecs(registry);
    datastream::register_datastream_publisher_codec(registry);
    crate::prompt::rpc::register_codecs(registry);
}
