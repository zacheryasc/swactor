//! Registers wire codecs for every Myelin actor message type.
//!
//! The runtime's [`CodecRegistry`](swactor_transport::CodecRegistry) needs an
//! encoder/decoder entry for each inter-node message type. This is the single
//! aggregator that wires up the node, orchestrator, and prompt codecs.

pub(crate) fn register_myelin_actor_codecs(registry: &mut swactor_transport::CodecRegistry) {
    crate::node_actor::register_codecs(registry);
    crate::orchestration::actor::register_codecs(registry);
    crate::orchestration::manual_control::register_codecs(registry);
    swactor_job_runner::register_job_codecs(registry);
}
