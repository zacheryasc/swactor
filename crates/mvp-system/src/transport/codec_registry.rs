//! MVP runtime codec registration.
//!
//! Actor behavior lives in the owning domain modules. This module wires their
//! message codecs into the transport registry used by distributed runtimes.

pub fn register_mvp_actor_codecs(registry: &mut swactor_transport::CodecRegistry) {
    crate::node::actor::register_codecs(registry);
    crate::orchestration::actor::register_codecs(registry);
    datastream::register_datastream_publisher_codec(registry);
    crate::prompt::rpc::register_codecs(registry);
}
