//! swactor actor shells for the MVP system runtime.
//!
//! Each actor module owns its message type. Pure state machines remain in the
//! existing domain modules; actors translate mailbox messages into those cores and
//! report emitted commands/events back through actor messages.

pub mod codec;
pub mod node_agent;
pub mod orchestrator;
pub mod stage_controller;

pub fn register_mvp_actor_codecs(registry: &mut swactor_transport::CodecRegistry) {
    node_agent::register_codecs(registry);
    orchestrator::register_codecs(registry);
    datastream::register_datastream_publisher_codec(registry);
    crate::prompt_rpc::register_codecs(registry);
}
