//! MVP edge transport public surface.

pub(crate) fn register_mvp_actor_codecs(registry: &mut swactor_transport::CodecRegistry) {
    crate::node_actor::register_codecs(registry);
    crate::orchestration::actor::register_codecs(registry);
    datastream::register_datastream_publisher_codec(registry);
    crate::prompt::rpc::register_codecs(registry);
}

pub(crate) mod endpoint_advertisement;
pub(crate) mod json_codec;
