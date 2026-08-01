use crate::run_plan::{self, NodeId};

use super::error::EngineBuildError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeFacts {
    pub node_id: NodeId,
    pub capabilities: Vec<NodeCapability>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeCapability {
    Coordinator,
    Worker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StaticPoolProvider {
    nodes: Vec<NodeFacts>,
}

impl StaticPoolProvider {
    pub(crate) fn new(nodes: Vec<NodeFacts>) -> Self {
        Self { nodes }
    }
    pub(crate) fn acquire_pool(
        &self,
        min_nodes: usize,
    ) -> Result<Vec<NodeFacts>, EngineBuildError> {
        if self.nodes.len() < min_nodes {
            return Err(EngineBuildError::InsufficientNodes {
                requested: min_nodes,
                available: self.nodes.len(),
            });
        }
        Ok(self.nodes.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModelSpec {
    pub model_id: String,
    pub gguf_source: run_plan::GgufSource,
    pub tokenizer: run_plan::TokenizerSource,
    pub num_layers: u32,
    pub hidden_dim: u64,
    pub dtype_family: run_plan::DTypeFamily,
    pub dtype_width_bytes: u64,
    pub max_seq_len: u64,
    pub eos_token_id: u32,
}

impl ModelSpec {
    pub(crate) fn pipelined_causal_llm(
        model_id: impl Into<String>,
        gguf_source: run_plan::GgufSource,
        num_layers: u32,
        hidden_dim: u64,
        dtype_family: run_plan::DTypeFamily,
        dtype_width_bytes: u64,
        max_seq_len: u64,
        eos_token_id: u32,
        tokenizer: run_plan::TokenizerSource,
    ) -> Self {
        Self {
            model_id: model_id.into(),
            gguf_source,
            tokenizer,
            num_layers,
            hidden_dim,
            dtype_family,
            dtype_width_bytes,
            max_seq_len,
            eos_token_id,
        }
    }

    pub(crate) fn to_run_plan_facts(&self) -> run_plan::ModelFacts {
        run_plan::ModelFacts {
            model_id: self.model_id.clone(),
            gguf_source: self.gguf_source.clone(),
            num_layers: self.num_layers,
            hidden_dim: self.hidden_dim,
            dtype_family: self.dtype_family,
            dtype_width_bytes: self.dtype_width_bytes,
            max_seq_len: self.max_seq_len,
            eos_token_id: self.eos_token_id,
            tokenizer: self.tokenizer.clone(),
        }
    }
}
