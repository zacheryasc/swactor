use crate::run_plan;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelSpec {
    pub model_id: String,
    pub artifact: ModelArtifact,
    pub tokenizer: run_plan::TokenizerSource,
    pub num_layers: u32,
    pub hidden_dim: u64,
    pub dtype_family: run_plan::DTypeFamily,
    pub dtype_width_bytes: u64,
    pub max_seq_len: u64,
    pub eos_token_id: u32,
}

impl ModelSpec {
    pub fn pipelined_causal_llm(
        model_id: impl Into<String>,
        artifact: ModelArtifact,
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
            artifact,
            tokenizer,
            num_layers,
            hidden_dim,
            dtype_family,
            dtype_width_bytes,
            max_seq_len,
            eos_token_id,
        }
    }

    pub fn to_run_plan_facts(&self) -> run_plan::ModelFacts {
        run_plan::ModelFacts {
            model_id: self.model_id.clone(),
            gguf_source: self.artifact.to_run_plan_source(),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelArtifact {
    TestTinyLlm { path: String },
}

impl ModelArtifact {
    fn to_run_plan_source(&self) -> run_plan::GgufSource {
        match self {
            Self::TestTinyLlm { path } => run_plan::GgufSource::LocalPath(path.clone()),
        }
    }
}
