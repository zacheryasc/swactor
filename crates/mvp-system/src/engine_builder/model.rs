use crate::run_plan;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelSpec {
    pub model_id: String,
    pub architecture: ModelArchitecture,
    pub artifact: ModelArtifact,
    pub num_layers: u32,
    pub hidden_dim: u64,
    pub dtype_family: DTypeFamily,
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
        dtype_family: DTypeFamily,
        dtype_width_bytes: u64,
        max_seq_len: u64,
        eos_token_id: u32,
    ) -> Self {
        Self {
            model_id: model_id.into(),
            architecture: ModelArchitecture::PipelinedCausalLlm,
            artifact,
            num_layers,
            hidden_dim,
            dtype_family,
            dtype_width_bytes,
            max_seq_len,
            eos_token_id,
        }
    }

    pub fn mvp_tiny_open_llm_fixture() -> Self {
        Self::pipelined_causal_llm(
            "mvp-tiny-open-llm-fixture",
            ModelArtifact::ContainerPath {
                path: "/models/mvp-tiny-open-llm.gguf".to_owned(),
            },
            4,
            8,
            DTypeFamily::BFloat,
            2,
            8,
            99,
        )
    }

    pub fn to_run_plan_facts(&self) -> run_plan::ModelFacts {
        run_plan::ModelFacts {
            model_id: self.model_id.clone(),
            num_layers: self.num_layers,
            hidden_dim: self.hidden_dim,
            dtype_family: self.dtype_family.into(),
            dtype_width_bytes: self.dtype_width_bytes,
            max_seq_len: self.max_seq_len,
            eos_token_id: self.eos_token_id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelArchitecture {
    PipelinedCausalLlm,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelArtifact {
    ContainerPath {
        path: String,
    },
    HuggingFaceGguf {
        repo: String,
        file: String,
        revision: Option<String>,
    },
    TestTinyLlm {
        path: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DTypeFamily {
    BFloat,
}

impl From<DTypeFamily> for run_plan::DTypeFamily {
    fn from(value: DTypeFamily) -> Self {
        match value {
            DTypeFamily::BFloat => Self::BFloat,
        }
    }
}
