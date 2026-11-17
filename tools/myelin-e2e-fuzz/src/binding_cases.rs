//! Binding-only behavior that has no counterpart in the Rust data-plane API.

use serde::{Deserialize, Serialize};

use crate::corpus::{SEEDED_MODEL_BYTES, SEEDED_MODEL_PATH};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PythonException {
    ValueError,
    BufferError,
}

impl PythonException {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValueError => "ValueError",
            Self::BufferError => "BufferError",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PythonBindingOp {
    InvalidOpenAccess { path: String, flags: i32 },
    MappingExportClose { path: String, length: u64 },
    SyntaxFailure,
    RuntimeFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum PythonBindingExpectation {
    Exception(PythonException),
    BootstrapFailure,
    RuntimeFailure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonBindingCase {
    pub id: String,
    pub operation: PythonBindingOp,
    pub expected: PythonBindingExpectation,
}

/// Keep Python argument validation, exported-buffer ownership and interpreter
/// failures separate from protocol cases and their language-neutral oracle.
pub fn python_binding_corpus(seed: u64) -> Vec<PythonBindingCase> {
    vec![
        PythonBindingCase {
            id: format!("python-invalid-open-access-{seed}"),
            operation: PythonBindingOp::InvalidOpenAccess {
                path: SEEDED_MODEL_PATH.to_owned(),
                flags: libc::O_ACCMODE,
            },
            expected: PythonBindingExpectation::Exception(PythonException::ValueError),
        },
        PythonBindingCase {
            id: format!("python-mapping-export-close-{seed}"),
            operation: PythonBindingOp::MappingExportClose {
                path: SEEDED_MODEL_PATH.to_owned(),
                length: SEEDED_MODEL_BYTES.len() as u64,
            },
            expected: PythonBindingExpectation::Exception(PythonException::BufferError),
        },
        PythonBindingCase {
            id: format!("python-syntax-failure-{seed}"),
            operation: PythonBindingOp::SyntaxFailure,
            expected: PythonBindingExpectation::BootstrapFailure,
        },
        PythonBindingCase {
            id: format!("python-runtime-failure-{seed}"),
            operation: PythonBindingOp::RuntimeFailure,
            expected: PythonBindingExpectation::RuntimeFailure,
        },
    ]
}
