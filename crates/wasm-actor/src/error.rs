use std::fmt;

/// Errors that can occur when building or running a WasmActor.
#[derive(Debug)]
pub enum WasmActorError {
    /// A required export is missing from the Wasm module.
    MissingExport(&'static str),
    /// The Wasm module failed to compile or instantiate.
    Wasmtime(wasmtime::Error),
}

impl fmt::Display for WasmActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingExport(name) => write!(f, "missing required export: `{name}`"),
            Self::Wasmtime(e) => write!(f, "wasmtime error: {e}"),
        }
    }
}

impl std::error::Error for WasmActorError {}

impl From<wasmtime::Error> for WasmActorError {
    fn from(e: wasmtime::Error) -> Self {
        Self::Wasmtime(e)
    }
}
