pub(crate) const MO01_HEADER_BYTES: u64 = 40;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum GgufSource {
    LocalPath(String),
    HuggingFaceGguf {
        repo: String,
        file: String,
        revision: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum TokenizerSource {
    EmbeddedGguf,
    LocalPath(String),
}
