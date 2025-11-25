pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;
pub fn convert_err<E: std::fmt::Debug>(e: E) -> Error {
    format!("{e:?}").into()
}
