#[derive(Debug)]
pub struct Error(Box<dyn std::error::Error + Send + Sync + 'static>);
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn convert_err<E: std::fmt::Debug>(e: E) -> Error {
    Error(format!("{e:?}").into())
}

impl<T: AsRef<str>> From<T> for Error {
    fn from(value: T) -> Self {
        convert_err(value.as_ref())
    }
}

impl ToString for Error {
    fn to_string(&self) -> String {
        format!("{:?}", self.0)
    }
}
