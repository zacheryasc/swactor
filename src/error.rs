/// Simple, ergonomic, local `Error` type.
/// # Usage
/// ```
/// use swactor::Error;
/// 
/// fn foo_if_even(num: u64) -> Result<String, Error> {
///     if num % 2 == 0 {
///         return Ok("foo".into());
///     }
///     else {
///         return Err(Error::from("baz"));
///     }
/// }
/// ```
#[derive(Debug)]
pub struct Error(Box<dyn std::error::Error + Send + Sync + 'static>);
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
