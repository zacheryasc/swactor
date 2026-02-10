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

impl<T: AsRef<str>> From<T> for Error {
    fn from(value: T) -> Self {
        Error(value.as_ref().to_string().into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
