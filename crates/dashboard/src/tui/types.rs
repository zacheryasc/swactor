use serde::{Deserialize, Serialize};
use swactor::runtime::RuntimeAddress;

/// A runtime identity paired with a network endpoint for dashboard connections.
///
/// The endpoint string is protocol-agnostic: `"http://host:port"`, `"unix:///path"`,
/// `"custom://..."`, etc. The transport layer interprets the scheme.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeEndpoint {
    pub id: RuntimeAddress,
    pub endpoint: String,
}

impl RuntimeEndpoint {
    /// Create from an HTTP URL. Generates a random `RuntimeAddress`.
    ///
    /// In the future the runtime provides its own ID; for now the dashboard assigns one.
    pub fn from_url(url: &str) -> Self {
        Self {
            id: RuntimeAddress::new_random(),
            endpoint: url.to_string(),
        }
    }

    /// Parse host and port from an `http://host:port` endpoint string.
    pub fn parse_http(&self) -> Option<(&str, u16)> {
        let s = self.endpoint.strip_prefix("http://")?;
        let (host, port_str) = s.rsplit_once(':')?;
        let port = port_str.parse().ok()?;
        Some((host, port))
    }
}

impl std::fmt::Display for RuntimeEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.id, self.endpoint)
    }
}
