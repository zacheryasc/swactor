use std::path::PathBuf;

/// Configuration for connecting to a remote host over SSH.
pub struct SshConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub key_file: PathBuf,
    pub key_passphrase: Option<String>,
}

impl SshConfig {
    pub fn new(host: impl Into<String>, username: impl Into<String>, key_file: PathBuf) -> Self {
        Self {
            host: host.into(),
            port: 22,
            username: username.into(),
            key_file,
            key_passphrase: None,
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_passphrase(mut self, passphrase: impl Into<String>) -> Self {
        self.key_passphrase = Some(passphrase.into());
        self
    }
}
