//! Logical data paths and host-side session authorization.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataPath(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataPathError {
    NotAbsolute,
    EmptySegment,
    DotSegment,
    Nul,
    RootNotData,
    InvalidExecutionId,
}

impl fmt::Display for DataPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAbsolute => f.write_str("data path must be absolute"),
            Self::EmptySegment => f.write_str("data path contains an empty segment"),
            Self::DotSegment => f.write_str("data path contains a dot segment"),
            Self::Nul => f.write_str("data path contains NUL"),
            Self::RootNotData => f.write_str("the root path does not name data"),
            Self::InvalidExecutionId => {
                f.write_str("execution id must be one non-dot path segment")
            }
        }
    }
}

impl std::error::Error for DataPathError {}

impl DataPath {
    pub fn parse(path: impl Into<String>) -> Result<Self, DataPathError> {
        let path = path.into();
        if !path.starts_with('/') {
            return Err(DataPathError::NotAbsolute);
        }
        if path == "/" {
            return Err(DataPathError::RootNotData);
        }
        if path.as_bytes().contains(&0) {
            return Err(DataPathError::Nul);
        }
        for segment in path[1..].split('/') {
            if segment.is_empty() {
                return Err(DataPathError::EmptySegment);
            }
            if segment == "." || segment == ".." {
                return Err(DataPathError::DotSegment);
            }
        }
        Ok(Self(path))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn starts_with(&self, prefix: &DataPath) -> bool {
        self.0 == prefix.0
            || self
                .0
                .strip_prefix(&prefix.0)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

impl fmt::Display for DataPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for DataPath {
    type Err = DataPathError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        Self::parse(path)
    }
}

impl Serialize for DataPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DataPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let path = String::deserialize(deserializer)?;
        Self::parse(path).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionAccess {
    pub execution_id: String,
    pub read_prefixes: Vec<DataPath>,
    pub write_prefixes: Vec<DataPath>,
}

impl SessionAccess {
    pub fn validate(&self) -> Result<(), DataPathError> {
        if self.execution_id.is_empty()
            || self.execution_id.contains('/')
            || self.execution_id == "."
            || self.execution_id == ".."
            || self.execution_id.as_bytes().contains(&0)
        {
            return Err(DataPathError::InvalidExecutionId);
        }
        Ok(())
    }

    pub fn resolve(&self, logical: &DataPath) -> Result<DataPath, DataPathError> {
        self.validate()?;
        let resolved = if logical.as_str() == "/runs/self" {
            format!("/runs/{}", self.execution_id)
        } else if let Some(suffix) = logical.as_str().strip_prefix("/runs/self/") {
            format!("/runs/{}/{suffix}", self.execution_id)
        } else {
            logical.as_str().to_owned()
        };
        DataPath::parse(resolved)
    }

    pub fn can_read(&self, path: &DataPath) -> bool {
        self.read_prefixes
            .iter()
            .any(|prefix| path.starts_with(prefix))
    }

    pub fn can_write(&self, path: &DataPath) -> bool {
        self.write_prefixes
            .iter()
            .any(|prefix| path.starts_with(prefix))
    }
}
