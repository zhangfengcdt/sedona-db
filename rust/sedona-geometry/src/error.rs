use std::{io, num};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SedonaGeometryError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    IO(io::Error),
    #[error("{0}")]
    External(Box<dyn std::error::Error + Send + Sync>),
    #[error("Unknown geometry error")]
    Unknown,
}

impl From<io::Error> for SedonaGeometryError {
    fn from(value: io::Error) -> Self {
        SedonaGeometryError::IO(value)
    }
}

impl From<num::TryFromIntError> for SedonaGeometryError {
    fn from(value: num::TryFromIntError) -> Self {
        SedonaGeometryError::External(Box::new(value))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn errors() {
        let invalid = SedonaGeometryError::Invalid("foofy".to_string());
        assert_eq!(invalid.to_string(), "foofy");

        let some_err = Box::new(std::io::Error::new(std::io::ErrorKind::Deadlock, "foofy"));
        let external = SedonaGeometryError::External(some_err);
        assert_eq!(external.to_string(), "foofy");

        let unknown = SedonaGeometryError::Unknown;
        assert_eq!(unknown.to_string(), "Unknown geometry error");
    }
}
