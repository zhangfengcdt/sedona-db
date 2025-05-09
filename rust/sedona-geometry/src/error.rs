use thiserror::Error;

#[derive(Error, Debug)]
pub enum SedonaGeometryError {
    #[error("Invalid: {0}")]
    Invalid(String),
    #[error("{0}")]
    External(Box<dyn std::error::Error + Send + Sync>),
    #[error("Unknown geometry error")]
    Unknown,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn errors() {
        let invalid = SedonaGeometryError::Invalid("foofy".to_string());
        assert_eq!(invalid.to_string(), "Invalid: foofy");

        let some_err = Box::new(std::io::Error::new(std::io::ErrorKind::Deadlock, "foofy"));
        let external = SedonaGeometryError::External(some_err);
        assert_eq!(external.to_string(), "foofy");

        let unknown = SedonaGeometryError::Unknown;
        assert_eq!(unknown.to_string(), "Unknown geometry error");
    }
}
