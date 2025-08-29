use thiserror::Error;

/// Error enumerator for the sedona-proj crate
#[derive(Error, Debug)]
pub enum SedonaProjError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    LibraryError(String),
    #[error("{0}")]
    CreateError(String),
    #[error("{0}")]
    TransformError(String),
}
