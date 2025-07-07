use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TgError {
    #[error("Out of memory")]
    Memory,
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Arrow(ArrowError),
    #[error("{0}")]
    External(Box<dyn std::error::Error + Send + Sync>),
}

impl From<ArrowError> for TgError {
    fn from(value: ArrowError) -> Self {
        TgError::Arrow(value)
    }
}

impl From<TgError> for DataFusionError {
    fn from(value: TgError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}
