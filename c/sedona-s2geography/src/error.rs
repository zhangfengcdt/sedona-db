use std::{fmt::Display, num::TryFromIntError};

use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
use errno::Errno;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum S2GeographyError {
    Internal(String),
    Arrow(ArrowError),
    Code(i32),
    Message(i32, String),
    External(Box<dyn std::error::Error + Send + Sync>),
}

impl Display for S2GeographyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S2GeographyError::Internal(message) => write!(f, "{message}"),
            S2GeographyError::Arrow(error) => {
                write!(f, "{error}")
            }
            S2GeographyError::Code(code) => {
                write!(f, "{}", Errno(*code))
            }
            S2GeographyError::Message(code, message) => {
                write!(f, "{}: {}", Errno(*code), message)
            }
            S2GeographyError::External(error) => {
                write!(f, "{error}")
            }
        }
    }
}

impl From<ArrowError> for S2GeographyError {
    fn from(value: ArrowError) -> Self {
        S2GeographyError::Arrow(value)
    }
}

impl From<S2GeographyError> for DataFusionError {
    fn from(value: S2GeographyError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}

impl From<TryFromIntError> for S2GeographyError {
    fn from(value: TryFromIntError) -> Self {
        S2GeographyError::External(Box::new(value))
    }
}
