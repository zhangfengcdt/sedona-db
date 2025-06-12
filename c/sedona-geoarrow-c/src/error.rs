use std::{fmt::Display, num::TryFromIntError};

use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
use errno::Errno;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GeoArrowCError {
    Invalid(String),
    Arrow(ArrowError),
    Code(i32),
    Message(i32, String),
    External(Box<dyn std::error::Error + Send + Sync>),
}

impl Display for GeoArrowCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeoArrowCError::Invalid(error) => {
                write!(f, "{}", error)
            }
            GeoArrowCError::Arrow(error) => {
                write!(f, "{}", error)
            }
            GeoArrowCError::Code(code) => {
                write!(f, "{}", Errno(*code))
            }
            GeoArrowCError::Message(code, message) => {
                write!(f, "{}: {}", Errno(*code), message)
            }
            GeoArrowCError::External(error) => {
                write!(f, "{}", error)
            }
        }
    }
}

impl From<ArrowError> for GeoArrowCError {
    fn from(value: ArrowError) -> Self {
        GeoArrowCError::Arrow(value)
    }
}

impl From<GeoArrowCError> for DataFusionError {
    fn from(value: GeoArrowCError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}

impl From<TryFromIntError> for GeoArrowCError {
    fn from(value: TryFromIntError) -> Self {
        GeoArrowCError::External(Box::new(value))
    }
}
