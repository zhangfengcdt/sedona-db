use arrow_schema::ArrowError;
use std::fmt;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GpuSpatialError {
    Arrow(ArrowError),
    Init(String),
    PushBuild(String),
    FinishBuild(String),
    PushStream(String),
}

impl From<ArrowError> for GpuSpatialError {
    fn from(value: ArrowError) -> Self {
        GpuSpatialError::Arrow(value)
    }
}

impl fmt::Display for GpuSpatialError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GpuSpatialError::Arrow(error) => {
                write!(f, "{error}")
            }
            GpuSpatialError::Init(errmsg) => {
                write!(f, "Initialization failed: {}", errmsg)
            }
            GpuSpatialError::PushBuild(errmsg) => {
                write!(f, "Push build failed: {}", errmsg)
            }
            GpuSpatialError::FinishBuild(errmsg) => {
                write!(f, "Finish building failed: {}", errmsg)
            }
            GpuSpatialError::PushStream(errmsg) => {
                write!(f, "Push stream failed: {}", errmsg)
            }
        }
    }
}
