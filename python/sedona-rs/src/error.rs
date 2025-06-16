use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
use pyo3::{exceptions::PyValueError, PyErr};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PySedonaError {
    #[error("{0}")]
    Py(PyErr),
    #[error("{0}")]
    DF(Box<DataFusionError>),
    #[error("{0}")]
    Invalid(String),
}

impl From<PySedonaError> for PyErr {
    fn from(e: PySedonaError) -> Self {
        match e {
            PySedonaError::Py(py_err) => py_err,
            PySedonaError::DF(e) => {
                let msg = e.message().to_string();
                PyValueError::new_err(msg)
            }
            PySedonaError::Invalid(msg) => PyValueError::new_err(msg),
        }
    }
}

impl From<DataFusionError> for PySedonaError {
    fn from(other: DataFusionError) -> Self {
        PySedonaError::DF(Box::new(other))
    }
}

impl From<PyErr> for PySedonaError {
    fn from(other: PyErr) -> Self {
        PySedonaError::Py(other)
    }
}

impl From<ArrowError> for PySedonaError {
    fn from(other: ArrowError) -> Self {
        PySedonaError::DF(Box::new(DataFusionError::ArrowError(other, None)))
    }
}
