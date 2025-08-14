use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;
use pyo3::{create_exception, PyErr};

use thiserror::Error;

create_exception!(sedonadb._lib, SedonaError, pyo3::exceptions::PyException);

#[derive(Error, Debug)]
pub enum PySedonaError {
    #[error("{0}")]
    Py(PyErr),
    #[error("{0}")]
    DF(Box<DataFusionError>),
    #[error("{0}")]
    SedonaPython(String),
}

impl From<PySedonaError> for PyErr {
    fn from(e: PySedonaError) -> Self {
        match e {
            PySedonaError::Py(py_err) => py_err,
            PySedonaError::DF(e) => {
                let msg = e.message().to_string();
                SedonaError::new_err(msg)
            }
            PySedonaError::SedonaPython(msg) => SedonaError::new_err(msg),
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
        PySedonaError::DF(Box::new(DataFusionError::ArrowError(Box::new(other), None)))
    }
}
