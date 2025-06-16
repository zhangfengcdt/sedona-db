use std::sync::Arc;

use pyo3::prelude::*;
use sedona::context::SedonaContext;
use tokio::runtime::Runtime;

use crate::{dataframe::InternalDataFrame, error::PySedonaError};

#[pyclass]
pub struct InternalContext {
    pub inner: SedonaContext,
    pub runtime: Arc<Runtime>,
}

#[pymethods]
impl InternalContext {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self {
            inner: SedonaContext::new(),
            runtime: Arc::new(Runtime::new().unwrap()),
        })
    }

    pub fn sql(&mut self, query: &str, py: Python) -> Result<InternalDataFrame, PySedonaError> {
        let df = py.allow_threads(|| self.runtime.block_on(self.inner.sql(query)))?;
        Ok(InternalDataFrame::new(df, self.runtime.clone()))
    }

    fn deregister_table(&self, table_ref: &str) -> Result<(), PySedonaError> {
        self.inner.ctx.deregister_table(table_ref)?;
        Ok(())
    }
}
