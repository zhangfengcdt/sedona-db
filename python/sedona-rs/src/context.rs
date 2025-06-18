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
    fn new(py: Python) -> Result<Self, PySedonaError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PySedonaError::SedonaPython(format!("Failed to build multithreaded runtime: {e}"))
            })?;

        let inner =
            py.allow_threads(|| runtime.block_on(SedonaContext::new_local_interactive()))?;

        Ok(Self {
            inner,
            runtime: Arc::new(runtime),
        })
    }

    pub fn read_parquet(
        &self,
        table_paths: Vec<String>,
        py: Python,
    ) -> Result<InternalDataFrame, PySedonaError> {
        let df = py.allow_threads(|| {
            self.runtime
                .block_on(self.inner.read_parquet(table_paths, Default::default()))
        })?;
        Ok(InternalDataFrame::new(df, self.runtime.clone()))
    }

    pub fn sql(&self, query: &str, py: Python) -> Result<InternalDataFrame, PySedonaError> {
        let df = py.allow_threads(|| self.runtime.block_on(self.inner.sql(query)))?;
        Ok(InternalDataFrame::new(df, self.runtime.clone()))
    }

    fn deregister_table(&self, table_ref: &str) -> Result<(), PySedonaError> {
        self.inner.ctx.deregister_table(table_ref)?;
        Ok(())
    }
}
