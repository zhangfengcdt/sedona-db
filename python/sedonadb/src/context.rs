use std::sync::Arc;

use pyo3::prelude::*;
use sedona::context::SedonaContext;
use tokio::runtime::Runtime;

use crate::{dataframe::InternalDataFrame, error::PySedonaError, runtime::wait_for_future};

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

        let inner = wait_for_future(py, &runtime, SedonaContext::new_local_interactive())??;

        Ok(Self {
            inner,
            runtime: Arc::new(runtime),
        })
    }

    pub fn read_parquet<'py>(
        &self,
        py: Python<'py>,
        table_paths: Vec<String>,
    ) -> Result<InternalDataFrame, PySedonaError> {
        let df = wait_for_future(
            py,
            &self.runtime,
            self.inner.read_parquet(table_paths, Default::default()),
        )??;
        Ok(InternalDataFrame::new(df, self.runtime.clone()))
    }

    pub fn sql<'py>(
        &self,
        py: Python<'py>,
        query: &str,
    ) -> Result<InternalDataFrame, PySedonaError> {
        let df = wait_for_future(py, &self.runtime, self.inner.sql(query))??;
        Ok(InternalDataFrame::new(df, self.runtime.clone()))
    }

    fn deregister_table(&self, table_ref: &str) -> Result<(), PySedonaError> {
        self.inner.ctx.deregister_table(table_ref)?;
        Ok(())
    }
}
