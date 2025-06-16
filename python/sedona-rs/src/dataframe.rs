use std::ffi::CString;
use std::sync::Arc;

use arrow_array::ffi::FFI_ArrowSchema;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use datafusion::prelude::DataFrame;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use sedona::context::SedonaDataFrame;
use sedona::reader::SedonaStreamReader;
use sedona_schema::projection::unwrap_schema;
use tokio::runtime::Runtime;

use crate::error::PySedonaError;

#[pyclass]
pub struct InternalDataFrame {
    pub inner: DataFrame,
    pub runtime: Arc<Runtime>,
}
impl InternalDataFrame {
    pub fn new(inner: DataFrame, runtime: Arc<Runtime>) -> Self {
        Self { inner, runtime }
    }
}

#[pymethods]
impl InternalDataFrame {
    fn primary_geometry_column(&self) -> Result<Option<String>, PySedonaError> {
        Ok(self
            .inner
            .primary_geometry_column_index()?
            .map(|i| self.inner.schema().field(i).name().to_string()))
    }

    fn geometry_columns(&self) -> Result<Vec<String>, PySedonaError> {
        let names = self
            .inner
            .geometry_column_indices()?
            .into_iter()
            .map(|i| self.inner.schema().field(i).name().to_string())
            .collect::<Vec<_>>();
        Ok(names)
    }

    fn __arrow_c_schema__<'py>(
        &'py mut self,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyCapsule>, PySedonaError> {
        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        let schema = unwrap_schema(self.inner.schema().as_arrow());
        let ffi_schema = FFI_ArrowSchema::try_from(schema)?;
        Ok(PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &'py mut self,
        py: Python<'py>,
        #[allow(unused_variables)] requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> Result<Bound<'py, PyCapsule>, PySedonaError> {
        if let Some(requested_capsule) = requested_schema {
            let schema_capsule_name = CString::new("arrow_schema").unwrap();
            if requested_capsule.name()? != Some(&schema_capsule_name) {
                return Err(PySedonaError::Invalid(
                    "Expected capsule with name 'arrow_schema'".to_string(),
                ));
            }

            let ffi_schema: &FFI_ArrowSchema = unsafe { requested_capsule.reference() };
            let requested_schema = Schema::try_from(ffi_schema)?;
            let actual_schema = self.inner.schema().as_arrow();
            if requested_schema != unwrap_schema(actual_schema) {
                // Eventually we can support this by inserting a cast
                return Err(PySedonaError::Invalid(
                    "Requested schema != DataFrame schema not yet supported".to_string(),
                ));
            }
        }

        let stream = self
            .runtime
            .block_on(self.inner.clone().execute_stream_sedona())?;
        let reader = SedonaStreamReader::new(self.runtime.clone(), stream);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);

        let ffi_stream = FFI_ArrowArrayStream::new(reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
        Ok(PyCapsule::new(py, ffi_stream, Some(stream_capsule_name))?)
    }
}
