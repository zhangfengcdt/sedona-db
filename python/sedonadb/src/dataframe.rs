// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use std::ffi::CString;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::ffi::FFI_ArrowSchema;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use datafusion::catalog::MemTable;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::DataFrame;
use datafusion_common::Column;
use datafusion_expr::{ExplainFormat, ExplainOption, Expr};
use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use sedona::context::{SedonaDataFrame, SedonaWriteOptions};
use sedona::show::{DisplayMode, DisplayTableOptions};
use sedona_geoparquet::options::{GeoParquetVersion, TableGeoParquetOptions};
use sedona_schema::schema::SedonaSchema;
use tokio::runtime::Runtime;

use crate::context::InternalContext;
use crate::error::PySedonaError;
use crate::import_from::check_pycapsule;
use crate::reader::PySedonaStreamReader;
use crate::runtime::wait_for_future;
use crate::schema::PySedonaSchema;

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
    fn schema(&self) -> PySedonaSchema {
        let arrow_schema = self.inner.schema().as_arrow();
        PySedonaSchema::new(arrow_schema.clone())
    }

    fn primary_geometry_column(&self) -> Result<Option<String>, PySedonaError> {
        Ok(self
            .inner
            .schema()
            .primary_geometry_column_index()?
            .map(|i| self.inner.schema().field(i).name().to_string()))
    }

    fn geometry_columns(&self) -> Result<Vec<String>, PySedonaError> {
        let names = self
            .inner
            .schema()
            .geometry_column_indices()?
            .into_iter()
            .map(|i| self.inner.schema().field(i).name().to_string())
            .collect::<Vec<_>>();
        Ok(names)
    }

    fn limit(
        &self,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<InternalDataFrame, PySedonaError> {
        let inner = self.inner.clone().limit(offset, limit)?;
        Ok(InternalDataFrame::new(inner, self.runtime.clone()))
    }

    fn execute<'py>(&self, py: Python<'py>) -> Result<usize, PySedonaError> {
        let mut c = 0;
        let stream = wait_for_future(py, &self.runtime, self.inner.clone().execute_stream())??;
        let reader = PySedonaStreamReader::new(self.runtime.clone(), stream);
        for batch in reader {
            c += batch?.num_rows();
        }

        Ok(c)
    }

    fn count<'py>(&self, py: Python<'py>) -> Result<usize, PySedonaError> {
        Ok(wait_for_future(
            py,
            &self.runtime,
            self.inner.clone().count(),
        )??)
    }

    fn to_view(
        &self,
        ctx: &InternalContext,
        table_ref: &str,
        overwrite: bool,
    ) -> Result<(), PySedonaError> {
        let provider = self.inner.clone().into_view();
        if overwrite && ctx.inner.ctx.table_exist(table_ref)? {
            ctx.drop_view(table_ref)?;
        }

        ctx.inner.ctx.register_table(table_ref, provider)?;
        Ok(())
    }

    fn to_memtable<'py>(
        &self,
        py: Python<'py>,
        ctx: &InternalContext,
    ) -> Result<Self, PySedonaError> {
        let schema = self.inner.schema();
        let partitions =
            wait_for_future(py, &self.runtime, self.inner.clone().collect_partitioned())??;
        let provider = MemTable::try_new(schema.as_arrow().clone().into(), partitions)?;

        Ok(Self::new(
            ctx.inner.ctx.read_table(Arc::new(provider))?,
            self.runtime.clone(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn to_parquet<'py>(
        &self,
        py: Python<'py>,
        ctx: &InternalContext,
        path: String,
        partition_by: Vec<String>,
        sort_by: Vec<String>,
        single_file_output: bool,
        geoparquet_version: Option<String>,
        overwrite_bbox_columns: bool,
    ) -> Result<(), PySedonaError> {
        // sort_by needs to be SortExpr. A Vec<String> can unambiguously be interpreted as
        // field names (ascending), but other types of expressions aren't supported here yet.
        let sort_by_expr = sort_by
            .into_iter()
            .map(|name| {
                let column = Expr::Column(Column::new_unqualified(name));
                SortExpr::new(column, true, false)
            })
            .collect::<Vec<_>>();

        let options = SedonaWriteOptions::new()
            .with_partition_by(partition_by)
            .with_sort_by(sort_by_expr)
            .with_single_file_output(single_file_output);

        let mut writer_options = TableGeoParquetOptions::new();
        writer_options.overwrite_bbox_columns = overwrite_bbox_columns;
        if let Some(geoparquet_version) = geoparquet_version {
            writer_options.geoparquet_version = geoparquet_version.parse()?;
        } else {
            writer_options.geoparquet_version = GeoParquetVersion::Omitted;
        }

        wait_for_future(
            py,
            &self.runtime,
            self.inner
                .clone()
                .write_geoparquet(&ctx.inner, &path, options, Some(writer_options)),
        )??;
        Ok(())
    }

    fn show<'py>(
        &self,
        py: Python<'py>,
        ctx: &InternalContext,
        limit: Option<usize>,
        width_chars: usize,
        ascii: bool,
    ) -> Result<String, PySedonaError> {
        let mut options = DisplayTableOptions::new();
        options.table_width = width_chars.try_into().unwrap_or(u16::MAX);
        options.arrow_options = options.arrow_options.with_types_info(true);
        if !ascii {
            options.display_mode = DisplayMode::Utf8;
        }

        let content = wait_for_future(
            py,
            &self.runtime,
            self.inner.clone().show_sedona(&ctx.inner, limit, options),
        )??;

        Ok(content)
    }

    fn explain(&self, explain_type: &str, format: &str) -> Result<Self, PySedonaError> {
        let format = ExplainFormat::from_str(format)?;
        let (analyze, verbose) = match explain_type {
            "standard" => (false, false),
            "extended" => (false, true),
            "analyze" => (true, false),
            _ => {
                return Err(PySedonaError::SedonaPython(
                    "explain type must be one of 'standard', 'extended', or 'analyze'".to_string(),
                ))
            }
        };
        let explain_option = ExplainOption::default()
            .with_analyze(analyze)
            .with_verbose(verbose)
            .with_format(format);
        let explain_df = self.inner.clone().explain_with_options(explain_option)?;
        Ok(Self::new(explain_df, self.runtime.clone()))
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyCapsule>, PySedonaError> {
        let name = cr"datafusion_table_provider".into();
        let provider = self.inner.clone().into_view();
        let ffi_provider =
            FFI_TableProvider::new(provider, true, Some(self.runtime.handle().clone()));
        Ok(PyCapsule::new(py, ffi_provider, Some(name))?)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        #[allow(unused_variables)] requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> Result<Bound<'py, PyCapsule>, PySedonaError> {
        if let Some(requested_capsule) = requested_schema {
            let contents = check_pycapsule(&requested_capsule, "arrow_schema")?;
            let ffi_schema = unsafe { FFI_ArrowSchema::from_raw(contents as _) };
            let requested_schema = Schema::try_from(&ffi_schema)?;
            let actual_schema = self.inner.schema().as_arrow();
            if &requested_schema != actual_schema {
                // Eventually we can support this by inserting a cast
                return Err(PySedonaError::SedonaPython(
                    "Requested schema != DataFrame schema not yet supported".to_string(),
                ));
            }
        }

        let stream = wait_for_future(py, &self.runtime, self.inner.clone().execute_stream())??;
        let reader = PySedonaStreamReader::new(self.runtime.clone(), stream);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);

        let ffi_stream = FFI_ArrowArrayStream::new(reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
        Ok(PyCapsule::new(py, ffi_stream, Some(stream_capsule_name))?)
    }
}
