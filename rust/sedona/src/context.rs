use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    common::{plan_datafusion_err, plan_err},
    error::{DataFusionError, Result},
    execution::{runtime_env::RuntimeEnvBuilder, SendableRecordBatchStream, SessionStateBuilder},
    prelude::{DataFrame, SessionConfig, SessionContext},
    sql::parser::DFParser,
};
use datafusion_expr::sqlparser::dialect::dialect_from_str;
use sedona_expr::{function_set::FunctionSet, projection::wrap_batch, scalar_udf::ScalarKernelRef};
use sedona_geoparquet::format::GeoParquetFormatFactory;

use crate::{
    catalog::DynamicObjectStoreCatalog,
    projection::{unwrap_df, wrap_df},
};
use crate::{exec::create_plan_from_sql, projection::unwrap_stream};

/// Sedona SessionContext wrapper
///
/// As Sedona extends DataFusion, we also extend its context and include the
/// default geometry-specific functions and datasources (which may vary depending
/// on the feature flags used to build the sedona crate). This provides a common
/// interface for configuring the behaviour of
pub struct SedonaContext {
    pub ctx: SessionContext,
    functions: FunctionSet,
}

impl SedonaContext {
    /// Creates a new context with default options
    pub fn new() -> Self {
        // This will panic only if the default build settings are
        // incorrect which we test!
        Self::new_from_context(SessionContext::new()).unwrap()
    }

    /// Creates a new context with default interactive options
    ///
    /// Initializes a context from the current environment and registers access
    /// to the local file system.
    pub async fn new_local_interactive() -> Result<Self> {
        // These three objects enable configuring various elements of the runtime.
        // Eventually we probably want to have a common set of configuration parameters
        // exposed via the CLI/Python as arguments, via ADBC as connection options,
        // and perhaps for all of these initializing them optionally from environment
        // variables.
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let rt_builder = RuntimeEnvBuilder::new();
        let runtime_env = rt_builder.build_arc()?;

        let mut state = SessionStateBuilder::new()
            .with_default_features()
            .with_runtime_env(runtime_env)
            .with_config(session_config)
            .build();
        state.register_file_format(Arc::new(GeoParquetFormatFactory::new()), true)?;

        // Enable dynamic file query (i.e., select * from 'filename')
        let ctx = SessionContext::new_with_state(state).enable_url_table();

        // Install dynamic catalog provider that can register required object stores
        ctx.refresh_catalogs().await?;
        ctx.register_catalog_list(Arc::new(DynamicObjectStoreCatalog::new(
            ctx.state().catalog_list().clone(),
            ctx.state_weak_ref(),
        )));

        Self::new_from_context(ctx)
    }

    /// Creates a new context from a previously configured DataFusion context
    pub fn new_from_context(ctx: SessionContext) -> Result<Self> {
        let mut out = Self {
            ctx,
            functions: FunctionSet::new(),
        };

        // Always register default function set
        out.register_function_set(sedona_functions::register::default_function_set());

        // Register geo kernels if built with geo support
        #[cfg(feature = "geo")]
        out.register_scalar_kernels(sedona_geo::register::scalar_kernels().into_iter())?;

        Ok(out)
    }

    /// Register all functions in a [FunctionSet] with this context
    pub fn register_function_set(&mut self, function_set: FunctionSet) {
        // Merge other functions and re-register with the context
        self.functions.merge(function_set);

        for scalar_udf in self.functions.scalar_udfs() {
            self.ctx.register_udf(scalar_udf.clone().into());
        }
    }

    /// Register a collection of kernels with this context
    pub fn register_scalar_kernels<'a>(
        &mut self,
        kernels: impl Iterator<Item = (&'a str, ScalarKernelRef)>,
    ) -> Result<()> {
        for (name, kernel) in kernels {
            let udf = self.functions.add_scalar_udf_kernel(name, kernel)?;
            self.ctx.register_udf(udf.clone().into());
        }

        Ok(())
    }

    /// Creates a [`DataFrame`] from SQL query text that may contain one or more
    /// statements
    pub async fn multi_sql(&self, sql: &str) -> Result<Vec<DataFrame>> {
        let task_ctx = self.ctx.task_ctx();
        let dialect = &task_ctx.session_config().options().sql_parser.dialect;
        let dialect = dialect_from_str(dialect)
            .ok_or_else(|| plan_datafusion_err!("Unsupported SQL dialect: {dialect}"))?;

        let statements = DFParser::parse_sql_with_dialect(sql, dialect.as_ref())?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            let plan = create_plan_from_sql(self, statement.clone()).await?;
            results.push(DataFrame::new(self.ctx.state(), plan));
        }

        Ok(results)
    }

    /// Creates a [`DataFrame`] from SQL query text containing a single statement
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let results = self.multi_sql(sql).await?;
        if results.len() != 1 {
            return plan_err!("Expected single SQL statement");
        }

        Ok(results[0].clone())
    }

    /// Registers the [`RecordBatch`] as the specified table name
    pub fn register_batch(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.ctx.register_batch(table_name, wrap_batch(batch))
    }

    /// Creates a [`DataFrame`] for reading a [`RecordBatch`]
    pub fn read_batch(&self, batch: RecordBatch) -> Result<DataFrame> {
        self.ctx.read_batch(wrap_batch(batch))
    }

    /// Create a [`DataFrame`] for reading a [`Vec[`RecordBatch`]`]
    pub fn read_batches(
        &self,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<DataFrame> {
        wrap_df(self.ctx.read_batches(batches)?)
    }
}

impl Default for SedonaContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Sedona-specific [`DataFrame`] actions
///
/// This trait, implemented for [`DataFrame`], extends the DataFrame API to make it
/// ergonomic to work with dataframes that contain geometry columns. Currently these
/// are limited to output functions, as geometry columns currently require special
/// handling when written or exported to an external system.
#[async_trait]
pub trait SedonaDataFrame {
    /// Execute this `DataFrame` and buffer all resulting `RecordBatch`es  into memory.
    ///
    /// Because user-defined data types currently require special handling to work with
    /// DataFusion internals, output with geometry columns must use this function to
    /// be recognized as an Arrow extension type in an external system.
    async fn collect_sedona(self) -> Result<Vec<RecordBatch>>;

    async fn execute_stream_sedona(self) -> Result<SendableRecordBatchStream>;
}

#[async_trait]
impl SedonaDataFrame for DataFrame {
    async fn collect_sedona(self) -> Result<Vec<RecordBatch>> {
        let (schema, df) = unwrap_df(self)?;
        let schema_ref = Arc::new(schema.as_arrow().clone());
        let batches = df.collect().await?;

        let unwrapped_batches: Result<Vec<_>> = batches
            .iter()
            .map(|batch| {
                batch
                    .clone()
                    .with_schema(schema_ref.clone())
                    .map_err(|err| {
                        DataFusionError::Internal(format!("batch.with_schema() failed {}", err))
                    })
            })
            .collect();

        unwrapped_batches
    }

    /// Executes this DataFrame and returns a stream over a single partition
    async fn execute_stream_sedona(self) -> Result<SendableRecordBatchStream> {
        Ok(unwrap_stream(self.execute_stream().await?))
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::create_array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::assert_batches_eq;
    use futures::TryStreamExt;
    use sedona_schema::datatypes::{lnglat, Edges, SedonaType, WKB_GEOMETRY};
    use sedona_testing::{create::create_array_storage, data::test_geoparquet};

    use super::*;

    fn test_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("idx", DataType::Int32, true),
            WKB_GEOMETRY.to_storage_field("geometry", true)?,
        ]);
        let idx = create_array!(Int32, [1, 2, 3]);
        let wkb_array = create_array_storage(
            &[
                Some("POINT (1 2)"),
                Some("POINT (3 4)"),
                Some("POINT (5 6)"),
            ],
            &WKB_GEOMETRY,
        );
        Ok(RecordBatch::try_new(Arc::new(schema), vec![idx, wkb_array]).unwrap())
    }

    #[tokio::test]
    async fn basic_sql() -> Result<()> {
        let ctx = SedonaContext::new();

        let batches = ctx
            .sql("SELECT ST_AsText(ST_Point(30, 10)) AS geom")
            .await?
            .collect()
            .await?;
        assert_batches_eq!(
            [
                "+--------------+",
                "| geom         |",
                "+--------------+",
                "| POINT(30 10) |",
                "+--------------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn register_batch() -> Result<()> {
        let ctx = SedonaContext::new();
        ctx.register_batch("test_batch", test_batch()?)?;
        let batches = ctx
            .sql("SELECT idx, ST_AsText(geometry) AS geometry FROM test_batch")
            .await?
            .collect()
            .await?;
        assert_batches_eq!(
            [
                "+-----+------------+",
                "| idx | geometry   |",
                "+-----+------------+",
                "| 1   | POINT(1 2) |",
                "| 2   | POINT(3 4) |",
                "| 3   | POINT(5 6) |",
                "+-----+------------+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_batch() -> Result<()> {
        let ctx = SedonaContext::new();
        let batch_in = test_batch()?;

        let df = ctx.read_batch(batch_in.clone())?;
        let geometry_physical_type: SedonaType = df.schema().field(1).data_type().try_into()?;
        assert_eq!(geometry_physical_type, WKB_GEOMETRY);

        let batches_out = df.collect_sedona().await?;
        assert_eq!(batches_out.len(), 1);
        assert_eq!(batches_out[0], batch_in);

        Ok(())
    }

    #[tokio::test]
    async fn read_batches() -> Result<()> {
        let ctx = SedonaContext::new();
        let batch_in = test_batch()?;

        let df = ctx.read_batches(vec![batch_in.clone(), batch_in.clone()])?;
        let geometry_physical_type: SedonaType = df.schema().field(1).data_type().try_into()?;
        assert_eq!(geometry_physical_type, WKB_GEOMETRY);

        let batches_out = df.collect_sedona().await?;
        assert_eq!(batches_out.len(), 2);
        assert_eq!(batches_out[0], batch_in);
        assert_eq!(batches_out[1], batch_in);

        Ok(())
    }

    #[tokio::test]
    async fn execute_stream() -> Result<()> {
        let ctx = SedonaContext::new();
        let batch_in = test_batch()?;

        let df = ctx.read_batches(vec![batch_in.clone(), batch_in.clone()])?;
        let stream = df.execute_stream_sedona().await?;
        let geometry_physical_type = SedonaType::from_storage_field(stream.schema().field(1))?;
        assert_eq!(geometry_physical_type, WKB_GEOMETRY);

        let batches_out: Vec<_> = stream.try_collect().await?;
        assert_eq!(batches_out.len(), 2);
        assert_eq!(batches_out[0], batch_in);
        assert_eq!(batches_out[1], batch_in);

        Ok(())
    }

    #[tokio::test]
    async fn geoparquet_format() {
        // Make sure that our context can be set up to identify and read
        // GeoParquet files
        let ctx = SedonaContext::new_local_interactive().await.unwrap();
        let example = test_geoparquet("example", "geometry").unwrap();
        let df = ctx.ctx.table(example).await.unwrap();
        let sedona_types: Result<Vec<_>> = df
            .schema()
            .as_arrow()
            .fields()
            .iter()
            .map(|f| SedonaType::from_data_type(f.data_type()))
            .collect();
        let sedona_types = sedona_types.unwrap();
        assert_eq!(sedona_types.len(), 2);
        assert_eq!(sedona_types[0], SedonaType::Arrow(DataType::Utf8View));
        assert_eq!(
            sedona_types[1],
            SedonaType::WkbView(Edges::Planar, lnglat())
        );
    }
}
