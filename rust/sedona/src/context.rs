use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    error::{DataFusionError, Result},
    execution::SendableRecordBatchStream,
    prelude::{DataFrame, SessionContext},
};
use sedona_expr::projection::wrap_batch;

use crate::projection::{unwrap_df, wrap_df};
use crate::{functions::register_sedona_scalar_udfs, projection::unwrap_stream};

/// Sedona SessionContext wrapper
///
/// As Sedona extends DataFusion, we also extend its context and include the
/// default geometry-specific functions and datasources (which may vary depending
/// on the feature flags used to build the sedona crate).
pub struct SedonaContext {
    pub ctx: SessionContext,
}

impl SedonaContext {
    /// Creates a new context with default options
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        register_sedona_scalar_udfs(&ctx);
        Self { ctx }
    }

    /// Creates a [`DataFrame`] from SQL query text.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        self.ctx.sql(sql).await
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
    use datafusion_expr::{ColumnarValue, ScalarUDF};
    use futures::TryStreamExt;
    use sedona_functions::st_point::st_point_udf;
    use sedona_schema::datatypes::{SedonaType, WKB_GEOMETRY};

    use super::*;

    fn test_batch() -> Result<RecordBatch> {
        let st_point: ScalarUDF = st_point_udf().into();
        let schema = Schema::new(vec![
            Field::new("idx", DataType::Int32, true),
            WKB_GEOMETRY.to_storage_field("geometry", true)?,
        ]);
        let idx = create_array!(Int32, [1, 2, 3]);
        let xs = create_array!(Float64, [1.0, 3.0, 5.0]);
        let ys = create_array!(Float64, [2.0, 4.0, 6.0]);
        let wkb_wrapped =
            st_point.invoke_batch(&[ColumnarValue::Array(xs), ColumnarValue::Array(ys)], 3)?;
        let wkb_array = WKB_GEOMETRY.unwrap_arg(&wkb_wrapped)?.to_array(3)?;
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
}
