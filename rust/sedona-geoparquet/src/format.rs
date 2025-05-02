use std::{any::Any, collections::HashMap, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    config::TableParquetOptions,
    datasource::{
        file_format::{
            file_compression_type::FileCompressionType,
            parquet::{ParquetFormat, ParquetFormatFactory},
            FileFormat, FileFormatFactory, FilePushdownSupport,
        },
        physical_plan::{FileOpener, FileScanConfig, FileSinkConfig, FileSource},
    },
};
use datafusion_catalog::Session;
use datafusion_common::{internal_err, not_impl_err, plan_err, GetExt, Result, Statistics};
use datafusion_expr::Expr;
use datafusion_physical_expr::{LexRequirement, PhysicalExpr};
use datafusion_physical_plan::{
    metrics::ExecutionPlanMetricsSet, projection::ProjectionExec, ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};
use sedona_expr::projection::wrap_physical_expressions;
use sedona_schema::{
    extension_type::ExtensionType,
    projection::{unwrap_schema, wrap_schema},
};

use crate::metadata::{GeoParquetColumnEncoding, GeoParquetMetadata};

/// GeoParquet FormatFactory
///
/// A DataFusion FormatFactory provides a means to allow creating a table
/// or referencing one from a SQL context like COPY TO.
#[derive(Debug)]
pub struct GeoParquetFormatFactory {
    inner: ParquetFormatFactory,
}

impl GeoParquetFormatFactory {
    /// Creates an instance of [GeoParquetFormatFactory]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            inner: ParquetFormatFactory::new(),
        }
    }

    /// Creates an instance of [GeoParquetFormatFactory] with customized default options
    pub fn new_with_options(options: TableParquetOptions) -> Self {
        Self {
            inner: ParquetFormatFactory::new_with_options(options),
        }
    }
}

impl FileFormatFactory for GeoParquetFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let inner_format = self.inner.create(state, format_options)?;
        if let Some(parquet_format) = inner_format.as_any().downcast_ref::<ParquetFormat>() {
            Ok(Arc::new(GeoParquetFormat::new(parquet_format)))
        } else {
            internal_err!(
                "Unexpected format from ParquetFormatFactory: {:?}",
                inner_format
            )
        }
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        Arc::new(ParquetFormat::default())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl GetExt for GeoParquetFormatFactory {
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }
}

/// GeoParquet FileFormat
///
/// This [FileFormat] wraps the [ParquetFormat]. The primary purpose of the
/// FileFormat is to be able to be used in a ListingTable (i.e., multi file table).
/// Here we also use it to implement a basic [TableProvider] that give us most if
/// not all of the features of the underlying Parquet reader.
#[derive(Debug)]
pub struct GeoParquetFormat {
    inner: ParquetFormat,
}

impl GeoParquetFormat {
    /// Create a new instance of the file format
    pub fn new(inner: &ParquetFormat) -> Self {
        // For GeoParquet we currently inspect metadata at the Arrow level,
        // so we need this to be exposed by the underlying reader. At some point
        // we may want to keep track of this and apply the same default as
        // DataFusion.
        let inner_clone = ParquetFormat::new().with_options(inner.options().clone());
        Self {
            inner: inner_clone.with_skip_metadata(false),
        }
    }

    /// Apply GeoArrow field metadata to the schema given to us by the underlying format
    ///
    /// This uses the "geo" metadata at the top level of the file to add extension type
    /// metadata to the underlying fields. At the scan level we then apply wrapping to
    /// ensure the geometry type is understood by other components of Sedona.
    pub fn with_geoarrow_field_metadata(&self, inner_schema: SchemaRef) -> Result<SchemaRef> {
        if let Some(geo_metadata_json) = inner_schema.metadata().get("geo") {
            let geo_metadata = GeoParquetMetadata::new(geo_metadata_json)?;

            let new_fields: Result<Vec<_>> = inner_schema
                .fields()
                .iter()
                .map(|field| {
                    if let Some(geo_column) = geo_metadata.columns.get(field.name()) {
                        match geo_column.encoding {
                            GeoParquetColumnEncoding::WKB => {
                                let extension = ExtensionType::new(
                                    "geoarrow.wkb",
                                    field.data_type().clone(),
                                    Some(geo_column.to_geoarrow_metadata()?),
                                );
                                Ok(Arc::new(
                                    extension.to_field(field.name(), field.is_nullable()),
                                ))
                            }
                            _ => plan_err!(
                                "Unsupported GeoParquet encoding: {}",
                                geo_column.encoding
                            ),
                        }
                    } else {
                        Ok(field.clone())
                    }
                })
                .collect();

            Ok(Arc::new(Schema::new(new_fields?)))
        } else {
            Ok(inner_schema)
        }
    }
}

impl Default for GeoParquetFormat {
    fn default() -> Self {
        Self::new(&ParquetFormat::default())
    }
}

#[async_trait]
impl FileFormat for GeoParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // Combining schemas is tricky because for GeoParquet the bboxes will result
        // in the schemas being rejected for inequality.
        if objects.len() != 1 {
            return plan_err!("GeoParquet infer_schema() for >1 file not supported");
        }

        let inner_schema = self.inner.infer_schema(state, store, objects).await?;
        let storage_schema = self.with_geoarrow_field_metadata(inner_schema)?;
        Ok(Arc::new(wrap_schema(&storage_schema)))
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.inner
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        config: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Build the parent Parquet scan, keeping a few references to the initial state
        let wrapped_schema = config.file_schema.clone();
        let inner_schema = unwrap_schema(&wrapped_schema);

        // Make sure we pass the schema that the underlying implementation was expecting
        let mut config = config.clone();
        config.file_schema = Arc::new(inner_schema);
        let inner_plan = self
            .inner
            .create_physical_plan(state, config, filters)
            .await?;

        // Calculate a list of expressions that are either a column reference to the original
        // or a user-defined function call to the function that performs the wrap operation.
        // wrap_physical_expressions() returns None if no columns needed wrapping so that
        // we can omit the new node completely.
        if let Some(column_exprs) = wrap_physical_expressions(inner_plan.schema().fields())? {
            let exec = ProjectionExec::try_new(column_exprs, inner_plan)?;
            Ok(Arc::new(exec))
        } else {
            Ok(inner_plan)
        }
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("GeoParquet writer not implemented")
    }

    fn supports_filters_pushdown(
        &self,
        file_schema: &Schema,
        table_schema: &Schema,
        filters: &[&Expr],
    ) -> Result<FilePushdownSupport> {
        self.inner
            .supports_filters_pushdown(file_schema, table_schema, filters)
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(GeoParquetFileSource::new(self.inner.file_source()))
    }
}

struct GeoParquetFileSource {
    inner: Arc<dyn FileSource>,
}

impl GeoParquetFileSource {
    pub fn new(inner: Arc<dyn FileSource>) -> Self {
        Self { inner }
    }
}

impl FileSource for GeoParquetFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        let mut inner_config = base_config.clone();
        inner_config.file_schema = Arc::new(unwrap_schema(&inner_config.file_schema));
        self.inner
            .create_file_opener(object_store, base_config, partition)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner: self.inner.with_batch_size(batch_size),
        })
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner: self.inner.with_schema(schema),
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner: self.inner.with_projection(config),
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner: self.inner.with_statistics(statistics),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use arrow_array::RecordBatch;
    use arrow_schema::DataType;
    use datafusion::{
        execution::SessionStateBuilder,
        prelude::{col, ParquetReadOptions, SessionContext},
    };
    use sedona_expr::projection::unwrap_batch;
    use sedona_schema::datatypes::{lnglat, Edges, SedonaType};
    use sedona_testing::data::test_geoparquet;

    use super::*;

    fn setup_context() -> SessionContext {
        let mut state = SessionStateBuilder::new().build();
        state
            .register_file_format(Arc::new(GeoParquetFormatFactory::new()), true)
            .unwrap();
        SessionContext::new_with_state(state).enable_url_table()
    }

    #[tokio::test]
    async fn format_from_listing_table() {
        let ctx = setup_context();
        let example = test_geoparquet("example", "geometry").unwrap();
        let df = ctx.table(&example).await.unwrap();

        // Check that the logical plan resulting from a read has the correct schema
        assert_eq!(
            df.schema().clone().strip_qualifiers().field_names(),
            ["wkt", "geometry"]
        );

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

        // Check that the batches resulting from the actual execution also have
        // the correct schema
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let sedona_types: Result<Vec<_>> = batches[0]
            .schema()
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

        // Check that the content is the same as if it were read by the normal reader
        let unwrapped_batches: Vec<_> = batches
            .into_iter()
            .map(unwrap_batch)
            .map(|batch| {
                let fields_without_metadata: Vec<_> = batch
                    .schema()
                    .clone()
                    .fields()
                    .iter()
                    .map(|f| f.deref().clone().with_metadata(HashMap::new()))
                    .collect();
                let schema = Schema::new_with_metadata(fields_without_metadata, HashMap::new());
                RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).unwrap()
            })
            .collect();

        let batches_from_parquet = ctx
            .read_parquet(example, ParquetReadOptions::default())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(unwrapped_batches, batches_from_parquet)
    }

    #[tokio::test]
    async fn projection_without_spatial() {
        let example = test_geoparquet("example", "geometry").unwrap();
        let ctx = setup_context();

        // Completely deselect all geometry columns
        let df = ctx
            .table(&example)
            .await
            .unwrap()
            .select(vec![col("wkt")])
            .unwrap();

        let sedona_types: Result<Vec<_>> = df
            .schema()
            .as_arrow()
            .fields()
            .iter()
            .map(|f| SedonaType::from_data_type(f.data_type()))
            .collect();
        let sedona_types = sedona_types.unwrap();
        assert_eq!(sedona_types.len(), 1);
        assert_eq!(sedona_types[0], SedonaType::Arrow(DataType::Utf8View));
    }

    #[tokio::test]
    async fn geoparquet_format_factory() {
        let ctx = SessionContext::new();
        let format_factory = Arc::new(GeoParquetFormatFactory::new());
        let dyn_format = format_factory
            .create(&ctx.state(), &HashMap::new())
            .unwrap();
        assert!(dyn_format
            .as_any()
            .downcast_ref::<GeoParquetFormat>()
            .is_some());
    }
}
