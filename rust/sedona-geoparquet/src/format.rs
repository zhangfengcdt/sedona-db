use std::{any::Any, collections::HashMap, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    config::TableParquetOptions,
    datasource::{
        file_format::{
            file_compression_type::FileCompressionType,
            parquet::{fetch_parquet_metadata, ParquetFormat, ParquetFormatFactory},
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
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use sedona_expr::projection::wrap_physical_expressions;
use sedona_schema::{
    extension_type::ExtensionType,
    projection::{unwrap_schema, wrap_schema},
};

use crate::metadata::{GeoParquetColumnEncoding, GeoParquetMetadata};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::schema_adapter::SchemaAdapterFactory;

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
    inner_with_metadata: ParquetFormat,
    inner_without_metadata: ParquetFormat,
}

impl GeoParquetFormat {
    /// Create a new instance of the file format
    pub fn new(inner: &ParquetFormat) -> Self {
        // For GeoParquet we currently inspect metadata at the Arrow level,
        // so we need this to be exposed by the underlying reader. Depending on
        // what exactly we're doing, we might need the underlying metadata or might
        // need it to be omitted.
        Self {
            inner_with_metadata: ParquetFormat::new()
                .with_options(inner.options().clone())
                .with_skip_metadata(false),
            inner_without_metadata: ParquetFormat::new()
                .with_options(inner.options().clone())
                .with_skip_metadata(true),
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
        self.inner_with_metadata.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner_with_metadata
            .get_ext_with_compression(file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // First, try the underlying format without schema metadata. This should work
        // for regular Parquet reads and will at least ensure that the underlying schemas
        // are compatible.
        let inner_schema_without_metadata = self
            .inner_without_metadata
            .infer_schema(state, store, objects)
            .await?;

        // Collect metadata separately. We can in theory do our own schema
        // inference too to save an extra server request, but then we have to
        // copy more ParqeutFormat code. It may be that caching at the object
        // store level is the way to go here.
        let metadatas: Vec<_> = futures::stream::iter(objects)
            .map(|object| {
                fetch_parquet_metadata(
                    store.as_ref(),
                    object,
                    self.inner_without_metadata.metadata_size_hint(),
                )
            })
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            .buffered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        let mut geoparquet_metadata: Option<GeoParquetMetadata> = None;
        for metadata in &metadatas {
            if let Some(kv) = metadata.file_metadata().key_value_metadata() {
                for item in kv {
                    if item.key == "geo" && item.value.is_some() {
                        let this_geoparquet_metadata =
                            GeoParquetMetadata::new(item.value.as_ref().unwrap())?;

                        match geoparquet_metadata.as_mut() {
                            Some(existing) => {
                                existing.try_update(&this_geoparquet_metadata)?;
                            }
                            None => geoparquet_metadata = Some(this_geoparquet_metadata),
                        }
                    }
                }
            }
        }

        if let Some(geo_metadata) = geoparquet_metadata {
            let new_fields: Result<Vec<_>> = inner_schema_without_metadata
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

            Ok(Arc::new(wrap_schema(&Schema::new(new_fields?))))
        } else {
            Ok(inner_schema_without_metadata)
        }
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.inner_with_metadata
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
            .inner_with_metadata
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
        self.inner_with_metadata
            .supports_filters_pushdown(file_schema, table_schema, filters)
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(GeoParquetFileSource::new(
            self.inner_with_metadata.file_source(),
        ))
    }
}

pub struct GeoParquetFileSource {
    inner: Arc<dyn FileSource>,
}

impl GeoParquetFileSource {
    pub fn new(inner: Arc<dyn FileSource>) -> Self {
        Self { inner }
    }

    /// Apply a predicate to the inner parquet file source if supported
    pub fn with_predicate(
        &self,
        file_schema: Arc<Schema>,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Self {
        // Get a reference to the inner source
        let inner_any = self.inner.as_any();

        // Try to downcast to ParquetSource
        if let Some(parquet_source) = inner_any.downcast_ref::<ParquetSource>() {
            // Call with_predicate with both the file_schema and predicate
            let new_inner = parquet_source.with_predicate(file_schema, predicate);
            // Wrap the ParquetSource in an Arc<dyn FileSource>
            Self {
                inner: Arc::new(new_inner),
            }
        } else {
            // If downcast failed, panic as this is unexpected
            unreachable!("GeoParquetFileSource constructed with non ParquetSource inner")
        }
    }

    /// Apply a schema adapter factory to the inner parquet file source if supported
    pub fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        // Get a reference to the inner source
        let inner_any = self.inner.as_any();

        // Try to downcast to ParquetSource
        if let Some(parquet_source) = inner_any.downcast_ref::<ParquetSource>() {
            // Call with_schema_adapter_factory on the parquet source
            let new_inner = parquet_source
                .clone()
                .with_schema_adapter_factory(schema_adapter_factory);
            // Wrap the ParquetSource in an Arc<dyn FileSource>
            Self {
                inner: Arc::new(new_inner),
            }
        } else {
            // If downcast failed, panic as this is unexpected
            unreachable!("GeoParquetFileSource constructed with non ParquetSource inner")
        }
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
    use std::fmt::Debug;
    use std::ops::Deref;

    use arrow_array::RecordBatch;
    use arrow_schema::DataType;
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory};
    use datafusion::{
        execution::SessionStateBuilder,
        prelude::{col, ParquetReadOptions, SessionContext},
    };
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion_physical_expr::PhysicalExpr;
    use sedona_expr::projection::unwrap_batch;
    use sedona_schema::datatypes::{lnglat, Edges, SedonaType};
    use sedona_testing::data::{geoarrow_data_dir, test_geoparquet};

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
    async fn multiple_files() {
        let ctx = setup_context();
        let data_dir = geoarrow_data_dir().unwrap();
        let df = ctx
            .table(format!("{data_dir}/example/files/*_geo.parquet"))
            .await
            .unwrap();

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

        // Make sure all the rows show up!
        let batches = df.collect().await.unwrap();
        let mut total_size = 0;
        for batch in batches {
            total_size += batch.num_rows();
        }
        assert_eq!(total_size, 244);
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

    #[tokio::test]
    async fn test_with_predicate() {
        // Create a parquet source with the correct constructor signature
        let schema = Arc::new(Schema::empty());
        let parquet_source = ParquetSource::new(TableParquetOptions::default());

        // Create a simple predicate (column > 0)
        let column = Arc::new(Column::new("test", 0));
        let value = Arc::new(Literal::new(ScalarValue::Int32(Some(0))));
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(column, Operator::Gt, value));

        // Create GeoParquetFileSource and apply predicate
        let geo_source = GeoParquetFileSource::new(Arc::new(parquet_source));
        let geo_source_with_predicate = geo_source.with_predicate(schema.clone(), predicate);

        // Check that inner has been modified - the resulting inner source should be different
        assert!(!Arc::ptr_eq(
            &geo_source.inner,
            &geo_source_with_predicate.inner
        ));
    }

    #[tokio::test]
    async fn test_with_schema_adapter_factory() {
        // Create a parquet source with the correct constructor signature
        let parquet_source = ParquetSource::new(TableParquetOptions::default());

        // Create a simple schema adapter factory
        #[derive(Debug)]
        struct MockSchemaAdapterFactory;

        impl SchemaAdapterFactory for MockSchemaAdapterFactory {
            fn create(
                &self,
                _projected_table_schema: SchemaRef,
                _table_schema: SchemaRef,
            ) -> Box<dyn SchemaAdapter> {
                // Since we never use the result, we can return unimplemented
                // We only want to test that the method chains properly
                unimplemented!()
            }
        }

        let schema_adapter_factory: Arc<dyn SchemaAdapterFactory> =
            Arc::new(MockSchemaAdapterFactory);

        // Create GeoParquetFileSource and apply schema adapter factory
        let geo_source = GeoParquetFileSource::new(Arc::new(parquet_source));
        let geo_source_with_adapter =
            geo_source.with_schema_adapter_factory(schema_adapter_factory);

        // Check that inner has been modified
        assert!(!Arc::ptr_eq(
            &geo_source.inner,
            &geo_source_with_adapter.inner
        ));
    }
}
