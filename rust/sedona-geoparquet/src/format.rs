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
        listing::PartitionedFile,
        physical_plan::{FileScanConfig, FileSinkConfig, FileSource},
    },
};
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{
    internal_err, not_impl_err, plan_err, DFSchema, GetExt, Result, Statistics,
};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{utils::conjunction, Expr};
use datafusion_physical_expr::{LexRequirement, PhysicalExpr};
use datafusion_physical_plan::{projection::ProjectionExec, ExecutionPlan};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use sedona_expr::projection::wrap_physical_expressions;
use sedona_schema::{extension_type::ExtensionType, projection::wrap_schema};

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
        let inner_schema = config.file_schema.clone();
        let storage_schema = self.with_geoarrow_field_metadata(inner_schema.clone())?;
        let config_projection = config.projection.clone();

        let inner_plan = self
            .inner
            .create_physical_plan(state, config, filters)
            .await?;

        // Project the input by wrapping the geometry fields such that our infrastructure
        // can identify them as geometry types.
        let projected_storage_fields: Vec<_> = if let Some(projection) = config_projection {
            projection
                .iter()
                .map(|i| Arc::new(storage_schema.field(*i).clone()))
                .collect()
        } else {
            storage_schema.fields().to_vec()
        };

        // Calculate a list of expressions that are either a column reference to the original
        // or a user-defined function call to the function that performs the wrap operation.
        // wrap_physical_expressions() returns None if no columns needed wrapping so that
        // we can omit the new node completely.
        if let Some(column_exprs) = wrap_physical_expressions(&projected_storage_fields)? {
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
        self.inner.file_source()
    }
}

/// A [TableProvider] for GeoParquet files
///
/// Implements a table provider that can be used to read a GeoParquet file.
/// a proper implementation using a ListingTable is likely a more performant
/// solution that will leverage more features of the underlying Parquet format.
#[derive(Debug)]
pub struct GeoParquetTableProvider {
    format: GeoParquetFormat,
    store: Arc<dyn ObjectStore>,
    object: ObjectMeta,
    inner_schema: SchemaRef,
    schema: SchemaRef,
}

impl GeoParquetTableProvider {
    pub async fn local(state: &dyn Session, path: impl AsRef<std::path::Path>) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let object = Self::local_file(path);
        Self::new(state, store, object).await
    }

    pub async fn new(
        state: &dyn Session,
        store: Arc<dyn ObjectStore>,
        object: ObjectMeta,
    ) -> Result<Self> {
        let format = GeoParquetFormat::default();
        let inner_schema = format
            .inner
            .infer_schema(state, &store, &[object.clone()])
            .await?;
        let schema = wrap_schema(
            format
                .with_geoarrow_field_metadata(inner_schema.clone())?
                .as_ref(),
        );
        Ok(Self {
            format,
            store,
            object,
            inner_schema,
            schema: Arc::new(schema),
        })
    }

    fn local_file(path: impl AsRef<std::path::Path>) -> ObjectMeta {
        let location = Path::from_filesystem_path(path.as_ref()).unwrap();
        let metadata = std::fs::metadata(path).expect("Local file metadata");
        ObjectMeta {
            location,
            last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
            size: metadata.len() as usize,
            e_tag: None,
            version: None,
        }
    }

    fn inner_filters(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let inner_df_schema = DFSchema::try_from(self.inner_schema.clone())?;

        let predicate = conjunction(filters.to_vec());
        let predicate = predicate
            .map(|predicate| state.create_physical_expr(predicate, &inner_df_schema))
            .transpose()?
            // if there are no filters, use a literal true to have a predicate
            // that always evaluates to true we can pass to the index
            .unwrap_or_else(|| datafusion::physical_expr::expressions::lit(true));

        Ok(predicate)
    }
}

#[async_trait]
impl TableProvider for GeoParquetTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Build file_groups, config, and physical filters based on the
        // inner_schema/inner format.
        let stats = self
            .format
            .inner
            .infer_stats(state, &self.store, self.inner_schema.clone(), &self.object)
            .await
            .unwrap()
            .project(projection);

        let file_groups = vec![vec![PartitionedFile {
            object_meta: self.object.clone(),
            partition_values: vec![],
            range: None,
            statistics: Some(stats),
            extensions: None,
            metadata_size_hint: self.format.inner.metadata_size_hint(),
        }]];

        let config = FileScanConfig::new(
            ObjectStoreUrl::local_filesystem(),
            self.inner_schema.clone(),
            self.format.file_source(),
        )
        .with_file_groups(file_groups)
        .with_projection(projection.cloned())
        .with_limit(limit);

        let inner_physical_filters = self.inner_filters(state, filters)?;

        self.format
            .create_physical_plan(state, config, Some(&inner_physical_filters))
            .await
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use arrow_array::RecordBatch;
    use arrow_schema::DataType;
    use datafusion::prelude::{col, ParquetReadOptions, SessionContext};
    use sedona_expr::projection::unwrap_batch;
    use sedona_schema::datatypes::{lnglat, Edges, SedonaType};

    use super::*;

    fn test_geoparquet(group: &str, name: &str) -> String {
        let geoarrow_data = "../../submodules/geoarrow-data";
        format!("{geoarrow_data}/{group}/files/{group}_{name}_geo.parquet")
    }

    #[tokio::test]
    async fn table_provider() {
        let ctx = SessionContext::new();

        let example = test_geoparquet("example", "geometry");
        let table = GeoParquetTableProvider::local(&ctx.state(), &example)
            .await
            .unwrap();

        // Check that the logical plan resulting from a read has the correct schema
        let df = ctx.read_table(Arc::new(table)).unwrap();
        assert_eq!(
            df.schema().field_names(),
            ["?table?.wkt", "?table?.geometry"]
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
    async fn projection() {
        let ctx = SessionContext::new();

        let example = test_geoparquet("example", "geometry");
        let table = GeoParquetTableProvider::local(&ctx.state(), &example)
            .await
            .unwrap();

        // Check that the logical plan resulting from a read has the correct schema
        // even when we explicitly reorder columns
        let df = ctx
            .read_table(Arc::new(table))
            .unwrap()
            .select(vec![col("geometry"), col("wkt")])
            .unwrap();
        assert_eq!(
            df.schema().field_names(),
            ["?table?.geometry", "?table?.wkt"]
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
        assert_eq!(
            sedona_types[0],
            SedonaType::WkbView(Edges::Planar, lnglat())
        );
        assert_eq!(sedona_types[1], SedonaType::Arrow(DataType::Utf8View));
    }

    #[tokio::test]
    async fn projection_without_spatial() {
        let ctx = SessionContext::new();

        let example = test_geoparquet("example", "geometry");
        let table = GeoParquetTableProvider::local(&ctx.state(), &example)
            .await
            .unwrap();

        // Completely deselect all geometry columns
        let df = ctx
            .read_table(Arc::new(table))
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
