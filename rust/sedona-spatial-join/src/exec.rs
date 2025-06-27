use std::{fmt::Formatter, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion_common::{project_schema, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::JoinType;
use datafusion_physical_expr::equivalence::{join_equivalence_properties, ProjectionMapping};
use datafusion_physical_plan::{
    execution_plan::EmissionType,
    joins::utils::{build_join_schema, check_join_is_valid, ColumnIndex, JoinFilter},
    metrics::ExecutionPlanMetricsSet,
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};

use crate::{
    build_index,
    index::SpatialIndex,
    once_fut::OnceAsync,
    option::SpatialJoinOptions,
    spatial_predicate::SpatialPredicate,
    stream::{SpatialJoinMetrics, SpatialJoinStream},
    utils::{asymmetric_join_output_partitioning, boundedness_from_children},
};

/// Physical execution plan for performing spatial joins between two tables. It uses a spatial
/// index to speed up the join operation.
///
/// ## Algorithm Overview
///
/// The spatial join execution follows a hash-join-like pattern:
/// 1. **Build Phase**: The left (smaller) table geometries are indexed using a spatial index
/// 2. **Probe Phase**: Each geometry from the right table is used to query the spatial index
/// 3. **Refinement**: Candidate pairs from the index are refined using exact spatial predicates
/// 4. **Output**: Matching pairs are combined according to the specified join type
#[derive(Debug)]
pub struct SpatialJoinExec {
    /// left (build) side which gets hashed
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    pub right: Arc<dyn ExecutionPlan>,
    /// Primary spatial join condition (the expression in the ON clause of the join)
    pub on: SpatialPredicate,
    /// Additional filters which are applied while finding matching rows. It could contain part of the ON clause,
    /// or expressions in the WHERE clause.
    pub filter: Option<JoinFilter>,
    /// How the join is performed (`OUTER`, `INNER`, etc)
    pub join_type: JoinType,
    /// The schema after join. Please be careful when using this schema,
    /// if there is a projection, the schema isn't the same as the output schema.
    join_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    /// The projection indices of the columns in the output schema of join
    projection: Option<Vec<usize>>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
    /// Spatial join options
    options: SpatialJoinOptions,
    /// Once future for building the spatial index.
    /// This futures run only once before the spatial index probing phase.
    once_async_spatial_index: OnceAsync<SpatialIndex>,
}

impl SpatialJoinExec {
    // Try to create a new [`SpatialJoinExec`]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: SpatialPredicate,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        projection: Option<Vec<usize>>,
        options: SpatialJoinOptions,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);
        let join_schema = Arc::new(join_schema);
        let cache = Self::compute_properties(
            &left,
            &right,
            Arc::clone(&join_schema),
            *join_type,
            projection.as_ref(),
        )?;

        Ok(SpatialJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            join_schema,
            column_indices,
            projection,
            metrics: Default::default(),
            cache,
            options,
            once_async_spatial_index: OnceAsync::default(),
        })
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// Does this join has a projection on the joined columns
    pub fn contains_projection(&self) -> bool {
        self.projection.is_some()
    }

    /// This function creates the cache object that stores the plan properties such as schema,
    /// equivalence properties, ordering, partitioning, etc.
    ///
    /// NOTICE: The implementation of this function should be identical to the one in
    /// [`datafusion_physical_plan::physical_plan::join::NestedLoopJoinExec::compute_properties`].
    /// This is because SpatialJoinExec is transformed from NestedLoopJoinExec in physical plan
    /// optimization phase. If the properties are not the same, the plan will be incorrect.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        let mut eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            Arc::clone(&schema),
            &[false, false],
            None,
            // No on columns (equi-join condition) in spatial join
            &[],
        );

        let mut output_partitioning = asymmetric_join_output_partitioning(left, right, &join_type);

        if let Some(projection) = projection {
            // construct a map from the input expressions to the output expression of the Projection
            let projection_mapping = ProjectionMapping::from_indices(projection, &schema)?;
            let out_schema = project_schema(&schema, Some(projection))?;
            output_partitioning = output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }

        let emission_type = if left.boundedness().is_unbounded() {
            EmissionType::Final
        } else if right.pipeline_behavior() == EmissionType::Incremental {
            match join_type {
                // If we only need to generate matched rows from the probe side,
                // we can emit rows incrementally.
                JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::Right
                | JoinType::RightAnti => EmissionType::Incremental,
                // If we need to generate unmatched rows from the *build side*,
                // we need to emit them at the end.
                JoinType::Left | JoinType::LeftAnti | JoinType::LeftMark | JoinType::Full => {
                    EmissionType::Both
                }
            }
        } else {
            right.pipeline_behavior()
        };

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            emission_type,
            boundedness_from_children([left, right]),
        ))
    }
}

impl DisplayAs for SpatialJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_on = format!("on={}", self.on);
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                let display_projections = if self.contains_projection() {
                    format!(
                        ", projection=[{}]",
                        self.projection
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|index| format!(
                                "{}@{}",
                                self.join_schema.fields().get(*index).unwrap().name(),
                                index
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_string()
                };
                write!(
                    f,
                    "SpatialJoinExec: join_type={:?}{}{}{}",
                    self.join_type, display_on, display_filter, display_projections
                )
            }
            DisplayFormatType::TreeRender => {
                if *self.join_type() != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl ExecutionPlan for SpatialJoinExec {
    fn name(&self) -> &str {
        "SpatialJoinExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SpatialJoinExec {
            left: children[0].clone(),
            right: children[1].clone(),
            on: self.on.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            join_schema: self.join_schema.clone(),
            column_indices: self.column_indices.clone(),
            projection: self.projection.clone(),
            metrics: Default::default(),
            cache: self.cache.clone(),
            options: self.options.clone(),
            once_async_spatial_index: OnceAsync::default(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let once_partial_leaf_nodes = self.once_async_spatial_index.try_once(|| {
            let build_side = &self.left;

            let num_partitions = build_side.output_partitioning().partition_count();
            let mut build_streams = Vec::with_capacity(num_partitions);
            let mut build_metrics = Vec::with_capacity(num_partitions);
            for k in 0..num_partitions {
                let stream = build_side.execute(k, Arc::clone(&context))?;
                build_streams.push(stream);
                build_metrics.push(SpatialJoinMetrics::new(k, &self.metrics));
            }

            Ok(build_index(
                build_streams,
                self.on.clone(),
                self.options.clone(),
                build_metrics,
                Arc::clone(context.memory_pool()),
            ))
        })?;

        // update column indices to reflect the projection
        let column_indices_after_projection = match &self.projection {
            Some(projection) => projection
                .iter()
                .map(|i| self.column_indices[*i].clone())
                .collect(),
            None => self.column_indices.clone(),
        };

        let join_metrics = SpatialJoinMetrics::new(partition, &self.metrics);
        let probe_stream = self.right.execute(partition, Arc::clone(&context))?;

        Ok(Box::pin(SpatialJoinStream::new(
            self.schema(),
            &self.on,
            self.filter.clone(),
            self.join_type,
            probe_stream,
            column_indices_after_projection,
            join_metrics,
            self.options.clone(),
            once_partial_leaf_nodes,
        )))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        catalog::{MemTable, TableProvider},
        execution::SessionStateBuilder,
        prelude::SessionContext,
    };
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use geo_types::{Coord, Rect};
    use sedona_geometry::types::GeometryTypeId;
    use sedona_schema::datatypes::WKB_GEOMETRY;
    use sedona_testing::datagen::RandomPartitionedDataBuilder;
    use std::sync::Arc;

    use crate::{register_spatial_join_optimizer, ExecutionMode};

    use super::*;

    fn setup_context(options: Option<SpatialJoinOptions>) -> Result<SessionContext> {
        let mut state_builder = SessionStateBuilder::new();
        if let Some(options) = options {
            state_builder = register_spatial_join_optimizer(state_builder, options);
        }
        let state = state_builder.build();
        let ctx = SessionContext::new_with_state(state);

        let mut function_set = sedona_functions::register::default_function_set();
        let scalar_kernels = sedona_geo::register::scalar_kernels();

        function_set.scalar_udfs().for_each(|udf| {
            ctx.register_udf(udf.clone().into());
        });

        for (name, kernel) in scalar_kernels.into_iter() {
            let udf = function_set.add_scalar_udf_kernel(name, kernel)?;
            ctx.register_udf(udf.clone().into());
        }

        Ok(ctx)
    }

    #[tokio::test]
    async fn test_empty_data() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("dist", DataType::Float64, false),
            Field::new("geometry", WKB_GEOMETRY.into(), true),
        ]));

        let test_data_vec = vec![vec![vec![]], vec![vec![], vec![]]];

        let options = SpatialJoinOptions {
            execution_mode: ExecutionMode::PrepareNone,
            max_batch_size: 10,
        };
        let ctx = setup_context(Some(options.clone()))?;
        for test_data in test_data_vec {
            let left_partitions = test_data.clone();
            let right_partitions = test_data;

            let mem_table_left: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
                Arc::clone(&schema),
                left_partitions.clone(),
            )?);
            let mem_table_right: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
                Arc::clone(&schema),
                right_partitions.clone(),
            )?);

            ctx.deregister_table("L")?;
            ctx.deregister_table("R")?;
            ctx.register_table("L", Arc::clone(&mem_table_left))?;
            ctx.register_table("R", Arc::clone(&mem_table_right))?;

            let sql = "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id";
            let df = ctx.sql(sql).await?;
            let result_batches = df.collect().await?;
            for result_batch in result_batches {
                assert_eq!(result_batch.num_rows(), 0);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_range_join() -> Result<()> {
        let bounds = Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 100.0, y: 100.0 });
        let size_range = (1.0, 10.0);

        let (left_schema, left_partitions) = RandomPartitionedDataBuilder::new()
            .seed(1)
            .num_partitions(2)
            .batches_per_partition(2)
            .rows_per_batch(30)
            .geometry_type(GeometryTypeId::Polygon)
            .sedona_type(WKB_GEOMETRY)
            .bounds(bounds)
            .size_range(size_range)
            .null_rate(0.1)
            .build()?;

        let (right_schema, right_partitions) = RandomPartitionedDataBuilder::new()
            .seed(2)
            .num_partitions(4)
            .batches_per_partition(4)
            .rows_per_batch(30)
            .geometry_type(GeometryTypeId::Point)
            .sedona_type(WKB_GEOMETRY)
            .bounds(bounds)
            .size_range(size_range)
            .null_rate(0.1)
            .build()?;

        for max_batch_size in [10, 30, 1000] {
            let options = SpatialJoinOptions {
                execution_mode: ExecutionMode::PrepareNone,
                max_batch_size,
            };
            test_spatial_join_query(&left_schema, &right_schema, left_partitions.clone(), right_partitions.clone(), &options,
                "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id").await?;
            test_spatial_join_query(&left_schema, &right_schema, left_partitions.clone(), right_partitions.clone(), &options,
                "SELECT * FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY L.id, R.id").await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_spatial_join_with_filter() -> Result<()> {
        let bounds = Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 100.0, y: 100.0 });
        let size_range = (0.1, 10.0);

        let (left_schema, left_partitions) = RandomPartitionedDataBuilder::new()
            .seed(1)
            .num_partitions(2)
            .batches_per_partition(2)
            .rows_per_batch(30)
            .geometry_type(GeometryTypeId::Polygon)
            .sedona_type(WKB_GEOMETRY)
            .bounds(bounds)
            .size_range(size_range)
            .null_rate(0.1)
            .build()?;

        let (right_schema, right_partitions) = RandomPartitionedDataBuilder::new()
            .seed(2)
            .num_partitions(4)
            .batches_per_partition(4)
            .rows_per_batch(30)
            .geometry_type(GeometryTypeId::Point)
            .sedona_type(WKB_GEOMETRY)
            .bounds(bounds)
            .size_range(size_range)
            .null_rate(0.1)
            .build()?;

        for max_batch_size in [10, 30, 100] {
            let options = SpatialJoinOptions {
                execution_mode: ExecutionMode::PrepareNone,
                max_batch_size,
            };
            test_spatial_join_query(&left_schema, &right_schema, left_partitions.clone(), right_partitions.clone(), &options,
                "SELECT * FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) AND L.dist < R.dist ORDER BY L.id, R.id").await?;
            test_spatial_join_query(&left_schema, &right_schema, left_partitions.clone(), right_partitions.clone(), &options,
                "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) AND L.dist < R.dist ORDER BY l_id, r_id").await?;
            test_spatial_join_query(&left_schema, &right_schema, left_partitions.clone(), right_partitions.clone(), &options,
                "SELECT L.id l_id, R.id r_id, L.dist l_dist, R.dist r_dist FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) AND L.dist < R.dist ORDER BY l_id, r_id").await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_range_join_with_empty_partitions() -> Result<()> {
        let bounds = Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 100.0, y: 100.0 });
        let size_range = (1.0, 10.0);

        let (left_schema, mut left_partitions) = RandomPartitionedDataBuilder::new()
            .seed(1)
            .num_partitions(2)
            .batches_per_partition(2)
            .rows_per_batch(30)
            .geometry_type(GeometryTypeId::Polygon)
            .sedona_type(WKB_GEOMETRY)
            .bounds(bounds)
            .size_range(size_range)
            .null_rate(0.1)
            .build()?;

        left_partitions.insert(0, vec![]);
        left_partitions.push(vec![]);

        let (right_schema, mut right_partitions) = RandomPartitionedDataBuilder::new()
            .seed(2)
            .num_partitions(4)
            .batches_per_partition(4)
            .rows_per_batch(30)
            .geometry_type(GeometryTypeId::Point)
            .sedona_type(WKB_GEOMETRY)
            .bounds(bounds)
            .size_range(size_range)
            .null_rate(0.1)
            .build()?;

        right_partitions.insert(0, vec![]);
        right_partitions.push(vec![]);

        for max_batch_size in [10, 30, 1000] {
            let options = SpatialJoinOptions {
                execution_mode: ExecutionMode::PrepareNone,
                max_batch_size,
            };
            test_spatial_join_query(&left_schema, &right_schema, left_partitions.clone(), right_partitions.clone(), &options,
                "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id").await?;
            test_spatial_join_query(&left_schema, &right_schema, left_partitions.clone(), right_partitions.clone(), &options,
                "SELECT * FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY L.id, R.id").await?;
        }

        Ok(())
    }

    async fn test_spatial_join_query(
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        left_partitions: Vec<Vec<RecordBatch>>,
        right_partitions: Vec<Vec<RecordBatch>>,
        options: &SpatialJoinOptions,
        sql: &str,
    ) -> Result<()> {
        let mem_table_left: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(left_schema.to_owned(), left_partitions)?);
        let mem_table_right: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
            right_schema.to_owned(),
            right_partitions,
        )?);

        // Run spatial join using SpatialJoinExec
        let ctx = setup_context(Some(options.clone()))?;
        ctx.register_table("L", Arc::clone(&mem_table_left))?;
        ctx.register_table("R", Arc::clone(&mem_table_right))?;
        let df = ctx.sql(sql).await?;
        let actual_schema = df.schema().as_arrow().clone();
        let plan = df.clone().create_physical_plan().await?;
        let spatial_join_execs = collect_spatial_join_exec(&plan)?;
        assert_eq!(spatial_join_execs.len(), 1);
        let actual_batches = df.collect().await?;
        let actual = arrow::compute::concat_batches(&Arc::new(actual_schema), &actual_batches)?;

        // Run spatial join using NestedLoopJoinExec
        let ctx = setup_context(None)?;
        ctx.register_table("L", mem_table_left)?;
        ctx.register_table("R", mem_table_right)?;
        let df = ctx.sql(sql).await?;
        let expected_schema = df.schema().as_arrow().clone();
        let expected_batches = df.collect().await?;
        let expected =
            arrow::compute::concat_batches(&Arc::new(expected_schema), &expected_batches)?;

        // Should produce the same result
        assert!(expected.num_rows() > 0);
        assert_eq!(expected, actual);

        Ok(())
    }

    fn collect_spatial_join_exec(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<&SpatialJoinExec>> {
        let mut spatial_join_execs = Vec::new();
        plan.apply(|node| {
            if let Some(spatial_join_exec) = node.as_any().downcast_ref::<SpatialJoinExec>() {
                spatial_join_execs.push(spatial_join_exec);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(spatial_join_execs)
    }
}
