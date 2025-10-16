use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::{
    joins::utils::build_join_schema, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::StreamExt;
use parking_lot::Mutex;

use crate::config::GpuSpatialJoinConfig;
use crate::once_fut::OnceAsync;

/// GPU-accelerated spatial join execution plan
///
/// This execution plan accepts two child inputs (e.g., ParquetExec) and performs:
/// 1. Reading data from child streams
/// 2. Data transfer to GPU memory
/// 3. GPU spatial join execution
/// 4. Result materialization
pub struct GpuSpatialJoinExec {
    /// Left child execution plan (build side)
    left: Arc<dyn ExecutionPlan>,

    /// Right child execution plan (probe side)
    right: Arc<dyn ExecutionPlan>,

    /// Join configuration
    config: GpuSpatialJoinConfig,

    /// Combined output schema
    schema: SchemaRef,

    /// Execution properties
    properties: PlanProperties,

    /// Metrics for this join operation
    metrics: datafusion_physical_plan::metrics::ExecutionPlanMetricsSet,

    /// Shared build data computed once and reused across all output partitions
    once_async_build_data: Arc<Mutex<Option<OnceAsync<crate::build_data::GpuBuildData>>>>,
}

impl GpuSpatialJoinExec {
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        config: GpuSpatialJoinConfig,
    ) -> Result<Self> {
        // Build join schema using DataFusion's utility to handle duplicate column names
        let left_schema = left.schema();
        let right_schema = right.schema();
        let (join_schema, _column_indices) =
            build_join_schema(&left_schema, &right_schema, &config.join_type);
        let schema = Arc::new(join_schema);

        // Create execution properties
        // Output partitioning matches right side to enable parallelism
        let eq_props = EquivalenceProperties::new(schema.clone());
        let partitioning = right.output_partitioning().clone();
        let properties = PlanProperties::new(
            eq_props,
            partitioning,
            EmissionType::Final, // GPU join produces all results at once
            Boundedness::Bounded,
        );

        Ok(Self {
            left,
            right,
            config,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            once_async_build_data: Arc::new(Mutex::new(None)),
        })
    }

    pub fn config(&self) -> &GpuSpatialJoinConfig {
        &self.config
    }

    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
}

impl Debug for GpuSpatialJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GpuSpatialJoinExec: join_type={:?}, predicate={:?}",
            self.config.join_type,
            self.config.predicate,
        )
    }
}

impl DisplayAs for GpuSpatialJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "GpuSpatialJoinExec: join_type={:?}, predicate={:?}",
            self.config.join_type, self.config.predicate
        )
    }
}

impl ExecutionPlan for GpuSpatialJoinExec {
    fn name(&self) -> &str {
        "GpuSpatialJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<datafusion_physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(datafusion::error::DataFusionError::Internal(
                "GpuSpatialJoinExec requires exactly 2 children".into(),
            ));
        }

        Ok(Arc::new(GpuSpatialJoinExec::new(
            children[0].clone(),
            children[1].clone(),
            self.config.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        log::info!(
            "Executing GPU spatial join on partition {}: {:?}",
            partition,
            self.config.predicate
        );

        // Phase 1: Build Phase (runs once, shared across all output partitions)
        // Get or create the shared build data future
        let once_async_build_data = {
            let mut once = self.once_async_build_data.lock();
            once.get_or_insert(OnceAsync::default())
                .try_once(|| {
                    let left = self.left.clone();
                    let config = self.config.clone();
                    let context = Arc::clone(&context);

                    // Build phase: read ALL left partitions and concatenate
                    Ok(async move {
                        let num_partitions = left.output_partitioning().partition_count();
                        let mut all_batches = Vec::new();

                        log::info!("Build phase: reading {} left partitions", num_partitions);

                        for k in 0..num_partitions {
                            let mut stream = left.execute(k, Arc::clone(&context))?;
                            while let Some(batch_result) = stream.next().await {
                                all_batches.push(batch_result?);
                            }
                        }

                        log::info!("Build phase: concatenating {} batches", all_batches.len());

                        // Concatenate all left batches
                        let left_batch = if all_batches.is_empty() {
                            return Err(DataFusionError::Internal(
                                "No data from left side".into()
                            ));
                        } else if all_batches.len() == 1 {
                            all_batches[0].clone()
                        } else {
                            let schema = all_batches[0].schema();
                            arrow::compute::concat_batches(&schema, &all_batches)
                                .map_err(|e| DataFusionError::Execution(
                                    format!("Failed to concatenate left batches: {}", e)
                                ))?
                        };

                        log::info!("Build phase complete: {} total left rows", left_batch.num_rows());

                        Ok(crate::build_data::GpuBuildData::new(left_batch, config))
                    })
                })?
        };

        // Phase 2: Probe Phase (per output partition)
        // Create a probe stream for this partition
        let stream = crate::stream::GpuSpatialJoinStream::new_probe(
            once_async_build_data,
            self.right.clone(),
            self.schema.clone(),
            context,
            partition,
            &self.metrics,
        )?;

        Ok(Box::pin(stream))
    }
}
