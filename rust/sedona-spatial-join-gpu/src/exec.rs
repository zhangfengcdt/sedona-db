use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    joins::utils::build_join_schema, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

use crate::config::GpuSpatialJoinConfig;

/// GPU-accelerated spatial join execution plan
///
/// This execution plan accepts two child inputs (e.g., ParquetExec) and performs:
/// 1. Reading data from child streams
/// 2. Data transfer to GPU memory
/// 3. GPU spatial join execution
/// 4. Result materialization
pub struct GpuSpatialJoinExec {
    /// Left child execution plan
    left: Arc<dyn ExecutionPlan>,

    /// Right child execution plan
    right: Arc<dyn ExecutionPlan>,

    /// Join configuration
    config: GpuSpatialJoinConfig,

    /// Combined output schema
    schema: SchemaRef,

    /// Execution properties
    properties: PlanProperties,

    /// Metrics for this join operation
    metrics: datafusion_physical_plan::metrics::ExecutionPlanMetricsSet,
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
        // GPU join reads all partitions from both sides and produces a single output partition
        let eq_props = EquivalenceProperties::new(schema.clone());
        // GPU join reads all partitions from both sides and produces a single output partition
        let eq_props = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
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
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(
                format!("GPU spatial join only supports partition 0, requested partition {}", partition)
            ));
        }
        
        log::info!(
            "Executing GPU spatial join on partition {}: {:?}",
            partition,
            self.config.predicate
        );

        // Create GPU spatial join stream
        let stream = crate::stream::GpuSpatialJoinStream::new(
            self.left.clone(),
            self.right.clone(),
            self.schema.clone(),
            self.config.clone(),
            context,
            partition,
            &self.metrics,
        )?;

        Ok(Box::pin(stream))
    }
}
