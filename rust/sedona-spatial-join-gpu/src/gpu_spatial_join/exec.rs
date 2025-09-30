use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use crate::gpu_spatial_join::config::GpuSpatialJoinConfig;

/// GPU-accelerated spatial join execution plan
pub struct GpuSpatialJoinExec {
    /// Left side input
    left: Arc<dyn ExecutionPlan>,

    /// Right side input
    right: Arc<dyn ExecutionPlan>,

    /// Join configuration
    config: GpuSpatialJoinConfig,

    /// Output schema
    schema: SchemaRef,

    /// Execution properties
    properties: PlanProperties,
}

impl GpuSpatialJoinExec {
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        config: GpuSpatialJoinConfig,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        // Combine schemas for output
        let mut fields = left_schema.fields().to_vec();
        fields.extend_from_slice(right_schema.fields());
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        // Create execution properties
        let eq_props = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let properties = PlanProperties::new(
            eq_props,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            left,
            right,
            config,
            schema,
            properties,
        })
    }
}

impl Debug for GpuSpatialJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GpuSpatialJoinExec")
    }
}

impl DisplayAs for GpuSpatialJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "GpuSpatialJoinExec: join_type={:?}",
            self.config.join_type
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
        Ok(Arc::new(GpuSpatialJoinExec::new(
            children[0].clone(),
            children[1].clone(),
            self.config.clone(),
        )?))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // This will be implemented with actual GPU spatial join logic
        todo!("GPU spatial join execution not yet implemented")
    }
}
