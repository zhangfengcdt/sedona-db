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

use crate::config::GpuSpatialJoinConfig;

/// GPU-accelerated spatial join execution plan
///
/// This execution plan handles the entire pipeline:
/// 1. Direct Parquet file reading
/// 2. Data transfer to GPU memory
/// 3. GPU spatial join execution
/// 4. Result materialization
pub struct GpuSpatialJoinExec {
    /// Left input schema
    left_schema: SchemaRef,

    /// Right input schema
    right_schema: SchemaRef,

    /// Join configuration
    config: GpuSpatialJoinConfig,

    /// Combined output schema
    schema: SchemaRef,

    /// Execution properties
    properties: PlanProperties,
}

impl GpuSpatialJoinExec {
    pub fn new(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        config: GpuSpatialJoinConfig,
    ) -> Result<Self> {
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
            EmissionType::Final, // GPU join produces all results at once
            Boundedness::Bounded,
        );

        Ok(Self {
            left_schema,
            right_schema,
            config,
            schema,
            properties,
        })
    }

    pub fn config(&self) -> &GpuSpatialJoinConfig {
        &self.config
    }
}

impl Debug for GpuSpatialJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GpuSpatialJoinExec: join_type={:?}, predicate={:?}, left_files={}, right_files={}",
            self.config.join_type,
            self.config.predicate,
            self.config.left_parquet_files.len(),
            self.config.right_parquet_files.len()
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

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // GPU join is a leaf node - it directly reads from Parquet files
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "GpuSpatialJoinExec should have no children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "GpuSpatialJoinExec only supports partition 0".into(),
            ));
        }

        // TODO: Implement GpuSpatialJoinStream
        log::info!(
            "Executing GPU spatial join with {} left files and {} right files",
            self.config.left_parquet_files.len(),
            self.config.right_parquet_files.len()
        );

        Err(datafusion::error::DataFusionError::NotImplemented(
            "GPU spatial join stream not yet implemented".into(),
        ))
    }
}
