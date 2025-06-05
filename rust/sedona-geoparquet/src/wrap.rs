use std::{any::Any, sync::Arc};

use datafusion::config::ConfigOptions;
use datafusion_common::{Result, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    execution_plan::CardinalityEffect, filter_pushdown::FilterDescription, metrics::MetricsSet,
    projection::ProjectionExec, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

/// Wrapper around a [ProjectionExec] that implements [ExecutionPlan::gather_filters_for_pushdown]
///
/// Without this wrapper, the datasource never receives the predicates from the plan.
/// This projection is used to wrap extension types and can be removed when the
/// wrapping/unwrapping is removed.
#[derive(Debug, Clone)]
pub struct WrapExec {
    pub inner: ProjectionExec,
}

impl ExecutionPlan for WrapExec {
    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // We need this node to stay put, or else our gather_filters_for_pushdown()
        // could disappear during optimization
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        Ok(
            FilterDescription::new_with_child_count(self.children().len())
                .all_parent_filters_supported(parent_filters),
        )
    }

    fn name(&self) -> &'static str {
        "WrapExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_inner = Arc::new(self.inner.clone()).with_new_children(children)?;
        Ok(Arc::new(Self {
            inner: new_inner
                .as_any()
                .downcast_ref::<ProjectionExec>()
                .unwrap()
                .clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    #[allow(deprecated)]
    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner.cardinality_effect()
    }
}

impl DisplayAs for WrapExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}
