use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

use crate::projection::{unwrap_logical_plan, wrap_table_scan};
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::Statistics;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{
    SendableRecordBatchStream, SessionState, SessionStateBuilder, TaskContext,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};

/// Add the extension sandwich optmizer rule to a SessionStateBuilder
///
/// This logical optimizer rule is applied to table scans that contain a source (to
/// wrap them in a struct such that the extension type name and metadata are propagated
/// through computations where field metadata is not available). The corresponding
/// operation on the end of the plan is also applied (unwrapping the modified types) to
/// satisfy the invariant that an optmizer rule should not change the output schema of
/// a logical plan. Unfortunately, DataFusion does not provide a way to easily get the
/// extension field information back into the output, so realistically tests will need
/// to use something like `ST_AsText()` and not depend on geometry output for now.
///
/// Because a custom physical node is required to prevent other optimizer rules from
/// combining the wrapping and unwrapping projections, we also need a list of other
/// extension planners (in case we need to add other custom physical nodes at any point,
/// which seems like we may).
///
/// The use of this workaround should be regarded as a temporary measure until it is
/// possible to propagate extension information and/or a user defined type through
/// DataFusion's expression API.
pub fn add_extension_sandwich_optimizer_rule(
    builder: SessionStateBuilder,
    other_extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
) -> SessionStateBuilder {
    builder
        .with_optimizer_rule(Arc::new(ExtensionSandwichOptimizerRule {}))
        .with_query_planner(Arc::new(IdentityExtensionQueryPlanner {
            other_extension_planners,
        }))
}

/// LogicalPlan optimizer applying the logical plan transformation
///
/// See [`add_extension_sandwich_optimizer_rule`] for the details of the approach.
#[derive(Default, Debug)]
struct ExtensionSandwichOptimizerRule {}

impl ExtensionSandwichOptimizerRule {
    fn apply_wrap(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|node| match &node {
            LogicalPlan::TableScan(scan) => wrap_table_scan(&node, scan),
            _ => Ok(Transformed::no(node.clone())),
        })
    }

    fn apply_unwrap(plan: Transformed<LogicalPlan>) -> Result<Transformed<LogicalPlan>> {
        let plan_identity = LogicalPlan::Extension(Extension {
            node: Arc::new(IdentityExtensionNode {
                input: plan.data.clone(),
            }),
        });

        let unwrapped_after_identity = unwrap_logical_plan(&plan_identity)?;
        if unwrapped_after_identity.transformed {
            Ok(Transformed::yes(unwrapped_after_identity.data))
        } else {
            Ok(plan)
        }
    }

    fn rule_already_applied(plan: &LogicalPlan) -> Result<bool> {
        let mut already_optimized = false;
        plan.apply(|plan| {
            if let LogicalPlan::Projection(projection) = plan {
                for expr in &projection.expr {
                    expr.apply(|expr| {
                        if let Expr::ScalarFunction(fun) = expr {
                            if fun.name() == "wrap_extension_internal" {
                                already_optimized = true;
                                return Ok(TreeNodeRecursion::Stop);
                            }
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
            }

            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(already_optimized)
    }
}

impl OptimizerRule for ExtensionSandwichOptimizerRule {
    fn name(&self) -> &str {
        "extension_sandwich"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // First, check for any instance of this optimization (basically: any call to
        // wrap_extension_internal()). After the first pass we don't need anything else
        // although our function calls may get bounced around and inlined into eachother
        // by other optimizations.
        if Self::rule_already_applied(&plan)? {
            return Ok(Transformed::no(plan));
        }

        // First, we need to recurse into the plan and add a wrap_table_scan()
        // to all scan nodes at the far ends of the plan.
        let maybe_wrapped_input = Self::apply_wrap(plan)?;

        // Next, we need to unwrap the output
        let plan_out = match &maybe_wrapped_input.data {
            // For a copy, we need the unwrap to occur before the COPY
            LogicalPlan::Copy(copy_to) => {
                let unwrapped_input =
                    Self::apply_unwrap(Transformed::yes(copy_to.input.deref().clone()))?;
                Transformed::yes(maybe_wrapped_input.data.with_new_exprs(
                    maybe_wrapped_input.data.expressions(),
                    vec![unwrapped_input.data],
                )?)
            }
            // For everything else, we just add the unwrap projection (if needed)
            // the end of the plan. We put an "identity" node as its input to prevent
            // subsequent optimizer passes from doing anything to it!
            _ => Self::apply_unwrap(maybe_wrapped_input)?,
        };

        Ok(plan_out)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
struct IdentityExtensionNode {
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for IdentityExtensionNode {
    fn name(&self) -> &str {
        "IdentityExtensionNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("IdentityExtensionNode::")?;
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<IdentityExtensionNode> {
        assert_eq!(exprs.len(), 0);
        assert_eq!(inputs.len(), 1);
        Ok(Self {
            input: inputs[0].clone(),
        })
    }
}

/// Physical planner for IdentityExtensionNode
struct IdentityExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for IdentityExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(_) = node.as_any().downcast_ref::<IdentityExtensionNode>() {
            assert_eq!(logical_inputs.len(), 1);
            assert_eq!(physical_inputs.len(), 1);
            Ok(Some(Arc::new(IdentityExec {
                input: physical_inputs[0].clone(),
            })))
        } else {
            Ok(None)
        }
    }
}

struct IdentityExtensionQueryPlanner {
    other_extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl Debug for IdentityExtensionQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityExtensionQueryPlanner")
            .field(
                "other_extension_planners",
                &self.other_extension_planners.len(),
            )
            .finish()
    }
}

#[async_trait]
impl QueryPlanner for IdentityExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut extension_planners = Vec::with_capacity(self.other_extension_planners.len() + 1);
        for planner in &self.other_extension_planners {
            extension_planners.push(planner.clone());
        }
        extension_planners.push(Arc::new(IdentityExtensionPlanner {}));

        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

struct IdentityExec {
    input: Arc<dyn ExecutionPlan>,
}

impl Debug for IdentityExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IdentityExec")
    }
}

impl DisplayAs for IdentityExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IdentityExec")
    }
}

#[async_trait]
impl ExecutionPlan for IdentityExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input.required_input_distribution()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IdentityExec {
            input: children[0].clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{create_array, record_batch, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{execution::SessionStateBuilder, prelude::SessionContext};

    use crate::logical_type::ExtensionType;

    use super::*;

    /// An ExtensionType for tests
    pub fn geoarrow_wkt() -> ExtensionType {
        ExtensionType::new("geoarrow.wkt", DataType::Utf8, None)
    }

    #[tokio::test]
    async fn optimizer_rule_wrap_unwrap() {
        let col1 = create_array!(Utf8, ["POINT (0 1)", "POINT (2 3)"]);
        let col2 = col1.clone();
        let batch_no_extensions = record_batch!(("col1", Utf8, ["abc", "def"])).unwrap();

        let builder = SessionStateBuilder::new();
        let state = add_extension_sandwich_optimizer_rule(builder, vec![]).build();
        let ctx = SessionContext::from(state);

        let results_no_extensions = ctx
            .read_batch(batch_no_extensions.clone())
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(results_no_extensions.len(), 1);
        assert_eq!(results_no_extensions[0], batch_no_extensions);

        let schema = Schema::new(vec![
            Field::new("col1", DataType::Utf8, true),
            geoarrow_wkt().to_field("col2"),
        ]);
        let batch = RecordBatch::try_new(schema.into(), vec![col1, col2]).unwrap();
        let df = ctx.read_batch(batch.clone()).unwrap();

        // No optimizer rules applied yet, so we get the original schema
        assert_eq!(*df.schema().as_arrow(), *batch.schema());

        // Optimizer stuff gets applied on collect
        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);

        // ..but strips extensions from the final node (OK for now)
        let batch_without_extensions = record_batch!(
            ("col1", Utf8, ["POINT (0 1)", "POINT (2 3)"]),
            ("col2", Utf8, ["POINT (0 1)", "POINT (2 3)"])
        )
        .unwrap();
        assert_eq!(results[0].schema(), batch_without_extensions.schema());
        assert_eq!(results[0], batch_without_extensions);
    }
}
