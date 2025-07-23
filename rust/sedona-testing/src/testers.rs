use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    Accumulator, AggregateUDF,
};
use datafusion_physical_expr::{expressions::Column, LexOrdering, PhysicalExpr};
use sedona_schema::datatypes::SedonaType;

use crate::create::create_array;

/// Low-level tester for aggregate functions
///
/// This struct provides a means by which to run a simple check of an
/// aggregate implementation by approximating one method DataFusion
/// might use to perform the aggregation. Whereas DataFusion may arrange
/// various calls to accumulate, state, and update_batch to optimize
/// for different cases, this tester is always created by aggregating
/// states that were in turn created from accumulating one batch.
///
/// This is not a replacement for testing at a higher level using
/// DataFusion's actual aggregate implementation but provides
/// a useful mechanism to ensure all the pieces of an accumulator
/// are plugged in.
pub struct AggregateUdfTester {
    udf: AggregateUDF,
    arg_types: Vec<SedonaType>,
}

impl AggregateUdfTester {
    /// Create a new tester
    pub fn new(udf: AggregateUDF, arg_types: Vec<SedonaType>) -> Self {
        Self { udf, arg_types }
    }

    /// Compute the return type
    pub fn return_type(&self) -> Result<SedonaType> {
        let arg_data_types = self
            .arg_types
            .iter()
            .map(|sedona_type| sedona_type.data_type())
            .collect::<Vec<_>>();
        let out_data_type = self.udf.return_type(&arg_data_types)?;
        SedonaType::from_data_type(&out_data_type)
    }

    /// Perform a simple aggregation using WKT as geometry input
    pub fn aggregate_wkt(&self, batches: Vec<Vec<Option<&str>>>) -> Result<ScalarValue> {
        let batches_array = batches
            .into_iter()
            .map(|batch| create_array(&batch, &self.arg_types[0]))
            .collect::<Vec<_>>();
        self.aggregate(batches_array)
    }

    /// Perform a simple aggregation
    ///
    /// Each batch in batches is accumulated with its own accumulator
    /// and serialized into its own state, after which the states are accumulated
    /// in batches of one. This has the effect of testing all the pieces of
    /// an aggregator in a somewhat configurable/predictable way.
    pub fn aggregate(&self, batches: Vec<ArrayRef>) -> Result<ScalarValue> {
        let state_schema = Arc::new(Schema::new(self.state_fields()?));
        let mut state_accumulator = self.new_accumulator()?;

        for batch in batches {
            let mut batch_accumulator = self.new_accumulator()?;
            batch_accumulator.update_batch(&[batch])?;
            let state_batch_of_one = RecordBatch::try_new(
                state_schema.clone(),
                batch_accumulator
                    .state()?
                    .into_iter()
                    .map(|v| v.to_array())
                    .collect::<Result<Vec<_>>>()?,
            )?;
            state_accumulator.merge_batch(state_batch_of_one.columns())?;
        }

        state_accumulator.evaluate()
    }

    fn new_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let mock_schema = Schema::new(self.arg_fields());
        let exprs = (0..self.arg_types.len())
            .map(|i| -> Arc<dyn PhysicalExpr> { Arc::new(Column::new("col", i)) })
            .collect::<Vec<_>>();
        let accumulator_args = AccumulatorArgs {
            return_field: self.udf.return_field(mock_schema.fields())?,
            schema: &mock_schema,
            ignore_nulls: true,
            ordering_req: LexOrdering::empty(),
            is_reversed: false,
            name: "",
            is_distinct: false,
            exprs: &exprs,
        };

        self.udf.accumulator(accumulator_args)
    }

    fn state_fields(&self) -> Result<Vec<FieldRef>> {
        let state_field_args = StateFieldsArgs {
            name: "",
            input_fields: &self.arg_fields(),
            return_field: self.udf.return_field(&self.arg_fields())?,
            ordering_fields: &[],
            is_distinct: false,
        };
        self.udf.state_fields(state_field_args)
    }

    fn arg_fields(&self) -> Vec<FieldRef> {
        self.arg_data_types()
            .into_iter()
            .map(|data_type| Arc::new(Field::new("", data_type, true)))
            .collect()
    }

    fn arg_data_types(&self) -> Vec<DataType> {
        self.arg_types
            .iter()
            .map(|sedona_type| sedona_type.data_type())
            .collect()
    }
}
