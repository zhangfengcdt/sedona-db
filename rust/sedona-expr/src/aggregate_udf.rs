use std::{any::Any, fmt::Debug, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion_common::{not_impl_err, Result, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_physical_expr::{expressions::Column, LexOrdering, PhysicalExpr};
use sedona_schema::datatypes::SedonaType;

use crate::scalar_udf::ArgMatcher;

pub type SedonaAccumulatorRef = Arc<dyn SedonaAccumulator + Send + Sync>;

/// Top-level aggregate user-defined function
///
/// This struct implements datafusion's AggregateUDFImpl and implements kernel dispatch
/// such that implementations can be registered flexibly.
#[derive(Debug, Clone)]
pub struct SedonaAggregateUDF {
    name: String,
    signature: Signature,
    kernels: Vec<SedonaAccumulatorRef>,
    documentation: Option<Documentation>,
}

impl SedonaAggregateUDF {
    /// Create a new SedonaAggregateUDF
    pub fn new(
        name: &str,
        kernels: Vec<SedonaAccumulatorRef>,
        volatility: Volatility,
        documentation: Option<Documentation>,
    ) -> Self {
        let signature = Signature::user_defined(volatility);
        Self {
            name: name.to_string(),
            signature,
            kernels,
            documentation,
        }
    }

    /// Create a new stub aggregate function
    ///
    /// Creates a new aggregate function that calculates a return type but fails when
    /// invoked with arguments. This is useful to create stub functions when it is
    /// expected that the actual functionality will be registered from one or more
    /// independent crates (e.g., ST_Union_Agg(), which may be implemented in
    /// sedona-geo or sedona-geography).
    pub fn new_stub(
        name: &str,
        arg_matcher: ArgMatcher,
        volatility: Volatility,
        documentation: Option<Documentation>,
    ) -> Self {
        let stub_kernel = StubAccumulator::new(name.to_string(), arg_matcher);
        Self::new(name, vec![Arc::new(stub_kernel)], volatility, documentation)
    }

    /// Add a new kernel to an Aggregate UDF
    ///
    /// Because kernels are resolved in reverse order, the new kernel will take
    /// precedence over any previously added kernels that apply to the same types.
    pub fn add_kernel(&mut self, kernel: SedonaAccumulatorRef) {
        self.kernels.push(kernel);
    }

    // List the current kernels
    pub fn kernels(&self) -> &[SedonaAccumulatorRef] {
        &self.kernels
    }

    fn dispatch_impl(&self, args: &[SedonaType]) -> Result<(&dyn SedonaAccumulator, SedonaType)> {
        // Resolve kernels in reverse so that more recently added ones are resolved first
        for kernel in self.kernels.iter().rev() {
            if let Some(return_type) = kernel.return_type(args)? {
                return Ok((kernel.as_ref(), return_type));
            }
        }

        not_impl_err!("{}({:?}): No kernel matching arguments", self.name, args)
    }

    fn sedona_types(args: &[DataType]) -> Result<Vec<SedonaType>> {
        args.iter().map(SedonaType::from_data_type).collect()
    }
}

impl AggregateUDFImpl for SedonaAggregateUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.into())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let arg_types = args
            .input_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let arg_physical_types = Self::sedona_types(&arg_types)?;
        let (accumulator, _) = self.dispatch_impl(&arg_physical_types)?;
        accumulator.state_fields(&arg_physical_types)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_physical_types = Self::sedona_types(arg_types)?;
        let (_, out_type) = self.dispatch_impl(&arg_physical_types)?;
        Ok(out_type.data_type())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let arg_types = acc_args
            .exprs
            .iter()
            .map(|expr| expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;
        let arg_physical_types = Self::sedona_types(&arg_types)?;
        let (accumulator, output_type) = self.dispatch_impl(&arg_physical_types)?;
        accumulator.accumulator(&arg_physical_types, &output_type)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.documentation.as_ref()
    }
}

pub trait SedonaAccumulator: Debug {
    /// Given input data types, calculate an output data type
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>>;

    /// Given input data types and previously-calculated output data type,
    /// resolve an [Accumulator]
    ///
    /// The Accumulator provides the underlying DataFusion implementation.
    /// The SedonaAccumulator does not perform any wrapping or unwrapping on the
    /// accumulator arguments or return values (in anticipation of wrapping/unwrapping
    /// being reverted in the near future).
    fn accumulator(
        &self,
        args: &[SedonaType],
        output_type: &SedonaType,
    ) -> Result<Box<dyn Accumulator>>;

    /// The fields representing the underlying serialized state of the Accumulator
    fn state_fields(&self, args: &[SedonaType]) -> Result<Vec<FieldRef>>;
}

#[derive(Debug)]
struct StubAccumulator {
    name: String,
    matcher: ArgMatcher,
}

impl StubAccumulator {
    fn new(name: String, matcher: ArgMatcher) -> Self {
        Self { name, matcher }
    }
}

impl SedonaAccumulator for StubAccumulator {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        self.matcher.match_args(args)
    }

    fn accumulator(
        &self,
        args: &[SedonaType],
        _output_type: &SedonaType,
    ) -> Result<Box<dyn Accumulator>> {
        not_impl_err!(
            "Implementation for {}({args:?}) was not registered",
            self.name
        )
    }

    fn state_fields(&self, _args: &[SedonaType]) -> Result<Vec<FieldRef>> {
        Ok(vec![])
    }
}

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
pub struct AggregateTester {
    udf: AggregateUDF,
    arg_types: Vec<SedonaType>,
}

impl AggregateTester {
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

#[cfg(test)]
mod test {
    use crate::aggregate_udf::SedonaAggregateUDF;

    use super::*;

    #[test]
    fn udaf_empty() -> Result<()> {
        // UDF with no implementations
        let udf = SedonaAggregateUDF::new("empty", vec![], Volatility::Immutable, None);
        assert_eq!(udf.name(), "empty");
        let err = udf.return_type(&[]).unwrap_err();
        assert_eq!(err.message(), "empty([]): No kernel matching arguments");
        assert!(udf.kernels().is_empty());
        assert_eq!(udf.coerce_types(&[])?, vec![]);

        let batch_err = udf.return_type(&[]).unwrap_err();
        assert_eq!(
            batch_err.message(),
            "empty([]): No kernel matching arguments"
        );

        Ok(())
    }

    #[test]
    fn stub() {
        let stub = SedonaAggregateUDF::new_stub(
            "stubby",
            ArgMatcher::new(vec![], SedonaType::Arrow(DataType::Boolean)),
            Volatility::Immutable,
            None,
        );

        // We registered the stub with zero arguments, so when we call it
        // with zero arguments it should calculate a return type but
        // produce our stub error message when used.
        let tester = AggregateTester::new(stub.clone().into(), vec![]);
        assert_eq!(
            tester.return_type().unwrap(),
            DataType::Boolean.try_into().unwrap()
        );

        let err = tester.aggregate(vec![]).unwrap_err();
        assert_eq!(
            err.message(),
            "Implementation for stubby([]) was not registered"
        );

        // If we call with anything else, we shouldn't be able to do anything
        let tester = AggregateTester::new(
            stub.clone().into(),
            vec![DataType::Binary.try_into().unwrap()],
        );
        let err = tester.return_type().unwrap_err();
        assert_eq!(
            err.message(),
            "stubby([Arrow(Binary)]): No kernel matching arguments"
        );
    }
}
