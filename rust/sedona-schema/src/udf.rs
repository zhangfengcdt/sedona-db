use std::iter::zip;
use std::sync::Arc;
use std::{any::Any, fmt::Debug};

use arrow_schema::DataType;
use datafusion_common::error::Result;
use datafusion_common::not_impl_err;
use datafusion_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility};

use crate::datatypes::SedonaType;

/// Top-level scalar user-defined function
///
/// This struct implements datafusion's ScalarUDF and implements kernel dispatch
/// and argument wrapping/unwrapping while this is still necessary to support
/// user-defined types.
#[derive(Debug)]
pub struct SedonaScalarUDF {
    name: String,
    signature: Signature,
    kernels: Vec<Arc<dyn SedonaScalarKernel + Send + Sync>>,
    documentation: Option<Documentation>,
}

/// User-defined function implementation
///
/// A [`SedonaScalarUdf`] is comprised of one or more kernels, to which it dispatches
/// the first whose return_type returns `Some()`. Whereas a SeondaScalarUdf represents
/// a logical operation (e.g., ST_Intersects()), a kernel wraps the logic around a specific
/// implementation.
pub trait SedonaScalarKernel: Debug {
    /// Calculate a return type given input types
    ///
    /// Returns Some(physical_type) if this kernel applies to the input types or
    /// None otherwise. This struct acts as a version of the Signature that can
    /// better accomodate the types we need to support (and might be able to be
    /// removed when there is better support for matching user-defined types/
    /// types with metadata in DataFusion).
    ///
    /// The [`ArgMatcher`] contains a set of helper functions to help implement this
    /// function.
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>>;

    /// Compute a batch of results
    ///
    /// Computes an output chunk based on the physical types of the input and the
    /// computed output type. The ColumnarValues passed are the "unwrapped" representation
    /// of any extension type (e.g., for Wkb the provided ColumnarValue will be Binary).
    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        out_type: &SedonaType,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue>;
}

/// Helper to match arguments and compute return types
#[derive(Debug)]
pub struct ArgMatcher {
    matchers: Vec<Arc<dyn TypeMatcher + Send + Sync>>,
    out_type: SedonaType,
}

impl ArgMatcher {
    /// Create a new ArgMatcher
    pub fn new(matchers: Vec<Arc<dyn TypeMatcher + Send + Sync>>, out_type: SedonaType) -> Self {
        Self { matchers, out_type }
    }

    /// Calculate a return type given input types
    ///
    /// Returns Some(physical_type) if this kernel applies to the input types or
    /// None otherwise.
    pub fn match_args(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        if args.len() != self.matchers.len() {
            return Ok(None);
        }

        for (actual, matcher) in zip(args, &self.matchers) {
            if !matcher.match_type(actual) {
                return Ok(None);
            }
        }

        Ok(Some(self.out_type.clone()))
    }

    /// Matches the given Arrow type using PartialEq
    pub fn is_arrow(data_type: DataType) -> Arc<dyn TypeMatcher + Send + Sync> {
        Arc::new(IsExact {
            exact_type: SedonaType::Arrow(data_type),
        })
    }

    /// Matches any geography or geometry argument without considering Crs
    pub fn is_geometry_or_geography() -> Arc<dyn TypeMatcher + Send + Sync> {
        Arc::new(IsGeometryOrGeography {})
    }

    /// Matches any numeric argument
    pub fn is_numeric() -> Arc<dyn TypeMatcher + Send + Sync> {
        Arc::new(IsNumeric {})
    }

    /// Matches any string argument
    pub fn is_string() -> Arc<dyn TypeMatcher + Send + Sync> {
        Arc::new(IsString {})
    }

    /// Matches any binary argument
    pub fn is_binary() -> Arc<dyn TypeMatcher + Send + Sync> {
        Arc::new(IsBinary {})
    }
}

pub trait TypeMatcher: Debug {
    fn match_type(&self, arg: &SedonaType) -> bool;
}

#[derive(Debug)]
struct IsExact {
    exact_type: SedonaType,
}

impl TypeMatcher for IsExact {
    fn match_type(&self, arg: &SedonaType) -> bool {
        self.exact_type.match_signature(arg)
    }
}

#[derive(Debug)]
struct IsGeometryOrGeography {}

impl TypeMatcher for IsGeometryOrGeography {
    fn match_type(&self, arg: &SedonaType) -> bool {
        matches!(arg, SedonaType::Wkb(_, _))
    }
}

#[derive(Debug)]
struct IsNumeric {}

impl TypeMatcher for IsNumeric {
    fn match_type(&self, arg: &SedonaType) -> bool {
        match arg {
            SedonaType::Arrow(data_type) => data_type.is_numeric(),
            _ => false,
        }
    }
}

#[derive(Debug)]
struct IsString {}

impl TypeMatcher for IsString {
    fn match_type(&self, arg: &SedonaType) -> bool {
        match arg {
            SedonaType::Arrow(data_type) => {
                matches!(
                    data_type,
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                )
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
struct IsBinary {}

impl TypeMatcher for IsBinary {
    fn match_type(&self, arg: &SedonaType) -> bool {
        match arg {
            SedonaType::Arrow(data_type) => {
                matches!(
                    data_type,
                    DataType::Binary
                        | DataType::BinaryView
                        | DataType::LargeBinary
                        | DataType::FixedSizeBinary(_)
                )
            }
            _ => false,
        }
    }
}

/// Type defenition for a Scalar kernel implementation function
pub type SedonaScalarKernelImpl = Arc<
    dyn Fn(&[SedonaType], &SedonaType, &[ColumnarValue], usize) -> Result<ColumnarValue>
        + Send
        + Sync,
>;

/// Scalar kernel based on a function for testing
pub struct SimpleSedonaScalarKernel {
    arg_matcher: ArgMatcher,
    fun: SedonaScalarKernelImpl,
}

impl Debug for SimpleSedonaScalarKernel {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SimpleSedonaScalarKernel").finish()
    }
}

impl SimpleSedonaScalarKernel {
    pub fn new_ref(
        arg_matcher: ArgMatcher,
        fun: SedonaScalarKernelImpl,
    ) -> Arc<dyn SedonaScalarKernel + Send + Sync> {
        Arc::new(Self { arg_matcher, fun })
    }
}

impl SedonaScalarKernel for SimpleSedonaScalarKernel {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        self.arg_matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        out_type: &SedonaType,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        (self.fun)(arg_types, out_type, args, num_rows)
    }
}

impl SedonaScalarUDF {
    pub fn new(
        name: &str,
        kernels: Vec<Arc<dyn SedonaScalarKernel + Send + Sync>>,
        volatility: Volatility,
        documentation: Option<Documentation>,
    ) -> SedonaScalarUDF {
        let signature = Signature::user_defined(volatility);
        Self {
            name: name.to_string(),
            signature,
            kernels,
            documentation,
        }
    }

    fn physical_types(args: &[DataType]) -> Result<Vec<SedonaType>> {
        args.iter().map(SedonaType::from_data_type).collect()
    }

    fn return_type_impl(
        &self,
        args: &[SedonaType],
    ) -> Result<(&dyn SedonaScalarKernel, SedonaType)> {
        for kernel in &self.kernels {
            if let Some(return_type) = kernel.return_type(args)? {
                return Ok((kernel.as_ref(), return_type));
            }
        }

        not_impl_err!("{}({:?}): No kernel matching arguments", self.name, args)
    }
}

impl ScalarUDFImpl for SedonaScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.documentation.as_ref()
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let args = Self::physical_types(args)?;
        let (_, out_type) = self.return_type_impl(&args)?;
        Ok(out_type.data_type())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let arg_types: Vec<DataType> = args.iter().map(|arg| arg.data_type()).collect();
        let arg_physical_types = Self::physical_types(&arg_types)?;
        let (kernel, out_type) = self.return_type_impl(&arg_physical_types)?;
        let args_unwrapped: Result<Vec<ColumnarValue>, _> = zip(&arg_physical_types, args)
            .map(|(a, b)| a.unwrap_arg(b))
            .collect();
        let result = kernel.invoke_batch(
            &arg_physical_types,
            &out_type,
            &args_unwrapped?,
            number_rows,
        )?;
        out_type.wrap_arg(&result)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::scalar::ScalarValue;

    use crate::datatypes::{WKB_GEOGRAPHY, WKB_GEOMETRY};

    use super::*;

    #[test]
    fn matchers() {
        assert!(ArgMatcher::is_arrow(DataType::Null).match_type(&SedonaType::Arrow(DataType::Null)));

        assert!(ArgMatcher::is_geometry_or_geography().match_type(&WKB_GEOMETRY));
        assert!(ArgMatcher::is_geometry_or_geography().match_type(&WKB_GEOGRAPHY));
        assert!(ArgMatcher::is_numeric().match_type(&SedonaType::Arrow(DataType::Int32)));
        assert!(ArgMatcher::is_numeric().match_type(&SedonaType::Arrow(DataType::Float64)));
        assert!(ArgMatcher::is_string().match_type(&SedonaType::Arrow(DataType::Utf8)));
        assert!(ArgMatcher::is_string().match_type(&SedonaType::Arrow(DataType::Utf8View)));
        assert!(ArgMatcher::is_string().match_type(&SedonaType::Arrow(DataType::LargeUtf8)));
        assert!(ArgMatcher::is_binary().match_type(&SedonaType::Arrow(DataType::Binary)));
        assert!(ArgMatcher::is_binary().match_type(&SedonaType::Arrow(DataType::BinaryView)));
        assert!(ArgMatcher::is_binary().match_type(&SedonaType::Arrow(DataType::LargeBinary)));
        assert!(
            ArgMatcher::is_binary().match_type(&SedonaType::Arrow(DataType::FixedSizeBinary(1)))
        );
    }

    #[test]
    fn udf_empty() -> Result<()> {
        // UDF with no implementations
        let udf = SedonaScalarUDF::new("empty", vec![], Volatility::Immutable, None);
        assert_eq!(udf.name(), "empty");
        let err = udf.return_type(&[]).unwrap_err();
        assert_eq!(err.message(), "empty([]): No kernel matching arguments");

        assert_eq!(udf.coerce_types(&[])?, vec![]);

        let batch_err = udf.invoke_batch(&[], 5).unwrap_err();
        assert_eq!(
            batch_err.message(),
            "empty([]): No kernel matching arguments"
        );

        Ok(())
    }

    #[test]
    fn simple_udf() -> Result<()> {
        // UDF with two implementations: one that matches any geometry and one that
        // matches a specific arrow type.
        let kernel_geo = SimpleSedonaScalarKernel::new_ref(
            ArgMatcher::new(
                vec![ArgMatcher::is_geometry_or_geography()],
                SedonaType::Arrow(DataType::Null),
            ),
            Arc::new(|_, _, _, _| Ok(ColumnarValue::Scalar(ScalarValue::Null))),
        );

        let kernel_arrow = SimpleSedonaScalarKernel::new_ref(
            ArgMatcher::new(
                vec![ArgMatcher::is_arrow(DataType::Boolean)],
                SedonaType::Arrow(DataType::Boolean),
            ),
            Arc::new(|_, _, _, _| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))),
        );

        let udf = SedonaScalarUDF::new(
            "simple_udf",
            vec![kernel_geo, kernel_arrow],
            Volatility::Immutable,
            None,
        );

        assert_eq!(udf.name(), "simple_udf");

        // Calling with a geo type should return a Null type
        let wkb_arrow = WKB_GEOMETRY.data_type();
        let wkb_dummy_val =
            WKB_GEOMETRY.wrap_arg(&ColumnarValue::Scalar(ScalarValue::Binary(None)))?;

        assert_eq!(udf.return_type(&[wkb_arrow.clone()])?, DataType::Null);
        assert_eq!(
            udf.coerce_types(&[wkb_arrow.clone()])?,
            vec![wkb_arrow.clone()]
        );

        if let ColumnarValue::Scalar(scalar) = udf.invoke_batch(&[wkb_dummy_val], 5)? {
            assert_eq!(scalar, ScalarValue::Null);
        } else {
            panic!("Unexpected batch result");
        }

        // Calling with a Boolean should result in a Boolean
        let bool_arrow = DataType::Boolean;
        let bool_dummy_val = ColumnarValue::Scalar(ScalarValue::Boolean(None));
        assert_eq!(
            udf.coerce_types(&[bool_arrow.clone()])?,
            vec![bool_arrow.clone()]
        );

        assert_eq!(udf.return_type(&[bool_arrow.clone()])?, DataType::Boolean);

        if let ColumnarValue::Scalar(scalar) = udf.invoke_batch(&[bool_dummy_val], 5)? {
            assert_eq!(scalar, ScalarValue::Boolean(None));
        } else {
            panic!("Unexpected batch result");
        }

        let batch_err = udf.invoke_batch(&[], 5).unwrap_err();
        assert_eq!(
            batch_err.message(),
            "simple_udf([]): No kernel matching arguments"
        );

        Ok(())
    }
}
