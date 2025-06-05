use std::iter::zip;
use std::sync::Arc;
use std::{any::Any, fmt::Debug};

use arrow_schema::{DataType, Field};
use datafusion_common::error::Result;
use datafusion_common::not_impl_err;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sedona_schema::datatypes::{Edges, SedonaType};

pub type ScalarKernelRef = Arc<dyn SedonaScalarKernel + Send + Sync>;

/// Top-level scalar user-defined function
///
/// This struct implements datafusion's ScalarUDF and implements kernel dispatch
/// and argument wrapping/unwrapping while this is still necessary to support
/// user-defined types.
#[derive(Debug, Clone)]
pub struct SedonaScalarUDF {
    name: String,
    signature: Signature,
    kernels: Vec<ScalarKernelRef>,
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
    /// better accommodate the types we need to support (and might be able to be
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
        args: &[ColumnarValue],
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
        if self.matches(args) {
            Ok(Some(self.out_type.clone()))
        } else {
            Ok(None)
        }
    }

    /// Check for an input type match
    ///
    /// Returns true if args applies to the input types.
    pub fn matches(&self, args: &[SedonaType]) -> bool {
        if args.len() != self.matchers.len() {
            return false;
        }

        for (actual, matcher) in zip(args, &self.matchers) {
            if !matcher.match_type(actual) {
                return false;
            }
        }

        true
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

    /// Matches any geometry argument without considering Crs
    pub fn is_geometry() -> Arc<dyn TypeMatcher + Send + Sync> {
        Arc::new(IsGeometry {})
    }

    /// Matches any geography argument without considering Crs
    pub fn is_geography() -> Arc<dyn TypeMatcher + Send + Sync> {
        Arc::new(IsGeography {})
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
        matches!(arg, SedonaType::Wkb(_, _) | SedonaType::WkbView(_, _))
    }
}

#[derive(Debug)]
struct IsGeometry {}

impl TypeMatcher for IsGeometry {
    fn match_type(&self, arg: &SedonaType) -> bool {
        match arg {
            SedonaType::Wkb(edges, _) | SedonaType::WkbView(edges, _) => {
                matches!(edges, Edges::Planar)
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
struct IsGeography {}

impl TypeMatcher for IsGeography {
    fn match_type(&self, arg: &SedonaType) -> bool {
        match arg {
            SedonaType::Wkb(edges, _) | SedonaType::WkbView(edges, _) => {
                matches!(edges, Edges::Spherical)
            }
            _ => false,
        }
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
                matches!(data_type, DataType::Binary | DataType::BinaryView)
            }
            _ => false,
        }
    }
}

/// Type definition for a Scalar kernel implementation function
pub type SedonaScalarKernelImpl =
    Arc<dyn Fn(&[SedonaType], &[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;

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
    pub fn new_ref(arg_matcher: ArgMatcher, fun: SedonaScalarKernelImpl) -> ScalarKernelRef {
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
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        (self.fun)(arg_types, args)
    }
}

impl SedonaScalarUDF {
    /// Create a new SedonaScalarUDF
    pub fn new(
        name: &str,
        kernels: Vec<ScalarKernelRef>,
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

    /// Create a new stub function
    ///
    /// Creates a new function that calculates a return type but fails when invoked with
    /// arguments. This is useful to create stub functions when it is expected that the
    /// actual functionality will be registered from one or more independent crates
    /// (e.g., ST_Intersects(), which may be implemented in sedona-geo or sedona-geography).
    pub fn new_stub(
        name: &str,
        arg_matcher: ArgMatcher,
        volatility: Volatility,
        documentation: Option<Documentation>,
    ) -> Self {
        let name_string = name.to_string();
        let stub_kernel = SimpleSedonaScalarKernel::new_ref(
            arg_matcher,
            Arc::new(move |arg_types, _| {
                not_impl_err!("Implementation for {name_string}({arg_types:?}) was not registered")
            }),
        );

        Self::new(name, vec![stub_kernel], volatility, documentation)
    }

    pub fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> Result<ColumnarValue> {
        let arg_types: Vec<_> = args.iter().map(|arg| arg.data_type()).collect();
        let return_type = self.return_type(&arg_types)?;
        let arg_fields: Vec<_> = arg_types
            .into_iter()
            .map(|data_type| Arc::new(Field::new("", data_type, true)))
            .collect();

        let args = ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("", return_type, true)),
        };

        self.invoke_with_args(args)
    }

    /// Add a new kernel to a Scalar UDF
    ///
    /// Because kernels are resolved in reverse order, the new kernel will take
    /// precedence over any previously added kernels that apply to the same types.
    pub fn add_kernel(&mut self, kernel: ScalarKernelRef) {
        self.kernels.push(kernel);
    }

    fn physical_types(args: &[DataType]) -> Result<Vec<SedonaType>> {
        args.iter().map(SedonaType::from_data_type).collect()
    }

    fn return_type_impl(
        &self,
        args: &[SedonaType],
    ) -> Result<(&dyn SedonaScalarKernel, SedonaType)> {
        // Resolve kernels in reverse so that more recently added ones are resolved first
        for kernel in self.kernels.iter().rev() {
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

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_types: Vec<DataType> = args.args.iter().map(|arg| arg.data_type()).collect();
        let arg_physical_types = Self::physical_types(&arg_types)?;
        let (kernel, out_type) = self.return_type_impl(&arg_physical_types)?;
        let args_unwrapped: Result<Vec<ColumnarValue>, _> = zip(&arg_physical_types, &args.args)
            .map(|(a, b)| a.unwrap_arg(b))
            .collect();
        let result = kernel.invoke_batch(&arg_physical_types, &args_unwrapped?)?;
        out_type.wrap_arg(&result)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::scalar::ScalarValue;

    use sedona_schema::datatypes::{WKB_GEOGRAPHY, WKB_GEOMETRY};

    use super::*;

    #[test]
    fn matchers() {
        assert!(ArgMatcher::is_arrow(DataType::Null).match_type(&SedonaType::Arrow(DataType::Null)));

        assert!(ArgMatcher::is_geometry_or_geography().match_type(&WKB_GEOMETRY));
        assert!(ArgMatcher::is_geometry_or_geography().match_type(&WKB_GEOGRAPHY));
        assert!(!ArgMatcher::is_geometry_or_geography()
            .match_type(&SedonaType::Arrow(DataType::Binary)));

        assert!(ArgMatcher::is_geometry().match_type(&WKB_GEOMETRY));
        assert!(!ArgMatcher::is_geometry().match_type(&WKB_GEOGRAPHY));

        assert!(ArgMatcher::is_geography().match_type(&WKB_GEOGRAPHY));
        assert!(!ArgMatcher::is_geography().match_type(&WKB_GEOMETRY));

        assert!(ArgMatcher::is_numeric().match_type(&SedonaType::Arrow(DataType::Int32)));
        assert!(ArgMatcher::is_numeric().match_type(&SedonaType::Arrow(DataType::Float64)));

        assert!(ArgMatcher::is_string().match_type(&SedonaType::Arrow(DataType::Utf8)));
        assert!(ArgMatcher::is_string().match_type(&SedonaType::Arrow(DataType::Utf8View)));
        assert!(ArgMatcher::is_string().match_type(&SedonaType::Arrow(DataType::LargeUtf8)));
        assert!(!ArgMatcher::is_string().match_type(&SedonaType::Arrow(DataType::Binary)));

        assert!(ArgMatcher::is_binary().match_type(&SedonaType::Arrow(DataType::Binary)));
        assert!(ArgMatcher::is_binary().match_type(&SedonaType::Arrow(DataType::BinaryView)));
        assert!(!ArgMatcher::is_binary().match_type(&SedonaType::Arrow(DataType::Utf8)));
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
            Arc::new(|_, _| Ok(ColumnarValue::Scalar(ScalarValue::Null))),
        );

        let kernel_arrow = SimpleSedonaScalarKernel::new_ref(
            ArgMatcher::new(
                vec![ArgMatcher::is_arrow(DataType::Boolean)],
                SedonaType::Arrow(DataType::Boolean),
            ),
            Arc::new(|_, _| Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))),
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

        // Calling with something where no types match should error
        let batch_err = udf.invoke_batch(&[], 5).unwrap_err();
        assert_eq!(
            batch_err.message(),
            "simple_udf([]): No kernel matching arguments"
        );

        // Adding a new kernel should result in that kernel getting picked first
        let mut udf = udf.clone();
        udf.add_kernel(SimpleSedonaScalarKernel::new_ref(
            ArgMatcher::new(
                vec![ArgMatcher::is_arrow(DataType::Boolean)],
                SedonaType::Arrow(DataType::Utf8),
            ),
            Arc::new(|_, _| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))),
        ));

        // Now, calling with a Boolean should result in a Utf8
        assert_eq!(udf.return_type(&[bool_arrow.clone()])?, DataType::Utf8);

        Ok(())
    }

    #[test]
    fn stub() {
        let stub = SedonaScalarUDF::new_stub(
            "stubby",
            ArgMatcher::new(vec![], SedonaType::Arrow(DataType::Boolean)),
            Volatility::Immutable,
            None,
        );

        assert_eq!(stub.return_type(&[]).unwrap(), DataType::Boolean);
        let err = stub.invoke_batch(&[], 1).unwrap_err();
        assert_eq!(
            err.message(),
            "Implementation for stubby([]) was not registered"
        );
    }
}
