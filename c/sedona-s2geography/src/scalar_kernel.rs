use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use sedona_expr::scalar_udf::{ArgMatcher, ScalarKernelRef, SedonaScalarKernel};
use sedona_schema::datatypes::SedonaType;

use crate::s2geography::S2ScalarUDF;

/// Implementation of ST_Length() for geography using s2geography
pub fn st_length_impl() -> ScalarKernelRef {
    Arc::new(S2Length {})
}

/// Implementation of ST_Intersects() for geography using s2geography
pub fn st_intersects_impl() -> ScalarKernelRef {
    Arc::new(S2Intersects {})
}

#[derive(Debug)]
struct S2Length {}

impl SedonaScalarKernel for S2Length {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geography()],
            SedonaType::Arrow(DataType::Float64),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let mut wrapper = S2ScalarUDFWrapper::from(S2ScalarUDF::Length());
        wrapper.invoke(arg_types, args)
    }
}

#[derive(Debug)]
struct S2Intersects {}

impl SedonaScalarKernel for S2Intersects {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geography(), ArgMatcher::is_geography()],
            SedonaType::Arrow(DataType::Boolean),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let mut wrapper = S2ScalarUDFWrapper::from(S2ScalarUDF::Intersects());
        wrapper.invoke(arg_types, args)
    }
}

/// Wrapper around the S2ScalarUDF that handles DataFusion-specific details
///
/// In our UDFs, we instantiate the wrapper and execute it for every batch
/// (even though the underlying object supports being reused for more than
/// one batch, it is not thread safe).
#[derive(Debug)]
struct S2ScalarUDFWrapper {
    inner: S2ScalarUDF,
}

impl From<S2ScalarUDF> for S2ScalarUDFWrapper {
    fn from(value: S2ScalarUDF) -> Self {
        Self { inner: value }
    }
}

impl S2ScalarUDFWrapper {
    fn invoke(
        &mut self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        // S2's scalar UDFs operate on fields with extension metadata
        let arg_fields = arg_types
            .iter()
            .map(|arg_type| arg_type.to_storage_field("", true))
            .collect::<Result<Vec<_>>>()?;

        // Initialize the UDF with a schema consisting of the output fields
        let out_ffi_schema = self.inner.init(arg_fields.into(), None)?;

        // Create arrays from each argument (scalars become arrays of size 1)
        let arg_arrays = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => Ok(array.clone()),
                ColumnarValue::Scalar(_) => arg.to_array(1),
            })
            .collect::<Result<Vec<_>>>()?;

        // Execute the batch
        let out_ffi_array = self.inner.execute(&arg_arrays)?;

        // Create the ArrayRef
        let out_array_data = unsafe { arrow_array::ffi::from_ffi(out_ffi_array, &out_ffi_schema)? };
        let out_array = arrow_array::make_array(out_array_data);

        // Ensure scalar inputs map to scalar output
        for arg in args {
            if let ColumnarValue::Array(_) = arg {
                return Ok(ColumnarValue::Array(out_array));
            }
        }

        Ok(ScalarValue::try_from_array(&out_array, 0)?.into())
    }
}

#[cfg(test)]
mod test {

    use arrow_array::ArrayRef;
    use sedona_schema::datatypes::WKB_GEOGRAPHY;
    use sedona_testing::{
        compare::assert_value_equal,
        create::{create_array_value, create_scalar_value},
    };

    use super::*;

    #[test]
    fn length() {
        let mut udf = sedona_functions::register::stubs::st_length_udf();
        udf.add_kernel(st_length_impl());

        // Check array input
        let result = udf
            .invoke_batch(
                &[create_array_value(
                    &[
                        Some("POINT (0 1)"),
                        Some("LINESTRING (0 0, 0 1)"),
                        Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
                        None,
                    ],
                    &WKB_GEOGRAPHY,
                )],
                4,
            )
            .unwrap();

        let expected: ArrayRef = arrow_array::create_array!(
            Float64,
            [Some(0.0), Some(111195.10117748393), Some(0.0), None]
        );
        assert_value_equal(&result, &ColumnarValue::Array(expected));

        // Check scalar input
        let result = udf
            .invoke_batch(
                &[create_scalar_value(
                    Some("LINESTRING (0 0, 0 1)"),
                    &WKB_GEOGRAPHY,
                )],
                1,
            )
            .unwrap();
        assert_value_equal(
            &result,
            &ColumnarValue::Scalar(ScalarValue::Float64(Some(111195.10117748393))),
        );
    }

    #[test]
    fn intersects() {
        let mut udf = sedona_functions::register::stubs::st_intersects_udf();
        udf.add_kernel(st_intersects_impl());

        let point_array = create_array_value(
            &[Some("POINT (0.25 0.25)"), Some("POINT (10 10)"), None],
            &WKB_GEOGRAPHY,
        );
        let polygon_scalar =
            create_scalar_value(Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"), &WKB_GEOGRAPHY);

        // Array, Scalar -> Array
        assert_value_equal(
            &udf.invoke_batch(&[point_array.clone(), polygon_scalar.clone()], 1)
                .unwrap(),
            &ColumnarValue::Array(arrow_array::create_array!(
                Boolean,
                [Some(true), Some(false), None]
            )),
        );
    }
}
