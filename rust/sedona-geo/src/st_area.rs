use std::sync::Arc;

use arrow_array::builder::Float64Builder;
use arrow_schema::DataType;
use datafusion_common::error::Result;
use datafusion_expr::ColumnarValue;
use geo_generic_alg::Area;
use sedona_expr::scalar_udf::{ArgMatcher, ScalarKernelRef, SedonaScalarKernel};
use sedona_functions::executor::GenericExecutor;
use sedona_schema::datatypes::SedonaType;
use wkb::reader::Wkb;

/// ST_Area() implementation using [Area]
pub fn st_area_impl() -> ScalarKernelRef {
    Arc::new(STArea {})
}

#[derive(Debug)]
struct STArea {}

impl SedonaScalarKernel for STArea {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry()],
            DataType::Float64.try_into().unwrap(),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let executor = GenericExecutor::new(arg_types, args);
        let mut builder = Float64Builder::with_capacity(executor.num_iterations());
        executor.execute_wkb_void(|_i, maybe_wkb| {
            match maybe_wkb {
                Some(wkb) => {
                    builder.append_value(invoke_scalar(wkb)?);
                }
                _ => builder.append_null(),
            }

            Ok(())
        })?;

        executor.finish(Arc::new(builder.finish()))
    }
}

fn invoke_scalar(wkb: &Wkb) -> Result<f64> {
    Ok(wkb.unsigned_area())
}

#[cfg(test)]
mod tests {
    use arrow_array::Float64Array;
    use datafusion_common::scalar::ScalarValue;
    use rstest::rstest;
    use sedona_schema::datatypes::{WKB_GEOMETRY, WKB_VIEW_GEOMETRY};
    use sedona_testing::{
        compare::assert_value_equal, create::create_array_value, create::create_scalar_value,
    };

    use super::*;

    #[rstest]
    fn udf(#[values(WKB_GEOMETRY, WKB_VIEW_GEOMETRY)] sedona_type: SedonaType) {
        use sedona_functions::register::stubs::st_area_udf;

        let mut udf = st_area_udf();
        udf.add_kernel(st_area_impl());

        assert_value_equal(
            &udf.invoke_batch(
                &[create_scalar_value(
                    Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
                    &sedona_type,
                )],
                1,
            )
            .unwrap(),
            &ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))),
        );

        assert_value_equal(
            &udf.invoke_batch(&[create_scalar_value(None, &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Scalar(ScalarValue::Float64(None)),
        );

        let wkt_values = [
            Some("POINT(1 2)"),
            None,
            Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
        ];
        let expected_array: Float64Array = vec![Some(0.0), None, Some(0.5)].into();
        assert_value_equal(
            &udf.invoke_batch(&[create_array_value(&wkt_values, &sedona_type)], 1)
                .unwrap(),
            &ColumnarValue::Array(Arc::new(expected_array)),
        );
    }
}
