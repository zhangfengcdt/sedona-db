use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion_common::{error::Result, DataFusionError};
use datafusion_expr::ColumnarValue;
use geos::Geom;
use sedona_expr::scalar_udf::{ArgMatcher, ScalarKernelRef, SedonaScalarKernel};
use sedona_schema::datatypes::SedonaType;

use crate::executor::GeosExecutor;

/// ST_Intersects() implementation using the geos crate
pub fn st_intersects_impl() -> ScalarKernelRef {
    Arc::new(STIntersects {})
}

#[derive(Debug)]
struct STIntersects {}

impl SedonaScalarKernel for STIntersects {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry(), ArgMatcher::is_geometry()],
            DataType::Boolean.try_into().unwrap(),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let executor = GeosExecutor::new(arg_types, args);
        let mut builder = BooleanBuilder::with_capacity(executor.num_iterations());
        executor.execute_wkb_wkb_void(|lhs, rhs| {
            match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => {
                    builder.append_value(invoke_scalar(lhs, rhs).unwrap());
                }
                _ => builder.append_null(),
            };
            Ok(())
        })?;
        executor.finish(Arc::new(builder.finish()))
    }
}

fn invoke_scalar(geos_geom: &geos::Geometry, other: &geos::Geometry) -> Result<bool> {
    let result = geos_geom
        .intersects(other)
        .map_err(|e| DataFusionError::Execution(format!("Failed to calculate intersects: {e}")))?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use arrow_array::{create_array as arrow_array, ArrayRef};
    use rstest::rstest;
    use sedona_expr::scalar_udf::SedonaScalarUDF;
    use sedona_schema::datatypes::{WKB_GEOMETRY, WKB_VIEW_GEOMETRY};
    use sedona_testing::compare::assert_array_equal;
    use sedona_testing::create::create_array;
    use sedona_testing::testers::ScalarUdfTester;

    use super::*;

    #[rstest]
    fn udf(#[values(WKB_GEOMETRY, WKB_VIEW_GEOMETRY)] sedona_type: SedonaType) {
        use datafusion_common::ScalarValue;

        let udf = SedonaScalarUDF::from_kernel("st_intersects", st_intersects_impl());
        let tester = ScalarUdfTester::new(udf.into(), vec![sedona_type.clone(), sedona_type]);
        tester.assert_return_type(DataType::Boolean);

        let result = tester
            .invoke_scalar_scalar("POINT (0 0)", "LINESTRING (2 0, 0 2)")
            .unwrap();
        tester.assert_scalar_result_equals(result, false);

        let result = tester
            .invoke_scalar_scalar(ScalarValue::Null, ScalarValue::Null)
            .unwrap();
        assert!(result.is_null());

        let arg1 = create_array(
            &[
                Some("POINT (0 0)"),
                Some("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
                None,
            ],
            &WKB_GEOMETRY,
        );
        let arg2 = create_array(
            &[
                Some("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"),
                Some("POINT (5 5)"),
                Some("POINT (0 0)"),
            ],
            &WKB_GEOMETRY,
        );

        let expected: ArrayRef = arrow_array!(Boolean, [Some(true), Some(false), None]);
        assert_array_equal(&tester.invoke_array_array(arg1, arg2).unwrap(), &expected);
    }
}
