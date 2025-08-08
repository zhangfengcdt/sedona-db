use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use datafusion_common::{error::Result, DataFusionError};
use datafusion_expr::ColumnarValue;
use geos::Geom;
use sedona_expr::scalar_udf::{ArgMatcher, ScalarKernelRef, SedonaScalarKernel};
use sedona_geometry::wkb_factory::WKB_MIN_PROBABLE_BYTES;
use sedona_schema::datatypes::{SedonaType, WKB_GEOMETRY};

use crate::executor::GeosExecutor;

/// ST_Intersection() implementation using the geos crate
pub fn st_intersection_impl() -> ScalarKernelRef {
    Arc::new(STIntersection {})
}

#[derive(Debug)]
struct STIntersection {}

impl SedonaScalarKernel for STIntersection {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry(), ArgMatcher::is_geometry()],
            WKB_GEOMETRY,
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let executor = GeosExecutor::new(arg_types, args);

        let mut builder = BinaryBuilder::with_capacity(
            executor.num_iterations(),
            WKB_MIN_PROBABLE_BYTES * executor.num_iterations(),
        );

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

fn invoke_scalar(geos_geom: &geos::Geometry, other_geos_geom: &geos::Geometry) -> Result<Vec<u8>> {
    let geometry = geos_geom.intersection(other_geos_geom).map_err(|e| {
        DataFusionError::Execution(format!("Failed to calculate intersection: {e}"))
    })?;

    let wkb = geometry
        .to_wkb()
        .map_err(|e| DataFusionError::Execution(format!("Failed to convert to WKB: {e}")))?;
    Ok(wkb.into())
}

#[cfg(test)]
mod tests {
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

        let udf = SedonaScalarUDF::from_kernel("st_intersection", st_intersection_impl());
        let tester = ScalarUdfTester::new(udf.into(), vec![sedona_type.clone(), sedona_type]);
        tester.assert_return_type(WKB_GEOMETRY);

        let result = tester
            .invoke_scalar_scalar("POINT (0 0)", "LINESTRING (0 0, 0 2)")
            .unwrap();
        tester.assert_scalar_result_equals(result, "POINT (0 0)");

        let result = tester
            .invoke_scalar_scalar(ScalarValue::Null, ScalarValue::Null)
            .unwrap();
        assert!(result.is_null());

        let arg1 = create_array(
            &[
                Some("POLYGON ((1 1, 8 1, 8 8, 1 8, 1 1))"),
                Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
                Some("POINT (0 0)"),
            ],
            &WKB_GEOMETRY,
        );
        let arg2 = create_array(
            &[
                Some("POLYGON ((2 2, 9 2, 9 9, 2 9, 2 2))"),
                Some("POINT (2 2)"),
                None,
            ],
            &WKB_GEOMETRY,
        );
        let expected = create_array(
            &[
                Some("POLYGON ((8 8, 8 2, 2 2, 2 8, 8 8))"),
                Some("POINT EMPTY"),
                None,
            ],
            &WKB_GEOMETRY,
        );
        assert_array_equal(&tester.invoke_array_array(arg1, arg2).unwrap(), &expected);
    }
}
