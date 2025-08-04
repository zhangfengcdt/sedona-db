use std::sync::Arc;

use arrow_array::builder::Float64Builder;
use arrow_schema::DataType;
use datafusion_common::{error::Result, DataFusionError};
use datafusion_expr::ColumnarValue;
use geos::Geom;
use sedona_expr::scalar_udf::{ArgMatcher, ScalarKernelRef, SedonaScalarKernel};
use sedona_functions::executor::GenericExecutor;
use sedona_schema::datatypes::SedonaType;
use wkb::reader::Wkb;

/// ST_Area() implementation using the geos crate
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
    let geos_geom = geos::Geometry::new_from_wkb(wkb.buf()).map_err(|e| {
        DataFusionError::Execution(format!("Failed to create GEOS geometry from WKB: {e}"))
    })?;
    geos_geom
        .area()
        .map_err(|e| DataFusionError::Execution(format!("Failed to calculate area: {e}")))
}

#[cfg(test)]
mod tests {
    use arrow_array::{create_array, ArrayRef};
    use datafusion_common::scalar::ScalarValue;
    use rstest::rstest;
    use sedona_functions::register::stubs::st_area_udf;
    use sedona_schema::datatypes::{WKB_GEOMETRY, WKB_VIEW_GEOMETRY};
    use sedona_testing::testers::ScalarUdfTester;

    use super::*;

    #[rstest]
    fn udf(#[values(WKB_GEOMETRY, WKB_VIEW_GEOMETRY)] sedona_type: SedonaType) {
        let mut udf = st_area_udf();
        udf.add_kernel(st_area_impl());
        let tester = ScalarUdfTester::new(udf.into(), vec![sedona_type]);

        assert_eq!(
            tester.return_type().unwrap(),
            SedonaType::Arrow(DataType::Float64)
        );

        assert_eq!(
            tester
                .invoke_wkb_scalar(Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"))
                .unwrap(),
            ScalarValue::Float64(Some(0.5))
        );

        let input_wkt = vec![
            Some("POINT(1 2)"),
            None,
            Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
        ];
        let expected: ArrayRef = create_array!(Float64, [Some(0.0), None, Some(0.5)]);
        assert_eq!(&tester.invoke_wkb_array(input_wkt).unwrap(), &expected);
    }
}
