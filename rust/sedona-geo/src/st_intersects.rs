use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion_common::{error::Result, not_impl_err, ScalarValue};
use datafusion_expr::ColumnarValue;
use geo::{Geometry, Intersects};
use geo_traits::GeometryTrait;
use sedona_expr::scalar_udf::{ArgMatcher, ScalarKernelRef, SedonaScalarKernel};
use sedona_functions::iter_geo_traits;
use sedona_schema::datatypes::SedonaType;

use crate::to_geo::{item_to_geometry, scalar_arg_to_geometry};

/// ST_Intersects() implementation using [Intersects]
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
        _: &SedonaType,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        let (arg_type, arg, maybe_geometry) = match (&args[0], &args[1]) {
            (ColumnarValue::Array(_), ColumnarValue::Scalar(_)) => (
                &arg_types[0],
                &args[0],
                scalar_arg_to_geometry(&arg_types[1], &args[1])?,
            ),
            (ColumnarValue::Scalar(_), ColumnarValue::Array(_)) => (
                &arg_types[1],
                &args[1],
                scalar_arg_to_geometry(&arg_types[0], &args[0])?,
            ),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => (
                &arg_types[0],
                &args[0],
                scalar_arg_to_geometry(&arg_types[1], &args[1])?,
            ),
            _ => {
                return not_impl_err!("geo::ST_Intersects(Array, Array) not yet implemented");
            }
        };

        if let Some(geometry) = maybe_geometry {
            // Initialize an output builder of the appropriate type
            let mut builder = BooleanBuilder::with_capacity(num_rows);

            // Use iter_geo_traits to handle looping over the most appropriate GeometryTrait
            iter_geo_traits!(arg_type, arg, |_i, maybe_item| -> Result<()> {
                match maybe_item {
                    Some(item) => {
                        builder.append_value(invoke_scalar(item?, &geometry)?);
                    }
                    None => builder.append_null(),
                }

                Ok(())
            });

            // Create the output array
            let new_array = builder.finish();

            // Ensure that scalar input maps to scalar output
            match arg {
                ColumnarValue::Array(_) => Ok(ColumnarValue::Array(Arc::new(new_array))),
                ColumnarValue::Scalar(_) => Ok(ScalarValue::try_from_array(&new_array, 0)?.into()),
            }
        } else {
            // We have an intersection with a null scalar, so return a null scalar
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
        }
    }
}

fn invoke_scalar(item_a: impl GeometryTrait<T = f64>, geom_b: &Geometry) -> Result<bool> {
    let geom_a = item_to_geometry(item_a)?;
    Ok(geom_a.intersects(geom_b))
}

#[cfg(test)]
mod tests {
    use arrow_array::create_array;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use sedona_functions::register::stubs::st_intersects_udf;
    use sedona_schema::datatypes::WKB_GEOMETRY;
    use sedona_testing::{
        compare::assert_value_equal, create::create_array_value, create::create_scalar_value,
    };

    use super::*;

    #[test]
    fn scalar_scalar() {
        let mut sedona_udf = st_intersects_udf();
        sedona_udf.add_kernel(st_intersects_impl());
        let udf: ScalarUDF = sedona_udf.into();

        let point_scalar = create_scalar_value(Some("POINT (0.25 0.25)"), &WKB_GEOMETRY);
        let point2_scalar = create_scalar_value(Some("POINT (10 10)"), &WKB_GEOMETRY);
        let polygon_scalar =
            create_scalar_value(Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"), &WKB_GEOMETRY);
        let null_scalar = create_scalar_value(None, &WKB_GEOMETRY);

        // Check something that intersects with both argument orders
        assert_value_equal(
            &udf.invoke_batch(&[point_scalar.clone(), polygon_scalar.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(Some(true)).into(),
        );

        assert_value_equal(
            &udf.invoke_batch(&[polygon_scalar.clone(), point_scalar.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(Some(true)).into(),
        );

        // Check something that doesn't intersect with both argument orders
        assert_value_equal(
            &udf.invoke_batch(&[point2_scalar.clone(), polygon_scalar.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(Some(false)).into(),
        );

        assert_value_equal(
            &udf.invoke_batch(&[polygon_scalar.clone(), point2_scalar.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(Some(false)).into(),
        );

        // Check a null in both argument orders
        assert_value_equal(
            &udf.invoke_batch(&[null_scalar.clone(), polygon_scalar.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(None).into(),
        );

        assert_value_equal(
            &udf.invoke_batch(&[polygon_scalar.clone(), null_scalar.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(None).into(),
        );

        // ...and check a null as both arguments
        assert_value_equal(
            &udf.invoke_batch(&[null_scalar.clone(), null_scalar.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(None).into(),
        );
    }

    #[test]
    fn scalar_array() {
        let mut sedona_udf = st_intersects_udf();
        sedona_udf.add_kernel(st_intersects_impl());
        let udf: ScalarUDF = sedona_udf.into();

        let point_array = create_array_value(
            &[Some("POINT (0.25 0.25)"), Some("POINT (10 10)"), None],
            &WKB_GEOMETRY,
        );
        let polygon_scalar =
            create_scalar_value(Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"), &WKB_GEOMETRY);
        let null_scalar = create_scalar_value(None, &WKB_GEOMETRY);

        // Array, Scalar -> Array
        assert_value_equal(
            &udf.invoke_batch(&[point_array.clone(), polygon_scalar.clone()], 1)
                .unwrap(),
            &ColumnarValue::Array(create_array!(Boolean, [Some(true), Some(false), None])),
        );

        // Scalar, Array -> Array
        assert_value_equal(
            &udf.invoke_batch(&[polygon_scalar.clone(), point_array.clone()], 1)
                .unwrap(),
            &ColumnarValue::Array(create_array!(Boolean, [Some(true), Some(false), None])),
        );

        // Null Scalar, Array -> Null Scalar
        assert_value_equal(
            &udf.invoke_batch(&[point_array.clone(), null_scalar.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(None).into(),
        );

        // Array, Null Scalar -> Null Scalar
        assert_value_equal(
            &udf.invoke_batch(&[null_scalar.clone(), point_array.clone()], 1)
                .unwrap(),
            &ScalarValue::Boolean(None).into(),
        );
    }
}
