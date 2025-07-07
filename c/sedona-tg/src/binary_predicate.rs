use std::{iter::zip, sync::Arc};

use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion_common::{cast::as_binary_view_array, error::Result, internal_err, ScalarValue};
use datafusion_expr::ColumnarValue;
use sedona_expr::scalar_udf::{ArgMatcher, ScalarKernelRef, SedonaScalarKernel};
use sedona_functions::executor::GenericExecutor;
use sedona_schema::datatypes::SedonaType;

use crate::tg;

/// ST_Equals() implementation using tg
pub fn st_equals_impl() -> ScalarKernelRef {
    Arc::new(TgPredicate::<tg::Equals>::default())
}

/// ST_Intersects() implementation using tg
pub fn st_intersects_impl() -> ScalarKernelRef {
    Arc::new(TgPredicate::<tg::Intersects>::default())
}

/// ST_Disjoint() implementation using tg
pub fn st_disjoint_impl() -> ScalarKernelRef {
    Arc::new(TgPredicate::<tg::Disjoint>::default())
}

/// ST_Contains() implementation using tg
pub fn st_contains_impl() -> ScalarKernelRef {
    Arc::new(TgPredicate::<tg::Contains>::default())
}

/// ST_Within() implementation using tg
pub fn st_within_impl() -> ScalarKernelRef {
    Arc::new(TgPredicate::<tg::Within>::default())
}

/// ST_Covers() implementation using tg
pub fn st_covers_impl() -> ScalarKernelRef {
    Arc::new(TgPredicate::<tg::Covers>::default())
}

/// ST_CoveredBy() implementation using tg
pub fn st_covered_by_impl() -> ScalarKernelRef {
    Arc::new(TgPredicate::<tg::CoveredBy>::default())
}

/// ST_Touches() implementation using tg
pub fn st_touches_impl() -> ScalarKernelRef {
    Arc::new(TgPredicate::<tg::Touches>::default())
}

#[derive(Debug)]
struct TgPredicate<Op> {
    _op: Op,
    index: tg::IndexType,
}

impl<Op: Default> Default for TgPredicate<Op> {
    fn default() -> Self {
        Self {
            _op: Default::default(),
            index: tg::IndexType::YStripes,
        }
    }
}

impl<Op: tg::BinaryPredicate> SedonaScalarKernel for TgPredicate<Op> {
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
        let executor = GenericExecutor::new(arg_types, args);
        let mut builder = BooleanBuilder::with_capacity(executor.num_iterations());

        eval_value_value::<Op>(
            &args[0],
            &args[1],
            executor.num_iterations(),
            self.index,
            &mut builder,
        )?;

        executor.finish(Arc::new(builder.finish()))
    }
}

fn eval_value_value<Op: tg::BinaryPredicate>(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    iterations: usize,
    index: tg::IndexType,
    builder: &mut BooleanBuilder,
) -> Result<()> {
    let lhs = lhs.cast_to(&DataType::BinaryView, None)?;
    let rhs = rhs.cast_to(&DataType::BinaryView, None)?;
    match (&lhs, &rhs) {
        (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar_value))
        | (ColumnarValue::Scalar(scalar_value), ColumnarValue::Array(array)) => {
            let array = as_binary_view_array(&array)?;

            if let ScalarValue::BinaryView(value) = scalar_value {
                return eval_array_scalar::<Op>(
                    array.iter(),
                    value.as_deref(),
                    tg::IndexType::Unindexed,
                    index,
                    builder,
                );
            } else {
                return internal_err!("Unexpected scalar value: {scalar_value:?}");
            }
        }
        _ => {}
    }

    let lhs_array = lhs.to_array(iterations)?;
    let rhs_array = rhs.to_array(iterations)?;
    let lhs = as_binary_view_array(&lhs_array)?;
    let rhs = as_binary_view_array(&rhs_array)?;

    eval_array_array::<Op>(lhs.iter(), rhs.iter(), index, index, builder)
}

fn eval_array_scalar<'a, 'b, Op: tg::BinaryPredicate>(
    lhs_iter: impl ExactSizeIterator<Item = Option<&'a [u8]>>,
    rhs: Option<&'b [u8]>,
    lhs_index: tg::IndexType,
    rhs_index: tg::IndexType,
    builder: &mut BooleanBuilder,
) -> Result<()> {
    if let Some(rhs) = rhs {
        let rhs_geom = tg::Geom::parse_wkb(rhs, rhs_index)?;

        for lhs in lhs_iter {
            if let Some(lhs) = lhs {
                let lhs_geom = tg::Geom::parse_wkb(lhs, lhs_index)?;
                builder.append_value(Op::evaluate(&lhs_geom, &rhs_geom));
            } else {
                builder.append_null();
            }
        }
    } else {
        builder.append_nulls(lhs_iter.len());
    }

    Ok(())
}

fn eval_array_array<'a, 'b, Op: tg::BinaryPredicate>(
    lhs_iter: impl ExactSizeIterator<Item = Option<&'a [u8]>>,
    rhs_iter: impl ExactSizeIterator<Item = Option<&'b [u8]>>,
    lhs_index: tg::IndexType,
    rhs_index: tg::IndexType,
    builder: &mut BooleanBuilder,
) -> Result<()> {
    for (lhs, rhs) in zip(lhs_iter, rhs_iter) {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => {
                let lhs_geom = tg::Geom::parse_wkb(lhs, lhs_index)?;
                let rhs_geom = tg::Geom::parse_wkb(rhs, rhs_index)?;
                builder.append_value(Op::evaluate(&lhs_geom, &rhs_geom));
            }
            _ => builder.append_null(),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_array::create_array;
    use datafusion_common::scalar::ScalarValue;
    use sedona_functions::register::stubs::st_intersects_udf;
    use sedona_schema::datatypes::WKB_GEOMETRY;
    use sedona_testing::{
        compare::assert_value_equal, create::create_array_value, create::create_scalar_value,
    };

    use super::*;

    #[test]
    fn scalar_scalar() {
        let mut udf = st_intersects_udf();
        udf.add_kernel(st_intersects_impl());

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
        let mut udf = st_intersects_udf();
        udf.add_kernel(st_intersects_impl());

        let point_array = create_array_value(
            &[Some("POINT (0.25 0.25)"), Some("POINT (10 10)"), None],
            &WKB_GEOMETRY,
        );
        let polygon_scalar =
            create_scalar_value(Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"), &WKB_GEOMETRY);

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
    }

    #[test]
    fn array_array() {
        let mut udf = st_intersects_udf();
        udf.add_kernel(st_intersects_impl());

        let point_array = create_array_value(
            &[
                Some("POINT (0.25 0.25)"),
                Some("POINT (10 10)"),
                None,
                Some("POINT (0.25 0.25)"),
            ],
            &WKB_GEOMETRY,
        );
        let polygon_array = create_array_value(
            &[
                Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
                Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
                Some("POLYGON ((0 0, 1 0, 0 1, 0 0))"),
                None,
            ],
            &WKB_GEOMETRY,
        );

        // Array, Array -> Array
        assert_value_equal(
            &udf.invoke_batch(&[point_array, polygon_array], 1).unwrap(),
            &ColumnarValue::Array(create_array!(
                Boolean,
                [Some(true), Some(false), None, None]
            )),
        );
    }
}
