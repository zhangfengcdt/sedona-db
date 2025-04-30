use arrow_array::{BinaryArray, BinaryViewArray};
use datafusion_common::error::Result;
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use geo_traits_ext::GeometryTraitExt as GeometryTrait;

/// Iterate over an array whose items can be viewed as a [`GeometryTrait`]
///
/// This macro handles dispatching a function of `(i: usize, item: Option<impl GeometryTrait>)`
/// to the appropriate iterator. This needs to be a macro because it is not possible
/// to return two `impl ExactSizeIterator` from multiple branches in a function, and
/// there are several cases we have to consider that return different iterator types
/// (e.g., scalar, array, and in the future GeoArrow types).
///
/// This macro is invoked with the SedonaType of the argument, the ColumnarValue
/// argument, and the function to call for each element (that returns `Result<()>`).
/// For scalars the function is called exactly once; for array input the function is
/// called once per element. Generally the function will perform the computation and
/// add the result to some mutable output builder.
#[macro_export]
macro_rules! iter_geo_traits {
    ( $arg_type:expr, $arg:expr, $lambda:expr ) => {{
        match $arg {
            ColumnarValue::Scalar(scalar) => {
                let geo_item = $crate::iter_geo_traits::parse_wkb_scalar(scalar);
                ($lambda)(0, geo_item)?;
            }
            ColumnarValue::Array(array) => match $arg_type {
                sedona_schema::datatypes::SedonaType::Wkb(_, _) => {
                    #[allow(unused_mut)]
                    let mut func = $lambda;
                    let concrete_array = datafusion_common::cast::as_binary_array(array)?;
                    for (i, maybe_item) in
                        $crate::iter_geo_traits::iter_wkb_binary(concrete_array).enumerate()
                    {
                        func(i, maybe_item)?
                    }
                }
                sedona_schema::datatypes::SedonaType::WkbView(..) => {
                    #[allow(unused_mut)]
                    let mut func = $lambda;
                    let concrete_array = datafusion_common::cast::as_binary_view_array(array)?;
                    for (i, maybe_item) in
                        $crate::iter_geo_traits::iter_wkb_binary_view(concrete_array).enumerate()
                    {
                        func(i, maybe_item)?
                    }
                }
                _ => {
                    return datafusion_common::internal_err!(
                        "Can't iterate over {:?} as GeometryTrait",
                        $arg_type
                    );
                }
            },
        }
    }};
}

pub fn iter_wkb_binary(
    array: &BinaryArray,
) -> impl ExactSizeIterator<Item = Option<Result<impl GeometryTrait<T = f64> + '_>>> {
    array.iter().map(parse_wkb_item)
}

pub fn iter_wkb_binary_view(
    array: &BinaryViewArray,
) -> impl ExactSizeIterator<Item = Option<Result<impl GeometryTrait<T = f64> + '_>>> {
    array.iter().map(parse_wkb_item)
}

pub fn parse_wkb_scalar(scalar: &ScalarValue) -> Option<Result<impl GeometryTrait<T = f64> + '_>> {
    match scalar {
        ScalarValue::Binary(maybe_item) | ScalarValue::BinaryView(maybe_item) => {
            match maybe_item {
                // This at least does the initial scan exactly once, although it would be
                // better to parse this into a structure that has more efficient access
                // on each iteration
                Some(vec_value) => parse_wkb_item(Some(vec_value)),
                None => None,
            }
        }
        _ => Some(internal_err!("Can't iterate over {:?} as WKB", scalar)),
    }
}

/// Parse a single well-known binary item
fn parse_wkb_item(maybe_item: Option<&[u8]>) -> Option<Result<impl GeometryTrait<T = f64> + '_>> {
    match maybe_item {
        Some(item) => {
            let geometry =
                wkb::reader::read_wkb(item).map_err(|err| DataFusionError::External(Box::new(err)));
            Some(geometry)
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Arc;

    use arrow_array::builder::BinaryBuilder;
    use arrow_schema::DataType;
    use datafusion_common::{cast::as_binary_view_array, scalar::ScalarValue};
    use datafusion_expr::ColumnarValue;
    use sedona_schema::datatypes::{WKB_GEOMETRY, WKB_VIEW_GEOMETRY};

    use super::*;

    const POINT: [u8; 21] = [
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
    ];

    fn write_to_test_output(
        wkt_out: &mut String,
        i: usize,
        maybe_geom: Option<Result<impl GeometryTrait<T = f64>>>,
    ) -> Result<()> {
        write!(wkt_out, " {i}: ").unwrap();
        match maybe_geom {
            Some(geom) => wkt::to_wkt::write_geometry(wkt_out, &geom?).unwrap(),
            None => write!(wkt_out, "None").unwrap(),
        };

        Ok(())
    }

    #[test]
    fn wkb_array() -> Result<()> {
        let mut builder = BinaryBuilder::new();
        builder.append_value(POINT);
        builder.append_null();
        builder.append_value(POINT);
        let wkb_array = builder.finish();

        let out: Vec<_> = iter_wkb_binary(&wkb_array).map(|x| x.is_some()).collect();
        assert_eq!(out, vec![true, false, true]);

        let mut wkt_out = String::new();
        iter_geo_traits!(
            &WKB_GEOMETRY,
            &ColumnarValue::Array(Arc::new(wkb_array.clone())),
            |i, maybe_geom| -> Result<()> {
                write_to_test_output(&mut wkt_out, i, maybe_geom).unwrap();
                Ok(())
            }
        );
        assert_eq!(wkt_out, " 0: POINT(1 2) 1: None 2: POINT(1 2)");

        let wkb_view_array_ref = ColumnarValue::Array(Arc::new(wkb_array))
            .cast_to(&DataType::BinaryView, None)
            .unwrap()
            .to_array(3)
            .unwrap();
        let wkb_view_array = as_binary_view_array(&wkb_view_array_ref).unwrap();
        let out: Vec<_> = iter_wkb_binary_view(wkb_view_array)
            .map(|x| x.is_some())
            .collect();
        assert_eq!(out, vec![true, false, true]);

        let mut wkt_out = String::new();
        iter_geo_traits!(
            &WKB_VIEW_GEOMETRY,
            &ColumnarValue::Array(wkb_view_array_ref),
            |i, maybe_geom| -> Result<()> {
                write_to_test_output(&mut wkt_out, i, maybe_geom).unwrap();
                Ok(())
            }
        );
        assert_eq!(wkt_out, " 0: POINT(1 2) 1: None 2: POINT(1 2)");

        Ok(())
    }

    #[test]
    fn wkb_scalar() -> Result<()> {
        assert!(parse_wkb_scalar(&ScalarValue::Binary(None)).is_none());

        let mut wkt_out = String::new();
        let binary_scalar = ScalarValue::Binary(Some(POINT.to_vec()));
        let wkb_item = parse_wkb_scalar(&binary_scalar).unwrap().unwrap();
        wkt::to_wkt::write_geometry(&mut wkt_out, &wkb_item).unwrap();
        assert_eq!(wkt_out, "POINT(1 2)");
        drop(wkb_item);

        let mut wkt_out = String::new();
        iter_geo_traits!(
            &WKB_GEOMETRY,
            &ColumnarValue::Scalar(binary_scalar),
            |i, maybe_geom| -> Result<()> {
                write_to_test_output(&mut wkt_out, i, maybe_geom).unwrap();
                Ok(())
            }
        );
        assert_eq!(wkt_out, " 0: POINT(1 2)");

        let mut wkt_out = String::new();
        let binary_scalar = ScalarValue::BinaryView(Some(POINT.to_vec()));
        let wkb_item = parse_wkb_scalar(&binary_scalar).unwrap().unwrap();
        wkt::to_wkt::write_geometry(&mut wkt_out, &wkb_item).unwrap();
        assert_eq!(wkt_out, "POINT(1 2)");
        drop(wkb_item);

        let mut wkt_out = String::new();
        iter_geo_traits!(
            &WKB_GEOMETRY,
            &ColumnarValue::Scalar(binary_scalar),
            |i, maybe_geom| -> Result<()> {
                write_to_test_output(&mut wkt_out, i, maybe_geom).unwrap();
                Ok(())
            }
        );
        assert_eq!(wkt_out, " 0: POINT(1 2)");

        let err = parse_wkb_scalar(&ScalarValue::Date32(Some(1))).unwrap();
        assert!(err.is_err_and(|e| e.message().contains("Can't iterate over")));

        Ok(())
    }

    #[test]
    fn wkb_item() {
        assert!(parse_wkb_item(None).is_none());

        let wkb_item = parse_wkb_item(Some(&POINT)).unwrap().unwrap();
        let mut wkt_out = String::new();
        wkt::to_wkt::write_geometry(&mut wkt_out, &wkb_item).unwrap();
        assert_eq!(wkt_out, "POINT(1 2)");

        let err = parse_wkb_item(Some(&[])).unwrap();
        assert!(err.is_err());
    }
}
