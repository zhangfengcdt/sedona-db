use std::{iter::zip, sync::Arc, vec};

use arrow_array::builder::BinaryBuilder;
use arrow_schema::DataType;
use datafusion::{common::cast::as_float64_array, error::Result, scalar::ScalarValue};
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_schema::{
    datatypes::{SedonaPhysicalType, WKB_GEOGRAPHY, WKB_GEOMETRY},
    udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF},
};

/// ST_Point() scalar UDF implementation
///
/// Native implementation to create geometries from coordinates.
/// See [`st_geogpoint_udf`] for the corresponding geography constructor.
pub fn st_point_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "ST_Point",
        vec![Arc::new(STGeoFromPoint {
            out_type: WKB_GEOMETRY,
        })],
        Volatility::Immutable,
        Some(doc("ST_Point", "Geometry")),
    )
}

/// ST_GeogPoint() scalar UDF implementation
///
/// Native implementation to create geometries from coordinates.
/// See [`st_geogpoint_udf`] for the corresponding geography constructor.
pub fn st_geogpoint_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "ST_GeogPoint",
        vec![Arc::new(STGeoFromPoint {
            out_type: WKB_GEOGRAPHY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeogPoint", "Geography")),
    )
}

fn doc(name: &str, out_type_name: &str) -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        format!(
            "Construct a Point {} from X and Y",
            out_type_name.to_lowercase()
        ),
        format!("{}(-64.36, 45.09)", name),
    )
    .with_argument("x", "double: X value")
    .with_argument("y", "double: Y value")
    .build()
}

#[derive(Debug)]
struct STGeoFromPoint {
    out_type: SedonaPhysicalType,
}

impl SedonaScalarKernel for STGeoFromPoint {
    fn return_type(&self, args: &[SedonaPhysicalType]) -> Result<Option<SedonaPhysicalType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_numeric(), ArgMatcher::is_numeric()],
            self.out_type.clone(),
        );
        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        _: &[SedonaPhysicalType],
        _: &SedonaPhysicalType,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        let x = &args[0];
        let y = &args[1];

        let mut item: [u8; 21] = [0x00; 21];
        item[0] = 0x01;
        item[1] = 0x01;

        // Handle the Scalar case to ensure that Scalar + Scalar -> Scalar
        if let (ColumnarValue::Scalar(x_scalar), ColumnarValue::Scalar(y_scalar)) = (x, y) {
            let scalar_floats = (
                x_scalar.cast_to(&DataType::Float64)?,
                y_scalar.cast_to(&DataType::Float64)?,
            );
            if let (ScalarValue::Float64(x_float), ScalarValue::Float64(y_float)) = scalar_floats {
                if let (Some(x), Some(y)) = (x_float, y_float) {
                    populate_wkb_item(&mut item, x, y);
                    return Ok(ScalarValue::Binary(Some(item.to_vec())).into());
                } else {
                    return Ok(ScalarValue::Binary(None).into());
                }
            } else {
                unreachable!()
            }
        }

        let x_array = x.to_array(num_rows)?;
        let y_array = y.to_array(num_rows)?;
        let x_f64 = as_float64_array(&x_array)?;
        let y_f64 = as_float64_array(&y_array)?;

        let mut builder = BinaryBuilder::with_capacity(num_rows, item.len() * num_rows);

        for (x_elem, y_elem) in zip(x_f64, y_f64) {
            match (x_elem, y_elem) {
                (Some(x), Some(y)) => {
                    populate_wkb_item(&mut item, x, y);
                    builder.append_value(item);
                }
                _ => {
                    builder.append_null();
                }
            }
        }

        let new_array = builder.finish();
        Ok(ColumnarValue::Array(Arc::new(new_array)))
    }
}

fn populate_wkb_item(item: &mut [u8], x: f64, y: f64) {
    item[5..13].copy_from_slice(&x.to_le_bytes());
    item[13..21].copy_from_slice(&y.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use arrow_array::create_array;
    use arrow_schema::DataType;
    use datafusion::{common::cast::as_binary_array, scalar::ScalarValue};
    use geo_traits::{to_geo::ToGeoGeometry, Dimensions, GeometryTrait};
    use geo_types::Point;
    use sedona_schema::datatypes::SedonaPhysicalType;

    use super::*;

    #[test]
    fn udf_signature() -> Result<()> {
        let udf = st_point_udf().to_udf();

        // All numeric combinations should work
        assert_eq!(
            udf.return_type(&[DataType::Float64, DataType::Float64])?,
            WKB_GEOMETRY.data_type()
        );

        assert_eq!(
            udf.return_type(&[DataType::Int8, DataType::Float16])?,
            WKB_GEOMETRY.data_type()
        );

        // Non-numeric things should not work
        assert_eq!(
            udf.return_type(&[DataType::Utf8, DataType::Float64])
                .unwrap_err()
                .message(),
            "ST_Point([Arrow(Utf8), Arrow(Float64)]): No kernel matching arguments"
        );

        // Wrong number of args
        assert_eq!(
            udf.return_type(&[]).unwrap_err().message(),
            "ST_Point([]): No kernel matching arguments"
        );

        Ok(())
    }

    #[test]
    fn udf_array() -> Result<()> {
        let udf = st_point_udf().to_udf();
        assert_eq!(udf.name(), "ST_Point");

        let n = 3;
        let xs = create_array!(Float64, [Some(1.0), Some(2.0), None]);
        let ys = create_array!(Float64, [5.0, 6.0, 7.0]);

        let out = udf.invoke_batch(
            &[
                ColumnarValue::Array(xs.clone()),
                ColumnarValue::Array(ys.clone()),
            ],
            n,
        )?;
        match &out {
            ColumnarValue::Array(_) => (),
            ColumnarValue::Scalar(_) => panic!("Expected array"),
        }

        let out_binary = WKB_GEOMETRY.unwrap_arg(&out)?.to_array(n)?;

        assert_eq!(out_binary.len(), n);
        assert!(WKB_GEOMETRY.match_signature(&out.data_type().try_into().unwrap()));
        for (i, item) in as_binary_array(&out_binary)?.iter().enumerate() {
            if i == 2 {
                assert!(item.is_none());
                continue;
            }

            let wkb_item = wkb::reader::read_wkb(item.unwrap()).unwrap();
            assert_eq!(wkb_item.dim(), Dimensions::Xy);
            let point: Point = wkb_item.to_geometry().try_into().unwrap();
            assert_eq!(point.x(), xs.value(i));
            assert_eq!(point.y(), ys.value(i));
        }

        Ok(())
    }

    #[test]
    fn udf_scalar() -> Result<()> {
        let udf = st_point_udf().to_udf();

        let out = udf.invoke_batch(
            &[
                ScalarValue::Float64(Some(1.0)).into(),
                ScalarValue::Float64(Some(2.0)).into(),
            ],
            3,
        )?;

        if let ColumnarValue::Scalar(out_scalar) = WKB_GEOMETRY.unwrap_arg(&out)? {
            if let ScalarValue::Binary(out_binary) = out_scalar {
                let bytes = out_binary.unwrap();
                let wkb_item = wkb::reader::read_wkb(&bytes).unwrap();
                assert_eq!(wkb_item.dim(), Dimensions::Xy);
                let point: Point = wkb_item.to_geometry().try_into().unwrap();
                assert_eq!(point.x(), 1.0);
                assert_eq!(point.y(), 2.0);
            } else {
                panic!("Expected binary scalar but got {:?}", out);
            }
        } else {
            panic!("Expected scalar but got {:?}", out);
        }

        Ok(())
    }

    #[test]
    fn udf_scalar_null() -> Result<()> {
        let udf = st_point_udf().to_udf();

        let out = udf.invoke_batch(
            &[
                ScalarValue::Float64(Some(1.0)).into(),
                ScalarValue::Float64(None).into(),
            ],
            3,
        )?;

        if let ColumnarValue::Scalar(out_scalar) = WKB_GEOMETRY.unwrap_arg(&out)? {
            if let ScalarValue::Binary(out_binary) = out_scalar {
                assert!(out_binary.is_none());
            } else {
                panic!("Expected binary scalar but got {:?}", out);
            }
        } else {
            panic!("Expected scalar but got {:?}", out);
        }

        Ok(())
    }

    #[test]
    fn udf_geog() -> Result<()> {
        let udf = st_geogpoint_udf().to_udf();
        assert_eq!(udf.name(), "ST_GeogPoint");

        let out = udf.invoke_batch(
            &[
                ScalarValue::Float64(None).into(),
                ScalarValue::Float64(None).into(),
            ],
            3,
        )?;

        assert_eq!(
            SedonaPhysicalType::from_data_type(&out.data_type())?,
            WKB_GEOGRAPHY
        );

        Ok(())
    }
}
