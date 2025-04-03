use std::{str::FromStr, sync::Arc, vec};

use arrow_array::builder::BinaryBuilder;
use arrow_schema::DataType;
use datafusion_common::cast::as_string_array;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_common::scalar::ScalarValue;
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_schema::{
    datatypes::{SedonaPhysicalType, WKB_GEOGRAPHY, WKB_GEOMETRY},
    udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF},
};
use wkb::writer::write_geometry;
use wkt::Wkt;

/// ST_GeomFromWKT() UDF implementation
///
/// An implementation of WKT reading using GeoRust's wkt crate.
/// See [`st_geogfromwkt_udf`] for the corresponding geography function.
pub fn st_geomfromwkt_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_geomfromwkt",
        vec![Arc::new(STGeoFromWKT {
            out_type: WKB_GEOMETRY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeomFromWKT", "Geometry")),
    )
}

/// ST_GeogFromWKT() UDF implementation
///
/// An implementation of WKT reading using GeoRust's wkt crate.
/// See [`st_geomfromwkt_udf`] for the corresponding geometry function.
pub fn st_geogfromwkt_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_geogfromwkt",
        vec![Arc::new(STGeoFromWKT {
            out_type: WKB_GEOGRAPHY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeogFromWKT", "Geography")),
    )
}

fn doc(name: &str, out_type_name: &str) -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        format!("Construct a {} from WKT", out_type_name),
        format!("SELECT {name}('POINT(40.7128 -74.0060)')"),
    )
    .with_argument(
        "WKT",
        format!(
            "string: Well-known text representation of the {}",
            out_type_name.to_lowercase()
        ),
    )
    .with_related_udf("ST_AsText")
    .build()
}

#[derive(Debug)]
struct STGeoFromWKT {
    out_type: SedonaPhysicalType,
}

impl SedonaScalarKernel for STGeoFromWKT {
    fn return_type(&self, args: &[SedonaPhysicalType]) -> Result<Option<SedonaPhysicalType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_arrow(DataType::Utf8)],
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
        let wkt = &args[0];

        // Would be slightly better to write directly to the builder's
        // data buffer but not sure exactly how to do that.
        let mut item_out = Vec::<u8>::with_capacity(64);

        if let ColumnarValue::Scalar(ScalarValue::Utf8(x_scalar)) = wkt {
            match x_scalar {
                Some(wkt_bytes) => {
                    invoke_scalar(wkt_bytes, &mut item_out)?;
                    return Ok(ScalarValue::Binary(Some(item_out)).into());
                }
                None => {
                    return Ok(ScalarValue::Binary(None).into());
                }
            }
        }

        let x_array = wkt.to_array(num_rows)?;
        let x_utf8 = as_string_array(&x_array)?;

        let mut builder = BinaryBuilder::with_capacity(num_rows, 21 * num_rows);

        for item in x_utf8 {
            if let Some(wkt_bytes) = item {
                invoke_scalar(wkt_bytes, &mut item_out)?;
                builder.append_value(&item_out);
            } else {
                builder.append_null();
            }
        }

        let new_array = builder.finish();
        Ok(ColumnarValue::Array(Arc::new(new_array)))
    }
}

fn invoke_scalar(wkt_bytes: &str, mut item_out: &mut Vec<u8>) -> Result<()> {
    let geometry: Wkt<f64> = Wkt::from_str(wkt_bytes)
        .map_err(|err| DataFusionError::Internal(format!("WKT parse error: {}", err)))?;

    item_out.truncate(0);
    write_geometry(&mut item_out, &geometry, wkb::Endianness::LittleEndian)
        .map_err(|err| DataFusionError::Internal(format!("WKB write error: {}", err)))
}

#[cfg(test)]
mod tests {
    use arrow_array::create_array;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use SedonaPhysicalType;

    use crate::st_point::st_point_udf;

    use super::*;

    #[test]
    fn udf_array() -> Result<()> {
        let udf: ScalarUDF = st_geomfromwkt_udf().into();
        assert_eq!(udf.name(), "st_geomfromwkt");

        let n = 3;
        let xs = create_array!(Float64, [Some(1.0), Some(2.0), None]);
        let ys = create_array!(Float64, [5.0, 6.0, 7.0]);
        let st_point: ScalarUDF = st_point_udf().into();
        let array_from_point =
            st_point.invoke_batch(&[ColumnarValue::Array(xs), ColumnarValue::Array(ys)], n)?;

        let wkt_point = create_array!(Utf8, [Some("POINT (1 5)"), Some("POINT (2 6)"), None]);
        let out = udf.invoke_batch(&[ColumnarValue::Array(wkt_point)], n)?;
        assert_eq!(&out.to_array(n)?, &array_from_point.to_array(n)?);

        Ok(())
    }

    #[test]
    fn udf_scalar() -> Result<()> {
        let udf: ScalarUDF = st_geomfromwkt_udf().into();

        let st_point: ScalarUDF = st_point_udf().into();
        let wkb_point = st_point.invoke_batch(
            &[
                ScalarValue::Float64(Some(1.0)).into(),
                ScalarValue::Float64(Some(2.0)).into(),
            ],
            1,
        )?;
        let wkb_from_point =
            if let ColumnarValue::Scalar(s) = WKB_GEOMETRY.unwrap_arg(&wkb_point)? {
                s
            } else {
                panic!("Expected scalar but got {:?}", wkb_point);
            };

        let out = udf.invoke_batch(
            &[ScalarValue::Utf8(Some("POINT (1 2)".to_string())).into()],
            1,
        )?;

        if let ColumnarValue::Scalar(wkb_from_text) = WKB_GEOMETRY.unwrap_arg(&out)? {
            assert_eq!(wkb_from_text, wkb_from_point);
        } else {
            panic!("Expected scalar but got {:?}", out);
        }

        Ok(())
    }

    #[test]
    fn udf_scalar_nulls() -> Result<()> {
        let udf: ScalarUDF = st_geomfromwkt_udf().into();

        let out = udf.invoke_batch(&[ScalarValue::Utf8(None).into()], 1)?;
        if let ColumnarValue::Scalar(ScalarValue::Binary(out_binary)) =
            WKB_GEOMETRY.unwrap_arg(&out)?
        {
            assert!(out_binary.is_none());
        } else {
            panic!("Expected scalar binary but got {:?}", out);
        }

        Ok(())
    }

    #[test]
    fn udf_invalid_wkt() -> Result<()> {
        let udf: ScalarUDF = st_geomfromwkt_udf().into();

        let err = udf
            .invoke_batch(
                &[ScalarValue::Utf8(Some("this is not valid wkt".to_string())).into()],
                1,
            )
            .unwrap_err();

        assert!(err.message().starts_with("WKT parse error"));

        Ok(())
    }

    #[test]
    fn udf_geog() -> Result<()> {
        let udf: ScalarUDF = st_geogfromwkt_udf().into();
        assert_eq!(udf.name(), "st_geogfromwkt");

        let out = udf.invoke_batch(
            &[ScalarValue::Utf8(Some("POINT (1 2)".to_string())).into()],
            1,
        )?;

        assert_eq!(
            SedonaPhysicalType::from_data_type(&out.data_type())?,
            WKB_GEOGRAPHY
        );

        Ok(())
    }
}
