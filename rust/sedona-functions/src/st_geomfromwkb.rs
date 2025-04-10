use std::{sync::Arc, vec};

use arrow_schema::DataType;
use datafusion_common::cast::as_binary_array;
use datafusion_common::error::Result;
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_schema::datatypes::{WKB_GEOGRAPHY, WKB_GEOMETRY};
use sedona_schema::{
    datatypes::SedonaPhysicalType,
    udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF},
};

use crate::geo_iterator::{iter_wkb_array, try_iter_wkb_scalar};

/// ST_GeomFromWKB() scalar UDF implementation
///
/// An implementation of WKB reading using GeoRust's wkb crate.
pub fn st_geomfromwkb_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_geomfromwkb",
        vec![Arc::new(STGeomFromWKB {
            validate: true,
            out_type: WKB_GEOMETRY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeomFromWKB", "Geometry")),
    )
}

/// ST_GeogFromWKB() scalar UDF implementation
///
/// An implementation of WKB reading using GeoRust's wkb crate.
pub fn st_geogfromwkb_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_geogfromwkb",
        vec![Arc::new(STGeomFromWKB {
            validate: true,
            out_type: WKB_GEOGRAPHY,
        })],
        Volatility::Immutable,
        Some(doc("ST_GeogFromWKB", "Geography")),
    )
}

fn doc(name: &str, out_type_name: &str) -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        format!("Construct a {} from WKB", out_type_name),
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
struct STGeomFromWKB {
    validate: bool,
    out_type: SedonaPhysicalType,
}

impl SedonaScalarKernel for STGeomFromWKB {
    fn return_type(&self, args: &[SedonaPhysicalType]) -> Result<Option<SedonaPhysicalType>> {
        let matcher = ArgMatcher::new(vec![ArgMatcher::is_binary()], self.out_type.clone());

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        _: &[SedonaPhysicalType],
        _: &SedonaPhysicalType,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        if let ColumnarValue::Scalar(scalar) = &args[0] {
            if self.validate {
                for item in try_iter_wkb_scalar(scalar, 1)?.flatten() {
                    item?;
                }
            }

            return args[0].cast_to(&DataType::Binary, None);
        }

        let x_array = args[0].to_array(num_rows)?;
        if self.validate {
            let x_binary_array = as_binary_array(&x_array)?;
            for item in iter_wkb_array(x_binary_array).flatten() {
                item?;
            }
        }

        Ok(ColumnarValue::Array(x_array))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::create_array;
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use sedona_schema::datatypes::WKB_GEOMETRY;

    use crate::st_point::st_point_udf;

    use super::*;

    const POINT: [u8; 21] = [
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
    ];

    #[test]
    fn udf_array() -> Result<()> {
        let udf: ScalarUDF = st_geomfromwkb_udf().into();
        assert_eq!(udf.name(), "st_geomfromwkb");

        let n = 3;
        let xs = create_array!(Float64, [Some(1.0), Some(2.0), None]);
        let ys = create_array!(Float64, [5.0, 6.0, 7.0]);
        let st_point: ScalarUDF = st_point_udf().into();
        let array_from_point =
            st_point.invoke_batch(&[ColumnarValue::Array(xs), ColumnarValue::Array(ys)], n)?;
        let wkb_from_point = WKB_GEOMETRY.unwrap_arg(&array_from_point)?;

        let out = udf.invoke_batch(&[wkb_from_point.clone()], n)?;
        assert_eq!(&out.to_array(n)?, &array_from_point.to_array(n)?);

        Ok(())
    }

    #[test]
    fn udf_scalar() -> Result<()> {
        let udf: ScalarUDF = st_geomfromwkb_udf().into();

        let wkb_scalar = ScalarValue::Binary(Some(POINT.to_vec()));
        let out = udf.invoke_batch(&[wkb_scalar.clone().into()], 1)?;
        if let ColumnarValue::Scalar(out_scalar) = WKB_GEOMETRY.unwrap_arg(&out)? {
            assert_eq!(out_scalar, wkb_scalar)
        } else {
            panic!("Expected scalar binary but got {:?}", out);
        }

        Ok(())
    }

    #[test]
    fn udf_scalar_nulls() -> Result<()> {
        let udf: ScalarUDF = st_geomfromwkb_udf().into();

        let out = udf.invoke_batch(&[ScalarValue::Binary(None).into()], 1)?;
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
    fn udf_invalid_wkb() -> Result<()> {
        let udf: ScalarUDF = st_geomfromwkb_udf().into();

        let err = udf
            .invoke_batch(&[ScalarValue::Binary(Some(vec![])).into()], 1)
            .unwrap_err();

        assert_eq!(err.message(), "failed to fill whole buffer");

        Ok(())
    }

    #[test]
    fn udf_geog() -> Result<()> {
        let udf: ScalarUDF = st_geogfromwkb_udf().into();
        assert_eq!(udf.name(), "st_geogfromwkb");

        let wkb_scalar = ScalarValue::Binary(Some(POINT.to_vec()));
        let out = udf.invoke_batch(&[wkb_scalar.clone().into()], 1)?;
        assert_eq!(
            SedonaPhysicalType::from_data_type(&out.data_type())?,
            WKB_GEOGRAPHY
        );

        Ok(())
    }
}
