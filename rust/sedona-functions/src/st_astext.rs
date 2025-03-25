use std::{sync::Arc, vec};

use arrow_array::builder::StringBuilder;
use arrow_schema::DataType;
use datafusion::{
    common::cast::as_binary_array,
    error::{DataFusionError, Result},
};
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_schema::{
    datatypes::SedonaPhysicalType,
    udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF},
};
use wkb::reader::read_wkb;
use wkt::to_wkt::write_geometry;

/// ST_AsText() scalar UDF implementation
///
/// An implementation of WKT writing using GeoRust's wkt crate.
pub fn st_astext_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "ST_AsText",
        vec![Arc::new(STAsText {})],
        Volatility::Immutable,
        Some(st_astext_doc()),
    )
}

fn st_astext_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Return the Well-Known Text string representation of a geometry or geography",
        "SELECT ST_AsText(ST_Point(1.0, 2.0))",
    )
    .with_argument("geom", "geometry: Input geometry or geography")
    .with_related_udf("ST_GeomFromWKT")
    .build()
}

#[derive(Debug)]
struct STAsText {}

impl SedonaScalarKernel for STAsText {
    fn return_type(&self, args: &[SedonaPhysicalType]) -> Result<Option<SedonaPhysicalType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry_or_geography()],
            DataType::Utf8.try_into().unwrap(),
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
        let x_array = &args[0].to_array(num_rows)?;
        let x_binary_array = as_binary_array(&x_array)?;

        // Use the WKB size as the proxy for the WKT size.
        // WKT is ~2.5 the size of WKB at full double precision (16), although WKT is up to
        // 50% smaller at very low precision (probably rare).
        let max_theoretical_wkt_size: f64 = x_binary_array.value_data().len() as f64 * 2.5;
        let mut builder =
            StringBuilder::with_capacity(num_rows, max_theoretical_wkt_size.floor() as usize);

        // Would be slightly better to write directly to the builder's
        // data buffer but not sure exactly how to do that
        let mut item_out = String::with_capacity(64);

        for item in x_binary_array {
            if let Some(wkb_bytes) = item {
                let geometry = read_wkb(wkb_bytes).map_err(|err| {
                    DataFusionError::Internal(format!("WKB parse error: {}", err))
                })?;

                item_out.truncate(0);
                write_geometry(&mut item_out, &geometry).map_err(|err| {
                    DataFusionError::Internal(format!("WKT Write error: {}", err))
                })?;

                builder.append_value(&item_out);
            } else {
                builder.append_null();
            }
        }

        let new_array = builder.finish();
        Ok(ColumnarValue::Array(Arc::new(new_array)))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::create_array;
    use datafusion::scalar::ScalarValue;
    use sedona_schema::datatypes::WKB_GEOMETRY;

    use crate::st_point::st_point_udf;

    use super::*;

    #[test]
    fn udf() -> Result<()> {
        let st_point = st_point_udf().to_udf();
        let wkb_point = st_point.invoke_batch(
            &[
                ScalarValue::Float64(Some(1.0)).into(),
                ScalarValue::Float64(Some(2.0)).into(),
            ],
            1,
        )?;

        let udf = st_astext_udf().to_udf();
        assert_eq!(udf.name(), "ST_AsText");
        let out = udf.invoke_batch(&[wkb_point], 1)?;

        assert_eq!(*out.to_array(1)?, *create_array!(Utf8, ["POINT(1 2)"]));

        Ok(())
    }

    #[test]
    fn udf_nulls() -> Result<()> {
        let udf = st_astext_udf().to_udf();
        let null_wkb = WKB_GEOMETRY.wrap_arg(&ScalarValue::Binary(None).into())?;

        let out = udf.invoke_batch(&[null_wkb], 1)?;
        match out {
            ColumnarValue::Array(array) => assert_eq!(array.null_count(), 1),
            ColumnarValue::Scalar(_) => panic!("Expected array"),
        }

        Ok(())
    }

    #[test]
    fn udf_invalid_wkb() -> Result<()> {
        let udf = st_astext_udf().to_udf();
        let invalid_wkb = WKB_GEOMETRY.wrap_arg(&ScalarValue::Binary(Some(vec![])).into())?;

        let err = udf.invoke_batch(&[invalid_wkb], 1).unwrap_err();
        assert!(err.message().starts_with("WKB parse error"));

        Ok(())
    }
}
