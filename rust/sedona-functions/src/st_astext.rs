use std::{sync::Arc, vec};

use arrow_array::builder::StringBuilder;
use arrow_schema::DataType;
use datafusion_common::cast::as_binary_array;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_common::ScalarValue;
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use sedona_schema::{
    datatypes::SedonaPhysicalType,
    udf::{ArgMatcher, SedonaScalarKernel, SedonaScalarUDF},
};
use wkt::to_wkt::write_geometry;

use crate::geo_iterator::{try_iter_wkb_array, try_iter_wkb_scalar};

/// ST_AsText() scalar UDF implementation
///
/// An implementation of WKT writing using GeoRust's wkt crate.
pub fn st_astext_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_astext",
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
        arg_types: &[SedonaPhysicalType],
        _: &SedonaPhysicalType,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        if let ColumnarValue::Scalar(scalar) = &args[0] {
            let mut iter = try_iter_wkb_scalar(scalar, 1)?;
            let item = iter.next().unwrap();
            let maybe_string = match item {
                Some(maybe_result) => {
                    let mut out = String::new();
                    write_geometry(&mut out, &maybe_result?)
                        .map_err(|err| DataFusionError::External(Box::new(err)))?;
                    Some(out)
                }
                None => None,
            };
            return Ok(ScalarValue::Utf8(maybe_string).into());
        }

        let x_array = args[0].to_array(num_rows)?;
        let x_binary_array = as_binary_array(&x_array)?;

        // Use the WKB size as the proxy for the WKT size.
        // WKT is ~2.5 the size of WKB at full double precision (16), although WKT is up to
        // 50% smaller at very low precision (probably rare).
        let max_theoretical_wkt_size: f64 = x_binary_array.value_data().len() as f64 * 2.5;
        let mut builder =
            StringBuilder::with_capacity(num_rows, max_theoretical_wkt_size.floor() as usize);

        for maybe_item in try_iter_wkb_array(&arg_types[0], &x_array)? {
            match maybe_item {
                Some(item) => {
                    write_geometry(&mut builder, &item?)
                        .map_err(|err| DataFusionError::External(Box::new(err)))?;
                    builder.append_value("");
                }
                None => builder.append_null(),
            }
        }

        let new_array = builder.finish();
        Ok(ColumnarValue::Array(Arc::new(new_array)))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{create_array, ArrayRef};
    use datafusion_common::scalar::ScalarValue;
    use datafusion_expr::ScalarUDF;
    use sedona_schema::datatypes::WKB_GEOMETRY;

    use crate::st_point::st_point_udf;

    use super::*;

    #[test]
    fn udf() -> Result<()> {
        let st_point: ScalarUDF = st_point_udf().into();
        let wkb_point = st_point.invoke_batch(
            &[
                ScalarValue::Float64(Some(1.0)).into(),
                ScalarValue::Float64(Some(2.0)).into(),
            ],
            1,
        )?;

        let udf: ScalarUDF = st_astext_udf().into();
        assert_eq!(udf.name(), "st_astext");

        // Check scalar input -> scalar output
        let out_scalar = udf.invoke_batch(&[wkb_point.clone()], 1)?;
        match out_scalar {
            ColumnarValue::Array(_) => panic!("expected scalar"),
            ColumnarValue::Scalar(scalar) => {
                assert_eq!(scalar, ScalarValue::Utf8(Some("POINT(1 2)".to_string())));
            }
        }

        // Check array input -> array output
        let wkb_point_array = ColumnarValue::Array(wkb_point.clone().to_array(1)?);
        let out_array = udf.invoke_batch(&[wkb_point_array], 1)?;
        match out_array {
            ColumnarValue::Array(array) => {
                let expected: ArrayRef = create_array!(Utf8, ["POINT(1 2)"]);
                assert_eq!(&array, &expected);
            }
            ColumnarValue::Scalar(_) => panic!("expected array"),
        }

        Ok(())
    }

    #[test]
    fn udf_nulls() -> Result<()> {
        let udf: ScalarUDF = st_astext_udf().into();
        let null_wkb_scalar = WKB_GEOMETRY.wrap_arg(&ScalarValue::Binary(None).into())?;

        // Check scalar input -> scalar output
        let out_scalar = udf.invoke_batch(&[null_wkb_scalar.clone()], 1)?;
        match out_scalar {
            ColumnarValue::Array(_) => panic!("Expected scalar"),
            ColumnarValue::Scalar(item) => assert!(item.is_null()),
        }

        // Check array input -> array output
        let null_wkb_array = ColumnarValue::Array(null_wkb_scalar.clone().to_array(1)?);
        let out_array = udf.invoke_batch(&[null_wkb_array], 1)?;
        match out_array {
            ColumnarValue::Array(array) => {
                let mut expected_builder = StringBuilder::new();
                expected_builder.append_null();
                let expected: ArrayRef = Arc::new(expected_builder.finish());
                assert_eq!(&array, &expected);
            }
            ColumnarValue::Scalar(_) => panic!("expected array"),
        }

        Ok(())
    }
}
